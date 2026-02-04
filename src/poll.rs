//! Single poll tick: load state → fetch pages (link-header) → emit NDJSON → commit state.

use crate::circuit::{self, CircuitStore};
use crate::client::build_client;
use crate::config::{
    CheckpointTiming, Config, CursorExpiredBehavior, GlobalConfig, InvalidUtf8Behavior,
    MaxEventBytesBehavior, OnParseErrorBehavior, OnStateWriteErrorBehavior, PaginationConfig,
    RateLimitConfig, SourceConfig,
};
use crate::dedupe::{self, DedupeStore};
use crate::dpop::DPoPKeyCache;
use crate::event::EmittedEvent;
use crate::metrics;
use crate::oauth2::OAuth2TokenCache;
use crate::output::EventSink;
use crate::pagination::next_link_from_headers;
use crate::replay::RecordState;
use crate::retry::{execute_with_retry, rate_limit_info_from_headers};
use crate::state::StateStore;
use anyhow::Context;
use chrono::Utc;
use governor::{Quota, RateLimiter};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::instrument;

/// Type for client-side rate limiter (governor direct limiter).
type ClientRateLimiter = RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
>;

/// Shared store of last error message per source (for health endpoints).
pub type LastErrorStore = Arc<RwLock<HashMap<String, String>>>;

/// Run one poll tick for all sources (or only those matching source_filter).
/// Sources are polled concurrently (one task per source).
pub async fn run_one_tick(
    config: &Config,
    store: Arc<dyn StateStore>,
    source_filter: Option<&str>,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    last_errors: LastErrorStore,
) -> anyhow::Result<()> {
    let mut handles = Vec::new();
    for (source_id, source) in &config.sources {
        if let Some(filter) = source_filter {
            if filter != source_id {
                continue;
            }
        }
        let store = store.clone();
        let source_id_key = source_id.clone();
        let source = source.clone();
        let global = config.global.clone();
        let circuit_store = circuit_store.clone();
        let token_cache = token_cache.clone();
        let dpop_key_cache = dpop_key_cache.clone();
        let dedupe_store = dedupe_store.clone();
        let event_sink = event_sink.clone();
        let record_state = record_state.clone();
        let rate_limiter: Option<Arc<ClientRateLimiter>> = source
            .resilience
            .as_ref()
            .and_then(|r| r.rate_limit.as_ref())
            .and_then(|r| {
                let rps = r
                    .max_requests_per_second
                    .filter(|&x| x > 0.0 && x.is_finite())?;
                let rps_u = (rps.ceil() as u32).max(1);
                let rps_nz = NonZeroU32::new(rps_u)?;
                let burst = r.burst_size.unwrap_or(rps_u).max(1);
                let burst_nz = NonZeroU32::new(burst).unwrap_or(NonZeroU32::MIN);
                let quota = Quota::per_second(rps_nz).allow_burst(burst_nz);
                Some(Arc::new(RateLimiter::direct(quota)))
            });
        let poll_tick_secs = source
            .resilience
            .as_ref()
            .and_then(|r| r.timeouts.as_ref())
            .and_then(|t| t.poll_tick_secs);
        let h = tokio::spawn(async move {
            let poll_fut = poll_one_source(
                store,
                &source_id_key,
                &source,
                &global,
                circuit_store,
                token_cache,
                dpop_key_cache,
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter,
            );
            match poll_tick_secs {
                Some(secs) => match tokio::time::timeout(Duration::from_secs(secs), poll_fut).await
                {
                    Ok(inner) => inner,
                    Err(_) => Err(anyhow::anyhow!("poll tick timed out after {}s", secs)),
                },
                None => poll_fut.await,
            }
        });
        handles.push((source_id, h));
    }
    for (source_id, h) in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                metrics::record_error(&source_id);
                let msg = e.to_string();
                last_errors
                    .write()
                    .await
                    .insert(source_id.clone(), msg.clone());
                tracing::error!(source = %source_id, "poll failed: {:#}", e);
                // Broken pipe to stdout is fatal: exit so caller can exit non-zero.
                if msg.to_lowercase().contains("broken pipe") {
                    return Err(e);
                }
            }
            Err(e) => {
                metrics::record_error(&source_id);
                let msg = e.to_string();
                last_errors
                    .write()
                    .await
                    .insert(source_id.clone(), msg.clone());
                tracing::error!(source = %source_id, "task join failed: {:#}", e);
            }
        }
    }
    Ok(())
}

#[instrument(skip(
    store,
    source,
    global,
    circuit_store,
    token_cache,
    dpop_key_cache,
    dedupe_store,
    event_sink,
    record_state
))]
async fn poll_one_source(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    rate_limiter: Option<Arc<ClientRateLimiter>>,
) -> anyhow::Result<()> {
    let client = build_client(source.resilience.as_ref())?;

    match &source.pagination {
        Some(PaginationConfig::LinkHeader { rel, max_pages }) => {
            poll_link_header(
                store,
                source_id,
                source,
                global,
                &client,
                rel.as_str(),
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter.as_ref(),
            )
            .await
        }
        Some(PaginationConfig::Cursor {
            cursor_param,
            cursor_path,
            max_pages,
        }) => {
            poll_cursor_pagination(
                store,
                source_id,
                source,
                global,
                &client,
                cursor_param,
                cursor_path,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter.as_ref(),
            )
            .await
        }
        Some(PaginationConfig::PageOffset {
            page_param,
            limit_param,
            limit,
            max_pages,
        }) => {
            poll_page_offset_pagination(
                store,
                source_id,
                source,
                global,
                &client,
                page_param,
                limit_param,
                *limit,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter.as_ref(),
            )
            .await
        }
        _ => {
            return poll_single_page(
                store,
                source_id,
                source,
                global,
                &client,
                &source.url,
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state.clone(),
                rate_limiter.as_ref(),
            )
            .await;
        }
    }
}

/// If adaptive rate limiting is enabled and remaining is 0 or low, sleep until reset (or a short delay).
async fn maybe_adaptive_sleep_after_response(
    headers: &reqwest::header::HeaderMap,
    source_id: &str,
    rate_limit_config: Option<&RateLimitConfig>,
) {
    let rl = match rate_limit_config {
        Some(r) if r.adaptive == Some(true) => r,
        _ => return,
    };
    let info = rate_limit_info_from_headers(headers, rl.headers.as_ref());
    if !info.remaining.map_or(false, |n| n <= 1) {
        return;
    }
    let reset_ts = match info.reset_ts {
        Some(t) => t,
        _ => return,
    };
    let now = Utc::now().timestamp();
    let wait_secs = (reset_ts - now).max(0) as u64;
    if wait_secs > 0 {
        tracing::debug!(
            source = %source_id,
            remaining = ?info.remaining,
            reset_ts,
            wait_secs,
            "adaptive rate limit: remaining <= 1, waiting until reset"
        );
        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
    }
}

/// Link-header pagination: follow rel="next" until no more or max_pages.
async fn poll_link_header(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    client: &reqwest::Client,
    rel: &str,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    rate_limiter: Option<&Arc<ClientRateLimiter>>,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let from_store = store
        .get(source_id, "next_url")
        .await?
        .filter(|s| !s.is_empty());
    let mut url: String = from_store.clone().unwrap_or_else(|| source.url.clone());
    if from_store.is_none() {
        url = url_with_first_request_params(&store, source_id, source, &url).await?;
    }

    let mut page = 0u32;
    let mut total_events = 0u64;
    let mut total_bytes: u64 = 0;
    let max_bytes = source.max_bytes;
    let mut pending_next_url: Option<String> = None;
    let mut incremental_max_ts: Option<String> = None;
    let mut watermark_max_ts: Option<String> = None;
    let page_delay = source
        .resilience
        .as_ref()
        .and_then(|r| r.rate_limit.as_ref())
        .and_then(|rl| rl.page_delay_secs);
    loop {
        page += 1;
        if page > max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
            break;
        }
        if page > 1 {
            if let Some(secs) = page_delay {
                tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }

        if let Some(cb) = source
            .resilience
            .as_ref()
            .and_then(|r| r.circuit_breaker.as_ref())
        {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        if let Some(limiter) = rate_limiter {
            limiter.until_ready().await;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            &client,
            source,
            source_id,
            &url,
            None, // link_header: same URL per page; body from source when POST
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source
                .resilience
                .as_ref()
                .and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
            dpop_key_cache.as_ref(),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source
                    .resilience
                    .as_ref()
                    .and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, success).await;
                }
                let status = r.status().as_u16();
                metrics::record_request(
                    source_id,
                    status_class(status),
                    req_start.elapsed().as_secs_f64(),
                );
                r
            }
            Err(e) => {
                if let Some(cb) = source
                    .resilience
                    .as_ref()
                    .and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };

        maybe_adaptive_sleep_after_response(
            response.headers(),
            source_id,
            source
                .resilience
                .as_ref()
                .and_then(|r| r.rate_limit.as_ref()),
        )
        .await;

        let status = response.status();
        if !status.is_success() {
            anyhow::bail!(
                "http {} {}",
                status,
                response.text().await.unwrap_or_default()
            );
        }

        let next_url = next_link_from_headers(response.headers(), rel);
        let base_url = response.url().clone();
        let path = base_url.path().to_string();
        let record_url = response.url().clone();
        let record_status = response.status().as_u16();
        let record_headers = response.headers().clone();
        let body_bytes = response.bytes().await.context("read body")?;
        if let Some(ref rs) = record_state {
            rs.save(
                source_id,
                record_url.as_str(),
                record_status,
                &record_headers,
                &body_bytes,
            )?;
        }
        let body = bytes_to_string(&body_bytes, source.on_invalid_utf8)?;
        if let Some(limit) = source.max_response_bytes {
            if body.len() as u64 > limit {
                anyhow::bail!(
                    "response body size {} exceeds max_response_bytes {}",
                    body.len(),
                    limit
                );
            }
        }
        total_bytes += body.len() as u64;
        let events = match parse_events_from_body(&body) {
            Ok(ev) => ev,
            Err(e) => {
                if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                    tracing::warn!(source = %source_id, error = %e, "parse error, stopping pagination");
                    break;
                }
                return Err(e).context("parse response");
            }
        };
        if let Some(ref inc) = source.incremental_from {
            update_max_timestamp(&mut incremental_max_ts, &events, &inc.event_timestamp_path);
        }
        if let Some(ref st) = source.state {
            update_max_timestamp(&mut watermark_max_ts, &events, &st.watermark_field);
        }

        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_path).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue; // duplicate
                }
            }
            total_events += 1;
            emitted_count += 1;
            let emitted = build_emitted_event(source, source_id, &path, event_value);
            emit_event_line(global, source_id, source, &event_sink, &emitted)?;
        }
        metrics::record_events(source_id, emitted_count);

        let checkpoint_per_page = source.checkpoint != Some(CheckpointTiming::EndOfTick);
        let hit_max_bytes = max_bytes.is_some() && total_bytes > max_bytes.unwrap();
        if let Some(next) = next_url {
            let absolute = base_url.join(&next).context("resolve next URL")?;
            if checkpoint_per_page {
                store.set(source_id, "next_url", absolute.as_str()).await?;
            }
            pending_next_url = Some(absolute.to_string());
            if hit_max_bytes {
                tracing::warn!(
                    source = %source_id,
                    total_bytes,
                    max_bytes = max_bytes.unwrap(),
                    "reached max_bytes limit, stopping pagination; next poll continues from saved cursor"
                );
                break;
            }
            url = absolute.to_string();
            tracing::debug!(
                source = %source_id,
                page = page,
                events = events.len(),
                total_bytes,
                "next page"
            );
        } else {
            pending_next_url = Some(String::new());
            if checkpoint_per_page {
                store_set_or_skip(&store, source_id, source, global, "next_url", "").await?;
            }
            tracing::info!(
                source = %source_id,
                pages = page,
                events = total_events,
                duration_ms = start.elapsed().as_millis(),
                "poll completed"
            );
            break;
        }
    }
    if source.checkpoint == Some(CheckpointTiming::EndOfTick) {
        if let Some(ref v) = pending_next_url {
            store_set_or_skip(&store, source_id, source, global, "next_url", v).await?;
        }
    }
    store_incremental_from_after_poll(&store, source_id, source, global, incremental_max_ts).await;
    store_watermark_after_poll(&store, source_id, source, global, watermark_max_ts).await;
    Ok(())
}

/// Cursor-in-body pagination: get cursor from response JSON path, pass as query param on next request.
async fn poll_cursor_pagination(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    client: &reqwest::Client,
    cursor_param: &str,
    cursor_path: &str,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    rate_limiter: Option<&Arc<ClientRateLimiter>>,
) -> anyhow::Result<()> {
    use reqwest::Url;
    let start = Instant::now();
    let base_url = source.url.as_str();
    let mut cursor: Option<String> = store
        .get(source_id, "cursor")
        .await?
        .filter(|s| !s.is_empty());
    let mut page = 0u32;
    let mut total_events = 0u64;
    let mut total_bytes: u64 = 0;
    let max_bytes = source.max_bytes;
    let mut pending_cursor: Option<String> = None;
    let mut incremental_max_ts: Option<String> = None;
    let mut watermark_max_ts: Option<String> = None;
    let page_delay = source
        .resilience
        .as_ref()
        .and_then(|r| r.rate_limit.as_ref())
        .and_then(|rl| rl.page_delay_secs);
    loop {
        page += 1;
        if page > max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
            break;
        }
        if page > 1 {
            if let Some(secs) = page_delay {
                tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }
        use crate::config::HttpMethod;
        let (url, body_override) = match (&source.method, &cursor) {
            (HttpMethod::Get, Some(c)) => {
                let mut u = Url::parse(base_url).context("cursor pagination base url")?;
                u.query_pairs_mut().append_pair(cursor_param, c);
                (u.to_string(), None)
            }
            (HttpMethod::Get, None) => (
                url_with_first_request_params(&store, source_id, source, base_url).await?,
                None,
            ),
            (HttpMethod::Post, Some(c)) => {
                let body = merge_cursor_into_body(source.body.as_ref(), cursor_param, c);
                (base_url.to_string(), Some(body))
            }
            (HttpMethod::Post, None) => (
                url_with_first_request_params(&store, source_id, source, base_url).await?,
                None,
            ),
        };
        if let Some(cb) = source
            .resilience
            .as_ref()
            .and_then(|r| r.circuit_breaker.as_ref())
        {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        if let Some(limiter) = rate_limiter {
            limiter.until_ready().await;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            client,
            source,
            source_id,
            &url,
            body_override.as_ref(),
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source
                .resilience
                .as_ref()
                .and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
            dpop_key_cache.as_ref(),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source
                    .resilience
                    .as_ref()
                    .and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, success).await;
                }
                metrics::record_request(
                    source_id,
                    status_class(r.status().as_u16()),
                    req_start.elapsed().as_secs_f64(),
                );
                r
            }
            Err(e) => {
                if let Some(cb) = source
                    .resilience
                    .as_ref()
                    .and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };

        maybe_adaptive_sleep_after_response(
            response.headers(),
            source_id,
            source
                .resilience
                .as_ref()
                .and_then(|r| r.rate_limit.as_ref()),
        )
        .await;

        let status = response.status();
        let path = response.url().path().to_string();
        let record_url = response.url().clone();
        let record_status = response.status().as_u16();
        let record_headers = response.headers().clone();
        let body_bytes = response.bytes().await.context("read body")?;
        if let Some(ref rs) = record_state {
            rs.save(
                source_id,
                record_url.as_str(),
                record_status,
                &record_headers,
                &body_bytes,
            )?;
        }
        let body = bytes_to_string(&body_bytes, source.on_invalid_utf8)?;
        if !status.is_success() {
            if cursor.is_some() && status.as_u16() >= 400 && status.as_u16() < 500 {
                let lower = body.to_lowercase();
                let is_expired = status.as_u16() == 410
                    || lower.contains("expired")
                    || lower.contains("invalid cursor")
                    || lower.contains("cursor invalid");
                if is_expired && source.on_cursor_error == Some(CursorExpiredBehavior::Reset) {
                    store_set_or_skip(&store, source_id, source, global, "cursor", "").await?;
                    tracing::warn!(
                        source = %source_id,
                        "cursor expired (4xx), reset; next poll from start"
                    );
                    return Ok(());
                }
            }
            anyhow::bail!("http {} {}", status, body);
        }
        if let Some(limit) = source.max_response_bytes {
            if body.len() as u64 > limit {
                anyhow::bail!(
                    "response body size {} exceeds max_response_bytes {}",
                    body.len(),
                    limit
                );
            }
        }
        total_bytes += body.len() as u64;
        if let Some(limit) = max_bytes {
            if total_bytes > limit {
                tracing::warn!(
                    source = %source_id,
                    total_bytes,
                    max_bytes = limit,
                    "reached max_bytes limit, stopping cursor pagination"
                );
                break;
            }
        }
        let value: serde_json::Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                    tracing::warn!(source = %source_id, error = %e, "parse error, stopping cursor pagination");
                    return Ok(());
                }
                return Err(e).context("parse response json");
            }
        };
        let events = match parse_events_from_value(&value) {
            Ok(ev) => ev,
            Err(e) => {
                if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                    tracing::warn!(source = %source_id, error = %e, "parse error, stopping cursor pagination");
                    return Ok(());
                }
                return Err(e).context("extract events");
            }
        };
        if let Some(ref inc) = source.incremental_from {
            update_max_timestamp(&mut incremental_max_ts, &events, &inc.event_timestamp_path);
        }
        if let Some(ref st) = source.state {
            update_max_timestamp(&mut watermark_max_ts, &events, &st.watermark_field);
        }
        let next_cursor = json_path_str(&value, cursor_path).filter(|s| !s.is_empty());
        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_path).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue;
                }
            }
            total_events += 1;
            emitted_count += 1;
            let emitted = build_emitted_event(source, source_id, &path, event_value);
            emit_event_line(global, source_id, source, &event_sink, &emitted)?;
        }
        metrics::record_events(source_id, emitted_count);
        let checkpoint_per_page = source.checkpoint != Some(CheckpointTiming::EndOfTick);
        match next_cursor {
            Some(c) => {
                if checkpoint_per_page {
                    store_set_or_skip(&store, source_id, source, global, "cursor", &c).await?;
                }
                pending_cursor = Some(c.clone());
                cursor = Some(c);
                tracing::debug!(
                    source = %source_id,
                    page = page,
                    events = events.len(),
                    "next cursor page"
                );
            }
            None => {
                if checkpoint_per_page {
                    store_set_or_skip(&store, source_id, source, global, "cursor", "").await?;
                }
                pending_cursor = Some(String::new());
                tracing::info!(
                    source = %source_id,
                    pages = page,
                    events = total_events,
                    duration_ms = start.elapsed().as_millis(),
                    "poll completed (cursor)"
                );
                break;
            }
        }
    }
    if source.checkpoint == Some(CheckpointTiming::EndOfTick) {
        if let Some(ref v) = pending_cursor {
            store_set_or_skip(&store, source_id, source, global, "cursor", v).await?;
        }
    }
    store_incremental_from_after_poll(&store, source_id, source, global, incremental_max_ts).await;
    store_watermark_after_poll(&store, source_id, source, global, watermark_max_ts).await;
    Ok(())
}

/// Page/offset pagination: increment page (or offset) each request, stop when empty or max_pages.
async fn poll_page_offset_pagination(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    client: &reqwest::Client,
    page_param: &str,
    limit_param: &str,
    limit: u32,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    rate_limiter: Option<&Arc<ClientRateLimiter>>,
) -> anyhow::Result<()> {
    use reqwest::Url;
    let start = Instant::now();
    let base_url = source.url.as_str();
    let mut total_events = 0u64;
    let mut incremental_max_ts: Option<String> = None;
    let mut watermark_max_ts: Option<String> = None;
    let page_delay = source
        .resilience
        .as_ref()
        .and_then(|r| r.rate_limit.as_ref())
        .and_then(|rl| rl.page_delay_secs);
    for page in 1..=max_pages {
        if page > 1 {
            if let Some(secs) = page_delay {
                tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }
        let mut u = Url::parse(base_url).context("page/offset base url")?;
        u.query_pairs_mut()
            .append_pair(page_param, &page.to_string());
        u.query_pairs_mut()
            .append_pair(limit_param, &limit.to_string());
        if page == 1 {
            if let Some(ref params) = source.query_params {
                for (k, v) in params {
                    u.query_pairs_mut().append_pair(k, &v.to_param_value());
                }
            }
        }
        let url = u.to_string();
        if let Some(cb) = source
            .resilience
            .as_ref()
            .and_then(|r| r.circuit_breaker.as_ref())
        {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        if let Some(limiter) = rate_limiter {
            limiter.until_ready().await;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            client,
            source,
            source_id,
            &url,
            None, // page_offset: body from source when POST
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source
                .resilience
                .as_ref()
                .and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
            dpop_key_cache.as_ref(),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source
                    .resilience
                    .as_ref()
                    .and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, success).await;
                }
                metrics::record_request(
                    source_id,
                    status_class(r.status().as_u16()),
                    req_start.elapsed().as_secs_f64(),
                );
                r
            }
            Err(e) => {
                if let Some(cb) = source
                    .resilience
                    .as_ref()
                    .and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };

        maybe_adaptive_sleep_after_response(
            response.headers(),
            source_id,
            source
                .resilience
                .as_ref()
                .and_then(|r| r.rate_limit.as_ref()),
        )
        .await;

        let path = response.url().path().to_string();
        let record_url = response.url().clone();
        let record_status = response.status().as_u16();
        let record_headers = response.headers().clone();
        let body_bytes = response.bytes().await.context("read body")?;
        if let Some(ref rs) = record_state {
            rs.save(
                source_id,
                record_url.as_str(),
                record_status,
                &record_headers,
                &body_bytes,
            )?;
        }
        if record_status < 200 || record_status >= 300 {
            let body_str = String::from_utf8_lossy(&body_bytes);
            anyhow::bail!("http {} {}", record_status, body_str);
        }
        let body = bytes_to_string(&body_bytes, source.on_invalid_utf8)?;
        if let Some(limit) = source.max_response_bytes {
            if body.len() as u64 > limit {
                anyhow::bail!(
                    "response body size {} exceeds max_response_bytes {}",
                    body.len(),
                    limit
                );
            }
        }
        let events = match parse_events_from_body(&body) {
            Ok(ev) => ev,
            Err(e) => {
                if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                    tracing::warn!(source = %source_id, error = %e, "parse error, skipping page");
                    continue;
                }
                return Err(e).context("parse response");
            }
        };
        if let Some(ref inc) = source.incremental_from {
            update_max_timestamp(&mut incremental_max_ts, &events, &inc.event_timestamp_path);
        }
        if let Some(ref st) = source.state {
            update_max_timestamp(&mut watermark_max_ts, &events, &st.watermark_field);
        }
        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_path).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue;
                }
            }
            total_events += 1;
            emitted_count += 1;
            let emitted = build_emitted_event(source, source_id, &path, event_value);
            emit_event_line(global, source_id, source, &event_sink, &emitted)?;
        }
        metrics::record_events(source_id, emitted_count);
        if events.len() < limit as usize {
            tracing::info!(
                source = %source_id,
                pages = page,
                events = total_events,
                duration_ms = start.elapsed().as_millis(),
                "poll completed (page/offset)"
            );
            break;
        }
        if page == max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
        }
    }
    store_set_or_skip(&store, source_id, source, global, "next_url", "").await?;
    store_incremental_from_after_poll(&store, source_id, source, global, incremental_max_ts).await;
    store_watermark_after_poll(&store, source_id, source, global, watermark_max_ts).await;
    Ok(())
}

/// Single page (no pagination loop): one GET, emit events, clear next_url.
async fn poll_single_page(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    client: &reqwest::Client,
    url: &str,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    rate_limiter: Option<&Arc<ClientRateLimiter>>,
) -> anyhow::Result<()> {
    let start = Instant::now();
    if let Some(cb) = source
        .resilience
        .as_ref()
        .and_then(|r| r.circuit_breaker.as_ref())
    {
        circuit::allow_request(&circuit_store, source_id, cb)
            .await
            .context("circuit open")?;
    }
    if let Some(limiter) = rate_limiter {
        limiter.until_ready().await;
    }
    let req_start = std::time::Instant::now();
    let response = match execute_with_retry(
        client,
        source,
        source_id,
        url,
        None, // single page: body from source when POST
        source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
        source
            .resilience
            .as_ref()
            .and_then(|r| r.rate_limit.as_ref()),
        Some(&token_cache),
        dpop_key_cache.as_ref(),
    )
    .await
    {
        Ok(r) => {
            let success = r.status().as_u16() < 500;
            if let Some(cb) = source
                .resilience
                .as_ref()
                .and_then(|r| r.circuit_breaker.as_ref())
            {
                circuit::record_result(&circuit_store, source_id, cb, success).await;
            }
            metrics::record_request(
                source_id,
                status_class(r.status().as_u16()),
                req_start.elapsed().as_secs_f64(),
            );
            r
        }
        Err(e) => {
            if let Some(cb) = source
                .resilience
                .as_ref()
                .and_then(|r| r.circuit_breaker.as_ref())
            {
                circuit::record_result(&circuit_store, source_id, cb, false).await;
            }
            metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
            metrics::record_error(source_id);
            return Err(e).context("http request");
        }
    };

    maybe_adaptive_sleep_after_response(
        response.headers(),
        source_id,
        source
            .resilience
            .as_ref()
            .and_then(|r| r.rate_limit.as_ref()),
    )
    .await;

    let record_url = response.url().clone();
    let record_status = response.status().as_u16();
    let record_headers = response.headers().clone();
    let body_bytes = response.bytes().await.context("read body")?;

    if let Some(ref rs) = record_state {
        rs.save(
            source_id,
            record_url.as_str(),
            record_status,
            &record_headers,
            &body_bytes,
        )?;
    }

    if record_status < 200 || record_status >= 300 {
        let body_str = String::from_utf8_lossy(&body_bytes);
        anyhow::bail!("http {} {}", record_status, body_str);
    }
    let path = record_url.path().to_string();
    let body = bytes_to_string(&body_bytes, source.on_invalid_utf8)?;
    if let Some(limit) = source.max_response_bytes {
        if body.len() as u64 > limit {
            anyhow::bail!(
                "response body size {} exceeds max_response_bytes {}",
                body.len(),
                limit
            );
        }
    }
    let events = match parse_events_from_body(&body) {
        Ok(ev) => ev,
        Err(e) => {
            if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                tracing::warn!(source = %source_id, error = %e, "parse error, skipping");
                let _ = store_set_or_skip(&store, source_id, source, global, "next_url", "").await;
                return Ok(());
            }
            return Err(e).context("parse response");
        }
    };

    let mut incremental_max_ts: Option<String> = None;
    let mut watermark_max_ts: Option<String> = None;
    if let Some(ref inc) = source.incremental_from {
        update_max_timestamp(&mut incremental_max_ts, &events, &inc.event_timestamp_path);
    }
    if let Some(ref st) = source.state {
        update_max_timestamp(&mut watermark_max_ts, &events, &st.watermark_field);
    }

    let mut emitted_count = 0u64;
    for event_value in &events {
        if let Some(d) = &source.dedupe {
            let id = event_id(event_value, &d.id_path).unwrap_or_default();
            if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                continue;
            }
        }
        emitted_count += 1;
        let emitted = build_emitted_event(source, source_id, &path, event_value);
        emit_event_line(global, source_id, source, &event_sink, &emitted)?;
    }
    metrics::record_events(source_id, emitted_count);

    store_set_or_skip(&store, source_id, source, global, "next_url", "").await?;
    store_incremental_from_after_poll(&store, source_id, source, global, incremental_max_ts).await;
    store_watermark_after_poll(&store, source_id, source, global, watermark_max_ts).await;
    tracing::info!(
        source = %source_id,
        events = emitted_count,
        duration_ms = start.elapsed().as_millis(),
        "poll completed"
    );
    Ok(())
}

/// Convert response body bytes to string; apply on_invalid_utf8 policy (replace/escape/fail).
fn bytes_to_string(
    bytes: &[u8],
    on_invalid_utf8: Option<InvalidUtf8Behavior>,
) -> anyhow::Result<String> {
    match on_invalid_utf8 {
        Some(InvalidUtf8Behavior::Replace) | Some(InvalidUtf8Behavior::Escape) => {
            Ok(String::from_utf8_lossy(bytes).into_owned())
        }
        _ => String::from_utf8(bytes.to_vec())
            .map_err(|e| anyhow::anyhow!("invalid UTF-8 in response: {}", e)),
    }
}

/// Value for the producer label in NDJSON: source_label_value if set, else the config source key.
fn effective_source_label(source: &SourceConfig, source_id: &str) -> String {
    source
        .source_label_value
        .as_deref()
        .unwrap_or(source_id)
        .to_string()
}

/// Key for the producer label in NDJSON: source override, else global, else "source".
fn effective_source_label_key<'a>(global: &'a GlobalConfig, source: &'a SourceConfig) -> &'a str {
    source
        .source_label_key
        .as_deref()
        .or(global.source_label_key.as_deref())
        .unwrap_or("source")
}

/// Emit one event line; enforce max_line_bytes, record output errors.
fn emit_event_line(
    global: &GlobalConfig,
    source_id: &str,
    source: &SourceConfig,
    event_sink: &Arc<dyn EventSink>,
    emitted: &EmittedEvent,
) -> anyhow::Result<()> {
    let label_key = effective_source_label_key(global, source);
    let line = emitted.to_ndjson_line_with_label_key(label_key)?;
    if let Some(max) = source.max_line_bytes {
        if line.len() as u64 > max {
            match source
                .max_line_bytes_behavior
                .unwrap_or(MaxEventBytesBehavior::Fail)
            {
                MaxEventBytesBehavior::Truncate => {
                    let max_usize = max as usize;
                    let truncated: String = if max_usize >= 3 {
                        format!(
                            "{}...",
                            line.chars()
                                .take(max_usize.saturating_sub(3))
                                .collect::<String>()
                        )
                    } else {
                        line.chars().take(max_usize).collect()
                    };
                    event_sink
                        .write_line_from_source(Some(source_id), &truncated)
                        .map_err(|e| {
                            metrics::record_output_error(source_id);
                            e
                        })?;
                    return Ok(());
                }
                MaxEventBytesBehavior::Skip => {
                    tracing::warn!(
                        source = %source_id,
                        line_len = line.len(),
                        max = max,
                        "event line exceeds max_line_bytes, skipping"
                    );
                    return Ok(());
                }
                MaxEventBytesBehavior::Fail => {
                    anyhow::bail!(
                        "event line length {} exceeds max_line_bytes {}",
                        line.len(),
                        max
                    );
                }
            }
        }
    }
    event_sink
        .write_line_from_source(Some(source_id), &line)
        .map_err(|e| {
            metrics::record_output_error(source_id);
            e
        })
}

/// Set state key; on error, fail or skip checkpoint per source config or global degradation.emit_without_checkpoint.
async fn store_set_or_skip(
    store: &Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    if let Err(e) = store.set(source_id, key, value).await {
        let skip = source.on_state_write_error == Some(OnStateWriteErrorBehavior::SkipCheckpoint)
            || global
                .degradation
                .as_ref()
                .and_then(|d| d.emit_without_checkpoint)
                .unwrap_or(false);
        if skip {
            tracing::warn!(
                source = %source_id,
                key,
                error = %e,
                "state store write failed, skipping checkpoint"
            );
            Ok(())
        } else {
            Err(e.into())
        }
    } else {
        Ok(())
    }
}

/// Merge cursor into POST body for cursor pagination. Returns a new JSON object with cursor_param set.
fn merge_cursor_into_body(
    base: Option<&serde_json::Value>,
    cursor_param: &str,
    cursor_value: &str,
) -> serde_json::Value {
    let mut obj = base
        .and_then(|v| v.as_object())
        .map(|m| m.clone())
        .unwrap_or_else(serde_json::Map::new);
    obj.insert(
        cursor_param.to_string(),
        serde_json::Value::String(cursor_value.to_string()),
    );
    serde_json::Value::Object(obj)
}

/// State key for per-source watermark (default "watermark").
fn watermark_state_key(source: &SourceConfig) -> Option<&str> {
    source
        .state
        .as_ref()
        .map(|s| s.state_key.as_deref().unwrap_or("watermark"))
}

/// Build first-request URL: add watermark state, incremental_from state, or from; and query_params.
/// Used when there is no saved cursor/next_url (link_header, cursor, and page_offset first page).
async fn url_with_first_request_params(
    store: &Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    url: &str,
) -> anyhow::Result<String> {
    let mut u = reqwest::Url::parse(url).context("parse url for first-request params")?;
    if let Some(ref st) = source.state {
        if let Some(key) = watermark_state_key(source) {
            if let Some(val) = store.get(source_id, key).await?.filter(|s| !s.is_empty()) {
                u.query_pairs_mut().append_pair(&st.watermark_param, &val);
            }
        }
    } else if let Some(ref inc) = source.incremental_from {
        if let Some(val) = store
            .get(source_id, &inc.state_key)
            .await?
            .filter(|s| !s.is_empty())
        {
            u.query_pairs_mut().append_pair(&inc.param_name, &val);
        }
    } else if let Some(ref from_val) = source.from {
        let param = source.from_param.as_deref().unwrap_or("since");
        u.query_pairs_mut().append_pair(param, from_val);
    }
    if let Some(ref params) = source.query_params {
        for (k, v) in params {
            u.query_pairs_mut().append_pair(k, &v.to_param_value());
        }
    }
    Ok(u.to_string())
}

/// Sync version for tests (no store; uses source.from only; state/incremental_from require store).
#[cfg(test)]
fn url_with_first_request_params_sync(url: &str, source: &SourceConfig) -> anyhow::Result<String> {
    let mut u = reqwest::Url::parse(url).context("parse url for first-request params")?;
    if source.state.is_none() && source.incremental_from.is_none() {
        if let Some(ref from_val) = source.from {
            let param = source.from_param.as_deref().unwrap_or("since");
            u.query_pairs_mut().append_pair(param, from_val);
        }
    }
    if let Some(ref params) = source.query_params {
        for (k, v) in params {
            u.query_pairs_mut().append_pair(k, &v.to_param_value());
        }
    }
    Ok(u.to_string())
}

/// Get value at dotted path as string (supports string and number for timestamps like date_create).
fn value_at_path_as_string(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut v = value;
    for segment in path.split('.') {
        v = v.get(segment)?;
    }
    Some(match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => return None,
    })
}

/// Update max_ts with the max value at path across events (lexicographic comparison).
fn update_max_timestamp(max_ts: &mut Option<String>, events: &[serde_json::Value], path: &str) {
    for event in events {
        if let Some(v) = value_at_path_as_string(event, path) {
            if max_ts.as_ref().map_or(true, |m| v.as_str() > m.as_str()) {
                *max_ts = Some(v);
            }
        }
    }
}

/// Store incremental_from state after poll when configured.
async fn store_incremental_from_after_poll(
    store: &Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    max_ts: Option<String>,
) {
    if let Some(ref inc) = source.incremental_from {
        let val = max_ts.unwrap_or_default();
        if !val.is_empty() {
            let _ = store_set_or_skip(store, source_id, source, global, &inc.state_key, &val).await;
        }
    }
}

/// Store per-source watermark state after poll when state.watermark_field is configured.
async fn store_watermark_after_poll(
    store: &Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    max_ts: Option<String>,
) {
    if let Some(key) = watermark_state_key(source) {
        let val = max_ts.unwrap_or_default();
        if !val.is_empty() {
            let _ = store_set_or_skip(store, source_id, source, global, key, &val).await;
        }
    }
}

/// Parse response body: top-level array, or object with "items"/"data"/"events" array.
fn parse_events_from_body(body: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let value: serde_json::Value = serde_json::from_str(body).context("parse response json")?;
    parse_events_from_value(&value)
}

/// Extract events array from parsed JSON (same keys as parse_events_from_body).
fn parse_events_from_value(value: &serde_json::Value) -> anyhow::Result<Vec<serde_json::Value>> {
    if let Some(arr) = value.as_array() {
        return Ok(arr.clone());
    }
    if let Some(obj) = value.as_object() {
        for key in &["items", "data", "events", "logs", "entries"] {
            if let Some(v) = obj.get(*key) {
                if let Some(arr) = v.as_array() {
                    return Ok(arr.clone());
                }
            }
        }
    }
    Ok(vec![value.clone()])
}

/// Get string at dotted path in JSON (e.g. "next_cursor", "meta.next_page_token").
fn json_path_str(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut v = value;
    for segment in path.split('.') {
        v = v.get(segment)?;
    }
    v.as_str().map(|s| s.to_string())
}

/// Extract event ID from JSON using dotted path (e.g. "uuid", "id", "event.id").
fn event_id(event: &serde_json::Value, id_path: &str) -> Option<String> {
    let mut v = event;
    for segment in id_path.split('.') {
        v = v.get(segment)?;
    }
    v.as_str().map(|s| s.to_string())
}

/// Fallback order when no timestamp_field config or path missing: published, timestamp, ts, created_at, then now.
fn event_ts_fallback(event: &serde_json::Value) -> String {
    let s = event
        .get("published")
        .or_else(|| event.get("timestamp"))
        .or_else(|| event.get("ts"))
        .or_else(|| event.get("created_at"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if s.is_empty() {
        Utc::now().to_rfc3339()
    } else {
        s
    }
}

/// Timestamp for envelope: from timestamp_field path when set and present, else event_ts_fallback.
fn event_ts_with_field(event: &serde_json::Value, timestamp_field: Option<&str>) -> String {
    if let Some(path) = timestamp_field {
        if let Some(s) = json_path_str(event, path) {
            if !s.is_empty() {
                return s;
            }
        }
    }
    event_ts_fallback(event)
}

/// Build NDJSON envelope from raw event using source transform (timestamp_field, id_field) when set.
fn build_emitted_event(
    source: &SourceConfig,
    source_id: &str,
    path: &str,
    event_value: &serde_json::Value,
) -> EmittedEvent {
    let ts = event_ts_with_field(
        event_value,
        source
            .transform
            .as_ref()
            .and_then(|t| t.timestamp_field.as_deref()),
    );
    let label = effective_source_label(source, source_id);
    let mut emitted = EmittedEvent::new(ts, label, path.to_string(), event_value.clone());
    if let Some(id_path) = source.transform.as_ref().and_then(|t| t.id_field.as_ref()) {
        if let Some(id) = event_id(event_value, id_path) {
            emitted = emitted.with_id(id);
        }
    }
    emitted
}

fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_events_from_value_top_level_array() {
        let v = serde_json::json!([{"id": 1}, {"id": 2}]);
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn test_parse_events_from_value_items_key() {
        let v = serde_json::json!({"items": [{"id": 1}], "next_cursor": "abc"});
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn test_json_path_str_simple() {
        let v = serde_json::json!({"next_cursor": "xyz"});
        assert_eq!(json_path_str(&v, "next_cursor"), Some("xyz".into()));
    }

    #[test]
    fn test_json_path_str_dotted() {
        let v = serde_json::json!({"meta": {"next_page_token": "token123"}});
        assert_eq!(
            json_path_str(&v, "meta.next_page_token"),
            Some("token123".into())
        );
    }

    #[test]
    fn test_json_path_str_missing() {
        let v = serde_json::json!({"items": []});
        assert_eq!(json_path_str(&v, "next_cursor"), None);
    }

    #[test]
    fn test_parse_events_from_value_empty_array() {
        let v = serde_json::json!([]);
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_parse_events_from_value_logs_key() {
        let v = serde_json::json!({"logs": [{"id": "a"}], "next": "x"});
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!("a")));
    }

    #[test]
    fn test_parse_events_from_value_entries_key() {
        let v = serde_json::json!({"entries": [{"id": "e1"}], "nextPageToken": "tok"});
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!("e1")));
    }

    #[test]
    fn test_merge_cursor_into_body() {
        let body = serde_json::json!({"resourceNames": ["projects/my-project"], "pageSize": 100});
        let out = merge_cursor_into_body(Some(&body), "pageToken", "next-token");
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("pageToken"), Some(&serde_json::json!("next-token")));
        assert_eq!(obj.get("resourceNames"), body.get("resourceNames"));
        assert_eq!(obj.get("pageSize"), body.get("pageSize"));
    }

    #[test]
    fn test_merge_cursor_into_body_empty_base() {
        let out = merge_cursor_into_body(None, "pageToken", "tok");
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("pageToken"), Some(&serde_json::json!("tok")));
        assert_eq!(obj.len(), 1);
    }

    #[test]
    fn test_parse_events_from_value_single_value_fallback() {
        let v = serde_json::json!({"id": 42});
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_url_with_first_request_params_sync() {
        let yaml = r#"
url: "https://example.com/logs"
from: "2024-01-01T00:00:00Z"
from_param: "since"
query_params:
  limit: 20
  sortOrder: "ASCENDING"
"#;
        let source: SourceConfig = serde_yaml::from_str(yaml).unwrap();
        let url = url_with_first_request_params_sync("https://example.com/logs", &source).unwrap();
        assert!(url.contains("since=2024-01-01T00%3A00%3A00Z"));
        assert!(url.contains("limit=20"));
        assert!(url.contains("sortOrder=ASCENDING"));
    }

    #[test]
    fn test_url_with_first_request_params_only_query_params() {
        let yaml = r#"
url: "https://example.com/logs"
query_params:
  limit: "20"
  filter: "eventType eq \"user.session.start\""
"#;
        let source: SourceConfig = serde_yaml::from_str(yaml).unwrap();
        let url = url_with_first_request_params_sync("https://example.com/logs", &source).unwrap();
        assert!(url.contains("limit=20"));
        assert!(url.contains("filter="));
    }

    #[test]
    fn test_url_with_first_request_params_sync_state_takes_precedence_over_from() {
        // When source has state.watermark_*, sync helper (no store) does not add from param.
        let yaml = r#"
url: "https://example.com/logs"
from: "2024-01-01T00:00:00Z"
from_param: "since"
state:
  watermark_field: "id.time"
  watermark_param: "startTime"
"#;
        let source: SourceConfig = serde_yaml::from_str(yaml).unwrap();
        let url = url_with_first_request_params_sync("https://example.com/logs", &source).unwrap();
        assert!(
            !url.contains("since="),
            "state takes precedence over from; sync has no store so no param added"
        );
    }

    #[test]
    fn test_update_max_timestamp_dotted_path() {
        let events = vec![
            serde_json::json!({"id": {"time": "1000"}}),
            serde_json::json!({"id": {"time": "3000"}}),
            serde_json::json!({"id": {"time": "2000"}}),
        ];
        let mut max_ts: Option<String> = None;
        update_max_timestamp(&mut max_ts, &events, "id.time");
        assert_eq!(max_ts.as_deref(), Some("3000"));
    }

    #[test]
    fn test_effective_source_label_default() {
        let yaml = r#"url: "https://example.com/logs""#;
        let source: SourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(effective_source_label(&source, "okta-audit"), "okta-audit");
    }

    #[test]
    fn test_effective_source_label_override() {
        let yaml = r#"
url: "https://example.com/logs"
source_label_value: "okta_audit"
"#;
        let source: SourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(effective_source_label(&source, "okta-audit"), "okta_audit");
    }

    #[test]
    fn test_effective_source_label_key() {
        use crate::config::GlobalConfig;
        let mut global = GlobalConfig::default();
        let yaml = r#"url: "https://example.com/logs""#;
        let source: SourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(effective_source_label_key(&global, &source), "source");
        global.source_label_key = Some("service".to_string());
        assert_eq!(effective_source_label_key(&global, &source), "service");
        let yaml_override = r#"url: "https://example.com/logs"
source_label_key: "origin"
"#;
        let source_override: SourceConfig = serde_yaml::from_str(yaml_override).unwrap();
        assert_eq!(
            effective_source_label_key(&global, &source_override),
            "origin"
        );
    }
}
