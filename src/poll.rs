//! Single poll tick: load state → fetch pages (link-header) → emit NDJSON → commit state.

use crate::circuit::{self, CircuitStore};
use crate::client::build_client;
use crate::config::{
    CheckpointTiming, Config, CursorExpiredBehavior, InvalidUtf8Behavior,
    MaxEventBytesBehavior, OnParseErrorBehavior, OnStateWriteErrorBehavior, PaginationConfig,
    SourceConfig,
};
use crate::dedupe::{self, DedupeStore};
use crate::event::EmittedEvent;
use crate::metrics;
use crate::oauth2::OAuth2TokenCache;
use crate::output::EventSink;
use crate::pagination::next_link_from_headers;
use crate::replay::RecordState;
use crate::retry::execute_with_retry;
use crate::state::StateStore;
use anyhow::Context;
use chrono::Utc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::instrument;

/// Run one poll tick for all sources (or only those matching source_filter).
/// Sources are polled concurrently (one task per source).
pub async fn run_one_tick(
    config: &Config,
    store: Arc<dyn StateStore>,
    source_filter: Option<&str>,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
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
        let circuit_store = circuit_store.clone();
        let token_cache = token_cache.clone();
        let dedupe_store = dedupe_store.clone();
        let event_sink = event_sink.clone();
        let record_state = record_state.clone();
        let h = tokio::spawn(async move {
            poll_one_source(
                store,
                &source_id_key,
                &source,
                circuit_store,
                token_cache,
                dedupe_store,
                event_sink,
                record_state,
            )
            .await
        });
        handles.push((source_id, h));
    }
    for (source_id, h) in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                metrics::record_error(&source_id);
                tracing::error!(source = %source_id, "poll failed: {:#}", e);
            }
            Err(e) => {
                metrics::record_error(&source_id);
                tracing::error!(source = %source_id, "task join failed: {:#}", e);
            }
        }
    }
    Ok(())
}

#[instrument(skip(store, source, circuit_store, token_cache, dedupe_store, event_sink, record_state))]
async fn poll_one_source(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
) -> anyhow::Result<()> {
    let client = build_client(source.resilience.as_ref())?;

    match &source.pagination {
        Some(PaginationConfig::LinkHeader { rel, max_pages }) => {
            poll_link_header(
                store,
                source_id,
                source,
                &client,
                rel.as_str(),
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dedupe_store,
                event_sink,
                record_state,
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
                &client,
                cursor_param,
                cursor_path,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dedupe_store,
                event_sink,
                record_state,
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
                &client,
                page_param,
                limit_param,
                *limit,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dedupe_store,
                event_sink,
                record_state,
            )
            .await
        }
        _ => {
            return poll_single_page(
                store,
                source_id,
                source,
                &client,
                &source.url,
                circuit_store,
                token_cache,
                dedupe_store,
                event_sink,
                record_state.clone(),
            )
            .await;
        }
    }
}

/// Link-header pagination: follow rel="next" until no more or max_pages.
async fn poll_link_header(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    rel: &str,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let from_store = store.get(source_id, "next_url").await?.filter(|s| !s.is_empty());
    let mut url: String = from_store
        .clone()
        .unwrap_or_else(|| source.url.clone());
    if from_store.is_none() && source.initial_since.is_some() {
        url = url_with_initial_since(&url, source)?;
    }

    let mut page = 0u32;
    let mut total_events = 0u64;
    let mut total_bytes: u64 = 0;
    let max_bytes = source.max_bytes;
    let mut pending_next_url: Option<String> = None;
    let delay_between_pages = source
        .resilience
        .as_ref()
        .and_then(|r| r.rate_limit.as_ref())
        .and_then(|rl| rl.delay_between_pages_secs);
    loop {
        page += 1;
        if page > max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
            break;
        }
        if page > 1 {
            if let Some(secs) = delay_between_pages {
                tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }

        if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            &client,
            source,
            source_id,
            &url,
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
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
                if let Some(cb) =
                    source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(
                    source_id,
                    "error",
                    req_start.elapsed().as_secs_f64(),
                );
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };

        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("http {} {}", status, response.text().await.unwrap_or_default());
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
        let body = bytes_to_string(&body_bytes, source.invalid_utf8)?;
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

        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_field).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue; // duplicate
                }
            }
            total_events += 1;
            emitted_count += 1;
            let ts = event_ts(event_value);
            let emitted = EmittedEvent::new(
                ts,
                source_id.to_string(),
                path.clone(),
                event_value.clone(),
            );
            emit_event_line(source_id, source, &event_sink, &emitted)?;
        }
        metrics::record_events(source_id, emitted_count);

        let checkpoint_per_page = source.checkpoint != Some(CheckpointTiming::EndOfTick);
        let hit_max_bytes = max_bytes.is_some() && total_bytes > max_bytes.unwrap();
        if let Some(next) = next_url {
            let absolute = base_url.join(&next).context("resolve next URL")?;
            if checkpoint_per_page {
                store
                    .set(source_id, "next_url", absolute.as_str())
                    .await?;
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
                store_set_or_skip(&store, source_id, source, "next_url", "").await?;
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
            store_set_or_skip(&store, source_id, source, "next_url", v).await?;
        }
    }
    Ok(())
}

/// Cursor-in-body pagination: get cursor from response JSON path, pass as query param on next request.
async fn poll_cursor_pagination(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    cursor_param: &str,
    cursor_path: &str,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
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
    let delay_between_pages = source
        .resilience
        .as_ref()
        .and_then(|r| r.rate_limit.as_ref())
        .and_then(|rl| rl.delay_between_pages_secs);
    loop {
        page += 1;
        if page > max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
            break;
        }
        if page > 1 {
            if let Some(secs) = delay_between_pages {
                tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }
        let url = if let Some(ref c) = cursor {
            let mut u = Url::parse(base_url).context("cursor pagination base url")?;
            u.query_pairs_mut().append_pair(cursor_param, c);
            u.to_string()
        } else {
            let u = base_url.to_string();
            if source.initial_since.is_some() {
                url_with_initial_since(&u, source)?
            } else {
                u
            }
        };
        if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            client,
            source,
            source_id,
            &url,
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
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
                if let Some(cb) =
                    source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };
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
        let body = bytes_to_string(&body_bytes, source.invalid_utf8)?;
        if !status.is_success() {
            if cursor.is_some()
                && status.as_u16() >= 400
                && status.as_u16() < 500
            {
                let lower = body.to_lowercase();
                let is_expired = status.as_u16() == 410
                    || lower.contains("expired")
                    || lower.contains("invalid cursor")
                    || lower.contains("cursor invalid");
                if is_expired && source.cursor_expired == Some(CursorExpiredBehavior::Reset) {
                    store_set_or_skip(&store, source_id, source, "cursor", "").await?;
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
        let next_cursor = json_path_str(&value, cursor_path).filter(|s| !s.is_empty());
        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_field).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue;
                }
            }
            total_events += 1;
            emitted_count += 1;
            let ts = event_ts(event_value);
            let emitted = EmittedEvent::new(
                ts,
                source_id.to_string(),
                path.clone(),
                event_value.clone(),
            );
            emit_event_line(source_id, source, &event_sink, &emitted)?;
        }
        metrics::record_events(source_id, emitted_count);
        let checkpoint_per_page = source.checkpoint != Some(CheckpointTiming::EndOfTick);
        match next_cursor {
            Some(c) => {
                if checkpoint_per_page {
                    store_set_or_skip(&store, source_id, source, "cursor", &c).await?;
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
                    store_set_or_skip(&store, source_id, source, "cursor", "").await?;
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
            store_set_or_skip(&store, source_id, source, "cursor", v).await?;
        }
    }
    Ok(())
}

/// Page/offset pagination: increment page (or offset) each request, stop when empty or max_pages.
async fn poll_page_offset_pagination(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    page_param: &str,
    limit_param: &str,
    limit: u32,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
) -> anyhow::Result<()> {
    use reqwest::Url;
    let start = Instant::now();
    let base_url = source.url.as_str();
    let mut total_events = 0u64;
    let delay_between_pages = source
        .resilience
        .as_ref()
        .and_then(|r| r.rate_limit.as_ref())
        .and_then(|rl| rl.delay_between_pages_secs);
    for page in 1..=max_pages {
        if page > 1 {
            if let Some(secs) = delay_between_pages {
                tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
                tokio::time::sleep(Duration::from_secs(secs)).await;
            }
        }
        let mut u = Url::parse(base_url).context("page/offset base url")?;
        u.query_pairs_mut().append_pair(page_param, &page.to_string());
        u.query_pairs_mut().append_pair(limit_param, &limit.to_string());
        let url = u.to_string();
        if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            client,
            source,
            source_id,
            &url,
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
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
                if let Some(cb) =
                    source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };
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
        let body = bytes_to_string(&body_bytes, source.invalid_utf8)?;
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
        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_field).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue;
                }
            }
            total_events += 1;
            emitted_count += 1;
            let ts = event_ts(event_value);
            let emitted = EmittedEvent::new(
                ts,
                source_id.to_string(),
                path.clone(),
                event_value.clone(),
            );
            emit_event_line(source_id, source, &event_sink, &emitted)?;
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
    store_set_or_skip(&store, source_id, source, "next_url", "").await?;
    Ok(())
}

/// Single page (no pagination loop): one GET, emit events, clear next_url.
async fn poll_single_page(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    url: &str,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
) -> anyhow::Result<()> {
    let start = Instant::now();
    if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
        circuit::allow_request(&circuit_store, source_id, cb)
            .await
            .context("circuit open")?;
    }
    let req_start = std::time::Instant::now();
    let response = match execute_with_retry(
        client,
        source,
        source_id,
        url,
        source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
        source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
        Some(&token_cache),
    )
    .await
    {
        Ok(r) => {
            let success = r.status().as_u16() < 500;
            if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
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
            if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
                circuit::record_result(&circuit_store, source_id, cb, false).await;
            }
            metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
            metrics::record_error(source_id);
            return Err(e).context("http request");
        }
    };

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
    let body = bytes_to_string(&body_bytes, source.invalid_utf8)?;
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
                let _ = store_set_or_skip(&store, source_id, source, "next_url", "").await;
                return Ok(());
            }
            return Err(e).context("parse response");
        }
    };

    let mut emitted_count = 0u64;
    for event_value in &events {
        if let Some(d) = &source.dedupe {
            let id = event_id(event_value, &d.id_field).unwrap_or_default();
            if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                continue;
            }
        }
        emitted_count += 1;
        let ts = event_ts(event_value);
        let emitted = EmittedEvent::new(
            ts,
            source_id.to_string(),
            path.clone(),
            event_value.clone(),
        );
        emit_event_line(source_id, source, &event_sink, &emitted)?;
    }
    metrics::record_events(source_id, emitted_count);

    store_set_or_skip(&store, source_id, source, "next_url", "").await?;
    tracing::info!(
        source = %source_id,
        events = emitted_count,
        duration_ms = start.elapsed().as_millis(),
        "poll completed"
    );
    Ok(())
}

/// Convert response body bytes to string; apply invalid_utf8 policy (replace/escape/fail).
fn bytes_to_string(bytes: &[u8], invalid_utf8: Option<InvalidUtf8Behavior>) -> anyhow::Result<String> {
    match invalid_utf8 {
        Some(InvalidUtf8Behavior::Replace) | Some(InvalidUtf8Behavior::Escape) => {
            Ok(String::from_utf8_lossy(bytes).into_owned())
        }
        _ => String::from_utf8(bytes.to_vec())
            .map_err(|e| anyhow::anyhow!("invalid UTF-8 in response: {}", e)),
    }
}

/// Emit one event line; enforce max_event_bytes, record output errors.
fn emit_event_line(
    source_id: &str,
    source: &SourceConfig,
    event_sink: &Arc<dyn EventSink>,
    emitted: &EmittedEvent,
) -> anyhow::Result<()> {
    let line = emitted.to_ndjson_line()?;
    if let Some(max) = source.max_event_bytes {
        if line.len() as u64 > max {
            match source.max_event_bytes_behavior.unwrap_or(MaxEventBytesBehavior::Fail) {
                MaxEventBytesBehavior::Truncate => {
                    let max_usize = max as usize;
                    let truncated: String = if max_usize >= 3 {
                        format!(
                            "{}...",
                            line.chars().take(max_usize.saturating_sub(3)).collect::<String>()
                        )
                    } else {
                        line.chars().take(max_usize).collect()
                    };
                    event_sink.write_line(&truncated).map_err(|e| {
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
                        "event line exceeds max_event_bytes, skipping"
                    );
                    return Ok(());
                }
                MaxEventBytesBehavior::Fail => {
                    anyhow::bail!(
                        "event line length {} exceeds max_event_bytes {}",
                        line.len(),
                        max
                    );
                }
            }
        }
    }
    event_sink.write_line(&line).map_err(|e| {
        metrics::record_output_error(source_id);
        e
    })
}

/// Set state key; on error, fail or skip checkpoint per source config (e.g. state store disk full).
async fn store_set_or_skip(
    store: &Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    if let Err(e) = store.set(source_id, key, value).await {
        if source.on_state_write_error == Some(OnStateWriteErrorBehavior::SkipCheckpoint) {
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

/// Append initial_since query param to URL when configured (for first request).
fn url_with_initial_since(url: &str, source: &SourceConfig) -> anyhow::Result<String> {
    let Some(ref since_val) = source.initial_since else {
        return Ok(url.to_string());
    };
    let param = source.since_param.as_deref().unwrap_or("since");
    let mut u = reqwest::Url::parse(url).context("parse url for initial_since")?;
    u.query_pairs_mut().append_pair(param, since_val);
    Ok(u.to_string())
}

/// Parse response body: top-level array, or object with "items"/"data"/"events" array.
fn parse_events_from_body(body: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("parse response json")?;
    parse_events_from_value(&value)
}

/// Extract events array from parsed JSON (same keys as parse_events_from_body).
fn parse_events_from_value(value: &serde_json::Value) -> anyhow::Result<Vec<serde_json::Value>> {
    if let Some(arr) = value.as_array() {
        return Ok(arr.clone());
    }
    if let Some(obj) = value.as_object() {
        for key in &["items", "data", "events", "logs"] {
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
fn event_id(event: &serde_json::Value, id_field: &str) -> Option<String> {
    let mut v = event;
    for segment in id_field.split('.') {
        v = v.get(segment)?;
    }
    v.as_str().map(|s| s.to_string())
}

fn event_ts(event: &serde_json::Value) -> String {
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
        assert_eq!(json_path_str(&v, "meta.next_page_token"), Some("token123".into()));
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
    fn test_parse_events_from_value_single_value_fallback() {
        let v = serde_json::json!({"id": 42});
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(42)));
    }
}
