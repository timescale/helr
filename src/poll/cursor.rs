use crate::circuit::{self, CircuitStore};
use crate::config::{
    CheckpointTiming, CursorExpiredBehavior, GlobalConfig, HttpMethod, OnParseErrorBehavior,
    SourceConfig,
};
use crate::dedupe::{self, DedupeStore};
use crate::dpop::DPoPKeyCache;
use crate::metrics;
use crate::oauth2::OAuth2TokenCache;
use crate::output::EventSink;
use crate::replay::RecordState;
use crate::retry::execute_with_retry;
use crate::state::StateStore;
use anyhow::Context;
use reqwest::Url;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use super::ClientRateLimiter;
use super::helpers::*;
use super::parse::*;

/// Cursor-in-body pagination: get cursor from response JSON path, pass as query param on next request.
#[allow(clippy::too_many_arguments)]
pub(super) async fn poll_cursor_pagination(
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
    request_semaphore: Option<Arc<Semaphore>>,
) -> anyhow::Result<()> {
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
        if page > 1
            && let Some(secs) = page_delay
        {
            tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
        let (url, body_override): (String, Option<serde_json::Value>) =
            match (&source.method, &cursor) {
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
        let _request_permit = match &request_semaphore {
            Some(s) => Some(
                s.acquire()
                    .await
                    .map_err(|e| anyhow::anyhow!("bulkhead request acquire: {}", e))?,
            ),
            None => None,
        };
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
            global.audit.as_ref(),
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
        if let Some(limit) = source.max_response_bytes
            && body.len() as u64 > limit
        {
            anyhow::bail!(
                "response body size {} exceeds max_response_bytes {}",
                body.len(),
                limit
            );
        }
        total_bytes += body.len() as u64;
        if let Some(limit) = max_bytes
            && total_bytes > limit
        {
            tracing::warn!(
                source = %source_id,
                total_bytes,
                max_bytes = limit,
                "reached max_bytes limit, stopping cursor pagination"
            );
            break;
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
        let events = match parse_events_from_value_for_source(&value, source) {
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
    if source.checkpoint == Some(CheckpointTiming::EndOfTick)
        && let Some(ref v) = pending_cursor
    {
        store_set_or_skip(&store, source_id, source, global, "cursor", v).await?;
    }
    store_incremental_from_after_poll(&store, source_id, source, global, incremental_max_ts).await;
    store_watermark_after_poll(&store, source_id, source, global, watermark_max_ts).await;
    Ok(())
}
