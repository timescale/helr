use crate::circuit::{self, CircuitStore};
use crate::config::{GlobalConfig, OnParseErrorBehavior, SourceConfig};
use crate::dedupe::{self, DedupeStore};
use crate::dpop::DPoPKeyCache;
use crate::metrics;
use crate::oauth2::OAuth2TokenCache;
use crate::output::EventSink;
use crate::replay::RecordState;
use crate::retry::execute_with_retry;
use crate::state::StateStore;
use anyhow::Context;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

use super::ClientRateLimiter;
use super::helpers::*;
use super::parse::*;
/// Single page (no pagination loop): one GET, emit events, clear next_url.
#[allow(clippy::too_many_arguments)]
pub(super) async fn poll_single_page(
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
    request_semaphore: Option<Arc<Semaphore>>,
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
        url,
        None, // single page: body from source when POST
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

    if !(200..300).contains(&record_status) {
        let body_str = String::from_utf8_lossy(&body_bytes);
        anyhow::bail!("http {} {}", record_status, body_str);
    }
    let path = record_url.path().to_string();
    if let Some(limit) = source.max_response_bytes
        && body_bytes.len() as u64 > limit
    {
        anyhow::bail!(
            "response body size {} exceeds max_response_bytes {}",
            body_bytes.len(),
            limit
        );
    }
    let events = match parse_events_from_body_for_source(&body_bytes, source) {
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
    for event_value in events {
        if let Some(d) = &source.dedupe {
            let id = event_id(&event_value, &d.id_path).unwrap_or_default();
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
