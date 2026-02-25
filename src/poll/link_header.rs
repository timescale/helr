use crate::circuit::{self, CircuitStore};
use crate::config::{CheckpointTiming, GlobalConfig, OnParseErrorBehavior, SourceConfig};
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
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use super::ClientRateLimiter;
use super::helpers::*;
use super::parse::*;
use crate::pagination::next_link_from_headers;

/// Link-header pagination: follow rel="next" until no more or max_pages.
#[allow(clippy::too_many_arguments)]
pub(super) async fn poll_link_header(
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
    request_semaphore: Option<Arc<Semaphore>>,
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
        if page > 1
            && let Some(secs) = page_delay
        {
            tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
            tokio::time::sleep(Duration::from_secs(secs)).await;
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
            None, // link_header: same URL per page; body from source when POST
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
        if let Some(limit) = source.max_response_bytes
            && body_bytes.len() as u64 > limit
        {
            anyhow::bail!(
                "response body size {} exceeds max_response_bytes {}",
                body_bytes.len(),
                limit
            );
        }
        total_bytes += body_bytes.len() as u64;
        let events = match parse_events_from_body_for_source(&body_bytes, source) {
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
    if source.checkpoint == Some(CheckpointTiming::EndOfTick)
        && let Some(ref v) = pending_next_url
    {
        store_set_or_skip(&store, source_id, source, global, "next_url", v).await?;
    }
    store_incremental_from_after_poll(&store, source_id, source, global, incremental_max_ts).await;
    store_watermark_after_poll(&store, source_id, source, global, watermark_max_ts).await;
    Ok(())
}
