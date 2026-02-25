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
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use super::ClientRateLimiter;
use super::helpers::*;
use super::parse::*;
/// Page/offset pagination: increment page (or offset) each request, stop when empty or max_pages.
#[allow(clippy::too_many_arguments)]
pub(super) async fn poll_page_offset_pagination(
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
    request_semaphore: Option<Arc<Semaphore>>,
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
        if page > 1
            && let Some(secs) = page_delay
        {
            tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
        let mut u = Url::parse(base_url).context("page/offset base url")?;
        u.query_pairs_mut()
            .append_pair(page_param, &page.to_string());
        u.query_pairs_mut()
            .append_pair(limit_param, &limit.to_string());
        if page == 1
            && let Some(ref params) = source.query_params
        {
            for (k, v) in params {
                u.query_pairs_mut().append_pair(k, &v.to_param_value());
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
            None, // page_offset: body from source when POST
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

        let path = response.url().path().to_string();
        let record_url = response.url().clone();
        let record_status = response.status().as_u16();
        let record_headers = response.headers().clone();
        let mut response = Some(response);
        let mut event_count = 0usize;
        let mut emitted_count = 0u64;
        let mut _streamed = false;

        #[cfg(feature = "streaming")]
        if source.response_streaming.is_some() {
            use super::streaming;
            use crate::config::StreamingMode;

            let use_full = source.response_streaming == Some(StreamingMode::Full)
                && source.on_invalid_utf8.is_none();

            if use_full {
                let resp = response.take().unwrap();
                if !(200..300).contains(&record_status) {
                    let body_bytes = read_body_with_limit(resp, source.max_response_bytes).await?;
                    if let Some(ref rs) = record_state {
                        rs.save(
                            source_id,
                            record_url.as_str(),
                            record_status,
                            &record_headers,
                            &body_bytes,
                        )?;
                    }
                    let body_str = String::from_utf8_lossy(&body_bytes);
                    anyhow::bail!("http {} {}", record_status, body_str);
                }
                let tee_path = record_state.as_ref().map(|_| {
                    std::env::temp_dir().join(format!(
                        "helr-tee-{}-{}.bin",
                        std::process::id(),
                        source_id.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
                    ))
                });
                let (mut event_rx, _meta_rx, join_handle) = streaming::stream_and_parse(
                    resp,
                    source.response_events_path.clone(),
                    source.max_response_bytes,
                    tee_path.clone(),
                )
                .await?;

                let obj_path = source.response_event_object_path.as_deref();
                while let Some(result) = event_rx.recv().await {
                    let event_value = match result {
                        Ok(v) => v,
                        Err(e) => {
                            if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                                tracing::warn!(source = %source_id, error = %e, "parse error, skipping page");
                                continue;
                            }
                            return Err(e).context("streaming parse element");
                        }
                    };
                    let event_value = match streaming::unwrap_event_object(event_value, obj_path) {
                        Some(v) => v,
                        None => continue,
                    };
                    event_count += 1;
                    if let Some(ref inc) = source.incremental_from {
                        update_max_timestamp_single(
                            &mut incremental_max_ts,
                            &event_value,
                            &inc.event_timestamp_path,
                        );
                    }
                    if let Some(ref st) = source.state {
                        update_max_timestamp_single(
                            &mut watermark_max_ts,
                            &event_value,
                            &st.watermark_field,
                        );
                    }
                    if let Some(d) = &source.dedupe {
                        let id = event_id(&event_value, &d.id_path).unwrap_or_default();
                        if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                            continue;
                        }
                    }
                    total_events += 1;
                    emitted_count += 1;
                    let emitted = build_emitted_event(source, source_id, &path, event_value);
                    emit_event_line(global, source_id, source, &event_sink, &emitted)?;
                }

                join_handle
                    .await
                    .map_err(|e| anyhow::anyhow!("streaming parser panicked: {}", e))??;

                if let (Some(rs), Some(tp)) = (&record_state, &tee_path) {
                    let body = std::fs::read(tp).context("read tee file for recording")?;
                    rs.save(
                        source_id,
                        record_url.as_str(),
                        record_status,
                        &record_headers,
                        &body,
                    )?;
                    let _ = std::fs::remove_file(tp);
                }
                _streamed = true;
            } else {
                let resp = response.take().unwrap();
                let body_bytes = read_body_with_limit(resp, source.max_response_bytes).await?;
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
                let parse_result = match streaming::parse_streaming(&body_bytes, source) {
                    Ok(r) => r,
                    Err(e) => {
                        if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                            tracing::warn!(source = %source_id, error = %e, "parse error, skipping page");
                            continue;
                        }
                        return Err(e).context("streaming parse");
                    }
                };
                let obj_path = source.response_event_object_path.as_deref();
                for result in parse_result.iter(&body_bytes) {
                    let event_value = result.context("parse event element")?;
                    let event_value = match streaming::unwrap_event_object(event_value, obj_path) {
                        Some(v) => v,
                        None => continue,
                    };
                    event_count += 1;
                    if let Some(ref inc) = source.incremental_from {
                        update_max_timestamp_single(
                            &mut incremental_max_ts,
                            &event_value,
                            &inc.event_timestamp_path,
                        );
                    }
                    if let Some(ref st) = source.state {
                        update_max_timestamp_single(
                            &mut watermark_max_ts,
                            &event_value,
                            &st.watermark_field,
                        );
                    }
                    if let Some(d) = &source.dedupe {
                        let id = event_id(&event_value, &d.id_path).unwrap_or_default();
                        if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                            continue;
                        }
                    }
                    total_events += 1;
                    emitted_count += 1;
                    let emitted = build_emitted_event(source, source_id, &path, event_value);
                    emit_event_line(global, source_id, source, &event_sink, &emitted)?;
                }
                _streamed = true;
            }
        }

        if !_streamed {
            let resp = response.take().unwrap();
            let body_bytes = read_body_with_limit(resp, source.max_response_bytes).await?;
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
            let events = match parse_events_from_body_for_source(&body_bytes, source) {
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
            event_count = events.len();
            for event_value in events {
                if let Some(d) = &source.dedupe {
                    let id = event_id(&event_value, &d.id_path).unwrap_or_default();
                    if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                        continue;
                    }
                }
                total_events += 1;
                emitted_count += 1;
                let emitted = build_emitted_event(source, source_id, &path, event_value);
                emit_event_line(global, source_id, source, &event_sink, &emitted)?;
            }
        }
        metrics::record_events(source_id, emitted_count);
        if event_count < limit as usize {
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

/// True offset-based pagination: offset increments by limit each page (offset=0, offset=100, ...).
#[allow(clippy::too_many_arguments)]
pub(super) async fn poll_offset_pagination(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    client: &reqwest::Client,
    offset_param: &str,
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
    request_semaphore: Option<Arc<Semaphore>>,
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
        let offset = (page - 1) * limit;
        if page > 1
            && let Some(secs) = page_delay
        {
            tracing::debug!(source = %source_id, delay_secs = secs, "delay between pages");
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
        let mut u = Url::parse(base_url).context("offset base url")?;
        u.query_pairs_mut()
            .append_pair(offset_param, &offset.to_string());
        u.query_pairs_mut()
            .append_pair(limit_param, &limit.to_string());
        if page == 1
            && let Some(ref params) = source.query_params
        {
            for (k, v) in params {
                u.query_pairs_mut().append_pair(k, &v.to_param_value());
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
            None,
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

        let path = response.url().path().to_string();
        let record_url = response.url().clone();
        let record_status = response.status().as_u16();
        let record_headers = response.headers().clone();
        let mut response = Some(response);
        let mut event_count = 0usize;
        let mut emitted_count = 0u64;
        let mut _streamed = false;

        #[cfg(feature = "streaming")]
        if source.response_streaming.is_some() {
            use super::streaming;
            use crate::config::StreamingMode;

            let use_full = source.response_streaming == Some(StreamingMode::Full)
                && source.on_invalid_utf8.is_none();

            if use_full {
                let resp = response.take().unwrap();
                if !(200..300).contains(&record_status) {
                    let body_bytes = read_body_with_limit(resp, source.max_response_bytes).await?;
                    if let Some(ref rs) = record_state {
                        rs.save(
                            source_id,
                            record_url.as_str(),
                            record_status,
                            &record_headers,
                            &body_bytes,
                        )?;
                    }
                    let body_str = String::from_utf8_lossy(&body_bytes);
                    anyhow::bail!("http {} {}", record_status, body_str);
                }
                let tee_path = record_state.as_ref().map(|_| {
                    std::env::temp_dir().join(format!(
                        "helr-tee-{}-{}.bin",
                        std::process::id(),
                        source_id.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
                    ))
                });
                let (mut event_rx, _meta_rx, join_handle) = streaming::stream_and_parse(
                    resp,
                    source.response_events_path.clone(),
                    source.max_response_bytes,
                    tee_path.clone(),
                )
                .await?;

                let obj_path = source.response_event_object_path.as_deref();
                while let Some(result) = event_rx.recv().await {
                    let event_value = match result {
                        Ok(v) => v,
                        Err(e) => {
                            if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                                tracing::warn!(source = %source_id, error = %e, "parse error, skipping page");
                                continue;
                            }
                            return Err(e).context("streaming parse element");
                        }
                    };
                    let event_value = match streaming::unwrap_event_object(event_value, obj_path) {
                        Some(v) => v,
                        None => continue,
                    };
                    event_count += 1;
                    if let Some(ref inc) = source.incremental_from {
                        update_max_timestamp_single(
                            &mut incremental_max_ts,
                            &event_value,
                            &inc.event_timestamp_path,
                        );
                    }
                    if let Some(ref st) = source.state {
                        update_max_timestamp_single(
                            &mut watermark_max_ts,
                            &event_value,
                            &st.watermark_field,
                        );
                    }
                    if let Some(d) = &source.dedupe {
                        let id = event_id(&event_value, &d.id_path).unwrap_or_default();
                        if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                            continue;
                        }
                    }
                    total_events += 1;
                    emitted_count += 1;
                    let emitted = build_emitted_event(source, source_id, &path, event_value);
                    emit_event_line(global, source_id, source, &event_sink, &emitted)?;
                }

                join_handle
                    .await
                    .map_err(|e| anyhow::anyhow!("streaming parser panicked: {}", e))??;

                if let (Some(rs), Some(tp)) = (&record_state, &tee_path) {
                    let body = std::fs::read(tp).context("read tee file for recording")?;
                    rs.save(
                        source_id,
                        record_url.as_str(),
                        record_status,
                        &record_headers,
                        &body,
                    )?;
                    let _ = std::fs::remove_file(tp);
                }
                _streamed = true;
            } else {
                let resp = response.take().unwrap();
                let body_bytes = read_body_with_limit(resp, source.max_response_bytes).await?;
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
                let parse_result = match streaming::parse_streaming(&body_bytes, source) {
                    Ok(r) => r,
                    Err(e) => {
                        if source.on_parse_error == Some(OnParseErrorBehavior::Skip) {
                            tracing::warn!(source = %source_id, error = %e, "parse error, skipping page");
                            continue;
                        }
                        return Err(e).context("streaming parse");
                    }
                };
                let obj_path = source.response_event_object_path.as_deref();
                for result in parse_result.iter(&body_bytes) {
                    let event_value = result.context("parse event element")?;
                    let event_value = match streaming::unwrap_event_object(event_value, obj_path) {
                        Some(v) => v,
                        None => continue,
                    };
                    event_count += 1;
                    if let Some(ref inc) = source.incremental_from {
                        update_max_timestamp_single(
                            &mut incremental_max_ts,
                            &event_value,
                            &inc.event_timestamp_path,
                        );
                    }
                    if let Some(ref st) = source.state {
                        update_max_timestamp_single(
                            &mut watermark_max_ts,
                            &event_value,
                            &st.watermark_field,
                        );
                    }
                    if let Some(d) = &source.dedupe {
                        let id = event_id(&event_value, &d.id_path).unwrap_or_default();
                        if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                            continue;
                        }
                    }
                    total_events += 1;
                    emitted_count += 1;
                    let emitted = build_emitted_event(source, source_id, &path, event_value);
                    emit_event_line(global, source_id, source, &event_sink, &emitted)?;
                }
                _streamed = true;
            }
        }

        if !_streamed {
            let resp = response.take().unwrap();
            let body_bytes = read_body_with_limit(resp, source.max_response_bytes).await?;
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
            let events = match parse_events_from_body_for_source(&body_bytes, source) {
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
            event_count = events.len();
            for event_value in events {
                if let Some(d) = &source.dedupe {
                    let id = event_id(&event_value, &d.id_path).unwrap_or_default();
                    if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                        continue;
                    }
                }
                total_events += 1;
                emitted_count += 1;
                let emitted = build_emitted_event(source, source_id, &path, event_value);
                emit_event_line(global, source_id, source, &event_sink, &emitted)?;
            }
        }
        metrics::record_events(source_id, emitted_count);
        if event_count < limit as usize {
            tracing::info!(
                source = %source_id,
                pages = page,
                events = total_events,
                duration_ms = start.elapsed().as_millis(),
                "poll completed (offset)"
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
