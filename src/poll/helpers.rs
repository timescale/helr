use crate::config::{
    GlobalConfig, InvalidUtf8Behavior, MaxEventBytesBehavior, OnStateWriteErrorBehavior,
    RateLimitConfig, SourceConfig,
};
use crate::event::EmittedEvent;
use crate::metrics;
use crate::output::EventSink;
use crate::retry::rate_limit_info_from_headers;
use crate::state::StateStore;
use anyhow::Context;
use chrono::Utc;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;

/// Convert response body bytes to string; apply on_invalid_utf8 policy (replace/escape/fail).
pub(crate) fn bytes_to_string(
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

/// Stream response body with early abort when `max_bytes` is exceeded.
/// Checks Content-Length upfront when available, then enforces the limit chunk-by-chunk.
pub(crate) async fn read_body_with_limit(
    response: reqwest::Response,
    max_bytes: Option<u64>,
) -> anyhow::Result<Vec<u8>> {
    let content_length = response.content_length();
    if let (Some(cl), Some(limit)) = (content_length, max_bytes)
        && cl > limit
    {
        anyhow::bail!("Content-Length {} exceeds max_response_bytes {}", cl, limit);
    }
    let mut stream = response.bytes_stream();
    let capacity = content_length.unwrap_or(8192).min(16 * 1024 * 1024) as usize;
    let mut buf = Vec::with_capacity(capacity);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read body chunk")?;
        buf.extend_from_slice(&chunk);
        if let Some(limit) = max_bytes
            && buf.len() as u64 > limit
        {
            anyhow::bail!(
                "response body size {} exceeds max_response_bytes {}",
                buf.len(),
                limit
            );
        }
    }
    Ok(buf)
}

/// Value for the producer label in NDJSON: source_label_value if set, else the config source key.
pub(crate) fn effective_source_label(source: &SourceConfig, source_id: &str) -> String {
    source
        .source_label_value
        .as_deref()
        .unwrap_or(source_id)
        .to_string()
}

/// Key for the producer label in NDJSON: source override, else global, else "source".
pub(crate) fn effective_source_label_key<'a>(
    global: &'a GlobalConfig,
    source: &'a SourceConfig,
) -> &'a str {
    source
        .source_label_key
        .as_deref()
        .or(global.source_label_key.as_deref())
        .unwrap_or("source")
}

/// Emit one event line; enforce max_line_bytes, record output errors.
pub(crate) fn emit_event_line(
    global: &GlobalConfig,
    source_id: &str,
    source: &SourceConfig,
    event_sink: &Arc<dyn EventSink>,
    emitted: &EmittedEvent,
) -> anyhow::Result<()> {
    let label_key = effective_source_label_key(global, source);
    let line = emitted.to_ndjson_line_with_label_key(label_key)?;
    if let Some(max) = source.max_line_bytes
        && line.len() as u64 > max
    {
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
                    .inspect_err(|_e| {
                        metrics::record_output_error(source_id);
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
    event_sink
        .write_line_from_source(Some(source_id), &line)
        .inspect_err(|_e| {
            metrics::record_output_error(source_id);
        })
}

/// Set state key; on error, fail or skip checkpoint per source config or global degradation.emit_without_checkpoint.
pub(crate) async fn store_set_or_skip(
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
            Err(e)
        }
    } else {
        Ok(())
    }
}

/// Merge cursor into POST body for cursor pagination. Returns a new JSON object with cursor_param set.
pub(crate) fn merge_cursor_into_body(
    base: Option<&serde_json::Value>,
    cursor_param: &str,
    cursor_value: &str,
) -> serde_json::Value {
    let mut obj = base
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    obj.insert(
        cursor_param.to_string(),
        serde_json::Value::String(cursor_value.to_string()),
    );
    serde_json::Value::Object(obj)
}

/// State key for per-source watermark (default "watermark").
pub(crate) fn watermark_state_key(source: &SourceConfig) -> Option<&str> {
    source
        .state
        .as_ref()
        .map(|s| s.state_key.as_deref().unwrap_or("watermark"))
}

/// Build first-request URL: add watermark state, incremental_from state, or from; and query_params.
/// Used when there is no saved cursor/next_url (link_header, cursor, and page_offset first page).
pub(crate) async fn url_with_first_request_params(
    store: &Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    url: &str,
) -> anyhow::Result<String> {
    let mut u = reqwest::Url::parse(url).context("parse url for first-request params")?;
    if let Some(ref st) = source.state {
        if let Some(key) = watermark_state_key(source)
            && let Some(val) = store.get(source_id, key).await?.filter(|s| !s.is_empty())
        {
            u.query_pairs_mut().append_pair(&st.watermark_param, &val);
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
pub(crate) fn url_with_first_request_params_sync(
    url: &str,
    source: &SourceConfig,
) -> anyhow::Result<String> {
    let mut u = reqwest::Url::parse(url).context("parse url for first-request params")?;
    if source.state.is_none()
        && source.incremental_from.is_none()
        && let Some(ref from_val) = source.from
    {
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

/// Get value at dotted path as string (supports string and number for timestamps like date_create).
pub(crate) fn value_at_path_as_string(value: &serde_json::Value, path: &str) -> Option<String> {
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
pub(crate) fn update_max_timestamp(
    max_ts: &mut Option<String>,
    events: &[serde_json::Value],
    path: &str,
) {
    for event in events {
        update_max_timestamp_single(max_ts, event, path);
    }
}

/// Update max_ts from a single event (used by streaming path to track incrementally).
pub(crate) fn update_max_timestamp_single(
    max_ts: &mut Option<String>,
    event: &serde_json::Value,
    path: &str,
) {
    if let Some(v) = value_at_path_as_string(event, path)
        && max_ts.as_ref().is_none_or(|m| v.as_str() > m.as_str())
    {
        *max_ts = Some(v);
    }
}

/// Store incremental_from state after poll when configured.
pub(crate) async fn store_incremental_from_after_poll(
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
pub(crate) async fn store_watermark_after_poll(
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

pub(crate) fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

/// If adaptive rate limiting is enabled and remaining is 0 or low, sleep until reset (or a short delay).
pub(crate) async fn maybe_adaptive_sleep_after_response(
    headers: &reqwest::header::HeaderMap,
    source_id: &str,
    rate_limit_config: Option<&RateLimitConfig>,
) {
    let rl = match rate_limit_config {
        Some(r) if r.adaptive == Some(true) => r,
        _ => return,
    };
    let info = rate_limit_info_from_headers(headers, rl.headers.as_ref());
    if info.remaining.is_none_or(|n| n > 1) {
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
