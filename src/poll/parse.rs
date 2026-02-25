use crate::config::{InvalidUtf8Behavior, SourceConfig};
use crate::event::EmittedEvent;
use anyhow::Context;
use chrono::Utc;

use super::helpers::{bytes_to_string, effective_source_label};

/// Parse response body bytes using source config. Uses `from_slice` for valid UTF-8 sources,
/// falls back to lossy `bytes_to_string` + `from_str` when `on_invalid_utf8` is Replace/Escape.
pub(crate) fn parse_events_from_body_for_source(
    body_bytes: &[u8],
    source: &SourceConfig,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let value: serde_json::Value = match source.on_invalid_utf8 {
        Some(InvalidUtf8Behavior::Replace) | Some(InvalidUtf8Behavior::Escape) => {
            let body = bytes_to_string(body_bytes, source.on_invalid_utf8)?;
            serde_json::from_str(&body).context("parse response json")?
        }
        _ => serde_json::from_slice(body_bytes).context("parse response json")?,
    };
    parse_events_from_value_for_source(value, source)
}

/// Extract events from parsed JSON using source's optional paths or default keys.
/// Takes ownership of the Value tree to avoid cloning the events array.
pub(crate) fn parse_events_from_value_for_source(
    value: serde_json::Value,
    source: &SourceConfig,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let path = source.response_events_path.as_deref();
    let obj_path = source.response_event_object_path.as_deref();
    if path.is_some() || obj_path.is_some() {
        parse_events_from_value_with_path(value, path, obj_path)
    } else {
        parse_events_from_value(value)
    }
}

/// Extract events array at dotted path, optionally unwrapping each element (e.g. edge.node).
fn parse_events_from_value_with_path(
    mut value: serde_json::Value,
    events_path: Option<&str>,
    event_object_path: Option<&str>,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let arr = match events_path {
        Some(p) => json_path_array(&mut value, p).ok_or_else(|| {
            anyhow::anyhow!("response_events_path {:?} did not resolve to an array", p)
        })?,
        None => {
            if let Some(arr) = value.as_array_mut() {
                std::mem::take(arr)
            } else if let Some(obj) = value.as_object_mut() {
                for key in &["items", "data", "events", "logs", "entries"] {
                    if let Some(v) = obj.get_mut(*key).and_then(|v| v.as_array_mut()) {
                        return Ok(unwrap_event_objects(std::mem::take(v), event_object_path));
                    }
                }
                anyhow::bail!(
                    "response has no top-level array or known events key (items/data/events/logs/entries)"
                );
            } else {
                anyhow::bail!("response root is not an object or array");
            }
        }
    };
    Ok(unwrap_event_objects(arr, event_object_path))
}

/// Take array at dotted path (e.g. "data.AndromedaEvents.edges"), draining it from the tree.
fn json_path_array(value: &mut serde_json::Value, path: &str) -> Option<Vec<serde_json::Value>> {
    let mut v = value;
    for segment in path.split('.') {
        v = v.get_mut(segment)?;
    }
    v.as_array_mut().map(std::mem::take)
}

/// For each element, optionally take the value at dotted path (e.g. "node"); otherwise return as-is.
fn unwrap_event_objects(
    arr: Vec<serde_json::Value>,
    object_path: Option<&str>,
) -> Vec<serde_json::Value> {
    let Some(path) = object_path else {
        return arr;
    };
    arr.into_iter()
        .filter_map(|mut el| {
            let mut v = &mut el;
            for segment in path.split('.') {
                v = v.get_mut(segment)?;
            }
            Some(std::mem::take(v))
        })
        .collect()
}

/// Extract events array from parsed JSON (same keys as parse_events_from_body).
/// Takes ownership to avoid cloning.
pub(crate) fn parse_events_from_value(
    mut value: serde_json::Value,
) -> anyhow::Result<Vec<serde_json::Value>> {
    if let Some(arr) = value.as_array_mut() {
        return Ok(std::mem::take(arr));
    }
    if let Some(obj) = value.as_object_mut() {
        for key in &["items", "data", "events", "logs", "entries"] {
            if let Some(v) = obj.get_mut(*key)
                && let Some(arr) = v.as_array_mut()
            {
                return Ok(std::mem::take(arr));
            }
        }
    }
    Ok(vec![value])
}

/// Get string at dotted path in JSON (e.g. "next_cursor", "meta.next_page_token").
pub(crate) fn json_path_str(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut v = value;
    for segment in path.split('.') {
        v = v.get(segment)?;
    }
    v.as_str().map(|s| s.to_string())
}

/// Extract event ID from JSON using dotted path (e.g. "uuid", "id", "event.id").
pub(crate) fn event_id(event: &serde_json::Value, id_path: &str) -> Option<String> {
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
pub(crate) fn event_ts_with_field(
    event: &serde_json::Value,
    timestamp_field: Option<&str>,
) -> String {
    if let Some(path) = timestamp_field
        && let Some(s) = json_path_str(event, path)
        && !s.is_empty()
    {
        return s;
    }
    event_ts_fallback(event)
}

/// Build NDJSON envelope from raw event using source transform (timestamp_field, id_field) when set.
/// Takes ownership of event_value to avoid cloning (EmittedEvent::new already accepts owned Value).
pub(crate) fn build_emitted_event(
    source: &SourceConfig,
    source_id: &str,
    path: &str,
    event_value: serde_json::Value,
) -> EmittedEvent {
    let ts = event_ts_with_field(
        &event_value,
        source
            .transform
            .as_ref()
            .and_then(|t| t.timestamp_field.as_deref()),
    );
    let label = effective_source_label(source, source_id);
    let id = source
        .transform
        .as_ref()
        .and_then(|t| t.id_field.as_ref())
        .and_then(|id_path| event_id(&event_value, id_path));
    let mut emitted = EmittedEvent::new(ts, label, path.to_string(), event_value);
    if let Some(id) = id {
        emitted = emitted.with_id(id);
    }
    emitted
}
