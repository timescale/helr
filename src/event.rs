//! Canonical event type emitted to stdout as NDJSON.
//!
//! One JSON object per line: ts, source, event (raw payload), meta (cursor, request_id).

#![allow(dead_code)] // used when implementing poll loop

use serde::Serialize;

/// One log event emitted to stdout (NDJSON line).
#[derive(Debug, Clone, Serialize)]
pub struct EmittedEvent {
    pub ts: String,   // ISO8601
    pub source: String,
    pub endpoint: String,
    pub event: serde_json::Value,
    pub meta: EventMeta,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl EmittedEvent {
    pub fn new(ts: String, source: String, endpoint: String, event: serde_json::Value) -> Self {
        Self {
            ts,
            source,
            endpoint,
            event,
            meta: EventMeta {
                cursor: None,
                request_id: None,
            },
        }
    }

    pub fn with_cursor(mut self, cursor: String) -> Self {
        self.meta.cursor = Some(cursor);
        self
    }

    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.meta.request_id = Some(request_id);
        self
    }

    /// Serialize to one NDJSON line (no trailing newline; caller adds).
    pub fn to_ndjson_line(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(Into::into)
    }
}
