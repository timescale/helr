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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emitted_event_new_and_to_ndjson_line() {
        let e = EmittedEvent::new(
            "2024-01-15T12:00:00Z".to_string(),
            "test-source".to_string(),
            "https://api.example.com/logs".to_string(),
            serde_json::json!({"id": 1, "msg": "hello"}),
        );
        let line = e.to_ndjson_line().unwrap();
        assert!(line.contains("\"ts\":\"2024-01-15T12:00:00Z\""));
        assert!(line.contains("\"source\":\"test-source\""));
        assert!(line.contains("\"event\":{\"id\":1,\"msg\":\"hello\"}"));
        assert!(line.contains("\"meta\":{}"));
    }

    #[test]
    fn emitted_event_with_cursor_and_request_id() {
        let e = EmittedEvent::new(
            "2024-01-15T12:00:00Z".to_string(),
            "s".to_string(),
            "https://x/".to_string(),
            serde_json::json!(null),
        )
        .with_cursor("next-page-token".to_string())
        .with_request_id("req-123".to_string());
        let line = e.to_ndjson_line().unwrap();
        assert!(line.contains("\"cursor\":\"next-page-token\""));
        assert!(line.contains("\"request_id\":\"req-123\""));
    }
}
