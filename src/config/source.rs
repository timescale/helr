use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{
    AuthConfig, HttpMethod, PaginationConfig, QueryParamValue, ResilienceConfig, SourceHooksConfig,
};

/// Per-source config (one entry under sources:).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub url: String,

    /// HTTP method: "get" (default) or "post". POST requires body for APIs like Cloud Logging entries.list.
    #[serde(default)]
    pub method: HttpMethod,

    /// Request body for POST (JSON object/array). Ignored for GET.
    #[serde(default)]
    pub body: Option<serde_json::Value>,

    /// Key for the producer label in emitted NDJSON (default from global.source_label_key). Use to align with downstream (e.g. "service", "origin").
    #[serde(default)]
    pub source_label_key: Option<String>,

    /// Value for the producer label in emitted NDJSON. Defaults to the source key (e.g. "okta-audit").
    #[serde(default)]
    pub source_label_value: Option<String>,

    #[serde(default)]
    pub schedule: ScheduleConfig,

    #[serde(default)]
    pub auth: Option<AuthConfig>,

    #[serde(default)]
    pub pagination: Option<PaginationConfig>,

    #[serde(default)]
    pub resilience: Option<ResilienceConfig>,

    #[serde(default)]
    pub headers: Option<HashMap<String, String>>,

    /// Optional safety limit: stop pagination when total response body bytes exceed this (per poll).
    #[serde(default)]
    pub max_bytes: Option<u64>,

    /// Optional deduplication: track last N event IDs and skip emitting duplicates.
    #[serde(default)]
    pub dedupe: Option<DedupeConfig>,

    /// Optional dotted JSON path to the array of events (e.g. "data.AndromedaEvents.edges"). When set, used instead of top-level "items"/"data"/"events"/"logs"/"entries".
    #[serde(default)]
    pub response_events_path: Option<String>,

    /// Optional dotted path within each array element to use as the event (e.g. "node" for GraphQL edges). When set, each emitted event is element[path]; otherwise the element itself.
    #[serde(default)]
    pub response_event_object_path: Option<String>,

    /// Optional transform: which raw-event fields map to envelope ts and meta.id.
    #[serde(default)]
    pub transform: Option<TransformConfig>,

    /// When cursor pagination gets 4xx (e.g. expired/invalid cursor): "reset" (clear cursor, next poll from start) or "fail".
    #[serde(default)]
    pub on_cursor_error: Option<CursorExpiredBehavior>,

    /// Start of range for first request (e.g. ISO timestamp). Sent as query param named by from_param.
    #[serde(default)]
    pub from: Option<String>,

    /// Query param name for from (e.g. "since", "after", "start_time"). Default "since" when from is set.
    #[serde(default)]
    pub from_param: Option<String>,

    /// Query params added only to the first request (when no saved cursor/next_url). Reusable across APIs (limit, until, filter, q, sortOrder, etc.). Values can be strings or numbers in YAML.
    #[serde(default)]
    pub query_params: Option<HashMap<String, QueryParamValue>>,

    /// Time-based incremental: read param from state on first request, store max event timestamp after each poll (e.g. Slack oldest from date_create).
    #[serde(default)]
    pub incremental_from: Option<IncrementalFromConfig>,

    /// Per-source state: watermark field/param for APIs that derive "start from" from last event (e.g. GWS startTime). Stored per source in the state store.
    #[serde(default)]
    pub state: Option<SourceStateConfig>,

    /// On response parse/event extraction error: "skip" (log and stop this poll) or "fail" (default).
    #[serde(default)]
    pub on_parse_error: Option<OnParseErrorBehavior>,

    /// Load-shedding priority (0–10, higher = higher priority). When load_shedding.skip_priority_below is set and under load, sources with priority below that threshold are not polled. Default 10 when unset.
    #[serde(default)]
    pub priority: Option<u32>,

    /// Optional max size in bytes for a single response body; if exceeded, poll fails.
    #[serde(default)]
    pub max_response_bytes: Option<u64>,

    /// When response body is not valid UTF-8: "replace" (U+FFFD), "escape", or "fail".
    #[serde(default)]
    pub on_invalid_utf8: Option<InvalidUtf8Behavior>,

    /// Optional max size in bytes for a single emitted NDJSON line; if exceeded, apply max_line_bytes_behavior.
    #[serde(default)]
    pub max_line_bytes: Option<u64>,

    /// When a single output line exceeds max_line_bytes: "truncate", "skip", or "fail".
    #[serde(default)]
    pub max_line_bytes_behavior: Option<MaxEventBytesBehavior>,

    /// When to checkpoint state: "end_of_tick" (only after full poll) or "per_page" (after each page).
    #[serde(default)]
    pub checkpoint: Option<CheckpointTiming>,

    /// When state store write fails (e.g. disk full): "fail" (default) or "skip_checkpoint" (log and continue).
    #[serde(default)]
    pub on_state_write_error: Option<OnStateWriteErrorBehavior>,

    /// Optional JS hooks script for this source (buildRequest, parseResponse, getNextPage, commitState). Requires global hooks.enabled.
    #[serde(default)]
    pub hooks: Option<SourceHooksConfig>,
}

/// Config for time-based incremental ingestion: use state for "from" param and store latest event timestamp after each poll.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementalFromConfig {
    /// State key to read for first-request param value and to write max timestamp after each poll.
    pub state_key: String,
    /// Dotted JSON path in each event for the timestamp (e.g. "date_create"). Max value (string) is stored after poll.
    pub event_timestamp_path: String,
    /// Query param name for the state value on first request (e.g. "oldest").
    pub param_name: String,
}

/// Per-source state: which event field to use as watermark and which API param receives it (e.g. GWS startTime).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceStateConfig {
    /// Dotted JSON path in each event for the watermark value (e.g. "id.time"). Max value (string) is stored after each poll.
    pub watermark_field: String,
    /// Query param name for the stored watermark on first request (e.g. "startTime").
    pub watermark_param: String,
    /// State key to read/write the watermark. Default "watermark" when omitted.
    #[serde(default)]
    pub state_key: Option<String>,
}

/// Behavior when state store write fails (e.g. disk full).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnStateWriteErrorBehavior {
    /// Return error and fail the tick.
    Fail,
    /// Log error and continue (checkpoint not persisted; next restart re-ingests from previous).
    SkipCheckpoint,
}

/// Behavior when response body contains invalid UTF-8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidUtf8Behavior {
    /// Replace invalid sequences with U+FFFD.
    Replace,
    /// Replace with U+FFFD and escape in JSON (same as replace for body).
    Escape,
    /// Fail the request.
    Fail,
}

/// Behavior when a single output line exceeds max_line_bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaxEventBytesBehavior {
    /// Truncate line and emit; increment metric.
    Truncate,
    /// Skip emitting this event; increment metric.
    Skip,
    /// Fail the poll.
    Fail,
}

/// When to persist state (cursor/next_url).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointTiming {
    /// Commit only after full poll tick (all pages). Fewer DB writes; on crash, re-ingest from previous tick.
    EndOfTick,
    /// Commit after each page. Fewer duplicates on crash.
    PerPage,
}

/// Behavior when cursor is expired (4xx from API).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorExpiredBehavior {
    /// Clear saved cursor; next poll starts from first page.
    Reset,
    /// Return error and do not clear cursor.
    Fail,
}

/// Behavior when parsing response or extracting events fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnParseErrorBehavior {
    /// Log warning and stop pagination for this tick (emit nothing for this response).
    Skip,
    /// Return error (default).
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DedupeConfig {
    /// JSON key or dotted path for record unique ID (e.g. "uuid", "id", "event.id"). Reusable across APIs.
    pub id_path: String,
    /// Max number of event IDs to keep (LRU eviction).
    #[serde(default = "default_dedupe_capacity")]
    pub capacity: u64,
}

fn default_dedupe_capacity() -> u64 {
    100_000
}

/// Per-source transform: which fields in the raw event map to envelope ts and id.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformConfig {
    /// Dotted path to the event timestamp (e.g. "published", "event.created_at"). Used for envelope `ts`. When unset, fallback: published, timestamp, ts, created_at, then now.
    #[serde(default)]
    pub timestamp_field: Option<String>,
    /// Dotted path to the event unique ID (e.g. "uuid", "id"). When set, value is included in envelope `meta.id`.
    #[serde(default)]
    pub id_field: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default)]
    pub jitter_secs: Option<u64>,
}

fn default_interval_secs() -> u64 {
    60
}
