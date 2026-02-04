//! Configuration schema for Hel (v0.1).
//!
//! YAML config: sources, schedule, auth, pagination, resilience.
//! Env overrides: HEL_*.

#![allow(dead_code)] // fields used when implementing poll loop

use anyhow::Context;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Root config (hel.yaml).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub global: GlobalConfig,

    pub sources: HashMap<String, SourceConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    /// Log level (e.g. "info", "debug"). Env HEL_LOG_LEVEL overrides when set.
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Log format: "json" or "pretty". Env HEL_LOG_FORMAT or RUST_LOG_JSON=1 override.
    #[serde(default)]
    pub log_format: Option<String>,

    /// Key for the producer label in NDJSON events and Hel's JSON log lines (default "source"). Overridable per source with source_label_key.
    #[serde(default)]
    pub source_label_key: Option<String>,

    /// Value for the producer label in Hel's own JSON log lines (default "hel").
    #[serde(default)]
    pub source_label_value: Option<String>,

    #[serde(default)]
    pub state: Option<GlobalStateConfig>,

    #[serde(default)]
    pub health: Option<HealthConfig>,

    #[serde(default)]
    pub metrics: Option<MetricsConfig>,

    /// Backpressure: detection (queue depth, memory) and strategy when downstream can't keep up (block, disk_buffer, drop).
    #[serde(default)]
    pub backpressure: Option<BackpressureConfig>,

    /// Graceful degradation: state store fallback, emit without checkpoint, reduced poll frequency when degraded.
    #[serde(default)]
    pub degradation: Option<DegradationConfig>,

    /// SIGHUP behaviour: reload config; optionally restart source tasks (clear circuit breaker and OAuth2 token cache).
    #[serde(default)]
    pub reload: Option<ReloadConfig>,
}

/// Config reload on SIGHUP.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReloadConfig {
    /// When true, on SIGHUP also clear circuit breaker and OAuth2 token cache so sources re-establish on next tick.
    #[serde(default)]
    pub restart_sources_on_sighup: bool,
}

/// Graceful degradation when state store fails or is unavailable.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DegradationConfig {
    /// When primary state store (e.g. SQLite) fails to open or becomes unavailable, fall back to this backend (e.g. "memory").
    #[serde(default)]
    pub state_store_fallback: Option<String>,

    /// When true, on state store write failure skip checkpoint and continue emitting (prefer data over consistency). Can be overridden per-source by on_state_write_error.
    #[serde(default)]
    pub emit_without_checkpoint: Option<bool>,

    /// When degraded (e.g. using state_store_fallback), multiply poll interval by this factor (e.g. 2.0 = double the delay).
    #[serde(default = "default_reduced_frequency_multiplier")]
    pub reduced_frequency_multiplier: f64,
}

fn default_reduced_frequency_multiplier() -> f64 {
    2.0
}

/// Backpressure detection and strategy when the output queue is full or memory is high.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackpressureConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub detection: BackpressureDetectionConfig,

    /// Strategy when queue is full: block (pause poll until drain), disk_buffer (spill to disk), drop (drop with policy).
    #[serde(default = "default_backpressure_strategy")]
    pub strategy: BackpressureStrategyConfig,

    /// For disk_buffer: path and size limits.
    #[serde(default)]
    pub disk_buffer: Option<BackpressureDiskBufferConfig>,

    /// For drop strategy: which event to drop when full.
    #[serde(default = "default_drop_policy")]
    pub drop_policy: DropPolicyConfig,

    /// For drop strategy: max age (secs) a queued event may sit before we drop it (optional).
    #[serde(default)]
    pub max_queue_age_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureStrategyConfig {
    /// Pause poll ticks until stdout/queue drains (no data loss).
    #[default]
    Block,
    /// Spill to disk when queue full; drain from disk when consumer catches up.
    DiskBuffer,
    /// Drop events when full (policy: oldest_first, newest_first, random); track in metrics.
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DropPolicyConfig {
    /// When full, drop the oldest queued event and enqueue the new one.
    #[default]
    OldestFirst,
    /// When full, drop the incoming event (keep queue as-is).
    NewestFirst,
    /// When full, drop a random queued event and enqueue the new one.
    Random,
}

fn default_backpressure_strategy() -> BackpressureStrategyConfig {
    BackpressureStrategyConfig::Block
}

fn default_drop_policy() -> DropPolicyConfig {
    DropPolicyConfig::OldestFirst
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackpressureDetectionConfig {
    /// Stdout (or output) buffer size in bytes before we consider backpressure; used as optional byte cap for queue.
    #[serde(default = "default_stdout_buffer_size")]
    pub stdout_buffer_size: u64,

    /// Max number of events in the internal queue before applying strategy.
    #[serde(default = "default_event_queue_size")]
    pub event_queue_size: usize,

    /// RSS memory limit in MB; when exceeded we apply backpressure (optional; not implemented on all platforms).
    #[serde(default)]
    pub memory_threshold_mb: Option<u64>,
}

fn default_stdout_buffer_size() -> u64 {
    65536
}

fn default_event_queue_size() -> usize {
    10_000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackpressureDiskBufferConfig {
    pub path: String,
    #[serde(default = "default_disk_buffer_max_size_mb")]
    pub max_size_mb: u64,
    #[serde(default = "default_disk_buffer_segment_size_mb")]
    pub segment_size_mb: u64,
}

fn default_disk_buffer_max_size_mb() -> u64 {
    1024
}

fn default_disk_buffer_segment_size_mb() -> u64 {
    64
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_bearer_prefix() -> String {
    "Bearer".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalStateConfig {
    /// Backend: "memory", "sqlite", "redis", or "postgres".
    pub backend: String,
    /// Path to state file (SQLite) or directory. Required when backend is "sqlite".
    #[serde(default)]
    pub path: Option<String>,
    /// Connection URL for Redis (`redis://...`) or Postgres (`postgres://...`). Required when backend is "redis" or "postgres".
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_health_address")]
    pub address: String,
    #[serde(default = "default_health_port")]
    pub port: u16,
}

fn default_health_address() -> String {
    "0.0.0.0".to_string()
}
fn default_health_port() -> u16 {
    8080
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_address")]
    pub address: String,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

fn default_metrics_address() -> String {
    "0.0.0.0".to_string()
}
fn default_metrics_port() -> u16 {
    9090
}

/// Query param value: string or number in YAML (e.g. limit: 20 or limit: "20").
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum QueryParamValue {
    String(String),
    Int(i64),
}
impl QueryParamValue {
    pub fn to_param_value(&self) -> String {
        match self {
            QueryParamValue::String(s) => s.clone(),
            QueryParamValue::Int(n) => n.to_string(),
        }
    }
}

/// HTTP method for the source request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
}

/// Per-source config (one entry under sources:).
#[derive(Debug, Clone, Deserialize)]
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
}

/// Config for time-based incremental ingestion: use state for "from" param and store latest event timestamp after each poll.
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnStateWriteErrorBehavior {
    /// Return error and fail the tick.
    Fail,
    /// Log error and continue (checkpoint not persisted; next restart re-ingests from previous).
    SkipCheckpoint,
}

/// Behavior when response body contains invalid UTF-8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointTiming {
    /// Commit only after full poll tick (all pages). Fewer DB writes; on crash, re-ingest from previous tick.
    EndOfTick,
    /// Commit after each page. Fewer duplicates on crash.
    PerPage,
}

/// Behavior when cursor is expired (4xx from API).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorExpiredBehavior {
    /// Clear saved cursor; next poll starts from first page.
    Reset,
    /// Return error and do not clear cursor.
    Fail,
}

/// Behavior when parsing response or extracting events fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnParseErrorBehavior {
    /// Log warning and stop pagination for this tick (emit nothing for this response).
    Skip,
    /// Return error (default).
    Fail,
}

#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformConfig {
    /// Dotted path to the event timestamp (e.g. "published", "event.created_at"). Used for envelope `ts`. When unset, fallback: published, timestamp, ts, created_at, then now.
    #[serde(default)]
    pub timestamp_field: Option<String>,
    /// Dotted path to the event unique ID (e.g. "uuid", "id"). When set, value is included in envelope `meta.id`.
    #[serde(default)]
    pub id_field: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum AuthConfig {
    Bearer {
        token_env: String,
        #[serde(default)]
        token_file: Option<String>,
        /// Authorization header prefix (default "Bearer"). Use "SSWS" for Okta API tokens.
        #[serde(default = "default_bearer_prefix")]
        prefix: String,
    },
    ApiKey {
        header: String,
        key_env: String,
        #[serde(default)]
        key_file: Option<String>,
    },
    Basic {
        user_env: String,
        #[serde(default)]
        user_file: Option<String>,
        password_env: String,
        #[serde(default)]
        password_file: Option<String>,
    },
    #[serde(rename = "oauth2")]
    OAuth2 {
        token_url: String,
        client_id_env: String,
        #[serde(default)]
        client_id_file: Option<String>,
        #[serde(default)]
        client_secret_env: Option<String>,
        #[serde(default)]
        client_secret_file: Option<String>,
        /// When set, use private_key_jwt for token endpoint client auth (e.g. Okta Org AS). PEM from env or file.
        #[serde(default)]
        client_private_key_env: Option<String>,
        #[serde(default)]
        client_private_key_file: Option<String>,
        /// When set, use refresh_token grant; when omitted, use client_credentials grant (any provider).
        #[serde(default)]
        refresh_token_env: Option<String>,
        #[serde(default)]
        refresh_token_file: Option<String>,
        #[serde(default)]
        scopes: Option<Vec<String>>,
        /// When true, send DPoP (Demonstrating Proof-of-Possession) header on token and API requests (e.g. Okta).
        #[serde(default)]
        dpop: bool,
    },
    /// Google Service Account (JWT bearer grant). For GWS Admin SDK use domain-wide delegation: set subject to admin user email.
    #[serde(rename = "google_service_account")]
    GoogleServiceAccount {
        /// Path to service account JSON key file (or use credentials_env).
        #[serde(default)]
        credentials_file: Option<String>,
        /// Env var containing full service account JSON string (or use credentials_file).
        #[serde(default)]
        credentials_env: Option<String>,
        /// Env var for delegated user email (domain-wide delegation). Required for Admin SDK Reports API.
        #[serde(default)]
        subject_env: Option<String>,
        /// File containing delegated user email (domain-wide delegation).
        #[serde(default)]
        subject_file: Option<String>,
        scopes: Vec<String>,
    },
}

/// Resolve a secret from file path (if set) or environment variable. File takes precedence.
pub fn read_secret(file_path: Option<&str>, env_var: &str) -> anyhow::Result<String> {
    if let Some(p) = file_path
        && !p.is_empty() {
            let s = std::fs::read_to_string(Path::new(p))
                .with_context(|| format!("read secret file {:?}", p))?;
            return Ok(s.trim().to_string());
        }
    std::env::var(env_var).with_context(|| format!("env {} not set", env_var))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum PaginationConfig {
    LinkHeader {
        #[serde(default = "default_rel")]
        rel: String,
        #[serde(default)]
        max_pages: Option<u32>,
    },
    Cursor {
        cursor_param: String,
        cursor_path: String, // JSONPath or simple key
        #[serde(default)]
        max_pages: Option<u32>,
    },
    PageOffset {
        page_param: String,
        limit_param: String,
        limit: u32,
        #[serde(default)]
        max_pages: Option<u32>,
    },
}

fn default_rel() -> String {
    "next".to_string()
}

/// Split timeouts: connect, request, read, idle (client), poll_tick (entire poll cycle).
/// When set, these override or supplement the legacy `timeout_secs` (used for request when timeouts.request is unset).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeoutsConfig {
    /// TCP connection establishment (seconds).
    #[serde(default)]
    pub connect_secs: Option<u64>,
    /// Entire request/response cycle per request (seconds).
    #[serde(default)]
    pub request_secs: Option<u64>,
    /// Reading response body (seconds). Should be ≤ request when both set.
    #[serde(default)]
    pub read_secs: Option<u64>,
    /// Idle connection in pool (seconds).
    #[serde(default)]
    pub idle_secs: Option<u64>,
    /// Entire poll cycle (all pages) per source (seconds).
    #[serde(default)]
    pub poll_tick_secs: Option<u64>,
}

/// TLS options: custom CA, client cert/key, min TLS version. Applied to the reqwest client for this source.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Path to PEM file (single cert or bundle) for custom CA. Merged with system roots unless `ca_only` is true.
    #[serde(default)]
    pub ca_file: Option<String>,
    /// Env var containing PEM (single cert or bundle) for custom CA. Used when `ca_file` is unset.
    #[serde(default)]
    pub ca_env: Option<String>,
    /// When true and custom CA is set, use only the provided CA(s); otherwise merge with system roots.
    #[serde(default)]
    pub ca_only: bool,
    /// Path to client certificate PEM (for mutual TLS).
    #[serde(default)]
    pub client_cert_file: Option<String>,
    /// Env var containing client certificate PEM. Used when `client_cert_file` is unset.
    #[serde(default)]
    pub client_cert_env: Option<String>,
    /// Path to client private key PEM (required when client cert is set).
    #[serde(default)]
    pub client_key_file: Option<String>,
    /// Env var containing client private key PEM. Used when `client_key_file` is unset.
    #[serde(default)]
    pub client_key_env: Option<String>,
    /// Minimum TLS version: "1.2" or "1.3".
    #[serde(default)]
    pub min_version: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResilienceConfig {
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Split timeouts: connect, request, read, idle, poll_tick. When set, overrides/supplements timeout_secs for client.
    #[serde(default)]
    pub timeouts: Option<TimeoutsConfig>,
    #[serde(default)]
    pub retries: Option<RetryConfig>,
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

/// Header names for rate limit info (limit, remaining, reset). When unset, defaults are used.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitHeaderMapping {
    /// Header for rate limit ceiling (e.g. "X-RateLimit-Limit").
    #[serde(default)]
    pub limit_header: Option<String>,
    /// Header for remaining requests in window (e.g. "X-RateLimit-Remaining").
    #[serde(default)]
    pub remaining_header: Option<String>,
    /// Header for window reset time, Unix timestamp (e.g. "X-RateLimit-Reset").
    #[serde(default)]
    pub reset_header: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// When true, use Retry-After or reset header from response on 429 instead of generic backoff.
    #[serde(default = "default_respect_headers")]
    pub respect_headers: bool,

    /// Optional delay in seconds between pagination requests. Reduces burst; reusable across APIs.
    #[serde(default)]
    pub page_delay_secs: Option<u64>,

    /// Header names for limit/remaining/reset. When unset, defaults: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset.
    #[serde(default)]
    pub headers: Option<RateLimitHeaderMapping>,

    /// Client-side max requests per second (RPS). When set, requests are throttled to this rate before sending.
    #[serde(default)]
    pub max_requests_per_second: Option<f64>,

    /// Client-side burst size (max requests allowed in a burst). When unset with max_requests_per_second, defaults to ceil(rps).
    #[serde(default)]
    pub burst_size: Option<u32>,

    /// When true, use remaining/reset from response headers to throttle: if remaining is 0 or low, wait until reset before next request.
    #[serde(default)]
    pub adaptive: Option<bool>,
}

fn default_respect_headers() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_cb_enabled")]
    pub enabled: bool,
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_success_threshold")]
    pub success_threshold: u32,
    #[serde(default = "default_half_open_timeout_secs")]
    pub half_open_timeout_secs: u64,
    /// Max time in open state (seconds). Open duration is min(half_open_timeout_secs, reset_timeout_secs).
    #[serde(default)]
    pub reset_timeout_secs: Option<u64>,
    /// Optional: open when failure rate >= this (0.0–1.0). Requires minimum_requests.
    #[serde(default)]
    pub failure_rate_threshold: Option<f64>,
    /// Minimum requests before evaluating failure_rate_threshold.
    #[serde(default)]
    pub minimum_requests: Option<u32>,
}

fn default_cb_enabled() -> bool {
    true
}
fn default_failure_threshold() -> u32 {
    5
}
fn default_success_threshold() -> u32 {
    2
}
fn default_half_open_timeout_secs() -> u64 {
    60
}

fn default_timeout_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_initial_backoff_secs")]
    pub initial_backoff_secs: u64,
    #[serde(default)]
    pub max_backoff_secs: Option<u64>,
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    /// Backoff jitter: multiply delay by (1 + random(-jitter, +jitter)). e.g. 0.1 = ±10%. When unset, no jitter.
    #[serde(default)]
    pub jitter: Option<f64>,
    /// HTTP status codes to retry. When unset, default: 408, 429, 5xx.
    #[serde(default)]
    pub retryable_status_codes: Option<Vec<u16>>,
}

fn default_max_attempts() -> u32 {
    3
}
fn default_initial_backoff_secs() -> u64 {
    1
}
fn default_multiplier() -> f64 {
    2.0
}

impl Config {
    /// Load and parse config from path. Expands env vars; fails if any placeholder is unset (no default).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {:?}: {}", path, e))?;
        let expanded = expand_env_vars_strict(&s)?;
        let config: Config =
            serde_yaml::from_str(&expanded).map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        if config.sources.is_empty() {
            anyhow::bail!("config must have at least one source");
        }
        validate_auth_secrets(&config)?;
        validate_tls(&config)?;
        Ok(config)
    }
}

/// Validate TLS config: client cert and key both set when either is set; resolve CA/cert/key so startup fails if missing.
pub fn validate_tls(config: &Config) -> anyhow::Result<()> {
    for (source_id, source) in &config.sources {
        let Some(tls) = source.resilience.as_ref().and_then(|r| r.tls.as_ref()) else {
            continue;
        };
        let has_cert = tls
            .client_cert_file
            .as_deref()
            .is_some_and(|p| !p.is_empty())
            || tls
                .client_cert_env
                .as_deref()
                .is_some_and(|e| !e.is_empty());
        let has_key = tls
            .client_key_file
            .as_deref()
            .is_some_and(|p| !p.is_empty())
            || tls
                .client_key_env
                .as_deref()
                .is_some_and(|e| !e.is_empty());
        if has_cert != has_key {
            anyhow::bail!(
                "source {}: tls client_cert and client_key must both be set (cert_file/cert_env and key_file/key_env)",
                source_id
            );
        }
        if has_cert {
            read_secret(
                tls.client_cert_file.as_deref(),
                tls.client_cert_env.as_deref().unwrap_or(""),
            )
            .with_context(|| format!("source {}: tls client_cert", source_id))?;
            read_secret(
                tls.client_key_file.as_deref(),
                tls.client_key_env.as_deref().unwrap_or(""),
            )
            .with_context(|| format!("source {}: tls client_key", source_id))?;
        }
        let has_ca = tls.ca_file.as_deref().is_some_and(|p| !p.is_empty())
            || tls.ca_env.as_deref().is_some_and(|e| !e.is_empty());
        if has_ca {
            read_secret(tls.ca_file.as_deref(), tls.ca_env.as_deref().unwrap_or(""))
                .with_context(|| format!("source {}: tls ca", source_id))?;
        }
        if let Some(v) = tls.min_version.as_deref()
            && v != "1.2" && v != "1.3" {
                anyhow::bail!(
                    "source {}: tls min_version must be \"1.2\" or \"1.3\", got {:?}",
                    source_id,
                    v
                );
            }
    }
    Ok(())
}

/// Validate that all auth secrets (env or file) can be resolved. Fail at startup so health reflects "not ready".
pub fn validate_auth_secrets(config: &Config) -> anyhow::Result<()> {
    for (source_id, source) in &config.sources {
        if let Some(auth) = &source.auth {
            match auth {
                AuthConfig::Bearer {
                    token_env,
                    token_file,
                    prefix: _,
                } => {
                    read_secret(token_file.as_deref(), token_env)
                        .with_context(|| format!("source {}: bearer token", source_id))?;
                }
                AuthConfig::ApiKey {
                    key_env, key_file, ..
                } => {
                    read_secret(key_file.as_deref(), key_env)
                        .with_context(|| format!("source {}: api key", source_id))?;
                }
                AuthConfig::Basic {
                    user_env,
                    user_file,
                    password_env,
                    password_file,
                } => {
                    read_secret(user_file.as_deref(), user_env)
                        .with_context(|| format!("source {}: basic user", source_id))?;
                    read_secret(password_file.as_deref(), password_env)
                        .with_context(|| format!("source {}: basic password", source_id))?;
                }
                AuthConfig::OAuth2 {
                    client_id_env,
                    client_id_file,
                    client_secret_env,
                    client_secret_file,
                    client_private_key_env,
                    client_private_key_file,
                    refresh_token_env,
                    refresh_token_file,
                    ..
                } => {
                    read_secret(client_id_file.as_deref(), client_id_env)
                        .with_context(|| format!("source {}: oauth2 client_id", source_id))?;
                    let has_private_key = client_private_key_env
                        .as_deref()
                        .is_some_and(|e| !e.is_empty())
                        || client_private_key_file
                            .as_deref()
                            .is_some_and(|p| !p.is_empty());
                    let has_secret = client_secret_env
                        .as_deref()
                        .is_some_and(|e| !e.is_empty())
                        || client_secret_file
                            .as_deref()
                            .is_some_and(|p| !p.is_empty());
                    if !has_private_key && !has_secret {
                        anyhow::bail!(
                            "source {}: oauth2 requires client_secret (client_secret_env/client_secret_file) \
                             or client_private_key (client_private_key_env/client_private_key_file) for token endpoint auth",
                            source_id
                        );
                    }
                    if has_private_key {
                        read_secret(
                            client_private_key_file.as_deref(),
                            client_private_key_env.as_deref().unwrap_or(""),
                        )
                        .with_context(|| {
                            format!("source {}: oauth2 client_private_key (PEM)", source_id)
                        })?;
                    } else {
                        read_secret(
                            client_secret_file.as_deref(),
                            client_secret_env.as_deref().unwrap_or(""),
                        )
                        .with_context(|| format!("source {}: oauth2 client_secret", source_id))?;
                    }
                    let has_refresh = refresh_token_env
                        .as_deref()
                        .is_some_and(|e| !e.is_empty())
                        || refresh_token_file
                            .as_deref()
                            .is_some_and(|p| !p.is_empty());
                    if has_refresh {
                        let rt_env = refresh_token_env.as_deref().unwrap_or("");
                        read_secret(refresh_token_file.as_deref(), rt_env).with_context(|| {
                            format!("source {}: oauth2 refresh_token", source_id)
                        })?;
                    }
                }
                AuthConfig::GoogleServiceAccount {
                    credentials_file,
                    credentials_env,
                    subject_env,
                    subject_file,
                    scopes: _,
                } => {
                    let json_str = if let Some(path) = credentials_file.as_deref() {
                        if path.is_empty() {
                            None
                        } else {
                            Some(std::fs::read_to_string(Path::new(path)).with_context(|| {
                                format!(
                                    "source {}: google_service_account credentials_file",
                                    source_id
                                )
                            })?)
                        }
                    } else {
                        None
                    };
                    let json_str = json_str.or_else(|| {
                        credentials_env
                            .as_deref()
                            .and_then(|e| std::env::var(e).ok())
                    });
                    let json_str = json_str
                        .with_context(|| format!("source {}: google_service_account credentials (set credentials_file or credentials_env)", source_id))?;
                    let creds: serde_json::Value =
                        serde_json::from_str(&json_str).with_context(|| {
                            format!(
                                "source {}: google_service_account credentials JSON",
                                source_id
                            )
                        })?;
                    creds.get("client_email").with_context(|| {
                        format!("source {}: credentials missing client_email", source_id)
                    })?;
                    creds.get("private_key").with_context(|| {
                        format!("source {}: credentials missing private_key", source_id)
                    })?;
                    if let Some(env) = subject_env.as_deref() {
                        read_secret(subject_file.as_deref(), env)
                            .with_context(|| format!("source {}: google_service_account subject (domain-wide delegation)", source_id))?;
                    } else if let Some(path) = subject_file.as_deref()
                        && !path.is_empty() {
                            std::fs::read_to_string(Path::new(path)).with_context(|| {
                                format!("source {}: google_service_account subject file", source_id)
                            })?;
                        }
                }
            }
        }
    }
    Ok(())
}

/// Expand env vars in config: `$VAR`, `${VAR}`, `${VAR:-default}`. Fails if any var is unset (no default).
/// Lines that are comments (after trim, empty or starting with #) are not expanded, so placeholders in comments are left as-is.
fn expand_env_vars_strict(s: &str) -> anyhow::Result<String> {
    let mut out = String::with_capacity(s.len());
    for line in s.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            out.push_str(line);
        } else {
            let expanded = shellexpand::env_with_context(line, |var: &str| {
                std::env::var(var)
                    .map(|v| Ok(Some(std::borrow::Cow::Owned(v))))
                    .unwrap_or_else(Err)
            })
            .map(|cow| cow.into_owned())
            .map_err(|e| {
                anyhow::anyhow!(
                    "config placeholder ${{{}}} is unset: {}",
                    e.var_name,
                    e.cause
                )
            })?;
            out.push_str(&expanded);
        }
        out.push('\n');
    }
    if s.ends_with('\n') {
        // lines() strips a trailing newline; preserve it if original had it
    } else if !s.is_empty() {
        out.pop(); // remove the extra \n we added for the last "line"
    }
    Ok(out)
}

/// Expand env vars; unset vars expand to empty. Used in tests.
pub(crate) fn expand_env_vars(s: &str) -> anyhow::Result<String> {
    fn context(var: &str) -> Result<Option<std::borrow::Cow<'static, str>>, std::env::VarError> {
        match std::env::var(var) {
            Ok(v) => Ok(Some(v.into())),
            Err(std::env::VarError::NotPresent) => Ok(Some("".into())),
            Err(e) => Err(e),
        }
    }
    shellexpand::env_with_context(s, context)
        .map(|cow| cow.into_owned())
        .map_err(|e| anyhow::anyhow!("config env expansion: {} ({})", e.var_name, e.cause))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn expand_env_vars_simple() {
        unsafe {
            std::env::set_var("HEL_TEST_EXPAND_A", "foo");
        }
        let s = "prefix_${HEL_TEST_EXPAND_A}_suffix";
        let out = expand_env_vars(s).unwrap();
        assert_eq!(out, "prefix_foo_suffix");
        unsafe {
            std::env::remove_var("HEL_TEST_EXPAND_A");
        }
    }

    #[test]
    fn expand_env_vars_unset_expands_empty() {
        unsafe {
            std::env::remove_var("HEL_TEST_UNSET_VAR_XYZ");
        }
        let s = "a${HEL_TEST_UNSET_VAR_XYZ}b";
        let out = expand_env_vars(s).unwrap();
        assert_eq!(out, "ab");
    }

    #[test]
    fn read_secret_from_env() {
        unsafe {
            std::env::set_var("HEL_TEST_SECRET_ENV", "secret-from-env");
        }
        let out = read_secret(None, "HEL_TEST_SECRET_ENV").unwrap();
        assert_eq!(out, "secret-from-env");
        unsafe {
            std::env::remove_var("HEL_TEST_SECRET_ENV");
        }
    }

    #[test]
    fn read_secret_file_overrides_env() {
        let dir = std::env::temp_dir().join("hel_config_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("token.txt");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"  secret-from-file  \n")
            .unwrap();
        unsafe {
            std::env::set_var("HEL_TEST_SECRET_BOTH", "secret-from-env");
        }
        let out = read_secret(Some(path.to_str().unwrap()), "HEL_TEST_SECRET_BOTH").unwrap();
        assert_eq!(out, "secret-from-file");
        unsafe {
            std::env::remove_var("HEL_TEST_SECRET_BOTH");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_minimal_valid() {
        let dir = std::env::temp_dir().join("hel_config_load_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global:
  log_level: info
sources:
  test-source:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.global.log_level, "info");
        assert!(config.sources.contains_key("test-source"));
        assert_eq!(
            config.sources["test-source"].url,
            "https://example.com/logs"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_backpressure() {
        let dir = std::env::temp_dir().join("hel_config_load_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global:
  log_level: info
  backpressure:
    enabled: true
    detection:
      event_queue_size: 5000
      memory_threshold_mb: 256
    strategy: drop
    drop_policy: oldest_first
sources:
  test-source:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let bp = config.global.backpressure.as_ref().unwrap();
        assert!(bp.enabled);
        assert_eq!(bp.detection.event_queue_size, 5000);
        assert_eq!(bp.detection.memory_threshold_mb, Some(256));
        assert_eq!(bp.strategy, BackpressureStrategyConfig::Drop);
        assert_eq!(bp.drop_policy, DropPolicyConfig::OldestFirst);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_degradation() {
        let dir = std::env::temp_dir().join("hel_config_degradation");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global:
  degradation:
    state_store_fallback: memory
    emit_without_checkpoint: true
    reduced_frequency_multiplier: 2.5
sources:
  x:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let d = config.global.degradation.as_ref().unwrap();
        assert_eq!(d.state_store_fallback.as_deref(), Some("memory"));
        assert_eq!(d.emit_without_checkpoint, Some(true));
        assert_eq!(d.reduced_frequency_multiplier, 2.5);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_reload() {
        let dir = std::env::temp_dir().join("hel_config_reload");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global:
  reload:
    restart_sources_on_sighup: true
sources:
  s1:
    url: "https://example.com/"
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let r = config.global.reload.as_ref().unwrap();
        assert!(r.restart_sources_on_sighup);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_rate_limit() {
        let dir = std::env::temp_dir().join("hel_config_rate_limit");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global: {}
sources:
  x:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      rate_limit:
        respect_headers: true
        page_delay_secs: 1
        headers:
          limit_header: "X-RateLimit-Limit"
          remaining_header: "X-RateLimit-Remaining"
          reset_header: "X-Rate-Limit-Reset"
        max_requests_per_second: 10.0
        burst_size: 5
        adaptive: true
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let rl = config.sources["x"]
            .resilience
            .as_ref()
            .and_then(|r| r.rate_limit.as_ref())
            .unwrap();
        assert!(rl.respect_headers);
        assert_eq!(rl.page_delay_secs, Some(1));
        let h = rl.headers.as_ref().unwrap();
        assert_eq!(h.limit_header.as_deref(), Some("X-RateLimit-Limit"));
        assert_eq!(h.remaining_header.as_deref(), Some("X-RateLimit-Remaining"));
        assert_eq!(h.reset_header.as_deref(), Some("X-Rate-Limit-Reset"));
        assert_eq!(rl.max_requests_per_second, Some(10.0));
        assert_eq!(rl.burst_size, Some(5));
        assert_eq!(rl.adaptive, Some(true));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_expands_env() {
        unsafe {
            std::env::set_var("HEL_TEST_BASE_URL", "https://api.test.com");
        }
        let dir = std::env::temp_dir().join("hel_config_expand_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global: {}
sources:
  x:
    url: "${HEL_TEST_BASE_URL}/logs"
    pagination:
      strategy: link_header
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.sources["x"].url, "https://api.test.com/logs");
        unsafe {
            std::env::remove_var("HEL_TEST_BASE_URL");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_empty_sources_fails() {
        let dir = std::env::temp_dir().join("hel_config_empty_sources");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources: {}
"#,
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("at least one source"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_invalid_yaml_fails() {
        let dir = std::env::temp_dir().join("hel_config_invalid_yaml");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(&path, "global:\n  log_level: [unclosed").unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("parse") || err.to_string().contains("yaml"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_corner_case_options() {
        let dir = std::env::temp_dir().join("hel_config_corner_case");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global:
  log_level: info
  state:
    backend: memory
sources:
  corner:
    url: "https://example.com/logs"
    on_cursor_error: reset
    from: "2024-01-01T00:00:00Z"
    from_param: after
    on_parse_error: skip
    max_response_bytes: 5242880
    on_state_write_error: skip_checkpoint
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let s = &config.sources["corner"];
        assert_eq!(s.on_cursor_error, Some(CursorExpiredBehavior::Reset));
        assert_eq!(s.from.as_deref(), Some("2024-01-01T00:00:00Z"));
        assert_eq!(s.from_param.as_deref(), Some("after"));
        assert!(s.query_params.is_none());
        assert_eq!(s.on_parse_error, Some(OnParseErrorBehavior::Skip));
        assert_eq!(s.max_response_bytes, Some(5_242_880));
        assert_eq!(
            s.on_state_write_error,
            Some(OnStateWriteErrorBehavior::SkipCheckpoint)
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_unset_placeholder_fails() {
        let dir = std::env::temp_dir().join("hel_config_unset_placeholder");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://${HEL_UNSET_PLACEHOLDER_TEST}/logs"
    pagination:
      strategy: link_header
"#,
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("unset")
                || err.to_string().contains("HEL_UNSET_PLACEHOLDER_TEST"),
            "expected unset placeholder error, got: {}",
            err
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_comment_lines_not_expanded() {
        // Placeholders in comment lines must not be expanded (so unset vars in comments don't fail load).
        let dir = std::env::temp_dir().join("hel_config_comment_no_expand");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  # use enterprise URL: https://${GITHUB_API_HOST}/enterprises/${GITHUB_ENTERPRISE}/audit-log
  x:
    url: "https://${HEL_TEST_COMMENT_EXPAND_BASE}/logs"
    pagination:
      strategy: link_header
"#,
        )
        .unwrap();
        unsafe {
            std::env::set_var("HEL_TEST_COMMENT_EXPAND_BASE", "api.example.com");
        }
        let result = Config::load(&path);
        unsafe {
            std::env::remove_var("HEL_TEST_COMMENT_EXPAND_BASE");
        }
        let config =
            result.expect("load should succeed; comment placeholders must not be expanded");
        assert_eq!(config.sources["x"].url, "https://api.example.com/logs");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_initial_query_params_string_and_number() {
        let dir = std::env::temp_dir().join("hel_config_initial_query_params");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  okta:
    url: "https://example.okta.com/api/v1/logs"
    pagination:
      strategy: link_header
      rel: next
    query_params:
      limit: 20
      sortOrder: "ASCENDING"
      filter: "eventType eq \"user.session.start\""
"#,
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        let s = &config.sources["okta"];
        let params = s.query_params.as_ref().unwrap();
        assert_eq!(
            params.get("limit").map(|v| v.to_param_value()),
            Some("20".to_string())
        );
        assert_eq!(
            params.get("sortOrder").map(|v| v.to_param_value()),
            Some("ASCENDING".to_string())
        );
        assert_eq!(
            params.get("filter").map(|v| v.to_param_value()),
            Some("eventType eq \"user.session.start\"".to_string())
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_split_timeouts() {
        let dir = std::env::temp_dir().join("hel_config_split_timeouts");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 30
      timeouts:
        connect_secs: 10
        request_secs: 25
        read_secs: 20
        idle_secs: 90
        poll_tick_secs: 300
"#,
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        let s = &config.sources["x"];
        let t = s
            .resilience
            .as_ref()
            .and_then(|r| r.timeouts.as_ref())
            .unwrap();
        assert_eq!(t.connect_secs, Some(10));
        assert_eq!(t.request_secs, Some(25));
        assert_eq!(t.read_secs, Some(20));
        assert_eq!(t.idle_secs, Some(90));
        assert_eq!(t.poll_tick_secs, Some(300));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_retry_jitter_and_retryable_codes() {
        let dir = std::env::temp_dir().join("hel_config_retry_jitter");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      retries:
        max_attempts: 5
        jitter: 0.1
        retryable_status_codes:
          - 408
          - 429
          - 500
          - 502
          - 503
          - 504
"#,
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        let s = &config.sources["x"];
        let r = s
            .resilience
            .as_ref()
            .and_then(|res| res.retries.as_ref())
            .unwrap();
        assert_eq!(r.max_attempts, 5);
        assert_eq!(r.jitter, Some(0.1));
        assert_eq!(
            r.retryable_status_codes.as_deref(),
            Some([408, 429, 500, 502, 503, 504].as_slice())
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_transform() {
        let dir = std::env::temp_dir().join("hel_config_transform");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
    transform:
      timestamp_field: "published"
      id_field: "uuid"
"#,
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        let s = &config.sources["x"];
        let t = s.transform.as_ref().unwrap();
        assert_eq!(t.timestamp_field.as_deref(), Some("published"));
        assert_eq!(t.id_field.as_deref(), Some("uuid"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_tls() {
        let dir = std::env::temp_dir().join("hel_config_tls");
        let _ = std::fs::create_dir_all(&dir);
        let ca_path = dir.join("ca.pem");
        let cert_path = dir.join("client.pem");
        let key_path = dir.join("client-key.pem");
        std::fs::write(
            &ca_path,
            "-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        std::fs::write(
            &cert_path,
            "-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        std::fs::write(
            &key_path,
            "-----BEGIN PRIVATE KEY-----\nMOCK\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        let path = dir.join("hel.yaml");
        let yaml = format!(
            r#"
global: {{}}
sources:
  tls-source:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      timeout_secs: 30
      tls:
        ca_file: "{}"
        ca_only: true
        client_cert_file: "{}"
        client_key_file: "{}"
        min_version: "1.2"
"#,
            ca_path.to_str().unwrap(),
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap()
        );
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let s = &config.sources["tls-source"];
        let t = s.resilience.as_ref().and_then(|r| r.tls.as_ref()).unwrap();
        assert_eq!(t.ca_file.as_deref(), Some(ca_path.to_str().unwrap()));
        assert!(t.ca_only);
        assert_eq!(
            t.client_cert_file.as_deref(),
            Some(cert_path.to_str().unwrap())
        );
        assert_eq!(
            t.client_key_file.as_deref(),
            Some(key_path.to_str().unwrap())
        );
        assert_eq!(t.min_version.as_deref(), Some("1.2"));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&ca_path);
        let _ = std::fs::remove_file(&cert_path);
        let _ = std::fs::remove_file(&key_path);
    }

    #[test]
    fn config_load_tls_client_cert_without_key_fails() {
        let dir = std::env::temp_dir().join("hel_config_tls_validate");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
    resilience:
      tls:
        client_cert_file: "/tmp/cert.pem"
"#,
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("client_cert") && err.to_string().contains("client_key"),
            "expected client_cert/client_key validation error, got: {}",
            err
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_tls_min_version_invalid_fails() {
        let dir = std::env::temp_dir().join("hel_config_tls_minver");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://example.com/logs"
    pagination:
      strategy: link_header
    resilience:
      tls:
        min_version: "1.1"
"#,
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("min_version") || err.to_string().contains("1.2"),
            "expected min_version validation error, got: {}",
            err
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_unknown_top_level_field_fails() {
        let dir = std::env::temp_dir().join("hel_config_unknown_field");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://example.com/"
    pagination:
      strategy: link_header
unknown_field: 1
"#,
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("unknown") || err.to_string().contains("parse"),
            "expected unknown field error, got: {}",
            err
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_source_state_watermark() {
        let yaml = r#"
url: "https://admin.googleapis.com/admin/reports/v1/activity/users/all/applications/login"
pagination:
  strategy: cursor
  cursor_param: pageToken
  cursor_path: nextPageToken
state:
  watermark_field: "id.time"
  watermark_param: "startTime"
query_params:
  maxResults: "500"
"#;
        let source: SourceConfig = serde_yaml::from_str(yaml).unwrap();
        let st = source.state.as_ref().expect("state block");
        assert_eq!(st.watermark_field, "id.time");
        assert_eq!(st.watermark_param, "startTime");
        assert_eq!(st.state_key.as_deref(), None); // default when omitted

        let yaml_with_key = r#"
url: "https://example.com/logs"
state:
  watermark_field: "timestamp"
  watermark_param: "startTime"
  state_key: "gws_watermark"
"#;
        let source2: SourceConfig = serde_yaml::from_str(yaml_with_key).unwrap();
        let st2 = source2.state.as_ref().unwrap();
        assert_eq!(st2.state_key.as_deref(), Some("gws_watermark"));
    }

    #[test]
    fn config_load_global_state_redis_and_postgres() {
        let yaml_redis = r#"
global:
  state:
    backend: redis
    url: "redis://127.0.0.1:6379/"
sources:
  x:
    url: "https://example.com/"
    pagination:
      strategy: link_header
"#;
        let config: Config = serde_yaml::from_str(yaml_redis).unwrap();
        let st = config.global.state.as_ref().unwrap();
        assert_eq!(st.backend, "redis");
        assert_eq!(st.url.as_deref(), Some("redis://127.0.0.1:6379/"));

        let yaml_pg = r#"
global:
  state:
    backend: postgres
    url: "postgres://user:pass@localhost/hel"
sources:
  x:
    url: "https://example.com/"
    pagination:
      strategy: link_header
"#;
        let config_pg: Config = serde_yaml::from_str(yaml_pg).unwrap();
        let st_pg = config_pg.global.state.as_ref().unwrap();
        assert_eq!(st_pg.backend, "postgres");
        assert_eq!(
            st_pg.url.as_deref(),
            Some("postgres://user:pass@localhost/hel")
        );
    }
}
