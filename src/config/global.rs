use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    /// Log level (e.g. "info", "debug"). Env HELR_LOG_LEVEL overrides when set.
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Log format: "json" or "pretty". Env HELR_LOG_FORMAT or RUST_LOG_JSON=1 override.
    #[serde(default)]
    pub log_format: Option<String>,

    /// Key for the producer label in NDJSON events and Helr's JSON log lines (default "source"). Overridable per source with source_label_key.
    #[serde(default)]
    pub source_label_key: Option<String>,

    /// Value for the producer label in Helr's own JSON log lines (default "helr").
    #[serde(default)]
    pub source_label_value: Option<String>,

    #[serde(default)]
    pub state: Option<GlobalStateConfig>,

    /// API and health server: REST API (/api/v1/*) plus health endpoints (/healthz, /readyz, /startupz).
    #[serde(default)]
    pub api: Option<ApiConfig>,

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

    /// SIGUSR1 behaviour: dump state and metrics to log or file.
    #[serde(default)]
    pub dump_on_sigusr1: Option<DumpOnSigusr1Config>,

    /// Bulkhead: global and per-source concurrency caps (semaphores).
    #[serde(default)]
    pub bulkhead: Option<BulkheadConfig>,

    /// Load shedding: when backpressure is active (queue full or memory over threshold), optionally skip polling low-priority sources. Uses backpressure.detection (max pending events, memory threshold).
    #[serde(default)]
    pub load_shedding: Option<LoadSheddingConfig>,

    /// Optional JS hooks (Boa): buildRequest, parseResponse, getNextPage, commitState. Sandbox: timeout, no network/fs.
    #[serde(default)]
    pub hooks: Option<HooksConfig>,

    /// Optional audit: log credential access, config reloads; redact secrets in logs.
    #[serde(default)]
    pub audit: Option<AuditConfig>,
}

/// Audit config: log credential access and config changes. Credential-access events never include secret values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditConfig {
    /// Enable audit logging (credential access, config load/reload).
    #[serde(default)]
    pub enabled: bool,
    /// Log when secrets are read (env/file). Logs event only, never the value.
    #[serde(default = "default_true")]
    pub log_credential_access: bool,
    /// Log when config is loaded or reloaded (e.g. on SIGHUP).
    #[serde(default = "default_true")]
    pub log_config_changes: bool,
}

fn default_true() -> bool {
    true
}

/// Global hooks config: script path, timeout, sandbox (no network/fs).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HooksConfig {
    /// Enable JS hooks for sources that specify a script.
    #[serde(default)]
    pub enabled: bool,
    /// Directory (or base path) for hook scripts. Per-source script is path/script or absolute if script starts with /.
    #[serde(default)]
    pub path: Option<String>,
    /// Max execution time per hook call (seconds). Default 5.
    #[serde(default = "default_hooks_timeout_secs")]
    pub timeout_secs: u64,
    /// Boa heap limit in MB (optional; not all Boa builds support it).
    #[serde(default)]
    pub memory_limit_mb: Option<u64>,
    /// Allow fetch() in hooks (default false; sandbox).
    #[serde(default)]
    pub allow_network: bool,
    /// Allow file system access in hooks (default false; sandbox).
    #[serde(default)]
    pub allow_fs: bool,
    /// Cache getAuth results for this many seconds (default 1800 = 30 min). 0 disables caching.
    #[serde(default = "default_hooks_auth_cache_ttl_secs")]
    pub auth_cache_ttl_secs: u64,
}

fn default_hooks_timeout_secs() -> u64 {
    5
}

fn default_hooks_auth_cache_ttl_secs() -> u64 {
    1800
}

/// Per-source hooks: script from file path and/or inline string.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHooksConfig {
    /// Script filename (e.g. "okta.js") under global hooks.path, or absolute path. Omit if using script_inline.
    #[serde(default)]
    pub script: Option<String>,

    /// Inline JavaScript for hooks (alternative to script path). Use YAML block scalar (e.g. `|`) for multi-line. One of script or script_inline must be set.
    #[serde(default)]
    pub script_inline: Option<String>,
}

/// Load shedding: skip low-priority sources when under load (backpressure active).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadSheddingConfig {
    /// When set, sources with priority below this (0–10) are not polled while under load. Requires per-source priority (default 10).
    #[serde(default)]
    pub skip_priority_below: Option<u32>,
}

/// Bulkhead (concurrency) limits.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BulkheadConfig {
    /// Max number of sources that may poll concurrently. When set, sources beyond this wait for a permit.
    #[serde(default)]
    pub max_concurrent_sources: Option<u32>,

    /// Max concurrent HTTP requests per source (default no limit). Overridable per source in resilience.bulkhead.
    #[serde(default)]
    pub max_concurrent_requests: Option<u32>,
}

/// Config reload on SIGHUP.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReloadConfig {
    /// When true, on SIGHUP also clear circuit breaker and OAuth2 token cache so sources re-establish on next tick.
    #[serde(default)]
    pub restart_sources_on_sighup: bool,
}

/// Dump state and metrics on SIGUSR1.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DumpOnSigusr1Config {
    /// Where to write the dump: "log" (tracing at INFO) or "file".
    #[serde(default = "default_dump_destination")]
    pub destination: String,

    /// Path when destination is "file". Required when destination is "file".
    #[serde(default)]
    pub path: Option<String>,
}

fn default_dump_destination() -> String {
    "log".to_string()
}

/// Graceful degradation when state store fails or is unavailable.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub(crate) fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Bind address for the API and health server (e.g. "0.0.0.0" or "127.0.0.1").
    #[serde(default = "default_api_address")]
    pub address: String,
    /// Port for the API and health server.
    #[serde(default = "default_api_port")]
    pub port: u16,
}

fn default_api_address() -> String {
    "0.0.0.0".to_string()
}
fn default_api_port() -> u16 {
    8080
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
