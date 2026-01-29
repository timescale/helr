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

    #[serde(default)]
    pub state: Option<GlobalStateConfig>,

    #[serde(default)]
    pub health: Option<HealthConfig>,

    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalStateConfig {
    pub backend: String, // "sqlite" | "memory"
    pub path: Option<String>,
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

/// Per-source config (one entry under sources:).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub url: String,

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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DedupeConfig {
    /// JSON key (or dotted path, e.g. "uuid", "id", "event.id") for event unique ID.
    pub id_field: String,
    /// Max number of event IDs to keep (LRU eviction).
    #[serde(default = "default_dedupe_capacity")]
    pub capacity: u64,
}

fn default_dedupe_capacity() -> u64 {
    100_000
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
    OAuth2 {
        token_url: String,
        client_id_env: String,
        #[serde(default)]
        client_id_file: Option<String>,
        client_secret_env: String,
        #[serde(default)]
        client_secret_file: Option<String>,
        refresh_token_env: String,
        #[serde(default)]
        refresh_token_file: Option<String>,
        #[serde(default)]
        scopes: Option<Vec<String>>,
    },
}

/// Resolve a secret from file path (if set) or environment variable. File takes precedence.
pub fn read_secret(file_path: Option<&str>, env_var: &str) -> anyhow::Result<String> {
    if let Some(p) = file_path {
        if !p.is_empty() {
            let s = std::fs::read_to_string(Path::new(p))
                .with_context(|| format!("read secret file {:?}", p))?;
            return Ok(s.trim().to_string());
        }
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResilienceConfig {
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub retries: Option<RetryConfig>,
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// When true, use Retry-After (and optionally X-RateLimit-Reset) on 429 instead of generic backoff.
    #[serde(default = "default_respect_headers")]
    pub respect_headers: bool,
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
    /// Load and parse config from path. Expands env vars (`$VAR`, `${VAR}`, `${VAR:-default}`) via shellexpand.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {:?}: {}", path, e))?;
        let expanded = expand_env_vars(&s)?;
        let config: Config = serde_yaml::from_str(&expanded)
            .map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        if config.sources.is_empty() {
            anyhow::bail!("config must have at least one source");
        }
        Ok(config)
    }
}

/// Expand env vars in config: `$VAR`, `${VAR}`, `${VAR:-default}`. Unset vars expand to empty (like os.ExpandEnv).
fn expand_env_vars(s: &str) -> anyhow::Result<String> {
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
