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

fn default_bearer_prefix() -> String {
    "Bearer".to_string()
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
    /// When true, use Retry-After or X-RateLimit-Reset from response on 429 instead of generic backoff.
    #[serde(default = "default_respect_headers")]
    pub respect_headers: bool,

    /// Optional delay in seconds between pagination requests. Reduces burst; reusable across APIs.
    #[serde(default)]
    pub page_delay_secs: Option<u64>,
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
    /// Load and parse config from path. Expands env vars; fails if any placeholder is unset (no default).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {:?}: {}", path, e))?;
        let expanded = expand_env_vars_strict(&s)?;
        let config: Config = serde_yaml::from_str(&expanded)
            .map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        if config.sources.is_empty() {
            anyhow::bail!("config must have at least one source");
        }
        validate_auth_secrets(&config)?;
        Ok(config)
    }
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
                AuthConfig::ApiKey { key_env, key_file, .. } => {
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
                    refresh_token_env,
                    refresh_token_file,
                    ..
                } => {
                    read_secret(client_id_file.as_deref(), client_id_env)
                        .with_context(|| format!("source {}: oauth2 client_id", source_id))?;
                    read_secret(client_secret_file.as_deref(), client_secret_env)
                        .with_context(|| format!("source {}: oauth2 client_secret", source_id))?;
                    read_secret(refresh_token_file.as_deref(), refresh_token_env)
                        .with_context(|| format!("source {}: oauth2 refresh_token", source_id))?;
                }
            }
        }
    }
    Ok(())
}

/// Expand env vars in config: `$VAR`, `${VAR}`, `${VAR:-default}`. Fails if any var is unset (no default).
fn expand_env_vars_strict(s: &str) -> anyhow::Result<String> {
    shellexpand::env_with_context(s, |var: &str| {
        std::env::var(var)
            .map(|v| Ok(Some(std::borrow::Cow::Owned(v))))
            .unwrap_or_else(|e| Err(e))
    })
    .map(|cow| cow.into_owned())
    .map_err(|e| anyhow::anyhow!("config placeholder ${{{}}} is unset: {}", e.var_name, e.cause))
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
        assert_eq!(config.sources["test-source"].url, "https://example.com/logs");
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
        assert_eq!(s.on_state_write_error, Some(OnStateWriteErrorBehavior::SkipCheckpoint));
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
            err.to_string().contains("unset") || err.to_string().contains("HEL_UNSET_PLACEHOLDER_TEST"),
            "expected unset placeholder error, got: {}",
            err
        );
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
        assert_eq!(params.get("limit").map(|v| v.to_param_value()), Some("20".to_string()));
        assert_eq!(params.get("sortOrder").map(|v| v.to_param_value()), Some("ASCENDING".to_string()));
        assert_eq!(
            params.get("filter").map(|v| v.to_param_value()),
            Some("eventType eq \"user.session.start\"".to_string())
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
}
