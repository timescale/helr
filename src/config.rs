//! Configuration schema for Hel (v0.1).
//!
//! YAML config: sources, schedule, auth, pagination, resilience.
//! Env overrides: HEL_*.

#![allow(dead_code)] // fields used when implementing poll loop

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
    #[serde(default = "default_log_level")]
    pub log_level: String,

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
    Bearer { token_env: String },
    ApiKey { header: String, key_env: String },
    Basic { user_env: String, password_env: String },
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
    /// Load and parse config from path. Expands env vars in string fields.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {:?}: {}", path, e))?;
        let expanded = expand_env_vars(&s);
        let config: Config = serde_yaml::from_str(&expanded)
            .map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        if config.sources.is_empty() {
            anyhow::bail!("config must have at least one source");
        }
        Ok(config)
    }
}

/// Naive env expansion: ${VAR} or $VAR.
fn expand_env_vars(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' {
            if chars.peek() == Some(&'{') {
                chars.next(); // consume '{'
                let var: String = chars.by_ref().take_while(|&c| c != '}').collect();
                let val = std::env::var(&var).unwrap_or_default();
                out.push_str(&val);
            } else {
                let var: String = chars
                    .by_ref()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                let val = std::env::var(&var).unwrap_or_default();
                out.push_str(&val);
            }
        } else {
            out.push(c);
        }
    }
    out
}
