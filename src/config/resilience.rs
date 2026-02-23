use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::Path;

use super::{AuthConfig, Config, read_secret};

/// Split timeouts: connect, request, read, idle (client), poll_tick (entire poll cycle).
/// When set, these override or supplement the legacy `timeout_secs` (used for request when timeouts.request is unset).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Per-source bulkhead: max concurrent requests for this source. Overrides global bulkhead.max_concurrent_requests when set.
    #[serde(default)]
    pub bulkhead: Option<SourceBulkheadConfig>,
}

/// Per-source bulkhead (overrides global when set).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBulkheadConfig {
    /// Max concurrent HTTP requests for this source.
    #[serde(default)]
    pub max_concurrent_requests: Option<u32>,
}

/// Header names for rate limit info (limit, remaining, reset). When unset, defaults are used.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Validate TLS config: client cert and key both set when either is set; resolve CA/cert/key so startup fails if missing.
pub fn validate_tls(config: &Config) -> anyhow::Result<()> {
    let audit = config.global.audit.as_ref();
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
            || tls.client_key_env.as_deref().is_some_and(|e| !e.is_empty());
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
            crate::audit::log_credential_access(audit, source_id, "tls_client_cert");
            read_secret(
                tls.client_key_file.as_deref(),
                tls.client_key_env.as_deref().unwrap_or(""),
            )
            .with_context(|| format!("source {}: tls client_key", source_id))?;
            crate::audit::log_credential_access(audit, source_id, "tls_client_key");
        }
        let has_ca = tls.ca_file.as_deref().is_some_and(|p| !p.is_empty())
            || tls.ca_env.as_deref().is_some_and(|e| !e.is_empty());
        if has_ca {
            read_secret(tls.ca_file.as_deref(), tls.ca_env.as_deref().unwrap_or(""))
                .with_context(|| format!("source {}: tls ca", source_id))?;
            crate::audit::log_credential_access(audit, source_id, "tls_ca");
        }
        if let Some(v) = tls.min_version.as_deref()
            && v != "1.2"
            && v != "1.3"
        {
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
    let audit = config.global.audit.as_ref();
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
                    crate::audit::log_credential_access(audit, source_id, "bearer_token");
                }
                AuthConfig::ApiKey {
                    key_env, key_file, ..
                } => {
                    read_secret(key_file.as_deref(), key_env)
                        .with_context(|| format!("source {}: api key", source_id))?;
                    crate::audit::log_credential_access(audit, source_id, "api_key");
                }
                AuthConfig::Basic {
                    user_env,
                    user_file,
                    password_env,
                    password_file,
                } => {
                    read_secret(user_file.as_deref(), user_env)
                        .with_context(|| format!("source {}: basic user", source_id))?;
                    crate::audit::log_credential_access(audit, source_id, "basic_user");
                    read_secret(password_file.as_deref(), password_env)
                        .with_context(|| format!("source {}: basic password", source_id))?;
                    crate::audit::log_credential_access(audit, source_id, "basic_password");
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
                    crate::audit::log_credential_access(audit, source_id, "oauth2_client_id");
                    let has_private_key = client_private_key_env
                        .as_deref()
                        .is_some_and(|e| !e.is_empty())
                        || client_private_key_file
                            .as_deref()
                            .is_some_and(|p| !p.is_empty());
                    let has_secret = client_secret_env.as_deref().is_some_and(|e| !e.is_empty())
                        || client_secret_file.as_deref().is_some_and(|p| !p.is_empty());
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
                        crate::audit::log_credential_access(
                            audit,
                            source_id,
                            "oauth2_client_private_key",
                        );
                    } else {
                        read_secret(
                            client_secret_file.as_deref(),
                            client_secret_env.as_deref().unwrap_or(""),
                        )
                        .with_context(|| format!("source {}: oauth2 client_secret", source_id))?;
                        crate::audit::log_credential_access(
                            audit,
                            source_id,
                            "oauth2_client_secret",
                        );
                    }
                    let has_refresh = refresh_token_env.as_deref().is_some_and(|e| !e.is_empty())
                        || refresh_token_file.as_deref().is_some_and(|p| !p.is_empty());
                    if has_refresh {
                        let rt_env = refresh_token_env.as_deref().unwrap_or("");
                        read_secret(refresh_token_file.as_deref(), rt_env).with_context(|| {
                            format!("source {}: oauth2 refresh_token", source_id)
                        })?;
                        crate::audit::log_credential_access(
                            audit,
                            source_id,
                            "oauth2_refresh_token",
                        );
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
                    crate::audit::log_credential_access(
                        audit,
                        source_id,
                        "google_service_account_credentials",
                    );
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
                        crate::audit::log_credential_access(
                            audit,
                            source_id,
                            "google_service_account_subject",
                        );
                    } else if let Some(path) = subject_file.as_deref()
                        && !path.is_empty()
                    {
                        std::fs::read_to_string(Path::new(path)).with_context(|| {
                            format!("source {}: google_service_account subject file", source_id)
                        })?;
                        crate::audit::log_credential_access(
                            audit,
                            source_id,
                            "google_service_account_subject",
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
