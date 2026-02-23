//! Configuration schema for Helr (v0.1).
//!
//! YAML config: sources, schedule, auth, pagination, resilience.
//! Env overrides: HELRR_*.

#![allow(dead_code)] // fields used when implementing poll loop

mod auth;
mod global;
mod pagination;
mod resilience;
mod source;

pub use auth::*;
pub use global::*;
pub use pagination::*;
pub use resilience::*;
pub use source::*;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Root config (helr.yaml).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub global: GlobalConfig,

    pub sources: HashMap<String, SourceConfig>,
}

impl Config {
    /// Load and parse config from path. Expands env vars; fails if any placeholder is unset (no default).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {:?}: {}", path, e))?;
        let expanded = expand_env_vars_strict(&s)?;
        let config: Config = serde_yaml_ng::from_str(&expanded)
            .map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        if config.sources.is_empty() {
            anyhow::bail!("config must have at least one source");
        }
        validate_auth_secrets(&config)?;
        validate_tls(&config)?;
        Ok(config)
    }
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
            std::env::set_var("HELR_TEST_EXPAND_A", "foo");
        }
        let s = "prefix_${HELR_TEST_EXPAND_A}_suffix";
        let out = expand_env_vars(s).unwrap();
        assert_eq!(out, "prefix_foo_suffix");
        unsafe {
            std::env::remove_var("HELR_TEST_EXPAND_A");
        }
    }

    #[test]
    fn expand_env_vars_unset_expands_empty() {
        unsafe {
            std::env::remove_var("HELR_TEST_UNSET_VAR_XYZ");
        }
        let s = "a${HELR_TEST_UNSET_VAR_XYZ}b";
        let out = expand_env_vars(s).unwrap();
        assert_eq!(out, "ab");
    }

    #[test]
    fn read_secret_from_env() {
        unsafe {
            std::env::set_var("HELR_TEST_SECRET_ENV", "secret-from-env");
        }
        let out = read_secret(None, "HELR_TEST_SECRET_ENV").unwrap();
        assert_eq!(out, "secret-from-env");
        unsafe {
            std::env::remove_var("HELR_TEST_SECRET_ENV");
        }
    }

    #[test]
    fn read_secret_file_overrides_env() {
        let dir = std::env::temp_dir().join("helr_config_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("token.txt");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"  secret-from-file  \n")
            .unwrap();
        unsafe {
            std::env::set_var("HELR_TEST_SECRET_BOTH", "secret-from-env");
        }
        let out = read_secret(Some(path.to_str().unwrap()), "HELR_TEST_SECRET_BOTH").unwrap();
        assert_eq!(out, "secret-from-file");
        unsafe {
            std::env::remove_var("HELR_TEST_SECRET_BOTH");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_minimal_valid() {
        let dir = std::env::temp_dir().join("helr_config_load_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_load_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_degradation");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_reload");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
    fn config_load_dump_on_sigusr1() {
        let dir = std::env::temp_dir().join("helr_config_dump_sigusr1");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        let yaml = r#"
global:
  dump_on_sigusr1:
    destination: file
    path: /tmp/helr-dump.txt
sources:
  s1:
    url: "https://example.com/"
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let d = config.global.dump_on_sigusr1.as_ref().unwrap();
        assert_eq!(d.destination, "file");
        assert_eq!(d.path.as_deref(), Some("/tmp/helr-dump.txt"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_bulkhead() {
        let dir = std::env::temp_dir().join("helr_config_bulkhead");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        let yaml = r#"
global:
  bulkhead:
    max_concurrent_sources: 4
    max_concurrent_requests: 2
sources:
  s1:
    url: "https://example.com/"
    pagination:
      strategy: link_header
      rel: next
    resilience:
      bulkhead:
        max_concurrent_requests: 1
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let b = config.global.bulkhead.as_ref().unwrap();
        assert_eq!(b.max_concurrent_sources, Some(4));
        assert_eq!(b.max_concurrent_requests, Some(2));
        let s1 = config.sources.get("s1").unwrap();
        let sb = s1.resilience.as_ref().unwrap().bulkhead.as_ref().unwrap();
        assert_eq!(sb.max_concurrent_requests, Some(1));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_load_shedding() {
        let dir = std::env::temp_dir().join("helr_config_load_shedding");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        let yaml = r#"
global:
  load_shedding:
    skip_priority_below: 5
sources:
  high:
    url: "https://example.com/high"
    priority: 8
    pagination:
      strategy: link_header
      rel: next
  low:
    url: "https://example.com/low"
    priority: 2
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        let ls = config.global.load_shedding.as_ref().unwrap();
        assert_eq!(ls.skip_priority_below, Some(5));
        assert_eq!(config.sources.get("high").unwrap().priority, Some(8));
        assert_eq!(config.sources.get("low").unwrap().priority, Some(2));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_rate_limit() {
        let dir = std::env::temp_dir().join("helr_config_rate_limit");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
            std::env::set_var("HELR_TEST_BASE_URL", "https://api.test.com");
        }
        let dir = std::env::temp_dir().join("helr_config_expand_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        let yaml = r#"
global: {}
sources:
  x:
    url: "${HELR_TEST_BASE_URL}/logs"
    pagination:
      strategy: link_header
"#;
        std::fs::write(&path, yaml).unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.sources["x"].url, "https://api.test.com/logs");
        unsafe {
            std::env::remove_var("HELR_TEST_BASE_URL");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_empty_sources_fails() {
        let dir = std::env::temp_dir().join("helr_config_empty_sources");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_invalid_yaml");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        std::fs::write(&path, "global:\n  log_level: [unclosed").unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("parse") || err.to_string().contains("yaml"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_corner_case_options() {
        let dir = std::env::temp_dir().join("helr_config_corner_case");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_unset_placeholder");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  x:
    url: "https://${HELR_UNSET_PLACEHOLDER_TEST}/logs"
    pagination:
      strategy: link_header
"#,
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("unset")
                || err.to_string().contains("HELR_UNSET_PLACEHOLDER_TEST"),
            "expected unset placeholder error, got: {}",
            err
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_comment_lines_not_expanded() {
        let dir = std::env::temp_dir().join("helr_config_comment_no_expand");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        std::fs::write(
            &path,
            r#"
global: {}
sources:
  # use enterprise URL: https://${GITHUB_API_HOST}/enterprises/${GITHUB_ENTERPRISE}/audit-log
  x:
    url: "https://${HELR_TEST_COMMENT_EXPAND_BASE}/logs"
    pagination:
      strategy: link_header
"#,
        )
        .unwrap();
        unsafe {
            std::env::set_var("HELR_TEST_COMMENT_EXPAND_BASE", "api.example.com");
        }
        let result = Config::load(&path);
        unsafe {
            std::env::remove_var("HELR_TEST_COMMENT_EXPAND_BASE");
        }
        let config =
            result.expect("load should succeed; comment placeholders must not be expanded");
        assert_eq!(config.sources["x"].url, "https://api.example.com/logs");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_load_initial_query_params_string_and_number() {
        let dir = std::env::temp_dir().join("helr_config_initial_query_params");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_split_timeouts");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_retry_jitter");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_transform");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_tls");
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
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_tls_validate");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_tls_minver");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let dir = std::env::temp_dir().join("helr_config_unknown_field");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
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
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
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
        let source2: SourceConfig = serde_yaml_ng::from_str(yaml_with_key).unwrap();
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
        let config: Config = serde_yaml_ng::from_str(yaml_redis).unwrap();
        let st = config.global.state.as_ref().unwrap();
        assert_eq!(st.backend, "redis");
        assert_eq!(st.url.as_deref(), Some("redis://127.0.0.1:6379/"));

        let yaml_pg = r#"
global:
  state:
    backend: postgres
    url: "postgres://user:pass@localhost/helr"
sources:
  x:
    url: "https://example.com/"
    pagination:
      strategy: link_header
"#;
        let config_pg: Config = serde_yaml_ng::from_str(yaml_pg).unwrap();
        let st_pg = config_pg.global.state.as_ref().unwrap();
        assert_eq!(st_pg.backend, "postgres");
        assert_eq!(
            st_pg.url.as_deref(),
            Some("postgres://user:pass@localhost/helr")
        );
    }

    #[test]
    fn config_load_source_hooks_script_path() {
        let yaml = r#"
url: "https://example.com/"
hooks:
  script: "okta.js"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let h = source.hooks.as_ref().unwrap();
        assert_eq!(h.script.as_deref(), Some("okta.js"));
        assert!(h.script_inline.is_none());
    }

    #[test]
    fn config_load_audit() {
        let yaml = r#"
global:
  audit:
    enabled: true
    log_credential_access: true
    log_config_changes: true
sources:
  x:
    url: "https://example.com/"
    pagination:
      strategy: link_header
"#;
        let config: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let audit = config.global.audit.as_ref().unwrap();
        assert!(audit.enabled);
        assert!(audit.log_credential_access);
        assert!(audit.log_config_changes);
    }

    #[test]
    fn config_load_source_hooks_script_inline() {
        let yaml = r#"
url: "https://example.com/"
hooks:
  script_inline: |
    function buildRequest(ctx) { return {}; }
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let h = source.hooks.as_ref().unwrap();
        assert!(h.script.is_none());
        assert!(h.script_inline.as_ref().unwrap().contains("buildRequest"));
    }
}
