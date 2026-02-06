//! Health endpoints: detailed JSON for /healthz, /readyz, /startupz.
//! Per-source status, circuit state, last_error, uptime, version.
//!
//! **Readyz semantics:** Ready when (1) output path is writable (or stdout), (2) state store is
//! connected, and (3) at least one source is healthy (circuit not open). All three are reported
//! in the JSON body.

use crate::circuit::{CircuitState, CircuitStore};
use crate::config::Config;
use crate::dedupe::DedupeStore;
use crate::dpop::DPoPKeyCache;
use crate::oauth2::OAuth2TokenCache;
use crate::output::EventSink;
use crate::state::StateStore;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, Semaphore};

/// Dependencies required to run a one-off poll from the API (trigger poll).
/// When None, POST /api/sources/:id/poll returns 503.
pub struct PollDeps {
    pub event_sink: Arc<dyn EventSink>,
    pub token_cache: OAuth2TokenCache,
    pub dpop_key_cache: Option<DPoPKeyCache>,
    pub dedupe_store: DedupeStore,
    pub global_sources_semaphore: Option<Arc<Semaphore>>,
}

/// Shared state for health and API handlers (config, circuits, last errors, start time, output path, state store).
pub struct HealthState {
    /// Current config; wrapped in RwLock so API reload can update it.
    pub config: Arc<RwLock<Config>>,
    pub circuit_store: CircuitStore,
    pub last_errors: Arc<RwLock<HashMap<String, String>>>,
    pub started_at: Instant,
    pub output_path: Option<PathBuf>,
    /// State store for readyz "connected" check. None in tests or when not configured.
    pub state_store: Option<Arc<dyn StateStore>>,
    /// True when primary state store failed and we fell back to memory (graceful degradation).
    pub state_store_fallback_active: bool,
    /// Path to config file for API reload. None when running with --once or replay.
    pub config_path: Option<PathBuf>,
    /// Dependencies for trigger poll. None when running with --once or replay. Also used for reload (clear token_cache on restart_sources_on_sighup).
    pub poll_deps: Option<Arc<PollDeps>>,
}

/// Circuit state as JSON: "closed" | "open" | "half_open" plus optional detail.
#[derive(Clone, Debug, Serialize)]
pub struct CircuitStateDto {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failures: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_until_secs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub successes: Option<u32>,
}

/// Per-source status in health response.
#[derive(Clone, Debug, Serialize)]
pub struct SourceStatusDto {
    pub status: String,
    pub circuit_state: CircuitStateDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Common health body: version, uptime, sources, optional degradation info.
#[derive(Debug, Serialize)]
pub struct HealthBody {
    pub version: String,
    pub uptime_secs: f64,
    pub sources: HashMap<String, SourceStatusDto>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub state_store_fallback_active: bool,
}

/// Ready body: ready flag plus per-condition flags (output_writable, state_store_connected, at_least_one_source_healthy).
#[derive(Debug, Serialize)]
pub struct ReadyBody {
    pub version: String,
    pub uptime_secs: f64,
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_writable: Option<bool>,
    pub state_store_connected: bool,
    pub at_least_one_source_healthy: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub state_store_fallback_active: bool,
    pub sources: HashMap<String, SourceStatusDto>,
}

/// Startup body adds started flag.
#[derive(Debug, Serialize)]
pub struct StartupBody {
    pub version: String,
    pub uptime_secs: f64,
    pub started: bool,
    pub sources: HashMap<String, SourceStatusDto>,
}

fn circuit_state_to_dto(s: &CircuitState) -> CircuitStateDto {
    let now = Instant::now();
    match s {
        CircuitState::Closed { failures, .. } => CircuitStateDto {
            state: "closed".to_string(),
            failures: Some(*failures),
            open_until_secs: None,
            successes: None,
        },
        CircuitState::Open { open_until } => {
            let remaining = open_until.saturating_duration_since(now).as_secs_f64();
            CircuitStateDto {
                state: "open".to_string(),
                failures: None,
                open_until_secs: Some(remaining.max(0.0)),
                successes: None,
            }
        }
        CircuitState::HalfOpen { successes } => CircuitStateDto {
            state: "half_open".to_string(),
            failures: None,
            open_until_secs: None,
            successes: Some(*successes),
        },
    }
}

fn source_status(
    status: &str,
    circuit: CircuitStateDto,
    last_error: Option<String>,
) -> SourceStatusDto {
    SourceStatusDto {
        status: status.to_string(),
        circuit_state: circuit,
        last_error,
    }
}

/// Build sources map: one entry per config source with circuit state and last_error.
async fn build_sources(
    config: &Config,
    circuit_store: &CircuitStore,
    last_errors: &RwLock<HashMap<String, String>>,
) -> HashMap<String, SourceStatusDto> {
    let circuits = circuit_store.read().await;
    let errors = last_errors.read().await;
    let mut sources = HashMap::new();
    for source_id in config.sources.keys() {
        let circuit_state = circuits
            .get(source_id)
            .map(circuit_state_to_dto)
            .unwrap_or_else(|| CircuitStateDto {
                state: "closed".to_string(),
                failures: Some(0),
                open_until_secs: None,
                successes: None,
            });
        let last_error = errors.get(source_id).cloned();
        let status = if circuit_state.state == "open" {
            "unhealthy"
        } else if last_error.is_some() {
            "degraded"
        } else {
            "ok"
        };
        sources.insert(
            source_id.clone(),
            source_status(status, circuit_state, last_error),
        );
    }
    sources
}

fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Build health JSON body (version, uptime, per-source status).
pub async fn build_health_body(state: &HealthState) -> HealthBody {
    let uptime_secs = state.started_at.elapsed().as_secs_f64();
    let config = state.config.read().await;
    let sources = build_sources(
        &config,
        &state.circuit_store,
        state.last_errors.as_ref(),
    )
    .await;
    HealthBody {
        version: version(),
        uptime_secs,
        sources,
        state_store_fallback_active: state.state_store_fallback_active,
    }
}

/// Build ready JSON body: same as health plus ready flag and per-condition flags.
/// Ready when: output writable (or stdout), state store connected, and ≥1 source healthy.
pub async fn build_ready_body(state: &HealthState) -> ReadyBody {
    let uptime_secs = state.started_at.elapsed().as_secs_f64();
    let config = state.config.read().await;
    let sources = build_sources(
        &config,
        &state.circuit_store,
        state.last_errors.as_ref(),
    )
    .await;
    let output_writable = state
        .output_path
        .as_ref()
        .map(|p| std::fs::OpenOptions::new().append(true).open(p).is_ok());
    let output_ok = output_writable.is_none_or(|w| w);
    let state_store_connected = match &state.state_store {
        Some(store) => store.list_sources().await.is_ok(),
        None => true,
    };
    let at_least_one_source_healthy = sources.values().any(|s| s.status != "unhealthy");
    let ready = output_ok && state_store_connected && at_least_one_source_healthy;
    ReadyBody {
        version: version(),
        uptime_secs,
        ready,
        output_writable,
        state_store_connected,
        at_least_one_source_healthy,
        sources,
        state_store_fallback_active: state.state_store_fallback_active,
    }
}

/// Build startup JSON body: started true, version, uptime, sources.
pub async fn build_startup_body(state: &HealthState) -> StartupBody {
    let uptime_secs = state.started_at.elapsed().as_secs_f64();
    let config = state.config.read().await;
    let sources = build_sources(
        &config,
        &state.circuit_store,
        state.last_errors.as_ref(),
    )
    .await;
    StartupBody {
        version: version(),
        uptime_secs,
        started: true,
        sources,
    }
}

/// Handler for GET /healthz: 200 + detailed JSON.
pub async fn healthz_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let body = build_health_body(state.as_ref()).await;
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        axum::Json(body),
    )
}

/// Handler for GET /readyz: 200 if ready, 503 if not; detailed JSON either way.
pub async fn readyz_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let body = build_ready_body(state.as_ref()).await;
    let status = if body.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        axum::Json(body),
    )
}

/// Handler for GET /startupz: 200 + detailed JSON.
pub async fn startupz_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let body = build_startup_body(state.as_ref()).await;
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        axum::Json(body),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit::{self, CircuitState};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static HEALTH_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn minimal_config() -> Config {
        let n = HEALTH_TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("hel_health_test_{}", n));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("hel.yaml");
        let yaml = r#"
global: {}
sources:
  s1:
    url: "https://example.com/"
    pagination:
      strategy: link_header
      rel: next
  s2:
    url: "https://example.com/two"
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        Config::load(&path).unwrap()
    }

    fn health_state_with(
        output_path: Option<std::path::PathBuf>,
        started_at: Instant,
    ) -> HealthState {
        let config = Arc::new(RwLock::new(minimal_config()));
        let circuit_store = circuit::new_circuit_store();
        let last_errors = Arc::new(RwLock::new(HashMap::new()));
        HealthState {
            config,
            circuit_store,
            last_errors,
            started_at,
            output_path,
            state_store: None,
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        }
    }

    #[tokio::test]
    async fn test_build_health_body_has_version_uptime_sources() {
        let started_at = Instant::now() - Duration::from_secs(5);
        let state = health_state_with(None, started_at);
        let body = build_health_body(&state).await;
        assert!(!body.version.is_empty(), "version should be non-empty");
        assert!(
            body.uptime_secs >= 5.0 && body.uptime_secs <= 10.0,
            "uptime_secs ~5"
        );
        assert_eq!(body.sources.len(), 2);
        assert!(body.sources.contains_key("s1"));
        assert!(body.sources.contains_key("s2"));
    }

    #[tokio::test]
    async fn test_build_health_body_source_ok_when_closed_no_error() {
        let state = health_state_with(None, Instant::now());
        let body = build_health_body(&state).await;
        let s1 = body.sources.get("s1").unwrap();
        assert_eq!(s1.status, "ok");
        assert_eq!(s1.circuit_state.state, "closed");
        assert_eq!(s1.circuit_state.failures, Some(0));
        assert!(s1.last_error.is_none());
    }

    #[tokio::test]
    async fn test_build_health_body_source_degraded_when_last_error() {
        let mut errors = HashMap::new();
        errors.insert("s1".to_string(), "connection refused".to_string());
        let config = Arc::new(RwLock::new(minimal_config()));
        let state = HealthState {
            config,
            circuit_store: circuit::new_circuit_store(),
            last_errors: Arc::new(RwLock::new(errors)),
            started_at: Instant::now(),
            output_path: None,
            state_store: None,
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        };
        let body = build_health_body(&state).await;
        let s1 = body.sources.get("s1").unwrap();
        assert_eq!(s1.status, "degraded");
        assert_eq!(s1.circuit_state.state, "closed");
        assert_eq!(s1.last_error.as_deref(), Some("connection refused"));
    }

    #[tokio::test]
    async fn test_build_health_body_source_unhealthy_when_circuit_open() {
        let config = Arc::new(RwLock::new(minimal_config()));
        let circuit_store = circuit::new_circuit_store();
        {
            let mut g = circuit_store.write().await;
            g.insert(
                "s1".to_string(),
                CircuitState::Open {
                    open_until: Instant::now() + Duration::from_secs(60),
                },
            );
        }
        let state = HealthState {
            config,
            circuit_store,
            last_errors: Arc::new(RwLock::new(HashMap::new())),
            started_at: Instant::now(),
            output_path: None,
            state_store: None,
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        };
        let body = build_health_body(&state).await;
        let s1 = body.sources.get("s1").unwrap();
        assert_eq!(s1.status, "unhealthy");
        assert_eq!(s1.circuit_state.state, "open");
        assert!(s1.circuit_state.open_until_secs.unwrap_or(0.0) > 0.0);
    }

    #[tokio::test]
    async fn test_build_health_body_circuit_half_open_has_successes() {
        let config = Arc::new(RwLock::new(minimal_config()));
        let circuit_store = circuit::new_circuit_store();
        {
            let mut g = circuit_store.write().await;
            g.insert("s1".to_string(), CircuitState::HalfOpen { successes: 1 });
        }
        let state = HealthState {
            config,
            circuit_store,
            last_errors: Arc::new(RwLock::new(HashMap::new())),
            started_at: Instant::now(),
            output_path: None,
            state_store: None,
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        };
        let body = build_health_body(&state).await;
        let s1 = body.sources.get("s1").unwrap();
        assert_eq!(s1.status, "ok");
        assert_eq!(s1.circuit_state.state, "half_open");
        assert_eq!(s1.circuit_state.successes, Some(1));
    }

    #[tokio::test]
    async fn test_build_ready_body_stdout_ready_true() {
        let state = health_state_with(None, Instant::now());
        let body = build_ready_body(&state).await;
        assert!(body.ready);
        assert!(body.output_writable.is_none());
        assert!(body.state_store_connected); // None -> true
        assert!(body.at_least_one_source_healthy);
        assert_eq!(body.sources.len(), 2);
    }

    #[tokio::test]
    async fn test_build_ready_body_output_writable_false_when_directory() {
        let dir = std::env::temp_dir().join("hel_health_ready_dir");
        let _ = std::fs::create_dir_all(&dir);
        let state = health_state_with(Some(dir), Instant::now());
        let body = build_ready_body(&state).await;
        // Opening a directory for append fails on Unix/Windows
        assert_eq!(body.output_writable, Some(false));
        assert!(!body.ready);
    }

    #[tokio::test]
    async fn test_build_startup_body_has_started_true() {
        let state = health_state_with(None, Instant::now());
        let body = build_startup_body(&state).await;
        assert!(body.started);
        assert!(!body.version.is_empty());
        assert!(body.uptime_secs >= 0.0);
        assert_eq!(body.sources.len(), 2);
    }

    #[tokio::test]
    async fn test_open_circuit_overrides_last_error_status() {
        // When circuit is open, status is unhealthy even if last_error is set
        let mut errors = HashMap::new();
        errors.insert("s1".to_string(), "previous failure".to_string());
        let config = Arc::new(RwLock::new(minimal_config()));
        let circuit_store = circuit::new_circuit_store();
        {
            let mut g = circuit_store.write().await;
            g.insert(
                "s1".to_string(),
                CircuitState::Open {
                    open_until: Instant::now() + Duration::from_secs(30),
                },
            );
        }
        let state = HealthState {
            config,
            circuit_store,
            last_errors: Arc::new(RwLock::new(errors)),
            started_at: Instant::now(),
            output_path: None,
            state_store: None,
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        };
        let body = build_health_body(&state).await;
        let s1 = body.sources.get("s1").unwrap();
        assert_eq!(s1.status, "unhealthy");
        assert_eq!(s1.circuit_state.state, "open");
        assert_eq!(s1.last_error.as_deref(), Some("previous failure"));
    }

    #[tokio::test]
    async fn test_build_ready_body_all_sources_unhealthy_not_ready() {
        let config = Arc::new(RwLock::new(minimal_config()));
        let circuit_store = circuit::new_circuit_store();
        {
            let mut g = circuit_store.write().await;
            g.insert(
                "s1".to_string(),
                CircuitState::Open {
                    open_until: Instant::now() + Duration::from_secs(60),
                },
            );
            g.insert(
                "s2".to_string(),
                CircuitState::Open {
                    open_until: Instant::now() + Duration::from_secs(60),
                },
            );
        }
        let state = HealthState {
            config,
            circuit_store,
            last_errors: Arc::new(RwLock::new(HashMap::new())),
            started_at: Instant::now(),
            output_path: None,
            state_store: None,
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        };
        let body = build_ready_body(&state).await;
        assert!(!body.at_least_one_source_healthy);
        assert!(!body.ready);
        assert!(body.state_store_connected);
    }

    #[tokio::test]
    async fn test_build_ready_body_state_store_connected() {
        use crate::state::MemoryStateStore;
        let config = Arc::new(RwLock::new(minimal_config()));
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let state = HealthState {
            config,
            circuit_store: circuit::new_circuit_store(),
            last_errors: Arc::new(RwLock::new(HashMap::new())),
            started_at: Instant::now(),
            output_path: None,
            state_store: Some(store),
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        };
        let body = build_ready_body(&state).await;
        assert!(body.state_store_connected);
        assert!(body.at_least_one_source_healthy);
        assert!(body.ready);
    }

    #[tokio::test]
    async fn test_build_health_body_state_store_fallback_active_true() {
        let state = health_state_with(None, Instant::now());
        let mut state_with_fallback = state;
        state_with_fallback.state_store_fallback_active = true;
        let body = build_health_body(&state_with_fallback).await;
        assert!(
            body.state_store_fallback_active,
            "health body should report state_store_fallback_active when true"
        );
    }

    #[tokio::test]
    async fn test_build_ready_body_state_store_fallback_active_true() {
        let state = health_state_with(None, Instant::now());
        let mut state_with_fallback = state;
        state_with_fallback.state_store_fallback_active = true;
        let body = build_ready_body(&state_with_fallback).await;
        assert!(
            body.state_store_fallback_active,
            "readyz body should report state_store_fallback_active when true"
        );
    }
}
