//! Health endpoints: detailed JSON for /healthz, /readyz, /startupz.
//! Per-source status, circuit state, last_error, uptime, version.

use crate::circuit::{CircuitState, CircuitStore};
use crate::config::Config;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Shared state for health handlers (config, circuits, last errors, start time, output path).
pub struct HealthState {
    pub config: Arc<Config>,
    pub circuit_store: CircuitStore,
    pub last_errors: Arc<RwLock<HashMap<String, String>>>,
    pub started_at: Instant,
    pub output_path: Option<std::path::PathBuf>,
}

/// Circuit state as JSON: "closed" | "open" | "half_open" plus optional detail.
#[derive(Debug, Serialize)]
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
#[derive(Debug, Serialize)]
pub struct SourceStatusDto {
    pub status: String,
    pub circuit_state: CircuitStateDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Common health body: version, uptime, sources.
#[derive(Debug, Serialize)]
pub struct HealthBody {
    pub version: String,
    pub uptime_secs: f64,
    pub sources: HashMap<String, SourceStatusDto>,
}

/// Ready body adds ready flag and optional output_writable.
#[derive(Debug, Serialize)]
pub struct ReadyBody {
    pub version: String,
    pub uptime_secs: f64,
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_writable: Option<bool>,
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
        CircuitState::Closed { failures } => CircuitStateDto {
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

fn source_status(status: &str, circuit: CircuitStateDto, last_error: Option<String>) -> SourceStatusDto {
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
    for (source_id, _) in &config.sources {
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
    let sources = build_sources(
        &state.config,
        &state.circuit_store,
        state.last_errors.as_ref(),
    )
    .await;
    HealthBody {
        version: version(),
        uptime_secs,
        sources,
    }
}

/// Build ready JSON body: same as health plus ready flag and output_writable when output to file.
pub async fn build_ready_body(state: &HealthState) -> ReadyBody {
    let uptime_secs = state.started_at.elapsed().as_secs_f64();
    let sources = build_sources(
        &state.config,
        &state.circuit_store,
        state.last_errors.as_ref(),
    )
    .await;
    let output_writable = state
        .output_path
        .as_ref()
        .map(|p| std::fs::OpenOptions::new().append(true).open(p).is_ok());
    let ready = output_writable.map_or(true, |w| w);
    ReadyBody {
        version: version(),
        uptime_secs,
        ready,
        output_writable,
        sources,
    }
}

/// Build startup JSON body: started true, version, uptime, sources.
pub async fn build_startup_body(state: &HealthState) -> StartupBody {
    let uptime_secs = state.started_at.elapsed().as_secs_f64();
    let sources = build_sources(
        &state.config,
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
