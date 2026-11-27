//! REST API: list sources, source status, state per source, config, trigger poll, optional reload.
//!
//! Served on the same server as health when `global.api.enabled` is true.
//! Base path: /api/v1. Endpoints: GET /api/v1/sources, GET /api/v1/sources/:id,
//! GET /api/v1/sources/:id/state, GET /api/v1/sources/:id/config, GET /api/v1/config,
//! POST /api/v1/sources/:id/poll, POST /api/v1/reload.

use crate::audit;
use crate::config::Config;
use crate::health::{self, HealthState, SourceStatusDto};
use crate::poll;
use axum::Json;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

/// Response for GET /api/v1/sources: list of sources with status.
#[derive(Debug, Serialize)]
pub struct ListSourcesResponse {
    pub version: String,
    pub uptime_secs: f64,
    pub sources: HashMap<String, SourceStatusDto>,
}

/// Response for GET /api/v1/sources/:id/state: key-value state for one source.
#[derive(Debug, Serialize)]
pub struct SourceStateResponse {
    pub source_id: String,
    pub state: HashMap<String, String>,
}

/// Response for POST /api/v1/sources/:id/poll.
#[derive(Debug, Serialize)]
pub struct TriggerPollResponse {
    pub source_id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for POST /api/v1/reload.
#[derive(Debug, Serialize)]
pub struct ReloadResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// GET /api/v1/sources — list all sources and their status (circuit, last_error).
pub async fn list_sources_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let body = health::build_health_body(state.as_ref()).await;
    let response = ListSourcesResponse {
        version: body.version,
        uptime_secs: body.uptime_secs,
        sources: body.sources,
    };
    (StatusCode::OK, Json(response))
}

/// GET /api/v1/sources/:id — status for a single source. 404 if not found.
pub async fn get_source_handler(
    State(state): State<Arc<HealthState>>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    let config = state.config.read().await;
    if !config.sources.contains_key(&source_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "source not found", "source_id": source_id })),
        );
    }
    let body = health::build_health_body(state.as_ref()).await;
    let source_status = match body.sources.get(&source_id) {
        Some(s) => s.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "source not found", "source_id": source_id })),
            );
        }
    };
    (
        StatusCode::OK,
        Json(serde_json::to_value(&source_status).unwrap_or(serde_json::Value::Null)),
    )
}

/// GET /api/v1/sources/:id/state — cursor, watermark, etc. for one source.
pub async fn get_source_state_handler(
    State(state): State<Arc<HealthState>>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    let config = state.config.read().await;
    if !config.sources.contains_key(&source_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "source not found", "source_id": source_id })),
        );
    }
    let store = match &state.state_store {
        Some(s) => s,
        None => {
            let empty = SourceStateResponse {
                source_id: source_id.clone(),
                state: HashMap::new(),
            };
            return (
                StatusCode::OK,
                Json(serde_json::to_value(&empty).unwrap_or(serde_json::Value::Null)),
            );
        }
    };
    let keys = match store.list_keys(&source_id).await {
        Ok(k) => k,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string(), "source_id": source_id })),
            );
        }
    };
    let mut state_map = HashMap::new();
    for key in keys {
        if let Ok(Some(value)) = store.get(&source_id, &key).await {
            state_map.insert(key, value);
        }
    }
    let response = SourceStateResponse {
        source_id,
        state: state_map,
    };
    (
        StatusCode::OK,
        Json(serde_json::to_value(&response).unwrap_or(serde_json::Value::Null)),
    )
}

/// GET /api/v1/sources/:id/config — config for a single source. 404 if not found.
pub async fn get_source_config_handler(
    State(state): State<Arc<HealthState>>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    let config = state.config.read().await;
    let source = match config.sources.get(&source_id) {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "source not found", "source_id": source_id })),
            );
        }
    };
    match serde_json::to_value(source) {
        Ok(mut v) => {
            redact_secrets(&mut v);
            (StatusCode::OK, Json(v))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

/// GET /api/v1/config — global config (log_level, health, metrics, state, etc.).
pub async fn get_global_config_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let config = state.config.read().await;
    match serde_json::to_value(&config.global) {
        Ok(mut v) => {
            redact_secrets(&mut v);
            (StatusCode::OK, Json(v))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

const REDACTED: &str = "[redacted]";

/// Config is served after `${VAR}` expansion, so values that commonly carry secrets are masked:
/// header, query param, and body values, plus URL passwords and query values.
fn redact_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                match key.as_str() {
                    "headers" | "query_params" => {
                        if let Some(inner) = v.as_object_mut() {
                            for inner_v in inner.values_mut() {
                                *inner_v = serde_json::Value::from(REDACTED);
                            }
                        }
                    }
                    "body" if !v.is_null() => *v = serde_json::Value::from(REDACTED),
                    _ => redact_secrets(v),
                }
            }
        }
        serde_json::Value::Array(arr) => arr.iter_mut().for_each(redact_secrets),
        serde_json::Value::String(s) => {
            if let Some(redacted) = redact_url(s) {
                *s = redacted;
            }
        }
        _ => {}
    }
}

fn redact_url(s: &str) -> Option<String> {
    if !s.contains("://") {
        return None;
    }
    let mut url = reqwest::Url::parse(s).ok()?;
    if url.password().is_none() && url.query().is_none() {
        return None;
    }
    if url.password().is_some() {
        let _ = url.set_password(Some(REDACTED));
    }
    if url.query().is_some() {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, _)| (k.into_owned(), REDACTED.to_string()))
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    Some(url.to_string())
}

/// POST /api/v1/sources/:id/poll — run one poll tick for the given source.
pub async fn trigger_poll_handler(
    State(state): State<Arc<HealthState>>,
    Path(source_id): Path<String>,
) -> impl IntoResponse {
    let config_guard = state.config.read().await;
    if !config_guard.sources.contains_key(&source_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(TriggerPollResponse {
                source_id: source_id.clone(),
                ok: false,
                error: Some("source not found".to_string()),
            }),
        );
    }
    let poll_deps = match &state.poll_deps {
        Some(d) => d,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(TriggerPollResponse {
                    source_id: source_id.clone(),
                    ok: false,
                    error: Some("trigger poll not available (run without --once)".to_string()),
                }),
            );
        }
    };
    let store = match &state.state_store {
        Some(s) => s.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(TriggerPollResponse {
                    source_id: source_id.clone(),
                    ok: false,
                    error: Some("state store not available".to_string()),
                }),
            );
        }
    };
    let config = config_guard.clone();
    drop(config_guard);

    let skip_priority_below = config
        .global
        .load_shedding
        .as_ref()
        .and_then(|l| l.skip_priority_below);
    let result = poll::run_one_tick(
        &config,
        store,
        Some(&source_id),
        state.circuit_store.clone(),
        poll_deps.token_cache.clone(),
        poll_deps.dpop_key_cache.clone(),
        poll_deps.dedupe_store.clone(),
        poll_deps.event_sink.clone(),
        None,
        state.last_errors.clone(),
        poll_deps.global_sources_semaphore.clone(),
        None,
        skip_priority_below,
    )
    .await;

    match result {
        Ok(()) => (
            StatusCode::OK,
            Json(TriggerPollResponse {
                source_id,
                ok: true,
                error: None,
            }),
        ),
        Err(e) => {
            let msg = e.to_string();
            tracing::warn!(source_id = %source_id, error = %msg, "API trigger poll failed");
            (
                StatusCode::OK,
                Json(TriggerPollResponse {
                    source_id,
                    ok: false,
                    error: Some(msg),
                }),
            )
        }
    }
}

/// POST /api/v1/reload — reload config from file (same as SIGHUP). 503 if reload not configured.
pub async fn reload_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let config_path = match &state.config_path {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReloadResponse {
                    ok: false,
                    error: Some("reload not available (no config path)".to_string()),
                }),
            );
        }
    };
    match Config::load(&config_path) {
        Ok(new_config) => {
            audit::log_config_change(new_config.global.audit.as_ref(), &config_path, true);
            let restart = new_config
                .global
                .reload
                .as_ref()
                .is_some_and(|r| r.restart_sources_on_sighup);
            {
                let mut guard = state.config.write().await;
                *guard = new_config;
            }
            if restart {
                state.circuit_store.write().await.clear();
                if let Some(d) = &state.poll_deps {
                    d.token_cache.write().await.clear();
                }
                tracing::info!("config reloaded via API, circuit breaker and token cache cleared");
            } else {
                tracing::info!("config reloaded via API");
            }
            (
                StatusCode::OK,
                Json(ReloadResponse {
                    ok: true,
                    error: None,
                }),
            )
        }
        Err(e) => {
            let msg = e.to_string();
            tracing::warn!(error = %msg, "API config reload failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ReloadResponse {
                    ok: false,
                    error: Some(msg),
                }),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;
    use tokio::sync::RwLock;

    static API_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn minimal_config() -> Config {
        let n = API_TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("hel_api_test_{}", n));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("helr.yaml");
        let yaml = r#"
global: {}
sources:
  src-a:
    url: "https://example.com/a"
    pagination:
      strategy: link_header
      rel: next
  src-b:
    url: "https://example.com/b"
    pagination:
      strategy: link_header
      rel: next
"#;
        std::fs::write(&path, yaml).unwrap();
        Config::load(&path).unwrap()
    }

    fn api_state_no_poll_deps() -> Arc<HealthState> {
        Arc::new(HealthState {
            config: Arc::new(RwLock::new(minimal_config())),
            circuit_store: circuit::new_circuit_store(),
            last_errors: Arc::new(RwLock::new(HashMap::new())),
            started_at: Instant::now(),
            output_path: None,
            state_store: None,
            state_store_fallback_active: false,
            config_path: None,
            poll_deps: None,
        })
    }

    #[tokio::test]
    async fn list_sources_returns_version_uptime_sources() {
        let state = api_state_no_poll_deps();
        let response = list_sources_handler(State(state)).await.into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(!json["version"].as_str().unwrap_or("").is_empty());
        assert!(json["uptime_secs"].as_f64().is_some());
        let sources = json["sources"].as_object().unwrap();
        assert_eq!(sources.len(), 2);
        assert!(sources.contains_key("src-a"));
        assert!(sources.contains_key("src-b"));
    }

    #[tokio::test]
    async fn get_source_found_returns_200() {
        let state = api_state_no_poll_deps();
        let response = get_source_handler(State(state), Path("src-a".to_string()))
            .await
            .into_response();
        let (parts, _) = response.into_parts();
        assert_eq!(parts.status, StatusCode::OK);
    }

    #[tokio::test]
    async fn get_source_not_found_returns_404() {
        let state = api_state_no_poll_deps();
        let response = get_source_handler(State(state), Path("nonexistent".to_string()))
            .await
            .into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::NOT_FOUND);
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"].as_str(), Some("source not found"));
    }

    #[tokio::test]
    async fn get_source_config_found_returns_200() {
        let state = api_state_no_poll_deps();
        let response = get_source_config_handler(State(state), Path("src-a".to_string()))
            .await
            .into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["url"], "https://example.com/a");
    }

    #[test]
    fn redact_secrets_masks_expanded_values() {
        let mut v = serde_json::json!({
            "url": "https://example.com/logs?token=abc&limit=10",
            "headers": { "Authorization": "Bearer abc" },
            "query_params": { "api_key": "abc" },
            "body": { "secret": "abc" },
            "state": { "backend": "postgres", "url": "postgres://helr:pw@db:5432/helr" },
            "output": { "http": { "headers": { "X-Api-Key": "abc" }, "batch_size": 100 } },
            "log_level": "info"
        });
        redact_secrets(&mut v);
        let s = v.to_string();
        assert!(!s.contains("abc"), "{s}");
        assert!(!s.contains(":pw@"), "{s}");
        assert_eq!(v["headers"]["Authorization"], REDACTED);
        assert_eq!(v["output"]["http"]["batch_size"], 100);
        assert_eq!(v["log_level"], "info");
        assert!(v["url"].as_str().unwrap().contains("limit="));
        assert!(
            v["state"]["url"]
                .as_str()
                .unwrap()
                .starts_with("postgres://helr:")
        );
    }

    #[tokio::test]
    async fn get_source_config_not_found_returns_404() {
        let state = api_state_no_poll_deps();
        let response = get_source_config_handler(State(state), Path("nonexistent".to_string()))
            .await
            .into_response();
        let (parts, _) = response.into_parts();
        assert_eq!(parts.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_global_config_returns_200() {
        let state = api_state_no_poll_deps();
        let response = get_global_config_handler(State(state))
            .await
            .into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json.get("log_level").is_some());
    }

    #[tokio::test]
    async fn get_source_state_not_found_returns_404() {
        let state = api_state_no_poll_deps();
        let response = get_source_state_handler(State(state), Path("nonexistent".to_string()))
            .await
            .into_response();
        let (parts, _) = response.into_parts();
        assert_eq!(parts.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_source_state_found_no_store_returns_empty_state() {
        let state = api_state_no_poll_deps();
        let response = get_source_state_handler(State(state), Path("src-a".to_string()))
            .await
            .into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::OK);
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["source_id"], "src-a");
        assert!(json["state"].as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn trigger_poll_no_poll_deps_returns_503() {
        let state = api_state_no_poll_deps();
        let response = trigger_poll_handler(State(state), Path("src-a".to_string()))
            .await
            .into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["ok"], false);
        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("trigger poll not available")
        );
    }

    #[tokio::test]
    async fn trigger_poll_source_not_found_returns_404() {
        let state = api_state_no_poll_deps();
        let response = trigger_poll_handler(State(state), Path("nonexistent".to_string()))
            .await
            .into_response();
        let (parts, _) = response.into_parts();
        assert_eq!(parts.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn reload_no_config_path_returns_503() {
        let state = api_state_no_poll_deps();
        let response = reload_handler(State(state)).await.into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["ok"], false);
        assert!(json["error"].as_str().unwrap().contains("no config path"));
    }
}
