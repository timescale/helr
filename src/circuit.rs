//! Circuit breaker: per-source state machine (closed / open / half-open).
//! 5xx and timeouts increment failure count; after threshold we open and fail fast.

use crate::config::CircuitBreakerConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub enum CircuitState {
    Closed { failures: u32 },
    Open { open_until: Instant },
    HalfOpen { successes: u32 },
}

/// Per-source circuit state. Shared across poll ticks.
pub type CircuitStore = Arc<RwLock<HashMap<String, CircuitState>>>;

/// Create an empty circuit store.
pub fn new_circuit_store() -> CircuitStore {
    Arc::new(RwLock::new(HashMap::new()))
}

#[derive(Debug)]
pub struct CircuitOpenError {
    pub open_until: Instant,
}

impl std::fmt::Display for CircuitOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "circuit open until {:?}", self.open_until)
    }
}

impl std::error::Error for CircuitOpenError {}

/// Returns Ok(()) if a request is allowed, Err if circuit is open.
pub async fn allow_request(
    store: &CircuitStore,
    source_id: &str,
    config: &CircuitBreakerConfig,
) -> Result<(), CircuitOpenError> {
    if !config.enabled {
        return Ok(());
    }
    let mut g = store.write().await;
    let state = g.get(source_id).cloned();
    let now = Instant::now();
    let (allowed, new_state) = match state {
        None => (true, Some(CircuitState::Closed { failures: 0 })),
        Some(CircuitState::Closed { failures: _ }) => (true, None),
        Some(CircuitState::Open { open_until }) => {
            if now >= open_until {
                // Transition to half-open; allow one request
                info!(source = %source_id, "circuit half-open, allowing probe");
                (true, Some(CircuitState::HalfOpen { successes: 0 }))
            } else {
                (false, None)
            }
        }
        Some(CircuitState::HalfOpen { successes: _ }) => (true, None),
    };
    if let Some(s) = new_state {
        g.insert(source_id.to_string(), s);
    }
    if !allowed {
        if let Some(CircuitState::Open { open_until }) = state {
            warn!(source = %source_id, "request rejected: circuit open");
            return Err(CircuitOpenError { open_until });
        }
    }
    Ok(())
}

/// Record request outcome: success (2xx/3xx) or failure (5xx, timeout).
pub async fn record_result(
    store: &CircuitStore,
    source_id: &str,
    config: &CircuitBreakerConfig,
    success: bool,
) {
    if !config.enabled {
        return;
    }
    let mut g = store.write().await;
    let state = g.get(source_id).cloned().unwrap_or(CircuitState::Closed {
        failures: 0,
    });
    let now = Instant::now();
    let new_state = match state {
        CircuitState::Closed { failures } => {
            if success {
                CircuitState::Closed { failures: 0 }
            } else {
                let f = failures + 1;
                if f >= config.failure_threshold {
                    let open_until = now
                        + std::time::Duration::from_secs(config.half_open_timeout_secs);
                    warn!(
                        source = %source_id,
                        failures = f,
                        open_until_secs = config.half_open_timeout_secs,
                        "circuit opened"
                    );
                    CircuitState::Open { open_until }
                } else {
                    CircuitState::Closed { failures: f }
                }
            }
        }
        CircuitState::Open { open_until } => CircuitState::Open { open_until },
        CircuitState::HalfOpen { successes } => {
            if success {
                let s = successes + 1;
                if s >= config.success_threshold {
                    info!(source = %source_id, "circuit closed after half-open success");
                    CircuitState::Closed { failures: 0 }
                } else {
                    CircuitState::HalfOpen { successes: s }
                }
            } else {
                let open_until = now
                    + std::time::Duration::from_secs(config.half_open_timeout_secs);
                warn!(source = %source_id, "circuit re-opened from half-open");
                CircuitState::Open { open_until }
            }
        }
    };
    g.insert(source_id.to_string(), new_state);
}

/// Returns true if the error is a circuit-open rejection (so caller can skip recording).
#[allow(dead_code)]
pub fn is_circuit_open_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<CircuitOpenError>().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_closed_opens_after_threshold() {
        let store = new_circuit_store();
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 3,
            success_threshold: 2,
            half_open_timeout_secs: 60,
        };
        for _ in 0..3 {
            allow_request(&store, "s1", &config).await.unwrap();
            record_result(&store, "s1", &config, false).await;
        }
        let err = allow_request(&store, "s1", &config).await.unwrap_err();
        assert!(err.open_until > Instant::now());
    }

    #[tokio::test]
    async fn test_circuit_success_resets_failures() {
        let store = new_circuit_store();
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 3,
            success_threshold: 2,
            half_open_timeout_secs: 60,
        };
        allow_request(&store, "s1", &config).await.unwrap();
        record_result(&store, "s1", &config, false).await;
        allow_request(&store, "s1", &config).await.unwrap();
        record_result(&store, "s1", &config, true).await; // reset
        allow_request(&store, "s1", &config).await.unwrap();
        record_result(&store, "s1", &config, false).await;
        // Still only 1 failure after reset
        allow_request(&store, "s1", &config).await.unwrap();
    }
}
