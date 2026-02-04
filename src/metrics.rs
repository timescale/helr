//! Prometheus metrics: requests, events, errors, duration, circuit breaker state.
//! When global.metrics.enabled, GET /metrics on the configured port exposes text format.

use once_cell::sync::OnceCell;
use prometheus::{Encoder, IntCounterVec, IntGaugeVec, Opts, TextEncoder};

static METRICS: OnceCell<MetricsInner> = OnceCell::new();

struct MetricsInner {
    requests_total: IntCounterVec,
    events_emitted_total: IntCounterVec,
    errors_total: IntCounterVec,
    output_errors_total: IntCounterVec,
    events_dropped_total: IntCounterVec,
    pending_events: IntGaugeVec,
    request_duration_seconds: prometheus::HistogramVec,
    circuit_breaker_state: IntGaugeVec,
}

/// Initialize metrics and register with the default registry. Call once when metrics are enabled.
pub fn init() -> Result<(), prometheus::Error> {
    let requests_total = IntCounterVec::new(
        Opts::new(
            "hel_requests_total",
            "Total HTTP requests by source and status",
        ),
        &["source", "status"],
    )?;
    let events_emitted_total = IntCounterVec::new(
        Opts::new(
            "hel_events_emitted_total",
            "Total events emitted to stdout by source",
        ),
        &["source"],
    )?;
    let errors_total = IntCounterVec::new(
        Opts::new("hel_errors_total", "Total errors by source"),
        &["source"],
    )?;
    let output_errors_total = IntCounterVec::new(
        Opts::new(
            "hel_output_errors_total",
            "Output write errors (e.g. broken pipe, disk full)",
        ),
        &["source"],
    )?;
    let request_duration_seconds = prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(
            "hel_request_duration_seconds",
            "HTTP request duration in seconds by source",
        )
        .buckets(prometheus::exponential_buckets(0.05, 2.0, 10).unwrap()),
        &["source"],
    )?;
    let circuit_breaker_state = IntGaugeVec::new(
        Opts::new(
            "hel_circuit_breaker_state",
            "Circuit breaker state: 0=closed, 1=open, 2=half_open",
        ),
        &["source"],
    )?;
    let events_dropped_total = IntCounterVec::new(
        Opts::new(
            "hel_events_dropped_total",
            "Events dropped (e.g. due to backpressure) by source and reason",
        ),
        &["source", "reason"],
    )?;
    let pending_events = IntGaugeVec::new(
        Opts::new(
            "hel_pending_events",
            "Events currently queued for output by source",
        ),
        &["source"],
    )?;

    prometheus::register(Box::new(requests_total.clone()))?;
    prometheus::register(Box::new(events_emitted_total.clone()))?;
    prometheus::register(Box::new(errors_total.clone()))?;
    prometheus::register(Box::new(output_errors_total.clone()))?;
    prometheus::register(Box::new(events_dropped_total.clone()))?;
    prometheus::register(Box::new(pending_events.clone()))?;
    prometheus::register(Box::new(request_duration_seconds.clone()))?;
    prometheus::register(Box::new(circuit_breaker_state.clone()))?;

    let _ = METRICS.set(MetricsInner {
        requests_total,
        events_emitted_total,
        errors_total,
        output_errors_total,
        events_dropped_total,
        pending_events,
        request_duration_seconds,
        circuit_breaker_state,
    });
    Ok(())
}

/// Record one HTTP request (success or failure). status_class: "2xx", "3xx", "4xx", "5xx", "error".
pub fn record_request(source: &str, status_class: &str, duration_secs: f64) {
    if let Some(m) = METRICS.get() {
        m
            .requests_total
            .with_label_values(&[source, status_class])
            .inc();
        m
            .request_duration_seconds
            .with_label_values(&[source])
            .observe(duration_secs);
    }
}

/// Record events emitted for a source.
pub fn record_events(source: &str, count: u64) {
    if let Some(m) = METRICS.get() {
        m.events_emitted_total
            .with_label_values(&[source])
            .inc_by(count);
    }
}

/// Record one error for a source.
pub fn record_error(source: &str) {
    if let Some(m) = METRICS.get() {
        m.errors_total.with_label_values(&[source]).inc();
    }
}

/// Record one output write error (e.g. broken pipe, disk full).
pub fn record_output_error(source: &str) {
    if let Some(m) = METRICS.get() {
        m.output_errors_total.with_label_values(&[source]).inc();
    }
}

/// Record one event dropped (e.g. backpressure).
pub fn record_event_dropped(source: &str, reason: &str) {
    if let Some(m) = METRICS.get() {
        m
            .events_dropped_total
            .with_label_values(&[source, reason])
            .inc();
    }
}

/// Set pending (queued) event count for a source. Used by backpressure sink.
pub fn set_pending_events(source: &str, count: i64) {
    if let Some(m) = METRICS.get() {
        m.pending_events
            .with_label_values(&[source])
            .set(std::cmp::max(0, count));
    }
}

/// Set circuit breaker state for a source: "closed" => 0, "open" => 1, "half_open" => 2.
pub fn set_circuit_state(source: &str, state: CircuitStateValue) {
    if let Some(m) = METRICS.get() {
        let v = match state {
            CircuitStateValue::Closed => 0,
            CircuitStateValue::Open => 1,
            CircuitStateValue::HalfOpen => 2,
        };
        m.circuit_breaker_state.with_label_values(&[source]).set(v);
    }
}

#[derive(Clone, Copy)]
pub enum CircuitStateValue {
    Closed,
    Open,
    HalfOpen,
}

/// Encode all metrics in Prometheus text format. Returns empty string if metrics not initialized.
pub fn encode() -> String {
    if METRICS.get().is_none() {
        return String::new();
    }
    let families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    if encoder.encode(&families, &mut buffer).is_ok() {
        String::from_utf8_lossy(&buffer).into_owned()
    } else {
        String::new()
    }
}
