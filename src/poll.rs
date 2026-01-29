//! Single poll tick: load state → fetch pages (link-header) → emit NDJSON → commit state.

use crate::circuit::{self, CircuitStore};
use crate::client::build_client;
use crate::config::{Config, PaginationConfig, SourceConfig};
use crate::event::EmittedEvent;
use crate::pagination::next_link_from_headers;
use crate::retry::execute_with_retry;
use crate::state::StateStore;
use anyhow::Context;
use chrono::Utc;
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;

/// Run one poll tick for all sources (or only those matching source_filter).
pub async fn run_one_tick(
    config: &Config,
    store: Arc<dyn StateStore>,
    source_filter: Option<&str>,
    circuit_store: CircuitStore,
) -> anyhow::Result<()> {
    for (source_id, source) in &config.sources {
        if let Some(filter) = source_filter {
            if filter != source_id {
                continue;
            }
        }
        if let Err(e) =
            poll_one_source(store.clone(), source_id, source, circuit_store.clone()).await
        {
            tracing::error!(source = %source_id, "poll failed: {}", e);
            // continue with other sources
        }
    }
    Ok(())
}

#[instrument(skip(store, source, circuit_store))]
async fn poll_one_source(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    circuit_store: CircuitStore,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let client = build_client(source.resilience.as_ref())?;

    let (rel, max_pages) = match &source.pagination {
        Some(PaginationConfig::LinkHeader { rel, max_pages }) => {
            (rel.as_str(), max_pages.unwrap_or(100))
        }
        _ => {
            // No link-header pagination: single request
            return poll_single_page(store, source_id, source, &client, &source.url, circuit_store)
                .await;
        }
    };

    let mut url: String = store
        .get(source_id, "next_url")
        .await?
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| source.url.clone());

    let mut page = 0u32;
    let mut total_events = 0u64;
    loop {
        page += 1;
        if page > max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
            break;
        }

        if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        let response = match execute_with_retry(
            &client,
            source,
            &url,
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, success).await;
                }
                r
            }
            Err(e) => {
                if let Some(cb) =
                    source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                return Err(e).context("http request");
            }
        };

        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("http {} {}", status, response.text().await.unwrap_or_default());
        }

        let next_url = next_link_from_headers(response.headers(), rel);
        let base_url = response.url().clone();
        let path = base_url.path().to_string();
        let body = response.text().await.context("read body")?;
        let events = parse_events_from_body(&body)?;

        total_events += events.len() as u64;
        for event_value in events.iter() {
            let ts = event_ts(event_value);
            let emitted = EmittedEvent::new(
                ts,
                source_id.to_string(),
                path.clone(),
                event_value.clone(),
            );
            println!("{}", emitted.to_ndjson_line()?);
        }

        if let Some(next) = next_url {
            let absolute = base_url.join(&next).context("resolve next URL")?;
            store
                .set(source_id, "next_url", absolute.as_str())
                .await?;
            url = absolute.to_string();
            tracing::debug!(source = %source_id, page = page, events = events.len(), "next page");
        } else {
            store.set(source_id, "next_url", "").await?;
            tracing::info!(
                source = %source_id,
                pages = page,
                events = total_events,
                duration_ms = start.elapsed().as_millis(),
                "poll completed"
            );
            break;
        }
    }

    Ok(())
}

/// Single page (no pagination loop): one GET, emit events, clear next_url.
async fn poll_single_page(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    url: &str,
    circuit_store: CircuitStore,
) -> anyhow::Result<()> {
    let start = Instant::now();
    if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
        circuit::allow_request(&circuit_store, source_id, cb)
            .await
            .context("circuit open")?;
    }
    let response = match execute_with_retry(
        client,
        source,
        url,
        source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
        source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
    )
    .await
    {
        Ok(r) => {
            let success = r.status().as_u16() < 500;
            if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
                circuit::record_result(&circuit_store, source_id, cb, success).await;
            }
            r
        }
        Err(e) => {
            if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
                circuit::record_result(&circuit_store, source_id, cb, false).await;
            }
            return Err(e).context("http request");
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("http {} {}", status, body);
    }

    let path = response.url().path().to_string();
    let body = response.text().await.context("read body")?;
    let events = parse_events_from_body(&body)?;

    for event_value in &events {
        let ts = event_ts(event_value);
        let emitted = EmittedEvent::new(
            ts,
            source_id.to_string(),
            path.clone(),
            event_value.clone(),
        );
        println!("{}", emitted.to_ndjson_line()?);
    }

    store.set(source_id, "next_url", "").await?;
    let events_count = events.len();
    tracing::info!(
        source = %source_id,
        events = events_count,
        duration_ms = start.elapsed().as_millis(),
        "poll completed"
    );
    Ok(())
}

/// Parse response body: top-level array, or object with "items"/"data"/"events" array.
fn parse_events_from_body(body: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("parse response json")?;
    if let Some(arr) = value.as_array() {
        return Ok(arr.clone());
    }
    if let Some(obj) = value.as_object() {
        for key in &["items", "data", "events", "logs"] {
            if let Some(v) = obj.get(*key) {
                if let Some(arr) = v.as_array() {
                    return Ok(arr.clone());
                }
            }
        }
    }
    Ok(vec![value])
}

fn event_ts(event: &serde_json::Value) -> String {
    let s = event
        .get("published")
        .or_else(|| event.get("timestamp"))
        .or_else(|| event.get("ts"))
        .or_else(|| event.get("created_at"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if s.is_empty() {
        Utc::now().to_rfc3339()
    } else {
        s
    }
}
