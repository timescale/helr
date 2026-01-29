//! Single poll tick: load state → fetch pages (link-header) → emit NDJSON → commit state.

use crate::circuit::{self, CircuitStore};
use crate::client::build_client;
use crate::config::{Config, PaginationConfig, SourceConfig};
use crate::dedupe::{self, DedupeStore};
use crate::event::EmittedEvent;
use crate::metrics;
use crate::oauth2::OAuth2TokenCache;
use crate::pagination::next_link_from_headers;
use crate::retry::execute_with_retry;
use crate::state::StateStore;
use anyhow::Context;
use chrono::Utc;
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;

/// Run one poll tick for all sources (or only those matching source_filter).
/// Sources are polled concurrently (one task per source).
pub async fn run_one_tick(
    config: &Config,
    store: Arc<dyn StateStore>,
    source_filter: Option<&str>,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
) -> anyhow::Result<()> {
    let mut handles = Vec::new();
    for (source_id, source) in &config.sources {
        if let Some(filter) = source_filter {
            if filter != source_id {
                continue;
            }
        }
        let store = store.clone();
        let source_id_key = source_id.clone();
        let source = source.clone();
        let circuit_store = circuit_store.clone();
        let token_cache = token_cache.clone();
        let dedupe_store = dedupe_store.clone();
        let h = tokio::spawn(async move {
            poll_one_source(
                store,
                &source_id_key,
                &source,
                circuit_store,
                token_cache,
                dedupe_store,
            )
            .await
        });
        handles.push((source_id, h));
    }
    for (source_id, h) in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                metrics::record_error(&source_id);
                tracing::error!(source = %source_id, "poll failed: {}", e);
            }
            Err(e) => {
                metrics::record_error(&source_id);
                tracing::error!(source = %source_id, "task join failed: {}", e);
            }
        }
    }
    Ok(())
}

#[instrument(skip(store, source, circuit_store, token_cache, dedupe_store))]
async fn poll_one_source(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
) -> anyhow::Result<()> {
    let client = build_client(source.resilience.as_ref())?;

    match &source.pagination {
        Some(PaginationConfig::LinkHeader { rel, max_pages }) => {
            poll_link_header(
                store,
                source_id,
                source,
                &client,
                rel.as_str(),
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dedupe_store,
            )
            .await
        }
        Some(PaginationConfig::Cursor {
            cursor_param,
            cursor_path,
            max_pages,
        }) => {
            poll_cursor_pagination(
                store,
                source_id,
                source,
                &client,
                cursor_param,
                cursor_path,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dedupe_store,
            )
            .await
        }
        Some(PaginationConfig::PageOffset {
            page_param,
            limit_param,
            limit,
            max_pages,
        }) => {
            poll_page_offset_pagination(
                store,
                source_id,
                source,
                &client,
                page_param,
                limit_param,
                *limit,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dedupe_store,
            )
            .await
        }
        _ => {
            return poll_single_page(
                store,
                source_id,
                source,
                &client,
                &source.url,
                circuit_store,
                token_cache,
                dedupe_store,
            )
            .await;
        }
    }
}

/// Link-header pagination: follow rel="next" until no more or max_pages.
async fn poll_link_header(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    rel: &str,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let mut url: String = store
        .get(source_id, "next_url")
        .await?
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| source.url.clone());

    let mut page = 0u32;
    let mut total_events = 0u64;
    let mut total_bytes: u64 = 0;
    let max_bytes = source.max_bytes;
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
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            &client,
            source,
            source_id,
            &url,
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, success).await;
                }
                let status = r.status().as_u16();
                metrics::record_request(
                    source_id,
                    status_class(status),
                    req_start.elapsed().as_secs_f64(),
                );
                r
            }
            Err(e) => {
                if let Some(cb) =
                    source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(
                    source_id,
                    "error",
                    req_start.elapsed().as_secs_f64(),
                );
                metrics::record_error(source_id);
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
        total_bytes += body.len() as u64;
        let events = parse_events_from_body(&body)?;

        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_field).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue; // duplicate
                }
            }
            total_events += 1;
            emitted_count += 1;
            let ts = event_ts(event_value);
            let emitted = EmittedEvent::new(
                ts,
                source_id.to_string(),
                path.clone(),
                event_value.clone(),
            );
            println!("{}", emitted.to_ndjson_line()?);
        }
        metrics::record_events(source_id, emitted_count);

        let hit_max_bytes = max_bytes.is_some() && total_bytes > max_bytes.unwrap();
        if let Some(next) = next_url {
            let absolute = base_url.join(&next).context("resolve next URL")?;
            store
                .set(source_id, "next_url", absolute.as_str())
                .await?;
            if hit_max_bytes {
                tracing::warn!(
                    source = %source_id,
                    total_bytes,
                    max_bytes = max_bytes.unwrap(),
                    "reached max_bytes limit, stopping pagination; next poll continues from saved cursor"
                );
                break;
            }
            url = absolute.to_string();
            tracing::debug!(
                source = %source_id,
                page = page,
                events = events.len(),
                total_bytes,
                "next page"
            );
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

/// Cursor-in-body pagination: get cursor from response JSON path, pass as query param on next request.
async fn poll_cursor_pagination(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    cursor_param: &str,
    cursor_path: &str,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
) -> anyhow::Result<()> {
    use reqwest::Url;
    let start = Instant::now();
    let base_url = source.url.as_str();
    let mut cursor: Option<String> = store
        .get(source_id, "cursor")
        .await?
        .filter(|s| !s.is_empty());
    let mut page = 0u32;
    let mut total_events = 0u64;
    let mut total_bytes: u64 = 0;
    let max_bytes = source.max_bytes;
    loop {
        page += 1;
        if page > max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
            break;
        }
        let url = if let Some(ref c) = cursor {
            let mut u = Url::parse(base_url).context("cursor pagination base url")?;
            u.query_pairs_mut().append_pair(cursor_param, c);
            u.to_string()
        } else {
            base_url.to_string()
        };
        if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            client,
            source,
            source_id,
            &url,
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, success).await;
                }
                metrics::record_request(
                    source_id,
                    status_class(r.status().as_u16()),
                    req_start.elapsed().as_secs_f64(),
                );
                r
            }
            Err(e) => {
                if let Some(cb) =
                    source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };
        if !response.status().is_success() {
            anyhow::bail!(
                "http {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let path = response.url().path().to_string();
        let body = response.text().await.context("read body")?;
        total_bytes += body.len() as u64;
        if let Some(limit) = max_bytes {
            if total_bytes > limit {
                tracing::warn!(
                    source = %source_id,
                    total_bytes,
                    max_bytes = limit,
                    "reached max_bytes limit, stopping cursor pagination"
                );
                break;
            }
        }
        let value: serde_json::Value =
            serde_json::from_str(&body).context("parse response json")?;
        let events = parse_events_from_value(&value)?;
        let next_cursor = json_path_str(&value, cursor_path).filter(|s| !s.is_empty());
        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_field).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue;
                }
            }
            total_events += 1;
            emitted_count += 1;
            let ts = event_ts(event_value);
            let emitted = EmittedEvent::new(
                ts,
                source_id.to_string(),
                path.clone(),
                event_value.clone(),
            );
            println!("{}", emitted.to_ndjson_line()?);
        }
        metrics::record_events(source_id, emitted_count);
        match next_cursor {
            Some(c) => {
                store.set(source_id, "cursor", &c).await?;
                cursor = Some(c);
                tracing::debug!(
                    source = %source_id,
                    page = page,
                    events = events.len(),
                    "next cursor page"
                );
            }
            None => {
                store.set(source_id, "cursor", "").await?;
                tracing::info!(
                    source = %source_id,
                    pages = page,
                    events = total_events,
                    duration_ms = start.elapsed().as_millis(),
                    "poll completed (cursor)"
                );
                break;
            }
        }
    }
    Ok(())
}

/// Page/offset pagination: increment page (or offset) each request, stop when empty or max_pages.
async fn poll_page_offset_pagination(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    client: &reqwest::Client,
    page_param: &str,
    limit_param: &str,
    limit: u32,
    max_pages: u32,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
) -> anyhow::Result<()> {
    use reqwest::Url;
    let start = Instant::now();
    let base_url = source.url.as_str();
    let mut total_events = 0u64;
    for page in 1..=max_pages {
        let mut u = Url::parse(base_url).context("page/offset base url")?;
        u.query_pairs_mut().append_pair(page_param, &page.to_string());
        u.query_pairs_mut().append_pair(limit_param, &limit.to_string());
        let url = u.to_string();
        if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
            circuit::allow_request(&circuit_store, source_id, cb)
                .await
                .context("circuit open")?;
        }
        let req_start = std::time::Instant::now();
        let response = match execute_with_retry(
            client,
            source,
            source_id,
            &url,
            source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
            source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
            Some(&token_cache),
        )
        .await
        {
            Ok(r) => {
                let success = r.status().as_u16() < 500;
                if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, success).await;
                }
                metrics::record_request(
                    source_id,
                    status_class(r.status().as_u16()),
                    req_start.elapsed().as_secs_f64(),
                );
                r
            }
            Err(e) => {
                if let Some(cb) =
                    source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref())
                {
                    circuit::record_result(&circuit_store, source_id, cb, false).await;
                }
                metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
                metrics::record_error(source_id);
                return Err(e).context("http request");
            }
        };
        if !response.status().is_success() {
            anyhow::bail!(
                "http {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
        let path = response.url().path().to_string();
        let body = response.text().await.context("read body")?;
        let events = parse_events_from_body(&body)?;
        let mut emitted_count = 0u64;
        for event_value in events.iter() {
            if let Some(d) = &source.dedupe {
                let id = event_id(event_value, &d.id_field).unwrap_or_default();
                if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                    continue;
                }
            }
            total_events += 1;
            emitted_count += 1;
            let ts = event_ts(event_value);
            let emitted = EmittedEvent::new(
                ts,
                source_id.to_string(),
                path.clone(),
                event_value.clone(),
            );
            println!("{}", emitted.to_ndjson_line()?);
        }
        metrics::record_events(source_id, emitted_count);
        if events.len() < limit as usize {
            tracing::info!(
                source = %source_id,
                pages = page,
                events = total_events,
                duration_ms = start.elapsed().as_millis(),
                "poll completed (page/offset)"
            );
            break;
        }
        if page == max_pages {
            tracing::warn!(source = %source_id, "reached max_pages {}", max_pages);
        }
    }
    store.set(source_id, "next_url", "").await?;
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
    token_cache: OAuth2TokenCache,
    dedupe_store: DedupeStore,
) -> anyhow::Result<()> {
    let start = Instant::now();
    if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
        circuit::allow_request(&circuit_store, source_id, cb)
            .await
            .context("circuit open")?;
    }
    let req_start = std::time::Instant::now();
    let response = match execute_with_retry(
        client,
        source,
        source_id,
        url,
        source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
        source.resilience.as_ref().and_then(|r| r.rate_limit.as_ref()),
        Some(&token_cache),
    )
    .await
    {
        Ok(r) => {
            let success = r.status().as_u16() < 500;
            if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
                circuit::record_result(&circuit_store, source_id, cb, success).await;
            }
            metrics::record_request(
                source_id,
                status_class(r.status().as_u16()),
                req_start.elapsed().as_secs_f64(),
            );
            r
        }
        Err(e) => {
            if let Some(cb) = source.resilience.as_ref().and_then(|r| r.circuit_breaker.as_ref()) {
                circuit::record_result(&circuit_store, source_id, cb, false).await;
            }
            metrics::record_request(source_id, "error", req_start.elapsed().as_secs_f64());
            metrics::record_error(source_id);
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

    let mut emitted_count = 0u64;
    for event_value in &events {
        if let Some(d) = &source.dedupe {
            let id = event_id(event_value, &d.id_field).unwrap_or_default();
            if dedupe::seen_and_add(&dedupe_store, source_id, id, d.capacity).await {
                continue;
            }
        }
        emitted_count += 1;
        let ts = event_ts(event_value);
        let emitted = EmittedEvent::new(
            ts,
            source_id.to_string(),
            path.clone(),
            event_value.clone(),
        );
        println!("{}", emitted.to_ndjson_line()?);
    }
    metrics::record_events(source_id, emitted_count);

    store.set(source_id, "next_url", "").await?;
    tracing::info!(
        source = %source_id,
        events = emitted_count,
        duration_ms = start.elapsed().as_millis(),
        "poll completed"
    );
    Ok(())
}

/// Parse response body: top-level array, or object with "items"/"data"/"events" array.
fn parse_events_from_body(body: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let value: serde_json::Value =
        serde_json::from_str(body).context("parse response json")?;
    parse_events_from_value(&value)
}

/// Extract events array from parsed JSON (same keys as parse_events_from_body).
fn parse_events_from_value(value: &serde_json::Value) -> anyhow::Result<Vec<serde_json::Value>> {
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
    Ok(vec![value.clone()])
}

/// Get string at dotted path in JSON (e.g. "next_cursor", "meta.next_page_token").
fn json_path_str(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut v = value;
    for segment in path.split('.') {
        v = v.get(segment)?;
    }
    v.as_str().map(|s| s.to_string())
}

/// Extract event ID from JSON using dotted path (e.g. "uuid", "id", "event.id").
fn event_id(event: &serde_json::Value, id_field: &str) -> Option<String> {
    let mut v = event;
    for segment in id_field.split('.') {
        v = v.get(segment)?;
    }
    v.as_str().map(|s| s.to_string())
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

fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_events_from_value_top_level_array() {
        let v = serde_json::json!([{"id": 1}, {"id": 2}]);
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn test_parse_events_from_value_items_key() {
        let v = serde_json::json!({"items": [{"id": 1}], "next_cursor": "abc"});
        let events = parse_events_from_value(&v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn test_json_path_str_simple() {
        let v = serde_json::json!({"next_cursor": "xyz"});
        assert_eq!(json_path_str(&v, "next_cursor"), Some("xyz".into()));
    }

    #[test]
    fn test_json_path_str_dotted() {
        let v = serde_json::json!({"meta": {"next_page_token": "token123"}});
        assert_eq!(json_path_str(&v, "meta.next_page_token"), Some("token123".into()));
    }

    #[test]
    fn test_json_path_str_missing() {
        let v = serde_json::json!({"items": []});
        assert_eq!(json_path_str(&v, "next_cursor"), None);
    }
}
