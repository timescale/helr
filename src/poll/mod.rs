//! Single poll tick: load state → fetch pages (link-header) → emit NDJSON → commit state.

mod cursor;
mod helpers;
#[cfg(feature = "hooks")]
mod hooks;
mod link_header;
mod page_offset;
mod parse;
mod single_page;
#[cfg(feature = "streaming")]
mod streaming;

use crate::circuit::CircuitStore;
use crate::client::build_client;
use crate::config::{Config, GlobalConfig, PaginationConfig, SourceConfig};
use crate::dedupe::DedupeStore;
use crate::dpop::DPoPKeyCache;
use crate::metrics;
use crate::oauth2::OAuth2TokenCache;
use crate::output::EventSink;
use crate::replay::RecordState;
use crate::state::StateStore;
use governor::{Quota, RateLimiter};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};

/// Type for client-side rate limiter (governor direct limiter).
type ClientRateLimiter = RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
>;

/// Shared store of last error message per source (for health endpoints).
pub type LastErrorStore = Arc<RwLock<HashMap<String, String>>>;

#[cfg(feature = "hooks")]
static HOOK_AUTH_CACHE: std::sync::LazyLock<crate::hooks::HookAuthCache> =
    std::sync::LazyLock::new(crate::hooks::new_hook_auth_cache);

/// Run one poll tick for all sources (or only those matching source_filter).
/// Sources are polled concurrently (one task per source).
#[allow(clippy::too_many_arguments)]
pub async fn run_one_tick(
    config: &Config,
    store: Arc<dyn StateStore>,
    source_filter: Option<&str>,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    last_errors: LastErrorStore,
    global_sources_semaphore: Option<Arc<Semaphore>>,
    under_load_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    skip_priority_below: Option<u32>,
) -> anyhow::Result<()> {
    let mut handles = Vec::new();
    for (source_id, source) in &config.sources {
        if let Some(filter) = source_filter
            && filter != source_id
        {
            continue;
        }
        if let (Some(flag), Some(threshold)) = (&under_load_flag, skip_priority_below)
            && flag.load(Ordering::Relaxed)
        {
            let priority = source.priority.unwrap_or(10);
            if priority < threshold {
                tracing::debug!(
                    source = %source_id,
                    priority,
                    threshold,
                    "load shedding: skipping low-priority source"
                );
                continue;
            }
        }
        let effective_request_cap = source
            .resilience
            .as_ref()
            .and_then(|r| r.bulkhead.as_ref())
            .and_then(|b| b.max_concurrent_requests)
            .or_else(|| {
                config
                    .global
                    .bulkhead
                    .as_ref()
                    .and_then(|b| b.max_concurrent_requests)
            });
        let request_semaphore = effective_request_cap
            .filter(|&n| n > 0)
            .map(|n| Arc::new(Semaphore::new(n as usize)));

        let store = store.clone();
        let source_id_key = source_id.clone();
        let source = source.clone();
        let global = config.global.clone();
        let circuit_store = circuit_store.clone();
        let token_cache = token_cache.clone();
        let dpop_key_cache = dpop_key_cache.clone();
        let dedupe_store = dedupe_store.clone();
        let event_sink = event_sink.clone();
        let record_state = record_state.clone();
        let global_sources_semaphore_clone = global_sources_semaphore.clone();
        let rate_limiter: Option<Arc<ClientRateLimiter>> = source
            .resilience
            .as_ref()
            .and_then(|r| r.rate_limit.as_ref())
            .and_then(|r| {
                let rps = r
                    .max_requests_per_second
                    .filter(|&x| x > 0.0 && x.is_finite())?;
                let rps_u = (rps.ceil() as u32).max(1);
                let rps_nz = NonZeroU32::new(rps_u)?;
                let burst = r.burst_size.unwrap_or(rps_u).max(1);
                let burst_nz = NonZeroU32::new(burst).unwrap_or(NonZeroU32::MIN);
                let quota = Quota::per_second(rps_nz).allow_burst(burst_nz);
                Some(Arc::new(RateLimiter::direct(quota)))
            });
        let poll_tick_secs = source
            .resilience
            .as_ref()
            .and_then(|r| r.timeouts.as_ref())
            .and_then(|t| t.poll_tick_secs);
        let h = tokio::spawn(async move {
            let _source_permit = match &global_sources_semaphore_clone {
                Some(s) => Some(
                    s.acquire()
                        .await
                        .map_err(|e| anyhow::anyhow!("bulkhead source acquire: {}", e))?,
                ),
                None => None,
            };
            let poll_fut = poll_one_source(
                store,
                &source_id_key,
                &source,
                &global,
                circuit_store,
                token_cache,
                dpop_key_cache,
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter,
                request_semaphore,
            );
            match poll_tick_secs {
                Some(secs) => match tokio::time::timeout(Duration::from_secs(secs), poll_fut).await
                {
                    Ok(inner) => inner,
                    Err(_) => Err(anyhow::anyhow!("poll tick timed out after {}s", secs)),
                },
                None => poll_fut.await,
            }
        });
        handles.push((source_id, h));
    }
    for (source_id, h) in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                metrics::record_error(source_id);
                let msg = format!("{:#}", e);
                last_errors
                    .write()
                    .await
                    .insert(source_id.clone(), msg.clone());
                tracing::error!(source = %source_id, "poll failed: {:#}", e);
                // Broken pipe to stdout is fatal: exit so caller can exit non-zero.
                if msg.to_lowercase().contains("broken pipe") {
                    return Err(e);
                }
            }
            Err(e) => {
                metrics::record_error(source_id);
                let msg = format!("{:#}", e);
                last_errors
                    .write()
                    .await
                    .insert(source_id.clone(), msg.clone());
                tracing::error!(source = %source_id, "task join failed: {:#}", e);
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(
    store,
    source,
    global,
    circuit_store,
    token_cache,
    dpop_key_cache,
    dedupe_store,
    event_sink,
    record_state,
    request_semaphore
))]
async fn poll_one_source(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    record_state: Option<Arc<RecordState>>,
    rate_limiter: Option<Arc<ClientRateLimiter>>,
    request_semaphore: Option<Arc<Semaphore>>,
) -> anyhow::Result<()> {
    #[cfg(feature = "hooks")]
    if let Some(ref global_hooks) = global.hooks
        && global_hooks.enabled
        && source.hooks.is_some()
    {
        let source_hooks = source.hooks.as_ref().unwrap();
        let script = match (&source_hooks.script_inline, &source_hooks.script) {
            (Some(inline), _) => inline.trim().to_string(),
            (_, Some(path)) => {
                let path_buf = crate::hooks::script_path(global_hooks, path)?;
                crate::hooks::load_script(&path_buf)?
            }
            (None, None) => anyhow::bail!(
                "hooks: set either script or script_inline for source {}",
                source_id
            ),
        };
        return hooks::poll_with_hooks(
            store,
            source_id,
            source,
            global,
            &script,
            global_hooks,
            source.hooks.as_ref().unwrap(),
            circuit_store,
            token_cache,
            dpop_key_cache,
            dedupe_store,
            event_sink,
            record_state,
            rate_limiter.as_ref(),
            request_semaphore,
        )
        .await;
    }

    let client = build_client(source.resilience.as_ref())?;

    match &source.pagination {
        Some(PaginationConfig::LinkHeader { rel, max_pages }) => {
            link_header::poll_link_header(
                store,
                source_id,
                source,
                global,
                &client,
                rel.as_str(),
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter.as_ref(),
                request_semaphore.clone(),
            )
            .await
        }
        Some(PaginationConfig::Cursor {
            cursor_param,
            cursor_path,
            max_pages,
        }) => {
            cursor::poll_cursor_pagination(
                store,
                source_id,
                source,
                global,
                &client,
                cursor_param,
                cursor_path,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter.as_ref(),
                request_semaphore.clone(),
            )
            .await
        }
        Some(PaginationConfig::PageOffset {
            page_param,
            limit_param,
            limit,
            max_pages,
        }) => {
            page_offset::poll_page_offset_pagination(
                store,
                source_id,
                source,
                global,
                &client,
                page_param,
                limit_param,
                *limit,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter.as_ref(),
                request_semaphore.clone(),
            )
            .await
        }
        Some(PaginationConfig::Offset {
            offset_param,
            limit_param,
            limit,
            max_pages,
        }) => {
            page_offset::poll_offset_pagination(
                store,
                source_id,
                source,
                global,
                &client,
                offset_param,
                limit_param,
                *limit,
                max_pages.unwrap_or(100),
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state,
                rate_limiter.as_ref(),
                request_semaphore.clone(),
            )
            .await
        }
        _ => {
            return single_page::poll_single_page(
                store,
                source_id,
                source,
                global,
                &client,
                &source.url,
                circuit_store,
                token_cache,
                dpop_key_cache.clone(),
                dedupe_store,
                event_sink,
                record_state.clone(),
                rate_limiter.as_ref(),
                request_semaphore,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::helpers::*;
    use super::parse::*;
    use crate::config::{GlobalConfig, SourceConfig};

    #[test]
    fn test_parse_events_from_value_top_level_array() {
        let v = serde_json::json!([{"id": 1}, {"id": 2}]);
        let events = parse_events_from_value(v).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn test_parse_events_from_value_items_key() {
        let v = serde_json::json!({"items": [{"id": 1}], "next_cursor": "abc"});
        let events = parse_events_from_value(v).unwrap();
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
        assert_eq!(
            json_path_str(&v, "meta.next_page_token"),
            Some("token123".into())
        );
    }

    #[test]
    fn test_json_path_str_missing() {
        let v = serde_json::json!({"items": []});
        assert_eq!(json_path_str(&v, "next_cursor"), None);
    }

    #[test]
    fn test_parse_events_from_value_empty_array() {
        let v = serde_json::json!([]);
        let events = parse_events_from_value(v).unwrap();
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_parse_events_from_value_logs_key() {
        let v = serde_json::json!({"logs": [{"id": "a"}], "next": "x"});
        let events = parse_events_from_value(v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!("a")));
    }

    #[test]
    fn test_parse_events_from_value_entries_key() {
        let v = serde_json::json!({"entries": [{"id": "e1"}], "nextPageToken": "tok"});
        let events = parse_events_from_value(v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!("e1")));
    }

    #[test]
    fn test_merge_cursor_into_body() {
        let body = serde_json::json!({"resourceNames": ["projects/my-project"], "pageSize": 100});
        let out = merge_cursor_into_body(Some(&body), "pageToken", "next-token");
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("pageToken"), Some(&serde_json::json!("next-token")));
        assert_eq!(obj.get("resourceNames"), body.get("resourceNames"));
        assert_eq!(obj.get("pageSize"), body.get("pageSize"));
    }

    #[test]
    fn test_merge_cursor_into_body_empty_base() {
        let out = merge_cursor_into_body(None, "pageToken", "tok");
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("pageToken"), Some(&serde_json::json!("tok")));
        assert_eq!(obj.len(), 1);
    }

    #[test]
    fn test_parse_events_from_value_single_value_fallback() {
        let v = serde_json::json!({"id": 42});
        let events = parse_events_from_value(v).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("id"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_url_with_first_request_params_sync() {
        let yaml = r#"
url: "https://example.com/logs"
from: "2024-01-01T00:00:00Z"
from_param: "since"
query_params:
  limit: 20
  sortOrder: "ASCENDING"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let url = url_with_first_request_params_sync("https://example.com/logs", &source).unwrap();
        assert!(url.contains("since=2024-01-01T00%3A00%3A00Z"));
        assert!(url.contains("limit=20"));
        assert!(url.contains("sortOrder=ASCENDING"));
    }

    #[test]
    fn test_url_with_first_request_params_only_query_params() {
        let yaml = r#"
url: "https://example.com/logs"
query_params:
  limit: "20"
  filter: "eventType eq \"user.session.start\""
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let url = url_with_first_request_params_sync("https://example.com/logs", &source).unwrap();
        assert!(url.contains("limit=20"));
        assert!(url.contains("filter="));
    }

    #[test]
    fn test_url_with_first_request_params_sync_state_takes_precedence_over_from() {
        let yaml = r#"
url: "https://example.com/logs"
from: "2024-01-01T00:00:00Z"
from_param: "since"
state:
  watermark_field: "id.time"
  watermark_param: "startTime"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let url = url_with_first_request_params_sync("https://example.com/logs", &source).unwrap();
        assert!(
            !url.contains("since="),
            "state takes precedence over from; sync has no store so no param added"
        );
    }

    #[test]
    fn test_update_max_timestamp_dotted_path() {
        let events = vec![
            serde_json::json!({"id": {"time": "1000"}}),
            serde_json::json!({"id": {"time": "3000"}}),
            serde_json::json!({"id": {"time": "2000"}}),
        ];
        let mut max_ts: Option<String> = None;
        update_max_timestamp(&mut max_ts, &events, "id.time");
        assert_eq!(max_ts.as_deref(), Some("3000"));
    }

    #[test]
    fn test_effective_source_label_default() {
        let yaml = r#"url: "https://example.com/logs""#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(effective_source_label(&source, "okta-audit"), "okta-audit");
    }

    #[test]
    fn test_effective_source_label_override() {
        let yaml = r#"
url: "https://example.com/logs"
source_label_value: "okta_audit"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(effective_source_label(&source, "okta-audit"), "okta_audit");
    }

    #[test]
    fn test_effective_source_label_key() {
        let mut global = GlobalConfig::default();
        let yaml = r#"url: "https://example.com/logs""#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(effective_source_label_key(&global, &source), "source");
        global.source_label_key = Some("service".to_string());
        assert_eq!(effective_source_label_key(&global, &source), "service");
        let yaml_override = r#"url: "https://example.com/logs"
source_label_key: "origin"
"#;
        let source_override: SourceConfig = serde_yaml_ng::from_str(yaml_override).unwrap();
        assert_eq!(
            effective_source_label_key(&global, &source_override),
            "origin"
        );
    }

    // --- Phase 1a tests: parse_events_from_body_for_source UTF-8 branching ---

    #[test]
    fn test_parse_body_from_slice_valid_utf8() {
        let yaml = r#"url: "https://example.com/logs""#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let body = br#"[{"id":1},{"id":2}]"#;
        let events = parse_events_from_body_for_source(body, &source).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["id"], serde_json::json!(1));
        assert_eq!(events[1]["id"], serde_json::json!(2));
    }

    #[test]
    fn test_parse_body_invalid_utf8_replace_uses_lossy_path() {
        let yaml = r#"
url: "https://example.com/logs"
on_invalid_utf8: replace
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        // Build JSON bytes with 0xFF byte inside a string value (invalid UTF-8).
        // With Replace, from_utf8_lossy converts 0xFF -> U+FFFD before JSON parsing.
        let mut body = Vec::new();
        body.extend_from_slice(b"[{\"msg\":\"hello ");
        body.push(0xFF);
        body.extend_from_slice(b" world\"}]");
        let events = parse_events_from_body_for_source(&body, &source);
        assert!(events.is_ok(), "should parse with lossy replacement");
        let events = events.unwrap();
        assert_eq!(events.len(), 1);
        let msg = events[0]["msg"].as_str().unwrap();
        assert!(msg.contains('\u{FFFD}'), "should contain replacement char");
    }

    #[test]
    fn test_parse_body_invalid_utf8_no_config_fails() {
        let yaml = r#"url: "https://example.com/logs""#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        // Without on_invalid_utf8, from_slice is used and requires valid UTF-8 JSON
        let body: Vec<u8> = vec![0xff, 0xfe];
        let result = parse_events_from_body_for_source(&body, &source);
        assert!(result.is_err(), "from_slice should reject non-JSON bytes");
    }

    // --- Phase 1b tests: ownership through paths + event_object_path ---

    #[test]
    fn test_parse_body_with_response_events_path() {
        let yaml = r#"
url: "https://example.com/api"
response_events_path: "data.records"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let body = br#"{"data":{"records":[{"id":"a"},{"id":"b"}]},"meta":"ignored"}"#;
        let events = parse_events_from_body_for_source(body, &source).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["id"], serde_json::json!("a"));
        assert_eq!(events[1]["id"], serde_json::json!("b"));
    }

    #[test]
    fn test_parse_body_with_event_object_path_graphql_edges() {
        let yaml = r#"
url: "https://example.com/graphql"
response_events_path: "data.edges"
response_event_object_path: "node"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let body = br#"{"data":{"edges":[{"node":{"id":1},"cursor":"c1"},{"node":{"id":2},"cursor":"c2"}]}}"#;
        let events = parse_events_from_body_for_source(body, &source).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], serde_json::json!({"id": 1}));
        assert_eq!(events[1], serde_json::json!({"id": 2}));
    }

    #[test]
    fn test_parse_body_with_event_object_path_deep_nested() {
        let yaml = r#"
url: "https://example.com/api"
response_events_path: "results"
response_event_object_path: "payload.data"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let body =
            br#"{"results":[{"payload":{"data":{"id":"x"}}},{"payload":{"data":{"id":"y"}}}]}"#;
        let events = parse_events_from_body_for_source(body, &source).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], serde_json::json!({"id": "x"}));
        assert_eq!(events[1], serde_json::json!({"id": "y"}));
    }

    #[test]
    fn test_parse_body_events_path_not_found_errors() {
        let yaml = r#"
url: "https://example.com/api"
response_events_path: "nonexistent.path"
"#;
        let source: SourceConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let body = br#"{"data":[{"id":1}]}"#;
        let result = parse_events_from_body_for_source(body, &source);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("did not resolve"),
            "error should mention path resolution: {err}"
        );
    }

    // --- Phase 1c tests: read_body_with_limit ---

    #[tokio::test]
    async fn test_read_body_with_limit_within_limit() {
        let server = wiremock::MockServer::start().await;
        let body = r#"[{"id":1},{"id":2}]"#;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let response = reqwest::get(server.uri()).await.unwrap();
        let result = read_body_with_limit(response, Some(1024)).await.unwrap();
        assert_eq!(result, body.as_bytes());
    }

    #[tokio::test]
    async fn test_read_body_with_limit_exceeds_limit() {
        let server = wiremock::MockServer::start().await;
        let body = "x".repeat(500);
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(&body))
            .mount(&server)
            .await;

        let response = reqwest::get(server.uri()).await.unwrap();
        let result = read_body_with_limit(response, Some(100)).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exceeds max_response_bytes"),
            "should report size exceeded: {err}"
        );
    }

    #[tokio::test]
    async fn test_read_body_with_limit_no_limit_reads_all() {
        let server = wiremock::MockServer::start().await;
        let body = "a]".repeat(5000);
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(&body))
            .mount(&server)
            .await;

        let response = reqwest::get(server.uri()).await.unwrap();
        let result = read_body_with_limit(response, None).await.unwrap();
        assert_eq!(result.len(), body.len());
    }
}

#[cfg(all(test, feature = "streaming"))]
mod streaming_equivalence_tests {
    use super::parse::*;
    use super::streaming;
    use crate::config::SourceConfig;

    fn standard_parse(body: &[u8], source: &SourceConfig) -> Vec<serde_json::Value> {
        parse_events_from_body_for_source(body, source).unwrap()
    }

    fn streaming_parse(body: &[u8], source: &SourceConfig) -> Vec<serde_json::Value> {
        let result = streaming::parse_streaming(body, source).unwrap();
        let obj_path = source.response_event_object_path.as_deref();
        result
            .iter(body)
            .filter_map(|r| {
                let v = r.unwrap();
                streaming::unwrap_event_object(v, obj_path)
            })
            .collect()
    }

    fn assert_equivalence(body: &[u8], source: &SourceConfig) {
        let std_events = standard_parse(body, source);
        let str_events = streaming_parse(body, source);
        assert_eq!(
            std_events, str_events,
            "streaming and standard parse must produce identical output"
        );
    }

    fn source(yaml: &str) -> SourceConfig {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[test]
    fn equiv_top_level_array() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"[{"id":1,"msg":"a"},{"id":2,"msg":"b"},{"id":3,"msg":"c"}]"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_empty_array() {
        let s = source(r#"url: "https://x.com""#);
        assert_equivalence(b"[]", &s);
    }

    #[test]
    fn equiv_single_element() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"[{"id":1}]"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_items_key() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"{"items":[{"id":1},{"id":2}],"cursor":"abc"}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_events_key() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"{"events":[{"id":"a"},{"id":"b"}],"total":2}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_data_key() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"{"data":[{"n":10}],"next":"page2"}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_logs_key() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"{"logs":[{"level":"info","msg":"ok"}]}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_entries_key() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"{"entries":[{"id":"e1"}],"nextPageToken":"t"}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_response_events_path_single_segment() {
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "records"
"#,
        );
        let body = br#"{"records":[{"id":1},{"id":2}],"meta":"ok"}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_response_events_path_nested() {
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "data.records"
"#,
        );
        let body = br#"{"data":{"records":[{"id":"a"},{"id":"b"}]},"cursor":"c"}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_response_events_path_deeply_nested() {
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "a.b.c"
"#,
        );
        let body = br#"{"a":{"b":{"c":[{"x":1},{"x":2},{"x":3}]}}}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_event_object_path_graphql() {
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "data.edges"
response_event_object_path: "node"
"#,
        );
        let body = br#"{"data":{"edges":[{"node":{"id":1},"cursor":"c1"},{"node":{"id":2},"cursor":"c2"}]}}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_event_object_path_deep() {
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "results"
response_event_object_path: "payload.data"
"#,
        );
        let body =
            br#"{"results":[{"payload":{"data":{"id":"x"}}},{"payload":{"data":{"id":"y"}}}]}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_whitespace_and_formatting() {
        let s = source(r#"url: "https://x.com""#);
        let body = b"  \n  [  { \"id\" : 1 }  ,  { \"id\" : 2 }  ]  \n  ";
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_nested_arrays_in_events() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"[{"id":1,"tags":["a","b"]},{"id":2,"tags":[]}]"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_nested_objects_in_events() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"[{"id":1,"meta":{"k":"v"}},{"id":2,"deep":{"a":{"b":true}}}]"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_strings_with_escapes() {
        let s = source(r#"url: "https://x.com""#);
        let body = br#"[{"msg":"hello \"world\""},{"msg":"line1\nline2"}]"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_cursor_metadata_extraction() {
        let body = br#"{"cursor":"next123","events":[{"id":1},{"id":2}],"total":2}"#;
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "events"
"#,
        );
        let result = streaming::parse_streaming(body, &s).unwrap();
        let meta = result.metadata();
        assert_eq!(meta["cursor"], "next123");
        assert_eq!(meta["total"], 2);
        assert!(meta["events"].as_array().unwrap().is_empty());
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_large_event_count() {
        let s = source(r#"url: "https://x.com""#);
        let events: Vec<serde_json::Value> = (0..500)
            .map(|i| serde_json::json!({"id": i, "name": format!("event_{}", i)}))
            .collect();
        let body = serde_json::to_vec(&events).unwrap();
        assert_equivalence(&body, &s);
    }

    #[test]
    fn equiv_on_invalid_utf8_replace() {
        let s = source(
            r#"
url: "https://x.com"
on_invalid_utf8: replace
"#,
        );
        let mut body = Vec::new();
        body.extend_from_slice(b"[{\"msg\":\"hello ");
        body.push(0xFF);
        body.extend_from_slice(b" world\"}]");
        assert_equivalence(&body, &s);
    }

    #[test]
    fn equiv_events_with_cursor_metadata() {
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "data.AndromedaEvents.edges"
response_event_object_path: "node"
"#,
        );
        let body = br#"{"data":{"AndromedaEvents":{"edges":[{"node":{"id":"e1","ts":"2024-01-01"},"cursor":"c1"},{"node":{"id":"e2","ts":"2024-01-02"},"cursor":"c2"}],"pageInfo":{"hasNextPage":true,"endCursor":"c2"}}}}"#;
        assert_equivalence(body, &s);
    }

    #[test]
    fn equiv_empty_events_with_metadata() {
        let s = source(
            r#"
url: "https://x.com"
response_events_path: "events"
"#,
        );
        let body = br#"{"events":[],"cursor":null,"total":0}"#;
        assert_equivalence(body, &s);
    }
}
