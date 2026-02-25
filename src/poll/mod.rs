//! Single poll tick: load state → fetch pages (link-header) → emit NDJSON → commit state.

mod cursor;
mod helpers;
#[cfg(feature = "hooks")]
mod hooks;
mod link_header;
mod page_offset;
mod parse;
mod single_page;

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
}
