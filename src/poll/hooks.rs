use crate::circuit::CircuitStore;
use crate::client::build_client;
use crate::config::{GlobalConfig, HooksConfig, HttpMethod, SourceConfig, SourceHooksConfig};
use crate::dedupe::{self, DedupeStore};
use crate::dpop::DPoPKeyCache;
use crate::event::EmittedEvent;
use crate::hooks::{
    GetAuthResult, HookContext, HookEvent, HookRequest, HookResponse, call_build_request,
    call_commit_state, call_get_auth, call_get_next_page, call_parse_response,
};
use crate::metrics;
use crate::oauth2::OAuth2TokenCache;
use crate::output::EventSink;
use crate::replay::RecordState;
use crate::retry::execute_with_retry;
use crate::state::StateStore;
use anyhow::Context;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use super::ClientRateLimiter;
use super::HOOK_AUTH_CACHE;
use super::helpers::*;
use super::parse::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn poll_with_hooks(
    store: Arc<dyn StateStore>,
    source_id: &str,
    source: &SourceConfig,
    global: &GlobalConfig,
    script: &str,
    hooks_config: &HooksConfig,
    _source_hooks: &SourceHooksConfig,
    _circuit_store: CircuitStore,
    token_cache: OAuth2TokenCache,
    dpop_key_cache: Option<DPoPKeyCache>,
    dedupe_store: DedupeStore,
    event_sink: Arc<dyn EventSink>,
    _record_state: Option<Arc<RecordState>>,
    rate_limiter: Option<&Arc<ClientRateLimiter>>,
    request_semaphore: Option<Arc<Semaphore>>,
) -> anyhow::Result<()> {
    let client = build_client(source.resilience.as_ref())?;
    let max_pages = 100u32;
    let mut url = store
        .get(source_id, "next_url")
        .await?
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| source.url.clone());
    let mut body_override: Option<serde_json::Value> = source.body.clone();
    let mut all_events: Vec<HookEvent> = Vec::new();
    let path_for_emit = url.clone();
    let label = effective_source_label(source, source_id);

    // Call getAuth once per poll (not per page). Results are cached across polls with a TTL.
    let auth_result: Option<GetAuthResult> = {
        let ttl = Duration::from_secs(hooks_config.auth_cache_ttl_secs);
        let cached = if ttl.is_zero() {
            None
        } else {
            let guard = HOOK_AUTH_CACHE.read().await;
            guard.get(source_id).and_then(|(result, fetched_at)| {
                if fetched_at.elapsed() < ttl {
                    Some(Some(result.clone()))
                } else {
                    None
                }
            })
        };
        if let Some(hit) = cached {
            tracing::debug!(source = %source_id, "using cached getAuth result");
            hit
        } else {
            let init_state_keys = store.list_keys(source_id).await?;
            let mut init_state = HashMap::new();
            for k in &init_state_keys {
                if let Some(v) = store.get(source_id, k).await? {
                    init_state.insert(k.clone(), v);
                }
            }
            let init_ctx = HookContext {
                env: std::env::vars().collect(),
                state: init_state,
                request_id: format!("helr-{}", Utc::now().timestamp_nanos_opt().unwrap_or(0)),
                source_id: source_id.to_string(),
                default_since: source.from.clone(),
                pagination: None,
                headers: source.headers.clone(),
            };
            let result = call_get_auth(script, &init_ctx, hooks_config).await?;
            if !ttl.is_zero()
                && let Some(ref r) = result
            {
                let mut guard = HOOK_AUTH_CACHE.write().await;
                guard.insert(source_id.to_string(), (r.clone(), Instant::now()));
                tracing::debug!(source = %source_id, ttl_secs = ttl.as_secs(), "cached getAuth result");
            }
            result
        }
    };

    for _page in 1..=max_pages {
        if let Some(limiter) = rate_limiter {
            limiter.until_ready().await;
        }
        let _permit = match &request_semaphore {
            Some(s) => Some(
                s.acquire()
                    .await
                    .map_err(|e| anyhow::anyhow!("bulkhead: {}", e))?,
            ),
            None => None,
        };

        let state_keys = store.list_keys(source_id).await?;
        let mut state_map = HashMap::new();
        for k in &state_keys {
            if let Some(v) = store.get(source_id, k).await? {
                state_map.insert(k.clone(), v);
            }
        }
        let mut pagination = HashMap::new();
        if let Some(c) = state_map.get("cursor") {
            pagination.insert("lastCursor".to_string(), c.clone());
        }
        let ctx = HookContext {
            env: std::env::vars().collect(),
            state: state_map,
            request_id: format!("helr-{}", Utc::now().timestamp_nanos_opt().unwrap_or(0)),
            source_id: source_id.to_string(),
            default_since: source.from.clone(),
            pagination: if pagination.is_empty() {
                None
            } else {
                Some(pagination)
            },
            headers: source.headers.clone(),
        };

        let build_result = call_build_request(script, &ctx, hooks_config).await?;
        let (final_url, final_body) = if let Some(ref br) = build_result {
            let u = br.url.as_deref().unwrap_or(url.as_str());
            let b = br.body.clone().or_else(|| body_override.clone());
            (u.to_string(), b)
        } else {
            (url.clone(), body_override.clone())
        };

        let auth_has_concrete = auth_result.as_ref().is_some_and(|ar| {
            ar.headers.is_some() || ar.cookie.is_some() || ar.body.is_some() || ar.query.is_some()
        });
        let response = if build_result.as_ref().and_then(|r| r.url.as_ref()).is_some()
            || build_result
                .as_ref()
                .and_then(|r| r.headers.as_ref())
                .is_some()
            || auth_has_concrete
        {
            let method = source.method;
            let mut request_url = final_url.clone();
            {
                let mut u = reqwest::Url::parse(&request_url).context("parse url")?;
                if let Some(ref ar) = auth_result
                    && let Some(ref q) = ar.query
                {
                    for (k, v) in q {
                        u.query_pairs_mut().append_pair(k, v);
                    }
                }
                if let Some(ref br) = build_result
                    && let Some(ref q) = br.query
                {
                    for (k, v) in q {
                        u.query_pairs_mut().append_pair(k, v);
                    }
                }
                request_url = u.to_string();
            }
            let mut req = match method {
                HttpMethod::Get => client.get(&request_url),
                HttpMethod::Post => client.post(&request_url),
            };
            if let Some(headers) = &source.headers {
                for (k, v) in headers {
                    if let (Ok(name), Ok(val)) = (
                        reqwest::header::HeaderName::try_from(k.as_str()),
                        reqwest::header::HeaderValue::try_from(v.as_str()),
                    ) {
                        req = req.header(name, val);
                    }
                }
            }
            if let Some(ref ar) = auth_result {
                if let Some(ref h) = ar.headers {
                    for (k, v) in h {
                        if let (Ok(name), Ok(val)) = (
                            reqwest::header::HeaderName::try_from(k.as_str()),
                            reqwest::header::HeaderValue::try_from(v.as_str()),
                        ) {
                            req = req.header(name, val);
                        }
                    }
                }
                if let Some(ref cookie) = ar.cookie
                    && let (Ok(name), Ok(val)) = (
                        reqwest::header::HeaderName::try_from("cookie"),
                        reqwest::header::HeaderValue::try_from(cookie.as_str()),
                    )
                {
                    req = req.header(name, val);
                }
            }
            if let Some(ref br) = build_result
                && let Some(ref h) = br.headers
            {
                for (k, v) in h {
                    if let (Ok(name), Ok(val)) = (
                        reqwest::header::HeaderName::try_from(k.as_str()),
                        reqwest::header::HeaderValue::try_from(v.as_str()),
                    ) {
                        req = req.header(name, val);
                    }
                }
            }
            let body_to_send = build_result
                .as_ref()
                .and_then(|br| br.body.clone())
                .or_else(|| auth_result.as_ref().and_then(|ar| ar.body.clone()))
                .or_else(|| final_body.clone())
                .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
            if matches!(method, HttpMethod::Post) {
                req = req.json(&body_to_send);
            }
            let req = req.build().context("build request")?;
            client.execute(req).await.context("http request")?
        } else {
            execute_with_retry(
                &client,
                source,
                source_id,
                &final_url,
                final_body.as_ref(),
                source.resilience.as_ref().and_then(|r| r.retries.as_ref()),
                source
                    .resilience
                    .as_ref()
                    .and_then(|r| r.rate_limit.as_ref()),
                Some(&token_cache),
                dpop_key_cache.as_ref(),
                global.audit.as_ref(),
            )
            .await?
        };

        let status = response.status().as_u16();
        let headers_map: HashMap<String, String> = response
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
            .collect();
        let body_bytes = read_body_with_limit(response, source.max_response_bytes).await?;
        let body_str = String::from_utf8_lossy(&body_bytes).to_string();
        let body_json: serde_json::Value =
            serde_json::from_str(&body_str).unwrap_or(serde_json::Value::String(body_str.clone()));
        let hook_response = HookResponse {
            status,
            headers: headers_map,
            body: body_json,
        };

        let events: Vec<HookEvent> =
            match call_parse_response(script, &ctx, &hook_response, hooks_config).await? {
                ev if !ev.is_empty() => ev,
                _ => {
                    let parsed = parse_events_from_body_for_source(&body_bytes, source)?;
                    parsed
                        .into_iter()
                        .map(|event_value| {
                            let ts = event_ts_with_field(
                                &event_value,
                                source
                                    .transform
                                    .as_ref()
                                    .and_then(|t| t.timestamp_field.as_deref()),
                            );
                            HookEvent {
                                ts,
                                source: label.clone(),
                                event: event_value,
                                meta: None,
                            }
                        })
                        .collect()
                }
            };

        for he in &events {
            if let Some(d) = &source.dedupe {
                let id = he
                    .meta
                    .as_ref()
                    .and_then(|m| m.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !id.is_empty()
                    && dedupe::seen_and_add(&dedupe_store, source_id, id.to_string(), d.capacity)
                        .await
                {
                    continue;
                }
            }
            let meta_cursor = he
                .meta
                .as_ref()
                .and_then(|m| m.get("cursor"))
                .and_then(|v| v.as_str());
            let mut emitted = EmittedEvent::new(
                he.ts.clone(),
                he.source.clone(),
                path_for_emit.clone(),
                he.event.clone(),
            );
            if let Some(c) = meta_cursor {
                emitted = emitted.with_cursor(c.to_string());
            }
            emitted = emitted.with_request_id(ctx.request_id.clone());
            emit_event_line(global, source_id, source, &event_sink, &emitted)?;
            metrics::record_events(source_id, 1);
        }
        all_events.extend(events);

        let hook_request = HookRequest {
            url: final_url.clone(),
            body: final_body.clone(),
        };
        let next =
            call_get_next_page(script, &ctx, &hook_request, &hook_response, hooks_config).await?;
        match next {
            Some(n) if n.url.is_some() || n.body.is_some() => {
                if let Some(u) = n.url {
                    url = u;
                }
                if let Some(b) = n.body {
                    body_override = Some(b);
                }
            }
            _ => break,
        }
    }

    let state_keys = store.list_keys(source_id).await?;
    let mut state_map = HashMap::new();
    for k in &state_keys {
        if let Some(v) = store.get(source_id, k).await? {
            state_map.insert(k.clone(), v);
        }
    }
    let mut pagination = HashMap::new();
    if let Some(c) = state_map.get("cursor") {
        pagination.insert("lastCursor".to_string(), c.clone());
    }
    let commit_ctx = HookContext {
        env: std::env::vars().collect(),
        state: state_map,
        request_id: format!("helr-{}", Utc::now().timestamp_nanos_opt().unwrap_or(0)),
        source_id: source_id.to_string(),
        default_since: source.from.clone(),
        pagination: if pagination.is_empty() {
            None
        } else {
            Some(pagination)
        },
        headers: source.headers.clone(),
    };
    let to_commit = call_commit_state(script, &commit_ctx, &all_events, hooks_config).await?;
    for (key, value) in to_commit {
        store_set_or_skip(&store, source_id, source, global, &key, &value).await?;
    }
    Ok(())
}
