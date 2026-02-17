//! Optional JS hooks (Boa): buildRequest, parseResponse, getNextPage, commitState.
//! Sandbox: timeout per call; no file system (require). When boa_runtime is built with the fetch feature, fetch() is available (subject to hook timeout).

use crate::config::HooksConfig;
use anyhow::{Context as AnyhowContext, bail};
use boa_engine::{Context, JsError, JsResult, JsValue, Source};
use boa_gc::{Finalize, Trace};
use boa_runtime::Console;
use boa_runtime::console::{ConsoleState, Logger};
use boa_runtime::extensions::FetchExtension;
use boa_runtime::fetch::BlockingReqwestFetcher;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::task;

/// Resolved path to the hook script (absolute or relative to cwd). Use when loading from file; pass the `script` path string.
pub fn script_path(global: &HooksConfig, script: &str) -> anyhow::Result<std::path::PathBuf> {
    let script = script.trim();
    if script.starts_with('/')
        || (script.len() > 1 && (script.starts_with(".\\") || script.starts_with("./")))
    {
        return Ok(Path::new(script).to_path_buf());
    }
    let base = global
        .path
        .as_deref()
        .filter(|p| !p.is_empty())
        .unwrap_or("./hooks");
    Ok(Path::new(base).join(script))
}

/// Load script source from path.
pub fn load_script(path: &Path) -> anyhow::Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("read hook script: {}", path.display()))
}

/// Context object passed to hooks: env (env vars), state (state store snapshot), requestId, sourceId, defaultSince, pagination, headers (source-configured headers).
#[derive(Debug, Clone, Serialize)]
pub struct HookContext {
    pub env: HashMap<String, String>,
    pub state: HashMap<String, String>,
    pub request_id: String,
    pub source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<HashMap<String, String>>,
    /// Source-configured headers (e.g. User-Agent, Referer). Hooks can reuse them for fetch() in getAuth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

/// Request override from buildRequest: url (optional), headers, query, body.
#[derive(Debug, Clone, Default)]
pub struct BuildRequestResult {
    pub url: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub query: Option<HashMap<String, String>>,
    pub body: Option<serde_json::Value>,
}

/// Response object passed to parseResponse / getNextPage: status, headers, body (parsed JSON).
#[derive(Debug, Clone, Serialize)]
pub struct HookResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: serde_json::Value,
}

/// Request that was sent (url and optional body). Passed to getNextPage so the hook can derive the next page (e.g. offset).
#[derive(Debug, Clone, Serialize)]
pub struct HookRequest {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
}

/// One event from parseResponse: ts, source, event, meta (optional).
#[derive(Debug, Clone)]
pub struct HookEvent {
    pub ts: String,
    pub source: String,
    pub event: serde_json::Value,
    pub meta: Option<serde_json::Value>,
}

/// Next page from getNextPage: url and/or body for the next request; null means no more pages.
#[derive(Debug, Clone)]
pub struct GetNextPageResult {
    pub url: Option<String>,
    pub body: Option<serde_json::Value>,
}

/// Auth result from getAuth(ctx): headers, cookie, body, and/or query to merge into the request. All values come from the hook (e.g. from ctx.env or from fetch()).
#[derive(Debug, Clone, Default)]
pub struct GetAuthResult {
    pub headers: Option<HashMap<String, String>>,
    pub cookie: Option<String>,
    pub body: Option<serde_json::Value>,
    pub query: Option<HashMap<String, String>>,
}

/// State to commit from commitState: key-value pairs to write to the state store.
pub type CommitStateResult = HashMap<String, String>;

/// Logger that forwards console.log/info/warn/error to Helr's tracing (JSON logs).
#[derive(Debug, Trace, Finalize)]
struct TracingLogger;

impl Logger for TracingLogger {
    fn log(&self, msg: String, _state: &ConsoleState, _context: &mut Context) -> JsResult<()> {
        tracing::info!(hook_console = %msg);
        Ok(())
    }
    fn info(&self, msg: String, _state: &ConsoleState, _context: &mut Context) -> JsResult<()> {
        tracing::info!(hook_console = %msg);
        Ok(())
    }
    fn warn(&self, msg: String, _state: &ConsoleState, _context: &mut Context) -> JsResult<()> {
        tracing::warn!(hook_console = %msg);
        Ok(())
    }
    fn error(&self, msg: String, _state: &ConsoleState, _context: &mut Context) -> JsResult<()> {
        tracing::error!(hook_console = %msg);
        Ok(())
    }
}

/// Run script, call fn_name with args (serialized as JSON), return result as JSON. Runs in spawn_blocking with timeout.
/// Boa types are not Send, so we convert to serde_json/String inside the blocking task.
/// When enable_fetch is true, the fetch() Web API is registered (subject to timeout).
async fn run_hook(
    script: &str,
    fn_name: &str,
    args_json: Vec<serde_json::Value>,
    timeout: Duration,
    enable_fetch: bool,
) -> anyhow::Result<Option<serde_json::Value>> {
    let script = script.to_string();
    let fn_name_owned = fn_name.to_string();
    let join_handle = task::spawn_blocking(move || {
        let mut context = Context::default();
        Console::register_with_logger(TracingLogger, &mut context)
            .map_err(|e: JsError| e.to_string())?;
        if enable_fetch {
            boa_runtime::register_extensions(
                (FetchExtension(BlockingReqwestFetcher::default()),),
                None,
                &mut context,
            )
            .map_err(|e: JsError| e.to_string())?;
        }
        context
            .eval(Source::from_bytes(script.as_bytes()))
            .map_err(|e: JsError| e.to_string())?;
        let global = context.global_object();
        let func_val = global
            .get(boa_engine::js_string!(fn_name_owned.as_str()), &mut context)
            .map_err(|e: JsError| e.to_string())?;
        let Some(func) = func_val.as_callable() else {
            return Ok(None);
        };
        let args: Vec<JsValue> = args_json
            .iter()
            .map(|v| JsValue::from_json(v, &mut context))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: JsError| e.to_string())?;
        let result = func
            .call(&JsValue::null(), &args, &mut context)
            .map_err(|e: JsError| e.to_string())?;
        let value = if let Some(promise) = result.as_promise() {
            promise
                .await_blocking(&mut context)
                .map_err(|e: JsError| e.to_string())?
        } else {
            result
        };
        let json = value
            .to_json(&mut context)
            .map_err(|e: JsError| e.to_string())?;
        Ok::<_, String>(json)
    });
    let guard = tokio::time::timeout(timeout, join_handle);
    match guard.await {
        Ok(Ok(Ok(v))) => Ok(v),
        Ok(Ok(Err(e))) => {
            // Use first line only so we don't duplicate Boa stack trace in logs
            let msg = e.lines().next().unwrap_or(e.as_str()).trim();
            bail!("hook {} error: {}", fn_name, msg)
        }
        Ok(Err(e)) => bail!("hook {} task join: {}", fn_name, e),
        Err(_) => bail!("hook {} timed out after {:?}", fn_name, timeout),
    }
}

/// Call getAuth(ctx). Returns null or { headers?, cookie?, body?, query? }. Hook can use ctx.env and fetch() (when allow_network) and return concrete auth.
pub async fn call_get_auth(
    script: &str,
    ctx: &HookContext,
    hooks_config: &HooksConfig,
) -> anyhow::Result<Option<GetAuthResult>> {
    let timeout = Duration::from_secs(hooks_config.timeout_secs);
    let ctx_json = serde_json::to_value(ctx).context("serialize hook ctx")?;
    let result = run_hook(
        script,
        "getAuth",
        vec![ctx_json],
        timeout,
        hooks_config.allow_network,
    )
    .await?;
    let Some(obj) = result.and_then(|v| v.as_object().cloned()) else {
        return Ok(None);
    };
    let mut out = GetAuthResult::default();
    if let Some(m) = obj.get("headers").and_then(|v| v.as_object()) {
        let mut headers = HashMap::new();
        for (k, v) in m {
            if let Some(s) = v.as_str() {
                headers.insert(k.clone(), s.to_string());
            }
        }
        out.headers = Some(headers);
    }
    if let Some(s) = obj.get("cookie").and_then(|v| v.as_str()) {
        out.cookie = Some(s.to_string());
    }
    if let Some(v) = obj.get("body").cloned() {
        out.body = Some(v);
    }
    if let Some(m) = obj.get("query").and_then(|v| v.as_object()) {
        let mut query = HashMap::new();
        for (k, v) in m {
            if let Some(s) = v.as_str() {
                query.insert(k.clone(), s.to_string());
            }
        }
        out.query = Some(query);
    }
    let has_any =
        out.headers.is_some() || out.cookie.is_some() || out.body.is_some() || out.query.is_some();
    Ok(if has_any { Some(out) } else { None })
}

/// Call buildRequest(ctx). Returns object with url?, headers?, query?, body?.
pub async fn call_build_request(
    script: &str,
    ctx: &HookContext,
    hooks_config: &HooksConfig,
) -> anyhow::Result<Option<BuildRequestResult>> {
    let timeout = Duration::from_secs(hooks_config.timeout_secs);
    let ctx_json = serde_json::to_value(ctx).context("serialize hook ctx")?;
    let result = run_hook(
        script,
        "buildRequest",
        vec![ctx_json],
        timeout,
        hooks_config.allow_network,
    )
    .await?;
    let Some(obj) = result.and_then(|v| v.as_object().cloned()) else {
        return Ok(None);
    };
    let mut out = BuildRequestResult::default();
    if let Some(v) = obj.get("url").and_then(|v| v.as_str()) {
        out.url = Some(v.to_string());
    }
    if let Some(m) = obj.get("headers").and_then(|v| v.as_object()) {
        let mut headers = HashMap::new();
        for (k, v) in m {
            if let Some(s) = v.as_str() {
                headers.insert(k.clone(), s.to_string());
            }
        }
        out.headers = Some(headers);
    }
    if let Some(m) = obj.get("query").and_then(|v| v.as_object()) {
        let mut query = HashMap::new();
        for (k, v) in m {
            if let Some(s) = v.as_str() {
                query.insert(k.clone(), s.to_string());
            }
        }
        out.query = Some(query);
    }
    if let Some(v) = obj.get("body").cloned() {
        out.body = Some(v);
    }
    Ok(Some(out))
}

/// Call parseResponse(ctx, response). Returns array of { ts, source, event, meta? }.
pub async fn call_parse_response(
    script: &str,
    ctx: &HookContext,
    response: &HookResponse,
    hooks_config: &HooksConfig,
) -> anyhow::Result<Vec<HookEvent>> {
    let timeout = Duration::from_secs(hooks_config.timeout_secs);
    let ctx_json = serde_json::to_value(ctx).context("serialize hook ctx")?;
    let resp_json = serde_json::to_value(response).context("serialize hook response")?;
    let result = run_hook(
        script,
        "parseResponse",
        vec![ctx_json, resp_json],
        timeout,
        hooks_config.allow_network,
    )
    .await?;
    let arr = match result.and_then(|v| v.as_array().cloned()) {
        Some(a) => a,
        None => return Ok(vec![]),
    };
    let mut events = Vec::new();
    for item in arr {
        let obj = item
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("parseResponse element must be object"))?;
        let ts = obj
            .get("ts")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let ts = if ts.is_empty() {
            chrono::Utc::now().to_rfc3339()
        } else {
            ts
        };
        let source = obj
            .get("source")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| ctx.source_id.clone());
        let event = obj.get("event").cloned().unwrap_or(serde_json::Value::Null);
        let meta = obj.get("meta").cloned();
        events.push(HookEvent {
            ts,
            source,
            event,
            meta,
        });
    }
    Ok(events)
}

/// Call getNextPage(ctx, request, response). Returns null or { url?, body? }. request is the request that was sent (url, body).
pub async fn call_get_next_page(
    script: &str,
    ctx: &HookContext,
    request: &HookRequest,
    response: &HookResponse,
    hooks_config: &HooksConfig,
) -> anyhow::Result<Option<GetNextPageResult>> {
    let timeout = Duration::from_secs(hooks_config.timeout_secs);
    let ctx_json = serde_json::to_value(ctx).context("serialize hook ctx")?;
    let request_json = serde_json::to_value(request).context("serialize hook request")?;
    let resp_json = serde_json::to_value(response).context("serialize hook response")?;
    let result = run_hook(
        script,
        "getNextPage",
        vec![ctx_json, request_json, resp_json],
        timeout,
        hooks_config.allow_network,
    )
    .await?;
    let Some(obj) = result.and_then(|v| v.as_object().cloned()) else {
        return Ok(None);
    };
    let url = obj
        .get("url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let body = obj.get("body").cloned();
    Ok(Some(GetNextPageResult { url, body }))
}

/// Call commitState(ctx, events). Returns object of key-value pairs to write to state store.
pub async fn call_commit_state(
    script: &str,
    ctx: &HookContext,
    events: &[HookEvent],
    hooks_config: &HooksConfig,
) -> anyhow::Result<CommitStateResult> {
    let timeout = Duration::from_secs(hooks_config.timeout_secs);
    let ctx_json = serde_json::to_value(ctx).context("serialize hook ctx")?;
    let events_json: serde_json::Value = events
        .iter()
        .map(|e| {
            serde_json::json!({
                "ts": e.ts,
                "source": e.source,
                "event": e.event,
                "meta": e.meta
            })
        })
        .collect::<Vec<_>>()
        .into();
    let result = run_hook(
        script,
        "commitState",
        vec![ctx_json, events_json],
        timeout,
        hooks_config.allow_network,
    )
    .await?;
    let Some(obj) = result.and_then(|v| v.as_object().cloned()) else {
        return Ok(CommitStateResult::new());
    };
    let mut out = CommitStateResult::new();
    for (k, v) in obj {
        if let Some(s) = v.as_str() {
            out.insert(k, s.to_string());
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_hooks_config() -> HooksConfig {
        HooksConfig {
            enabled: true,
            path: Some("./hooks".to_string()),
            timeout_secs: 5,
            memory_limit_mb: None,
            allow_network: false,
            allow_fs: false,
        }
    }

    #[tokio::test]
    async fn run_hook_build_request_returns_object() {
        let script = r#"
            function buildRequest(ctx) {
                return { url: "https://example.com", query: { limit: "10" }, headers: { "X-Foo": "bar" } };
            }
        "#;
        let ctx = HookContext {
            env: std::collections::HashMap::new(),
            state: std::collections::HashMap::new(),
            request_id: "req-1".to_string(),
            source_id: "test".to_string(),
            default_since: None,
            pagination: None,
            headers: None,
        };
        let cfg = default_hooks_config();
        let result = call_build_request(script, &ctx, &cfg).await.unwrap();
        let r = result.expect("buildRequest should return object");
        assert_eq!(r.url.as_deref(), Some("https://example.com"));
        assert_eq!(
            r.query.as_ref().and_then(|q| q.get("limit")),
            Some(&"10".to_string())
        );
        assert_eq!(
            r.headers.as_ref().and_then(|h| h.get("X-Foo")),
            Some(&"bar".to_string())
        );
    }

    #[tokio::test]
    async fn run_hook_build_request_undefined_returns_none() {
        let script = r#" function other() { return 1; } "#;
        let ctx = HookContext {
            env: std::collections::HashMap::new(),
            state: std::collections::HashMap::new(),
            request_id: "req-1".to_string(),
            source_id: "test".to_string(),
            default_since: None,
            pagination: None,
            headers: None,
        };
        let cfg = default_hooks_config();
        let result = call_build_request(script, &ctx, &cfg).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn run_hook_parse_response_returns_events() {
        let script = r#"
            function parseResponse(ctx, response) {
                return [
                    { ts: "2024-01-01T00:00:00Z", source: ctx.sourceId, event: { id: 1 }, meta: {} }
                ];
            }
        "#;
        let ctx = HookContext {
            env: std::collections::HashMap::new(),
            state: std::collections::HashMap::new(),
            request_id: "req-1".to_string(),
            source_id: "test".to_string(),
            default_since: None,
            pagination: None,
            headers: None,
        };
        let response = HookResponse {
            status: 200,
            headers: std::collections::HashMap::new(),
            body: serde_json::json!({ "items": [] }),
        };
        let cfg = default_hooks_config();
        let events = call_parse_response(script, &ctx, &response, &cfg)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].ts, "2024-01-01T00:00:00Z");
        assert_eq!(events[0].source, "test");
        assert_eq!(events[0].event.get("id"), Some(&serde_json::json!(1)));
    }

    #[tokio::test]
    async fn run_hook_get_auth_returns_headers() {
        let script = r#"
            function getAuth(ctx) {
                return { headers: { "Authorization": "Bearer " + (ctx.env.TOKEN || "") } };
            }
        "#;
        let mut env = std::collections::HashMap::new();
        env.insert("TOKEN".to_string(), "secret".to_string());
        let ctx = HookContext {
            env,
            state: std::collections::HashMap::new(),
            request_id: "req-1".to_string(),
            source_id: "test".to_string(),
            default_since: None,
            pagination: None,
            headers: None,
        };
        let cfg = default_hooks_config();
        let result = call_get_auth(script, &ctx, &cfg).await.unwrap();
        let ar = result.expect("getAuth should return object");
        assert_eq!(
            ar.headers.as_ref().and_then(|h| h.get("Authorization")),
            Some(&"Bearer secret".to_string())
        );
    }

    #[tokio::test]
    async fn run_hook_get_auth_undefined_returns_none() {
        let script = r#" function buildRequest(ctx) { return {}; } "#;
        let ctx = HookContext {
            env: std::collections::HashMap::new(),
            state: std::collections::HashMap::new(),
            request_id: "req-1".to_string(),
            source_id: "test".to_string(),
            default_since: None,
            pagination: None,
            headers: None,
        };
        let cfg = default_hooks_config();
        let result = call_get_auth(script, &ctx, &cfg).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn run_hook_get_next_page_returns_null() {
        let script = r#" function getNextPage(ctx, request, response) { return null; } "#;
        let ctx = HookContext {
            env: std::collections::HashMap::new(),
            state: std::collections::HashMap::new(),
            request_id: "req-1".to_string(),
            source_id: "test".to_string(),
            default_since: None,
            pagination: None,
            headers: None,
        };
        let response = HookResponse {
            status: 200,
            headers: std::collections::HashMap::new(),
            body: serde_json::json!({}),
        };
        let request = HookRequest {
            url: "https://example.com".to_string(),
            body: None,
        };
        let cfg = default_hooks_config();
        let next = call_get_next_page(script, &ctx, &request, &response, &cfg)
            .await
            .unwrap();
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn run_hook_commit_state_returns_object() {
        let script = r#"
            function commitState(ctx, events) {
                return { cursor: "next-abc", watermark: "2024-01-01T00:00:00Z" };
            }
        "#;
        let ctx = HookContext {
            env: std::collections::HashMap::new(),
            state: std::collections::HashMap::new(),
            request_id: "req-1".to_string(),
            source_id: "test".to_string(),
            default_since: None,
            pagination: None,
            headers: None,
        };
        let events: Vec<HookEvent> = vec![];
        let cfg = default_hooks_config();
        let state = call_commit_state(script, &ctx, &events, &cfg)
            .await
            .unwrap();
        assert_eq!(state.get("cursor"), Some(&"next-abc".to_string()));
        assert_eq!(
            state.get("watermark"),
            Some(&"2024-01-01T00:00:00Z".to_string())
        );
    }

    #[tokio::test]
    async fn script_path_absolute() {
        let global = HooksConfig::default();
        let p = script_path(&global, "/abs/path/okta.js").unwrap();
        assert!(p.to_string_lossy().contains("okta.js"));
    }

    #[tokio::test]
    async fn script_path_relative_under_base() {
        let global = HooksConfig {
            path: Some("/base".to_string()),
            ..default_hooks_config()
        };
        let p = script_path(&global, "okta.js").unwrap();
        assert_eq!(p, std::path::Path::new("/base/okta.js"));
    }
}
