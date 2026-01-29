//! Mock HTTP server for development and testing.
//!
//! Run with `hel mock-server mocks/okta.yaml`. The server
//! serves responses according to the YAML: match request path and query params,
//! then return the first matching response (status, headers, body from file or inline).

use anyhow::Context;
use axum::response::IntoResponse;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Mock server config (YAML): endpoint path and list of response rules.
#[derive(Debug, Clone, Deserialize)]
pub struct MockConfig {
    /// Path this mock serves (e.g. "/api/v1/logs"). Request path must match exactly.
    pub endpoint: String,

    /// Optional bind address:port (default "127.0.0.1:0" = pick port).
    #[serde(default)]
    pub bind: Option<String>,

    /// Response rules in order; first match wins.
    pub responses: Vec<ResponseRule>,
}

/// One rule: match criteria and response to serve.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponseRule {
    #[serde(rename = "match", default)]
    pub match_: MatchRule,

    pub response: ResponseSpec,
}

/// Request match: query params. Key = param name, value = exact value or "*" for any.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MatchRule {
    #[serde(default)]
    pub query: HashMap<String, String>,
}

/// Response to serve: status, optional headers, body from file or inline.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponseSpec {
    #[serde(default = "default_status")]
    pub status: u16,

    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Path to file containing response body (relative to config file dir).
    #[serde(default, rename = "body_file")]
    pub body_file: Option<String>,

    /// Inline body (used if body_file is not set).
    #[serde(default)]
    pub body: Option<String>,
}

fn default_status() -> u16 {
    200
}

/// Load mock config from YAML file.
pub fn load_mock_config(path: &Path) -> anyhow::Result<MockConfig> {
    let s = std::fs::read_to_string(path).context("read mock config")?;
    let config: MockConfig = serde_yaml::from_str(&s).context("parse mock config YAML")?;
    Ok(config)
}

/// Resolve body: load from file (relative to config_dir) or use inline. Returns bytes.
fn resolve_body(spec: &ResponseSpec, config_dir: &Path) -> anyhow::Result<Vec<u8>> {
    if let Some(ref path) = spec.body_file {
        let full = config_dir.join(path);
        let bytes =
            std::fs::read(&full).with_context(|| format!("read body_file {}", full.display()))?;
        return Ok(bytes);
    }
    if let Some(ref s) = spec.body {
        return Ok(s.as_bytes().to_vec());
    }
    Ok(Vec::new())
}

/// Parse query string into key-value map (no percent-decoding).
fn parse_query(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter_map(|p| {
            let mut it = p.splitn(2, '=');
            let k = it.next()?.trim();
            if k.is_empty() {
                return None;
            }
            let v = it.next().unwrap_or("").trim();
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

/// Check if request query params satisfy the match rule. "*" means any value.
fn query_matches(rule: &MatchRule, request_query: &str) -> bool {
    let params = parse_query(request_query);
    for (key, expected) in &rule.query {
        match params.get(key) {
            None => return false,
            Some(actual) => {
                if expected != "*" && expected != actual {
                    return false;
                }
            }
        }
    }
    true
}

/// Run the mock server: load config, bind, serve requests until shutdown.
pub async fn run_mock_server(config_path: &Path) -> anyhow::Result<()> {
    let config = load_mock_config(config_path)?;
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let endpoint = config.endpoint.clone();
    let bind = config
        .bind
        .as_deref()
        .unwrap_or("127.0.0.1:0")
        .to_string();

    // Preload response bodies (paths relative to config_dir).
    let rules: Vec<(MatchRule, ResponseSpec, Vec<u8>)> = config
        .responses
        .into_iter()
        .map(|r| {
            let body = resolve_body(&r.response, config_dir).context("resolve response body")?;
            Ok((r.match_, r.response, body))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let rules = Arc::new(rules);

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .context("bind mock server")?;
    let addr = listener.local_addr().context("mock server local_addr")?;
    info!(%addr, endpoint = %endpoint, "mock server listening (Ctrl+C to stop)");
    eprintln!("mock server at http://{}", addr);
    eprintln!("endpoint: {}", endpoint);

    let rules_clone = rules.clone();
    let endpoint_clone = endpoint.clone();
    let handler = move |req: axum::extract::Request| {
        let rules = rules_clone.clone();
        let endpoint = endpoint_clone.clone();
        async move {
            let path = req.uri().path();
            if path != endpoint {
                return (
                    axum::http::StatusCode::NOT_FOUND,
                    format!("path {} does not match endpoint {}", path, endpoint),
                )
                    .into_response();
            }
            let query = req.uri().query().unwrap_or("");

            for (match_rule, spec, body) in rules.as_ref() {
                if query_matches(match_rule, query) {
                    let status = axum::http::StatusCode::from_u16(spec.status)
                        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    let mut response =
                        axum::response::Response::new(axum::body::Body::from(body.clone()));
                    *response.status_mut() = status;
                    for (k, v) in &spec.headers {
                        if let (Ok(name), Ok(value)) = (
                            axum::http::header::HeaderName::try_from(k.as_str()),
                            axum::http::header::HeaderValue::try_from(v.as_str()),
                        ) {
                            response.headers_mut().insert(name, value);
                        }
                    }
                    return response;
                }
            }

            (
                axum::http::StatusCode::NOT_FOUND,
                format!("no mock response matched query {}", query),
            )
                .into_response()
        }
    };

    let app = axum::Router::new().route("/{*path}", axum::routing::any(handler));

    axum::serve(listener, app).await.context("serve mock server")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_mock_config_parses_yaml() {
        let yaml = r#"
endpoint: "/api/v1/logs"
responses:
  - match:
      query:
        since: "*"
    response:
      status: 200
      body: '{"items":[]}'
  - match:
      query:
        after: "cursor1"
    response:
      status: 200
      headers:
        Content-Type: "application/json"
      body: '[]'
"#;
        let temp = std::env::temp_dir().join("hel_mock_test.yaml");
        std::fs::write(&temp, yaml).unwrap();
        let config = load_mock_config(&temp).unwrap();
        assert_eq!(config.endpoint, "/api/v1/logs");
        assert_eq!(config.responses.len(), 2);
        assert_eq!(config.responses[0].match_.query.get("since"), Some(&"*".to_string()));
        assert_eq!(config.responses[1].match_.query.get("after"), Some(&"cursor1".to_string()));
        assert_eq!(config.responses[0].response.status, 200);
        assert_eq!(config.responses[0].response.body.as_deref(), Some("{\"items\":[]}"));
        let _ = std::fs::remove_file(&temp);
    }

    #[test]
    fn query_matches_wildcard_and_exact() {
        let mut rule = MatchRule::default();
        rule.query.insert("since".to_string(), "*".to_string());
        assert!(query_matches(&rule, "since=2024-01-01"));
        assert!(query_matches(&rule, "since=anything"));
        assert!(!query_matches(&rule, "other=1"));

        rule.query.insert("after".to_string(), "cursor1".to_string());
        assert!(query_matches(&rule, "since=2024-01-01&after=cursor1"));
        assert!(!query_matches(&rule, "since=2024-01-01&after=cursor2"));
    }
}
