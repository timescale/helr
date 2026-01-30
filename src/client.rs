//! HTTP client wrapper: build reqwest client with timeouts and auth from config.
//! Single GET request; pagination is handled by the caller.
//! Auth secrets can come from env vars or files (config parity).

use crate::config::{self, AuthConfig, ResilienceConfig, SourceConfig};
use anyhow::Context;
use base64::Engine;
use reqwest::header::{HeaderName, HeaderValue, AUTHORIZATION};
use reqwest::Client;
use std::time::Duration;

/// Build a reqwest client with timeouts from resilience config.
pub fn build_client(resilience: Option<&ResilienceConfig>) -> anyhow::Result<Client> {
    let timeout_secs = resilience.map(|r| r.timeout_secs).unwrap_or(30);
    let timeout = Duration::from_secs(timeout_secs);
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(std::cmp::min(10, timeout_secs)))
        .timeout(timeout)
        .build()
        .context("build reqwest client")?;
    Ok(client)
}

/// Build GET or POST request with auth and optional headers. bearer_override/dpop_proof used when set (e.g. OAuth2).
pub fn build_request(
    client: &Client,
    source: &SourceConfig,
    url: &str,
    bearer_override: Option<&str>,
    body_override: Option<&serde_json::Value>,
    dpop_proof: Option<String>,
) -> anyhow::Result<reqwest::Request> {
    use crate::config::HttpMethod;
    let mut req = match source.method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Post => {
            let empty: serde_json::Value = serde_json::Value::Object(serde_json::Map::new());
            let body_value = body_override.or(source.body.as_ref()).unwrap_or(&empty);
            let body_bytes = serde_json::to_vec(body_value).context("serialize POST body")?;
            client
                .post(url)
                .header("Content-Type", "application/json")
                .body(body_bytes)
        }
    };
    if let Some(token) = bearer_override {
        let scheme = if dpop_proof.is_some() { "DPoP" } else { "Bearer" };
        let value = format!("{} {}", scheme, token);
        let hv = HeaderValue::try_from(value).context("invalid bearer token")?;
        req = req.header(AUTHORIZATION, hv);
    }
    if let Some(proof) = dpop_proof {
        let hv = HeaderValue::try_from(proof).context("invalid DPoP proof")?;
        req = req.header("DPoP", hv);
    }
    if bearer_override.is_none() {
        if let Some(auth) = &source.auth {
            req = add_auth(req, auth)?;
        }
    }
    if let Some(headers) = &source.headers {
        for (k, v) in headers {
            let name = HeaderName::try_from(k.as_str())
                .with_context(|| format!("invalid header name: {:?}", k))?;
            let value = HeaderValue::try_from(v.as_str())
                .with_context(|| format!("invalid header value for {}: {:?}", k, v))?;
            req = req.header(name, value);
        }
    }
    req.build().context("build request")
}

fn add_auth(
    req: reqwest::RequestBuilder,
    auth: &AuthConfig,
) -> anyhow::Result<reqwest::RequestBuilder> {
    let req = match auth {
        AuthConfig::Bearer {
            token_env,
            token_file,
            prefix,
        } => {
            let token = config::read_secret(token_file.as_deref(), token_env)?;
            let value = format!("{} {}", prefix, token);
            let hv = HeaderValue::try_from(value).context("invalid bearer token")?;
            req.header(AUTHORIZATION, hv)
        }
        AuthConfig::ApiKey {
            header,
            key_env,
            key_file,
        } => {
            let key = config::read_secret(key_file.as_deref(), key_env)?;
            let name = HeaderName::try_from(header.as_str())
                .with_context(|| format!("invalid api key header name: {:?}", header))?;
            let hv = HeaderValue::try_from(key.as_str()).context("invalid api key value")?;
            req.header(name, hv)
        }
        AuthConfig::Basic {
            user_env,
            user_file,
            password_env,
            password_file,
        } => {
            let user = config::read_secret(user_file.as_deref(), user_env)?;
            let password = config::read_secret(password_file.as_deref(), password_env)?;
            let encoded = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", user, password).as_bytes());
            let value = format!("Basic {}", encoded);
            let hv = HeaderValue::try_from(value).context("invalid basic auth")?;
            req.header(AUTHORIZATION, hv)
        }
        AuthConfig::OAuth2 { .. } => {
            unreachable!("OAuth2 auth must use bearer_override in build_request")
        }
        AuthConfig::GoogleServiceAccount { .. } => {
            unreachable!("GoogleServiceAccount auth must use bearer_override in build_request")
        }
    };
    Ok(req)
}
