//! HTTP client wrapper: build reqwest client with timeouts and auth from config.
//! Single GET request; pagination is handled by the caller.

use crate::config::{AuthConfig, ResilienceConfig, SourceConfig};
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

/// Build a GET request for the given URL with auth and optional extra headers from source config.
pub fn build_request(
    client: &Client,
    source: &SourceConfig,
    url: &str,
) -> anyhow::Result<reqwest::Request> {
    let mut req = client.get(url);
    if let Some(auth) = &source.auth {
        req = add_auth(req, auth)?;
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
        AuthConfig::Bearer { token_env } => {
            let token = std::env::var(token_env)
                .with_context(|| format!("env {} not set (bearer auth)", token_env))?;
            let value = format!("Bearer {}", token);
            let hv = HeaderValue::try_from(value).context("invalid bearer token")?;
            req.header(AUTHORIZATION, hv)
        }
        AuthConfig::ApiKey { header, key_env } => {
            let key = std::env::var(key_env)
                .with_context(|| format!("env {} not set (api key)", key_env))?;
            let name = HeaderName::try_from(header.as_str())
                .with_context(|| format!("invalid api key header name: {:?}", header))?;
            let hv = HeaderValue::try_from(key.as_str()).context("invalid api key value")?;
            req.header(name, hv)
        }
        AuthConfig::Basic {
            user_env,
            password_env,
        } => {
            let user = std::env::var(user_env)
                .with_context(|| format!("env {} not set (basic auth user)", user_env))?;
            let password = std::env::var(password_env)
                .with_context(|| format!("env {} not set (basic auth password)", password_env))?;
            let encoded = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", user, password).as_bytes());
            let value = format!("Basic {}", encoded);
            let hv = HeaderValue::try_from(value).context("invalid basic auth")?;
            req.header(AUTHORIZATION, hv)
        }
    };
    Ok(req)
}
