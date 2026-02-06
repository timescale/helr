//! HTTP client wrapper: build reqwest client with timeouts, TLS, and auth from config.
//! Single GET request; pagination is handled by the caller.
//! Auth secrets can come from env vars or files (config parity).

use crate::config::{self, AuthConfig, ResilienceConfig, SourceConfig, TlsConfig};
use anyhow::Context;
use base64::Engine;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use reqwest::tls::{Certificate, Identity, Version};
use std::time::Duration;

/// Effective timeout values: from split timeouts when set, else from legacy timeout_secs.
/// Connect and request are always set (fallback to legacy); read and idle only when configured.
fn effective_timeouts(
    resilience: Option<&ResilienceConfig>,
) -> (Duration, Duration, Option<Duration>, Option<Duration>) {
    let legacy_secs = resilience.map(|r| r.timeout_secs).unwrap_or(30);
    let t = resilience.and_then(|r| r.timeouts.as_ref());
    let connect_secs = t
        .and_then(|t| t.connect_secs)
        .unwrap_or_else(|| std::cmp::min(10, legacy_secs));
    let request_secs = t.and_then(|t| t.request_secs).unwrap_or(legacy_secs);
    let connect = Duration::from_secs(connect_secs);
    let request = Duration::from_secs(request_secs);
    let read = t.and_then(|t| t.read_secs).map(Duration::from_secs);
    let idle = t.and_then(|t| t.idle_secs).map(Duration::from_secs);
    (connect, request, read, idle)
}

/// Load CA cert(s) from PEM (single or bundle) and return certs for reqwest.
fn load_ca_certs(tls: &TlsConfig) -> anyhow::Result<Vec<Certificate>> {
    let pem = config::read_secret(tls.ca_file.as_deref(), tls.ca_env.as_deref().unwrap_or(""))?;
    let bytes = pem.as_bytes();
    // Bundle = multiple BEGIN CERTIFICATE blocks; otherwise single cert.
    let is_bundle = pem.contains("-----BEGIN CERTIFICATE-----")
        && pem.matches("-----BEGIN CERTIFICATE-----").count() > 1;
    if is_bundle {
        Certificate::from_pem_bundle(bytes).context("parse TLS CA PEM bundle")
    } else {
        let cert = Certificate::from_pem(bytes).context("parse TLS CA PEM")?;
        Ok(vec![cert])
    }
}

/// Load client identity (cert + key) for mutual TLS. Cert and key can be separate PEMs; we concatenate for Identity::from_pem.
fn load_client_identity(tls: &TlsConfig) -> anyhow::Result<Identity> {
    let cert = config::read_secret(
        tls.client_cert_file.as_deref(),
        tls.client_cert_env.as_deref().unwrap_or(""),
    )?;
    let key = config::read_secret(
        tls.client_key_file.as_deref(),
        tls.client_key_env.as_deref().unwrap_or(""),
    )?;
    // reqwest Identity::from_pem expects one PEM buffer with private key and at least one certificate.
    let mut pem = key.trim().to_string();
    pem.push('\n');
    pem.push_str(cert.trim());
    Identity::from_pem(pem.as_bytes()).context("parse TLS client cert/key PEM")
}

/// Apply TLS config to the client builder: custom CA, client identity, min TLS version.
fn apply_tls(
    builder: reqwest::ClientBuilder,
    tls: &TlsConfig,
) -> anyhow::Result<reqwest::ClientBuilder> {
    let has_ca = tls.ca_file.as_deref().is_some_and(|p| !p.is_empty())
        || tls.ca_env.as_deref().is_some_and(|e| !e.is_empty());
    let has_identity = tls
        .client_cert_file
        .as_deref()
        .is_some_and(|p| !p.is_empty())
        || tls
            .client_cert_env
            .as_deref()
            .is_some_and(|e| !e.is_empty());

    let mut builder = builder;

    if has_ca {
        let certs = load_ca_certs(tls)?;
        builder = if tls.ca_only {
            builder.tls_certs_only(certs)
        } else {
            builder.tls_certs_merge(certs)
        };
    }

    if has_identity {
        let identity = load_client_identity(tls)?;
        builder = builder.identity(identity);
    }

    if let Some(v) = tls.min_version.as_deref() {
        let version = match v {
            "1.2" => Version::TLS_1_2,
            "1.3" => Version::TLS_1_3,
            _ => anyhow::bail!("tls min_version must be \"1.2\" or \"1.3\", got {:?}", v),
        };
        builder = builder.tls_version_min(version);
    }

    Ok(builder)
}

/// Build a reqwest client with timeouts and optional TLS from resilience config.
/// Uses split timeouts (connect, request, read, idle) when set; otherwise timeout_secs for request and min(10, timeout_secs) for connect.
pub fn build_client(resilience: Option<&ResilienceConfig>) -> anyhow::Result<Client> {
    let (connect, request, read, idle) = effective_timeouts(resilience);
    let mut builder = Client::builder().connect_timeout(connect).timeout(request);
    if let Some(d) = read {
        builder = builder.read_timeout(d);
    }
    if let Some(d) = idle {
        builder = builder.pool_idle_timeout(d);
    }
    if let Some(tls) = resilience.and_then(|r| r.tls.as_ref()) {
        builder = apply_tls(builder, tls)?;
    }
    let client = builder.build().context("build reqwest client")?;
    Ok(client)
}

/// Context passed to build_request: overrides and audit for auth.
#[derive(Default)]
pub struct BuildRequestContext<'a> {
    pub bearer_override: Option<&'a str>,
    pub body_override: Option<&'a serde_json::Value>,
    pub dpop_proof: Option<String>,
    pub source_id: &'a str,
    pub audit: Option<&'a crate::config::AuditConfig>,
}

/// Build GET or POST request with auth and optional headers. bearer_override/dpop_proof used when set (e.g. OAuth2).
pub fn build_request(
    client: &Client,
    source: &SourceConfig,
    url: &str,
    ctx: &BuildRequestContext<'_>,
) -> anyhow::Result<reqwest::Request> {
    use crate::config::HttpMethod;
    let mut req = match source.method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Post => {
            let empty: serde_json::Value = serde_json::Value::Object(serde_json::Map::new());
            let body_value = ctx.body_override.or(source.body.as_ref()).unwrap_or(&empty);
            let body_bytes = serde_json::to_vec(body_value).context("serialize POST body")?;
            client
                .post(url)
                .header("Content-Type", "application/json")
                .body(body_bytes)
        }
    };
    if let Some(token) = ctx.bearer_override {
        let scheme = if ctx.dpop_proof.is_some() {
            "DPoP"
        } else {
            "Bearer"
        };
        let value = format!("{} {}", scheme, token);
        let hv = HeaderValue::try_from(value).context("invalid bearer token")?;
        req = req.header(AUTHORIZATION, hv);
    }
    if let Some(proof) = &ctx.dpop_proof {
        let hv = HeaderValue::try_from(proof.as_str()).context("invalid DPoP proof")?;
        req = req.header("DPoP", hv);
    }
    if ctx.bearer_override.is_none()
        && let Some(auth) = &source.auth
    {
        req = add_auth(req, auth, ctx.source_id, ctx.audit)?;
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
    source_id: &str,
    audit: Option<&crate::config::AuditConfig>,
) -> anyhow::Result<reqwest::RequestBuilder> {
    let req = match auth {
        AuthConfig::Bearer {
            token_env,
            token_file,
            prefix,
        } => {
            let token = config::read_secret(token_file.as_deref(), token_env)?;
            crate::audit::log_credential_access(audit, source_id, "bearer_token");
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
            crate::audit::log_credential_access(audit, source_id, "api_key");
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
            crate::audit::log_credential_access(audit, source_id, "basic_user");
            let password = config::read_secret(password_file.as_deref(), password_env)?;
            crate::audit::log_credential_access(audit, source_id, "basic_password");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ResilienceConfig, TlsConfig};

    #[test]
    fn build_client_with_tls_min_version_only() {
        let resilience = ResilienceConfig {
            timeout_secs: 30,
            timeouts: None,
            retries: None,
            circuit_breaker: None,
            rate_limit: None,
            bulkhead: None,
            tls: Some(TlsConfig {
                ca_file: None,
                ca_env: None,
                ca_only: false,
                client_cert_file: None,
                client_cert_env: None,
                client_key_file: None,
                client_key_env: None,
                min_version: Some("1.3".to_string()),
            }),
        };
        let client = build_client(Some(&resilience)).unwrap();
        drop(client);
    }
}
