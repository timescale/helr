//! OAuth2: refresh_token grant and Google Service Account JWT bearer grant; cache in memory.
//! Client id/secret/refresh_token can come from env or files (config parity).

use crate::config::{self, AuthConfig};
use anyhow::Context;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Client;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::debug;

/// Per-source cache: (access_token, expires_at). Refreshed when expired or missing.
pub type OAuth2TokenCache = Arc<RwLock<HashMap<String, (String, Instant)>>>;

/// Buffer before expiry to refresh (seconds).
const REFRESH_BUFFER_SECS: u64 = 60;

pub fn new_oauth2_token_cache() -> OAuth2TokenCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Invalidate cached token for a source so the next request triggers a refresh (e.g. after 401).
pub async fn invalidate_token(cache: &OAuth2TokenCache, source_id: &str) {
    let mut g = cache.write().await;
    g.remove(source_id);
    debug!(source = %source_id, "oauth2 token invalidated");
}

/// Returns current valid access_token for the source, refreshing if needed.
pub async fn get_oauth_token(
    cache: &OAuth2TokenCache,
    client: &Client,
    source_id: &str,
    auth: &AuthConfig,
) -> anyhow::Result<String> {
    let oauth = match auth {
        AuthConfig::OAuth2 {
            token_url,
            client_id_env,
            client_id_file,
            client_secret_env,
            client_secret_file,
            refresh_token_env,
            refresh_token_file,
            scopes: _,
        } => (
            token_url,
            client_id_env,
            client_id_file.as_deref(),
            client_secret_env,
            client_secret_file.as_deref(),
            refresh_token_env,
            refresh_token_file.as_deref(),
        ),
        _ => anyhow::bail!("get_oauth_token requires OAuth2 auth"),
    };

    let (
        token_url,
        client_id_env,
        client_id_file,
        client_secret_env,
        client_secret_file,
        refresh_token_env,
        refresh_token_file,
    ) = oauth;
    let now = Instant::now();
    let buffer = Duration::from_secs(REFRESH_BUFFER_SECS);

    {
        let g = cache.read().await;
        if let Some((token, expires_at)) = g.get(source_id) {
            if now + buffer < *expires_at {
                return Ok(token.clone());
            }
        }
    }

    let client_id = config::read_secret(client_id_file, client_id_env)?;
    let client_secret = config::read_secret(client_secret_file, client_secret_env)?;
    let refresh_token = config::read_secret(refresh_token_file, refresh_token_env)?;

    let mut form = std::collections::HashMap::new();
    form.insert("grant_type", "refresh_token");
    form.insert("client_id", client_id.as_str());
    form.insert("client_secret", client_secret.as_str());
    form.insert("refresh_token", refresh_token.as_str());

    let response: reqwest::Response = client
        .post(token_url.as_str())
        .form(&form)
        .send()
        .await
        .context("oauth2 token request")?;

    let status = response.status();
    let body = response.text().await.context("oauth2 token response body")?;
    if !status.is_success() {
        anyhow::bail!("oauth2 token error {}: {}", status, body);
    }

    let json: serde_json::Value =
        serde_json::from_str(&body).context("oauth2 token response json")?;
    let access_token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .context("oauth2 response missing access_token")?
        .to_string();
    let expires_in = json
        .get("expires_in")
        .and_then(|v| v.as_u64())
        .unwrap_or(3600);
    let expires_at = now + Duration::from_secs(expires_in);

    {
        let mut g = cache.write().await;
        g.insert(source_id.to_string(), (access_token.clone(), expires_at));
    }
    debug!(source = %source_id, expires_in, "oauth2 token refreshed");
    Ok(access_token)
}

/// Google OAuth2 token endpoint for JWT bearer grant.
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Returns current valid access_token for the source when auth is Google Service Account (JWT bearer); cached.
pub async fn get_google_sa_token(
    cache: &OAuth2TokenCache,
    client: &Client,
    source_id: &str,
    auth: &AuthConfig,
) -> anyhow::Result<String> {
    let sa = match auth {
        AuthConfig::GoogleServiceAccount {
            credentials_file,
            credentials_env,
            subject_env,
            subject_file,
            scopes,
        } => (credentials_file.as_deref(), credentials_env.as_deref(), subject_env.as_deref(), subject_file.as_deref(), scopes),
        _ => anyhow::bail!("get_google_sa_token requires GoogleServiceAccount auth"),
    };
    let (creds_path, creds_env, subject_env, subject_file, scopes) = sa;
    let now = Instant::now();
    let buffer = Duration::from_secs(REFRESH_BUFFER_SECS);
    {
        let g = cache.read().await;
        if let Some((token, expires_at)) = g.get(source_id) {
            if now + buffer < *expires_at {
                return Ok(token.clone());
            }
        }
    }
    let json_str = if let Some(p) = creds_path {
        if p.is_empty() {
            None
        } else {
            Some(std::fs::read_to_string(Path::new(p)).context("read credentials file")?)
        }
    } else {
        None
    };
    let json_str = json_str.or_else(|| creds_env.and_then(|e| std::env::var(e).ok()));
    let json_str = json_str.context("google_service_account credentials not set (credentials_file or credentials_env)")?;
    let creds: serde_json::Value = serde_json::from_str(&json_str).context("credentials JSON")?;
    let client_email = creds
        .get("client_email")
        .and_then(|v| v.as_str())
        .context("credentials missing client_email")?;
    let private_key_str = creds
        .get("private_key")
        .and_then(|v| v.as_str())
        .context("credentials missing private_key")?;
    let private_key = private_key_str.replace("\\n", "\n");
    let subject = subject_env
        .and_then(|e| std::env::var(e).ok())
        .or_else(|| {
            subject_file.and_then(|p| {
                if p.is_empty() {
                    None
                } else {
                    std::fs::read_to_string(Path::new(p)).ok().map(|s| s.trim().to_string())
                }
            })
        });
    let scope = scopes.join(" ");
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    #[derive(serde::Serialize)]
    struct GoogleJwtClaims {
        iss: String,
        scope: String,
        aud: String,
        iat: u64,
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        sub: Option<String>,
    }
    let claims = GoogleJwtClaims {
        iss: client_email.to_string(),
        scope,
        aud: GOOGLE_TOKEN_URL.to_string(),
        iat: now_secs,
        exp: now_secs + 3600,
        sub: subject,
    };
    let key = EncodingKey::from_rsa_pem(private_key.as_bytes()).context("parse private_key PEM")?;
    let token = encode(&Header::new(Algorithm::RS256), &claims, &key).context("sign JWT")?;
    let mut form = std::collections::HashMap::new();
    form.insert("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
    form.insert("assertion", token.as_str());
    let response = client
        .post(GOOGLE_TOKEN_URL)
        .form(&form)
        .send()
        .await
        .context("google token request")?;
    let status = response.status();
    let body = response.text().await.context("google token response body")?;
    if !status.is_success() {
        anyhow::bail!("google token error {}: {}", status, body);
    }
    let json: serde_json::Value = serde_json::from_str(&body).context("google token response json")?;
    let access_token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .context("response missing access_token")?
        .to_string();
    let expires_in = json.get("expires_in").and_then(|v| v.as_u64()).unwrap_or(3600);
    let expires_at = now + Duration::from_secs(expires_in);
    {
        let mut g = cache.write().await;
        g.insert(source_id.to_string(), (access_token.clone(), expires_at));
    }
    debug!(source = %source_id, expires_in, "google service account token obtained");
    Ok(access_token)
}
