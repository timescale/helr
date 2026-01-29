//! OAuth2 refresh using the oauth2 crate (RFC 6749). Tokens cached in memory.

use crate::config::AuthConfig;
use anyhow::Context;
use oauth2::basic::BasicClient;
use oauth2::{ClientId, ClientSecret, RefreshToken, TokenResponse, TokenUrl};
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;

/// Per-source cache: (access_token, expires_at). Refreshed when expired or missing.
pub type OAuth2TokenCache = Arc<RwLock<HashMap<String, (String, Instant)>>>;

/// Buffer before expiry to refresh (seconds).
const REFRESH_BUFFER_SECS: u64 = 60;

pub fn new_oauth2_token_cache() -> OAuth2TokenCache {
    Arc::new(RwLock::new(HashMap::new()))
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
            client_secret_env,
            refresh_token_env,
            scopes: _,
        } => (token_url, client_id_env, client_secret_env, refresh_token_env),
        _ => anyhow::bail!("get_oauth_token requires OAuth2 auth"),
    };

    let (token_url, client_id_env, client_secret_env, refresh_token_env) = oauth;
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

    let client_id = std::env::var(client_id_env)
        .with_context(|| format!("env {} not set (oauth2 client_id)", client_id_env))?;
    let client_secret = std::env::var(client_secret_env)
        .with_context(|| format!("env {} not set (oauth2 client_secret)", client_secret_env))?;
    let refresh_token = std::env::var(refresh_token_env)
        .with_context(|| format!("env {} not set (oauth2 refresh_token)", refresh_token_env))?;

    let oauth2_client = BasicClient::new(ClientId::new(client_id))
        .set_client_secret(ClientSecret::new(client_secret))
        .set_token_uri(
            TokenUrl::new(token_url.clone()).context("oauth2 token_url invalid")?,
        );

    let token_result = oauth2_client
        .exchange_refresh_token(&RefreshToken::new(refresh_token))
        .request_async(client)
        .await
        .map_err(|e| anyhow::anyhow!("oauth2 token request: {}", e))?;

    let access_token = token_result.access_token().secret().to_string();
    let expires_in = token_result
        .expires_in()
        .map(|d| d.as_secs())
        .unwrap_or(3600);
    let expires_at = now + Duration::from_secs(expires_in);

    {
        let mut g = cache.write().await;
        g.insert(source_id.to_string(), (access_token.clone(), expires_at));
    }
    debug!(source = %source_id, expires_in, "oauth2 token refreshed");
    Ok(access_token)
}
