//! OAuth2 token acquisition and caching: refresh_token or client_credentials; optional private_key_jwt and DPoP.

use crate::config::{self, AuthConfig};
use crate::dpop::{DPoPKeyCache, build_dpop_proof, get_or_create_dpop_key};
use anyhow::Context;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::Client;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};
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

/// Returns valid access_token for the source, refreshing if needed. With dpop: true, dpop_key_cache must be Some.
pub async fn get_oauth_token(
    cache: &OAuth2TokenCache,
    client: &Client,
    source_id: &str,
    auth: &AuthConfig,
    dpop_key_cache: Option<&DPoPKeyCache>,
    audit: Option<&crate::config::AuditConfig>,
) -> anyhow::Result<String> {
    let oauth = match auth {
        AuthConfig::OAuth2 {
            token_url,
            client_id_env,
            client_id_file,
            client_secret_env,
            client_secret_file,
            client_private_key_env,
            client_private_key_file,
            refresh_token_env,
            refresh_token_file,
            scopes,
            dpop,
        } => (
            token_url,
            client_id_env,
            client_id_file.as_deref(),
            client_secret_env.as_deref(),
            client_secret_file.as_deref(),
            client_private_key_env.as_deref().filter(|s| !s.is_empty()),
            client_private_key_file.as_deref(),
            refresh_token_env.as_deref().filter(|s| !s.is_empty()),
            refresh_token_file.as_deref(),
            scopes.as_deref().unwrap_or(&[]),
            *dpop,
        ),
        _ => anyhow::bail!("get_oauth_token requires OAuth2 auth"),
    };

    let (
        token_url,
        client_id_env,
        client_id_file,
        client_secret_env,
        client_secret_file,
        client_private_key_env,
        client_private_key_file,
        refresh_token_env,
        refresh_token_file,
        scopes,
        dpop,
    ) = oauth;

    let use_private_key_jwt =
        client_private_key_env.is_some() || client_private_key_file.is_some_and(|p| !p.is_empty());
    let now = Instant::now();
    let buffer = Duration::from_secs(REFRESH_BUFFER_SECS);

    {
        let g = cache.read().await;
        if let Some((token, expires_at)) = g.get(source_id)
            && now + buffer < *expires_at
        {
            return Ok(token.clone());
        }
    }

    let client_id = config::read_secret(client_id_file, client_id_env)?;
    crate::audit::log_credential_access(audit, source_id, "oauth2_client_id");
    let client_secret = if use_private_key_jwt {
        None
    } else {
        let secret = config::read_secret(client_secret_file, client_secret_env.unwrap_or(""))?;
        crate::audit::log_credential_access(audit, source_id, "oauth2_client_secret");
        Some(secret)
    };

    let mut form: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let refresh_token = refresh_token_env
        .and_then(|e| config::read_secret(refresh_token_file, e).ok())
        .or_else(|| refresh_token_file.and_then(|p| config::read_secret(Some(p), "").ok()));
    if refresh_token.is_some() {
        crate::audit::log_credential_access(audit, source_id, "oauth2_refresh_token");
    }

    if let Some(rt) = refresh_token {
        form.insert("grant_type".into(), "refresh_token".into());
        form.insert("client_id".into(), client_id.clone());
        if let Some(ref secret) = client_secret {
            form.insert("client_secret".into(), secret.clone());
        }
        form.insert("refresh_token".into(), rt);
    } else {
        form.insert("grant_type".into(), "client_credentials".into());
        form.insert("client_id".into(), client_id.clone());
        if let Some(ref secret) = client_secret {
            form.insert("client_secret".into(), secret.clone());
        }
        if !scopes.is_empty() {
            form.insert("scope".into(), scopes.join(" "));
        }
    }

    if use_private_key_jwt {
        let private_key_pem = config::read_secret(
            client_private_key_file,
            client_private_key_env.unwrap_or(""),
        )?;
        crate::audit::log_credential_access(audit, source_id, "oauth2_client_private_key");
        let client_assertion =
            build_client_assertion(&client_id, token_url.as_str(), &private_key_pem, source_id)?;
        form.insert(
            "client_assertion_type".into(),
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".into(),
        );
        form.insert("client_assertion".into(), client_assertion);
    }

    let mut token_req = client.post(token_url.as_str()).form(&form);
    if dpop {
        let key_cache = dpop_key_cache.context("oauth2 dpop: true requires dpop_key_cache")?;
        let key = get_or_create_dpop_key(key_cache, source_id).await?;
        let iat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time")?
            .as_secs();
        let jti = format!("{}-{}", source_id, iat);
        let proof = build_dpop_proof("POST", token_url.as_str(), &key, &jti, iat, None, None)?;
        token_req = token_req.header("DPoP", proof);
    }
    let response: reqwest::Response = token_req.send().await.context("oauth2 token request")?;

    let status = response.status();
    let dpop_nonce_header = response
        .headers()
        .get("DPoP-Nonce")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string());
    let body = response
        .text()
        .await
        .context("oauth2 token response body")?;

    if dpop && status.as_u16() == 400 && body.contains("use_dpop_nonce") {
        let nonce = dpop_nonce_header.or_else(|| {
            serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| {
                    v.get("nonce")
                        .or(v.get("dpop_nonce"))
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
        });
        if let Some(nonce) = nonce {
            let key_cache = dpop_key_cache.context("oauth2 dpop: true requires dpop_key_cache")?;
            let key = get_or_create_dpop_key(key_cache, source_id).await?;
            tracing::debug!(source = %source_id, "oauth2 retrying token request with DPoP nonce");
            let iat = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system time")?
                .as_secs();
            let jti = format!("{}-{}", source_id, iat);
            let proof = build_dpop_proof(
                "POST",
                token_url.as_str(),
                &key,
                &jti,
                iat,
                Some(&nonce),
                None,
            )?;
            if use_private_key_jwt {
                let private_key_pem = config::read_secret(
                    client_private_key_file,
                    client_private_key_env.unwrap_or(""),
                )?;
                crate::audit::log_credential_access(audit, source_id, "oauth2_client_private_key");
                let new_assertion = build_client_assertion(
                    &client_id,
                    token_url.as_str(),
                    &private_key_pem,
                    source_id,
                )?;
                form.insert("client_assertion".into(), new_assertion);
            }
            let retry_req = client
                .post(token_url.as_str())
                .form(&form)
                .header("DPoP", proof);
            let retry_response = retry_req
                .send()
                .await
                .context("oauth2 token retry request")?;
            let retry_status = retry_response.status();
            let retry_body = retry_response
                .text()
                .await
                .context("oauth2 token retry response body")?;
            if !retry_status.is_success() {
                anyhow::bail!("oauth2 token error {}: {}", retry_status, retry_body);
            }
            let json: serde_json::Value =
                serde_json::from_str(&retry_body).context("oauth2 token response json")?;
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
            debug!(source = %source_id, expires_in, "oauth2 token refreshed (with DPoP nonce)");
            return Ok(access_token);
        }
        tracing::warn!(
            source = %source_id,
            "server returned use_dpop_nonce but no DPoP-Nonce header or nonce in body; \
             ensure the authorization server sends DPoP-Nonce with the 400 response per RFC 9449"
        );
    }

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

/// Build client_assertion JWT for private_key_jwt (RFC 7523): iss/sub=client_id, aud=token_url, RS256.
fn build_client_assertion(
    client_id: &str,
    token_url: &str,
    private_key_pem: &str,
    source_id: &str,
) -> anyhow::Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time")?;
    let iat = now.as_secs();
    let exp = iat + 300;
    let jti = format!("{}-{}-{}", source_id, iat, now.as_nanos());
    #[derive(serde::Serialize)]
    struct ClientAssertionClaims {
        iss: String,
        sub: String,
        aud: String,
        iat: u64,
        exp: u64,
        jti: String,
    }
    let claims = ClientAssertionClaims {
        iss: client_id.to_string(),
        sub: client_id.to_string(),
        aud: token_url.to_string(),
        iat,
        exp,
        jti,
    };
    let pem = private_key_pem.replace("\\n", "\n");
    let key = EncodingKey::from_rsa_pem(pem.as_bytes()).context("parse client private_key PEM")?;
    let token = encode(&Header::new(Algorithm::RS256), &claims, &key)
        .context("sign client_assertion JWT")?;
    Ok(token)
}

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Returns current valid access_token for the source when auth is Google Service Account; cached.
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
        } => (
            credentials_file.as_deref(),
            credentials_env.as_deref(),
            subject_env.as_deref(),
            subject_file.as_deref(),
            scopes,
        ),
        _ => anyhow::bail!("get_google_sa_token requires GoogleServiceAccount auth"),
    };
    let (creds_path, creds_env, subject_env, subject_file, scopes) = sa;
    let now = Instant::now();
    let buffer = Duration::from_secs(REFRESH_BUFFER_SECS);
    {
        let g = cache.read().await;
        if let Some((token, expires_at)) = g.get(source_id)
            && now + buffer < *expires_at
        {
            return Ok(token.clone());
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
    let json_str = json_str.context(
        "google_service_account credentials not set (credentials_file or credentials_env)",
    )?;
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
    let subject = subject_env.and_then(|e| std::env::var(e).ok()).or_else(|| {
        subject_file.and_then(|p| {
            if p.is_empty() {
                None
            } else {
                std::fs::read_to_string(Path::new(p))
                    .ok()
                    .map(|s| s.trim().to_string())
            }
        })
    });
    let scope = scopes.join(" ");
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
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
    let body = response
        .text()
        .await
        .context("google token response body")?;
    if !status.is_success() {
        anyhow::bail!("google token error {}: {}", status, body);
    }
    let json: serde_json::Value =
        serde_json::from_str(&body).context("google token response json")?;
    let access_token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .context("response missing access_token")?
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
    debug!(source = %source_id, expires_in, "google service account token obtained");
    Ok(access_token)
}
