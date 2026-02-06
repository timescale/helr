//! Generic "login for cookie" auth: POST credential to a login URL, use Set-Cookie on subsequent requests.
//!
//! Reusable for any API that exchanges a token/code for a session cookie (e.g. Andromeda PAT, other SaaS).

#![allow(dead_code)] // only used when hooks feature is enabled

use anyhow::Context;
use reqwest::Client;

/// Exchange a credential for a session cookie. POSTs `{ body_key: credential }` to login_url, returns Set-Cookie value.
pub async fn exchange_credential_for_cookie(
    client: &Client,
    login_url: &str,
    credential: &str,
    body_key: &str,
) -> anyhow::Result<String> {
    let body = serde_json::json!({ body_key: credential });
    let response = client
        .post(login_url)
        .json(&body)
        .send()
        .await
        .context("login-for-cookie request")?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "login-for-cookie failed: {} {}",
            status.as_u16(),
            text.lines().next().unwrap_or("").trim()
        );
    }
    let set_cookies: Vec<_> = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_string())
        .collect();
    if set_cookies.is_empty() {
        anyhow::bail!("login-for-cookie response missing set-cookie header");
    }
    Ok(set_cookies.join("; "))
}
