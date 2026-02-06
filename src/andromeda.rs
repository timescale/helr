//! Andromeda Security: convenience defaults for login-for-cookie auth.
//!
//! Uses the generic [login_cookie](crate::login_cookie) exchange with Andromeda's login URL and body key.

#![allow(dead_code)] // only used when hooks feature is enabled

use reqwest::Client;

pub const DEFAULT_LOGIN_URL: &str = "https://api.live.andromedasecurity.com/login/access-key";
pub const DEFAULT_BODY_KEY: &str = "code";

/// Exchange Andromeda PAT for session cookie (calls generic [login_cookie](crate::login_cookie) exchange with Andromeda defaults).
pub async fn exchange_pat_for_cookie(
    client: &Client,
    login_url: Option<&str>,
    pat: &str,
) -> anyhow::Result<String> {
    let url = login_url.unwrap_or(DEFAULT_LOGIN_URL);
    crate::login_cookie::exchange_credential_for_cookie(client, url, pat, DEFAULT_BODY_KEY).await
}
