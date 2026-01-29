//! Retry layer for HTTP requests: exponential backoff, retryable status codes.
//! On 429, uses Retry-After (or X-RateLimit-Reset) when configured.

use crate::client::build_request;
use crate::config::{AuthConfig, RateLimitConfig, RetryConfig, SourceConfig};
use crate::oauth2::{get_oauth_token, invalidate_token, OAuth2TokenCache};
use anyhow::Context;
use reqwest::header::HeaderMap;
use reqwest::{Client, Response};
use std::time::Duration;
use tracing::warn;

/// HTTP status codes that are retried: 408 Request Timeout, 429 Too Many Requests, 5xx.
pub fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    let code = status.as_u16();
    code == 408 || code == 429 || (500..600).contains(&code)
}

/// Compute backoff duration for the given attempt (0 = first retry).
fn backoff_duration(retry: &RetryConfig, attempt: u32) -> Duration {
    let secs = (retry.initial_backoff_secs as f64) * retry.multiplier.powi(attempt as i32);
    let capped = retry
        .max_backoff_secs
        .map(|max| secs.min(max as f64))
        .unwrap_or(secs);
    Duration::from_secs_f64(capped.min(u64::MAX as f64))
}

/// Parse Retry-After (delta-seconds or HTTP-date) and optionally X-RateLimit-Reset (Unix timestamp).
/// Returns None if header missing or unparseable. Caps duration by max_cap_secs when given.
pub fn retry_after_from_headers(
    headers: &HeaderMap,
    max_cap_secs: Option<u64>,
) -> Option<Duration> {
    // Retry-After: delta-seconds (e.g. "120") or HTTP-date (e.g. "Wed, 21 Oct 2015 07:28:00 GMT")
    if let Some(v) = headers.get("Retry-After") {
        let s = v.to_str().ok()?.trim();
        if let Ok(secs) = s.parse::<u64>() {
            let d = Duration::from_secs(secs);
            return Some(cap_duration(d, max_cap_secs));
        }
        if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(s) {
            let until = dt.with_timezone(&chrono::Utc);
            let now = chrono::Utc::now();
            let secs = (until - now).num_seconds().max(0) as u64;
            return Some(cap_duration(Duration::from_secs(secs), max_cap_secs));
        }
        // Try common HTTP-date format
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S %Z") {
            let until = dt.with_timezone(&chrono::Utc);
            let now = chrono::Utc::now();
            let secs = (until - now).num_seconds().max(0) as u64;
            return Some(cap_duration(Duration::from_secs(secs), max_cap_secs));
        }
    }
    // Fallback: X-RateLimit-Reset or X-Rate-Limit-Reset (Okta) — Unix timestamp when limit resets
    for name in ["X-RateLimit-Reset", "X-Rate-Limit-Reset"] {
        if let Some(v) = headers.get(name) {
            let s = v.to_str().ok()?.trim();
            if let Ok(reset_ts) = s.parse::<i64>() {
                let now_secs = chrono::Utc::now().timestamp();
                let secs = (reset_ts - now_secs).max(0) as u64;
                return Some(cap_duration(Duration::from_secs(secs), max_cap_secs));
            }
        }
    }
    None
}

fn cap_duration(d: Duration, max_secs: Option<u64>) -> Duration {
    match max_secs {
        Some(cap) if d.as_secs() > cap => Duration::from_secs(cap),
        _ => d,
    }
}

/// Resolve Bearer token when auth is OAuth2 (refresh if needed).
async fn bearer_for_request(
    client: &Client,
    source: &SourceConfig,
    source_id: &str,
    token_cache: Option<&OAuth2TokenCache>,
) -> anyhow::Result<Option<String>> {
    match (&source.auth, token_cache) {
        (Some(auth @ AuthConfig::OAuth2 { .. }), Some(cache)) => {
            let token = get_oauth_token(cache, client, source_id, auth).await?;
            Ok(Some(token))
        }
        _ => Ok(None),
    }
}

/// Execute a GET or POST request with optional retries. Uses source auth and headers.
/// For POST, body is the request body (merged with cursor etc. by caller); when None, source.body is used.
/// Retries on: 408, 429, 5xx, and transport errors. On 429, uses Retry-After when rate_limit.respect_headers is true.
pub async fn execute_with_retry(
    client: &Client,
    source: &SourceConfig,
    source_id: &str,
    url: &str,
    body: Option<&serde_json::Value>,
    retry: Option<&RetryConfig>,
    rate_limit: Option<&RateLimitConfig>,
    token_cache: Option<&OAuth2TokenCache>,
) -> anyhow::Result<Response> {
    let retry = match retry {
        Some(r) if r.max_attempts > 0 => r,
        _ => {
            let bearer = bearer_for_request(client, source, source_id, token_cache).await?;
            let req = build_request(client, source, url, bearer.as_deref(), body)?;
            return client.execute(req).await.context("http request");
        }
    };

    let mut last_err = None;
    let mut auth_refresh_attempted = false;
    for attempt in 0..retry.max_attempts {
        let bearer = bearer_for_request(client, source, source_id, token_cache).await?;
        let req = build_request(client, source, url, bearer.as_deref(), body)?;
        match client.execute(req).await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                }
                if response.status().as_u16() == 401
                    && matches!(source.auth, Some(AuthConfig::OAuth2 { .. }))
                    && token_cache.is_some()
                    && !auth_refresh_attempted
                {
                    auth_refresh_attempted = true;
                    let _ = response.text().await;
                    invalidate_token(token_cache.unwrap(), source_id).await;
                    warn!(source = %source_id, "401 Unauthorized, refreshed OAuth token, retrying");
                    continue;
                }
                if !is_retryable_status(response.status()) {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    anyhow::bail!("http {} {}", status, body);
                }
                // Retryable status: prefer Retry-After on 429 when rate_limit.respect_headers
                let status = response.status();
                let delay = if status.as_u16() == 429
                    && rate_limit.map_or(false, |r| r.respect_headers)
                    && let Some(d) =
                        retry_after_from_headers(response.headers(), retry.max_backoff_secs)
                {
                    let d = if d.as_secs() > 0 {
                        d
                    } else {
                        backoff_duration(retry, attempt)
                    };
                    warn!(
                        status = %status,
                        attempt = attempt + 1,
                        max_attempts = retry.max_attempts,
                        delay_secs = d.as_secs_f64(),
                        "429 rate limit, waiting Retry-After"
                    );
                    d
                } else {
                    backoff_duration(retry, attempt)
                };
                let body = response.text().await.unwrap_or_default();
                last_err = Some(anyhow::anyhow!("http {} {}", status, body));
                if attempt + 1 < retry.max_attempts {
                    if !(status.as_u16() == 429 && rate_limit.map_or(false, |r| r.respect_headers)) {
                        warn!(
                            status = %status,
                            attempt = attempt + 1,
                            max_attempts = retry.max_attempts,
                            delay_secs = delay.as_secs_f64(),
                            "retryable response, backing off"
                        );
                    }
                    tokio::time::sleep(delay).await;
                }
            }
            Err(e) => {
                // Transport/network errors: retry
                last_err = Some(e.into());
                if attempt + 1 < retry.max_attempts {
                    let delay = backoff_duration(retry, attempt);
                    warn!(
                        error = %last_err.as_ref().unwrap(),
                        attempt = attempt + 1,
                        max_attempts = retry.max_attempts,
                        delay_secs = delay.as_secs_f64(),
                        "request failed, backing off"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    return Err(last_err.unwrap()).context("http request");
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no attempts")))
        .context("http request")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retryable_status() {
        use reqwest::StatusCode;
        assert!(is_retryable_status(StatusCode::REQUEST_TIMEOUT)); // 408
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS)); // 429
        assert!(is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_status(StatusCode::OK));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(StatusCode::UNAUTHORIZED));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn test_backoff_duration() {
        let retry = RetryConfig {
            max_attempts: 3,
            initial_backoff_secs: 1,
            max_backoff_secs: Some(30),
            multiplier: 2.0,
        };
        assert_eq!(backoff_duration(&retry, 0), Duration::from_secs(1));
        assert_eq!(backoff_duration(&retry, 1), Duration::from_secs(2));
        assert_eq!(backoff_duration(&retry, 2), Duration::from_secs(4));
        assert_eq!(backoff_duration(&retry, 10), Duration::from_secs(30)); // capped
    }

    #[test]
    fn test_retry_after_delta_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            "120".parse().unwrap(),
        );
        let d = retry_after_from_headers(&headers, None).unwrap();
        assert_eq!(d, Duration::from_secs(120));
        let d = retry_after_from_headers(&headers, Some(60)).unwrap();
        assert_eq!(d, Duration::from_secs(60)); // capped
    }

    #[test]
    fn test_retry_after_x_ratelimit_reset() {
        let mut headers = HeaderMap::new();
        let reset_ts = chrono::Utc::now().timestamp() + 90;
        headers.insert(
            "X-RateLimit-Reset",
            reset_ts.to_string().parse().unwrap(),
        );
        let d = retry_after_from_headers(&headers, None).unwrap();
        assert!(d.as_secs() >= 88 && d.as_secs() <= 92);
    }

    #[test]
    fn test_retry_after_x_rate_limit_reset_okta() {
        let mut headers = HeaderMap::new();
        let reset_ts = chrono::Utc::now().timestamp() + 45;
        headers.insert(
            "X-Rate-Limit-Reset",
            reset_ts.to_string().parse().unwrap(),
        );
        let d = retry_after_from_headers(&headers, None).unwrap();
        assert!(d.as_secs() >= 43 && d.as_secs() <= 47);
    }
}
