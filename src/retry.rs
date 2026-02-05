//! Retry layer for HTTP requests: exponential backoff, retryable status codes.
//! On 429, uses Retry-After (or X-RateLimit-Reset) when configured.

use crate::client::build_request;
use crate::config::{
    AuthConfig, AuditConfig, HttpMethod, RateLimitConfig, RateLimitHeaderMapping, RetryConfig,
    SourceConfig,
};
use crate::dpop::{DPoPKeyCache, build_dpop_proof, get_or_create_dpop_key};
use crate::oauth2::{OAuth2TokenCache, get_google_sa_token, get_oauth_token, invalidate_token};
use anyhow::Context;
use rand::Rng;
use reqwest::header::HeaderMap;
use reqwest::{Client, Response};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

fn is_5xx(code: u16) -> bool {
    (500..600).contains(&code)
}

/// Returns true if status should be retried. Uses `retryable_status_codes` when set, else 408, 429, 5xx.
pub fn is_retryable_status_with_codes(
    status: reqwest::StatusCode,
    retryable_codes: Option<&[u16]>,
) -> bool {
    let code = status.as_u16();
    match retryable_codes {
        Some(codes) => codes.contains(&code),
        None => code == 408 || code == 429 || is_5xx(code),
    }
}

/// Compute backoff duration for the given attempt (0 = first retry). Applies jitter when retry.jitter is set.
fn backoff_duration(retry: &RetryConfig, attempt: u32) -> Duration {
    let secs = (retry.initial_backoff_secs as f64) * retry.multiplier.powi(attempt as i32);
    let capped = retry
        .max_backoff_secs
        .map(|max| secs.min(max as f64))
        .unwrap_or(secs);
    let base_secs = capped.min(u64::MAX as f64);
    let delay_secs = if let Some(j) = retry.jitter {
        let j = j.clamp(0.0, 1.0);
        let mut rng = rand::rng();
        let factor = 1.0 + rng.random_range(-j..=j);
        (base_secs * factor).max(0.0)
    } else {
        base_secs
    };
    Duration::from_secs_f64(delay_secs)
}

/// Parse Retry-After (delta-seconds or HTTP-date) and optionally reset header (e.g. X-RateLimit-Reset).
/// Uses header_mapping.reset_header when set, else tries X-RateLimit-Reset, X-Rate-Limit-Reset.
/// Returns None if header missing or unparseable. Caps duration by max_cap_secs when given.
pub fn retry_after_from_headers(
    headers: &HeaderMap,
    max_cap_secs: Option<u64>,
    header_mapping: Option<&RateLimitHeaderMapping>,
) -> Option<Duration> {
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
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S %Z") {
            let until = dt.with_timezone(&chrono::Utc);
            let now = chrono::Utc::now();
            let secs = (until - now).num_seconds().max(0) as u64;
            return Some(cap_duration(Duration::from_secs(secs), max_cap_secs));
        }
    }
    let reset_names: Vec<&str> = header_mapping
        .and_then(|m| m.reset_header.as_deref())
        .map(|n| vec![n])
        .unwrap_or_else(|| vec!["X-RateLimit-Reset", "X-Rate-Limit-Reset"]);
    for name in &reset_names {
        if let Some(v) = headers.get(*name) {
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

/// Rate limit info parsed from response headers (for adaptive rate limiting).
#[derive(Debug, Clone, Default)]
pub struct RateLimitInfo {
    /// Rate limit ceiling (e.g. X-RateLimit-Limit). Used for logging and future adaptive use.
    #[allow(dead_code)]
    pub limit: Option<u64>,
    pub remaining: Option<i64>,
    pub reset_ts: Option<i64>,
}

/// Parse limit, remaining, reset from response headers using optional header mapping.
pub fn rate_limit_info_from_headers(
    headers: &HeaderMap,
    header_mapping: Option<&RateLimitHeaderMapping>,
) -> RateLimitInfo {
    let limit_h = header_mapping
        .and_then(|m| m.limit_header.as_deref())
        .unwrap_or("X-RateLimit-Limit");
    let remaining_h = header_mapping
        .and_then(|m| m.remaining_header.as_deref())
        .unwrap_or("X-RateLimit-Remaining");
    let reset_h = header_mapping
        .and_then(|m| m.reset_header.as_deref())
        .unwrap_or("X-RateLimit-Reset");
    let limit = headers
        .get(limit_h)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok());
    let remaining = headers
        .get(remaining_h)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok());
    let reset_ts = headers
        .get(reset_h)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok());
    RateLimitInfo {
        limit,
        remaining,
        reset_ts,
    }
}

fn cap_duration(d: Duration, max_secs: Option<u64>) -> Duration {
    match max_secs {
        Some(cap) if d.as_secs() > cap => Duration::from_secs(cap),
        _ => d,
    }
}

/// Resolve Bearer token when auth is OAuth2 or Google Service Account (refresh if needed).
async fn bearer_for_request(
    client: &Client,
    source: &SourceConfig,
    source_id: &str,
    token_cache: Option<&OAuth2TokenCache>,
    dpop_key_cache: Option<&DPoPKeyCache>,
    audit: Option<&AuditConfig>,
) -> anyhow::Result<Option<String>> {
    match (&source.auth, token_cache) {
        (Some(auth @ AuthConfig::OAuth2 { .. }), Some(cache)) => {
            let token =
                get_oauth_token(cache, client, source_id, auth, dpop_key_cache, audit).await?;
            Ok(Some(token))
        }
        (Some(auth @ AuthConfig::GoogleServiceAccount { .. }), Some(cache)) => {
            let token = get_google_sa_token(cache, client, source_id, auth).await?;
            Ok(Some(token))
        }
        _ => Ok(None),
    }
}

fn need_dpop_proof(source: &SourceConfig) -> bool {
    matches!(&source.auth, Some(AuthConfig::OAuth2 { dpop: true, .. }))
}

/// DPoP proof for this request when source has dpop; optional nonce and access_token for ath.
async fn dpop_proof_for_request(
    source_id: &str,
    source: &SourceConfig,
    url: &str,
    dpop_key_cache: Option<&DPoPKeyCache>,
    nonce: Option<&str>,
    access_token: Option<&str>,
) -> anyhow::Result<Option<String>> {
    if !need_dpop_proof(source) {
        return Ok(None);
    }
    let key_cache = match dpop_key_cache {
        Some(c) => c,
        None => return Ok(None),
    };
    let key = get_or_create_dpop_key(key_cache, source_id).await?;
    let method = match source.method {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
    };
    let iat = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time")?
        .as_secs();
    let jti = format!(
        "{}-{}-{}",
        source_id,
        iat,
        std::time::Instant::now().elapsed().as_nanos()
    );
    let proof = build_dpop_proof(method, url, &key, &jti, iat, nonce, access_token)?;
    Ok(Some(proof))
}

/// Execute a GET or POST request with optional retries. Uses source auth and headers.
/// Retries on 408, 429, 5xx, and transport errors; on 429 uses Retry-After when rate_limit.respect_headers is true.
#[allow(clippy::too_many_arguments)]
pub async fn execute_with_retry(
    client: &Client,
    source: &SourceConfig,
    source_id: &str,
    url: &str,
    body: Option<&serde_json::Value>,
    retry: Option<&RetryConfig>,
    rate_limit: Option<&RateLimitConfig>,
    token_cache: Option<&OAuth2TokenCache>,
    dpop_key_cache: Option<&DPoPKeyCache>,
    audit: Option<&AuditConfig>,
) -> anyhow::Result<Response> {
    let retry = match retry {
        Some(r) if r.max_attempts > 0 => r,
        _ => {
            let bearer = bearer_for_request(
                client,
                source,
                source_id,
                token_cache,
                dpop_key_cache,
                audit,
            )
            .await?;
            let dpop_proof = dpop_proof_for_request(
                source_id,
                source,
                url,
                dpop_key_cache,
                None,
                bearer.as_deref(),
            )
            .await?;
            let req = build_request(
                client,
                source,
                url,
                bearer.as_deref(),
                body,
                dpop_proof,
                source_id,
                audit,
            )?;
            return client.execute(req).await.context("http request");
        }
    };

    let mut last_err = None;
    let mut auth_refresh_attempted = false;
    for attempt in 0..retry.max_attempts {
        let bearer = bearer_for_request(
            client,
            source,
            source_id,
            token_cache,
            dpop_key_cache,
            audit,
        )
        .await?;
        let dpop_proof = dpop_proof_for_request(
            source_id,
            source,
            url,
            dpop_key_cache,
            None,
            bearer.as_deref(),
        )
        .await?;
        let req = build_request(
            client,
            source,
            url,
            bearer.as_deref(),
            body,
            dpop_proof,
            source_id,
            audit,
        )?;
        match client.execute(req).await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                }
                if response.status().as_u16() == 401
                    && matches!(
                        source.auth,
                        Some(AuthConfig::OAuth2 { .. } | AuthConfig::GoogleServiceAccount { .. })
                    )
                    && !auth_refresh_attempted
                    && let Some(cache) = token_cache
                {
                    auth_refresh_attempted = true;
                    let _ = response.text().await;
                    invalidate_token(cache, source_id).await;
                    warn!(source = %source_id, "401 Unauthorized, refreshed OAuth token, retrying");
                    continue;
                }
                if !is_retryable_status_with_codes(
                    response.status(),
                    retry.retryable_status_codes.as_deref(),
                ) {
                    let status = response.status();
                    let dpop_nonce_header = response
                        .headers()
                        .get("DPoP-Nonce")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.trim().to_string());
                    let response_body = response.text().await.unwrap_or_default();
                    if need_dpop_proof(source)
                        && (status.as_u16() == 400 || status.as_u16() == 401)
                        && dpop_key_cache.is_some()
                    {
                        let nonce = dpop_nonce_header.or_else(|| {
                            serde_json::from_str::<serde_json::Value>(&response_body)
                                .ok()
                                .and_then(|v| {
                                    v.get("nonce")
                                        .or(v.get("dpop_nonce"))
                                        .and_then(|n| n.as_str())
                                        .map(|s| s.to_string())
                                })
                        });
                        if let Some(ref nonce) = nonce {
                            let dpop_proof_with_nonce = dpop_proof_for_request(
                                source_id,
                                source,
                                url,
                                dpop_key_cache,
                                Some(nonce),
                                bearer.as_deref(),
                            )
                            .await?;
                            let retry_req = build_request(
                                client,
                                source,
                                url,
                                bearer.as_deref(),
                                body,
                                dpop_proof_with_nonce,
                                source_id,
                                audit,
                            )?;
                            match client.execute(retry_req).await {
                                Ok(retry_response) if retry_response.status().is_success() => {
                                    return Ok(retry_response);
                                }
                                Ok(retry_response) => {
                                    let retry_status = retry_response.status();
                                    let retry_body =
                                        retry_response.text().await.unwrap_or_default();
                                    anyhow::bail!(
                                        "http {} {} (url: {}, after DPoP nonce retry)",
                                        retry_status,
                                        retry_body,
                                        url
                                    );
                                }
                                Err(e) => {
                                    return Err(anyhow::Error::from(e)).context("http request");
                                }
                            }
                        }
                    }
                    anyhow::bail!("http {} {} (url: {})", status, response_body, url);
                }
                let status = response.status();
                let delay = if status.as_u16() == 429
                    && rate_limit.is_some_and(|r| r.respect_headers)
                    && let Some(d) = retry_after_from_headers(
                        response.headers(),
                        retry.max_backoff_secs,
                        rate_limit.and_then(|r| r.headers.as_ref()),
                    ) {
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
                    if !(status.as_u16() == 429 && rate_limit.is_some_and(|r| r.respect_headers)) {
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

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no attempts"))).context("http request")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retryable_status() {
        use reqwest::StatusCode;
        // default (None) = 408, 429, 5xx
        assert!(is_retryable_status_with_codes(
            StatusCode::REQUEST_TIMEOUT,
            None
        )); // 408
        assert!(is_retryable_status_with_codes(
            StatusCode::TOO_MANY_REQUESTS,
            None
        )); // 429
        assert!(is_retryable_status_with_codes(
            StatusCode::INTERNAL_SERVER_ERROR,
            None
        ));
        assert!(is_retryable_status_with_codes(
            StatusCode::BAD_GATEWAY,
            None
        ));
        assert!(is_retryable_status_with_codes(
            StatusCode::SERVICE_UNAVAILABLE,
            None
        ));
        assert!(!is_retryable_status_with_codes(StatusCode::OK, None));
        assert!(!is_retryable_status_with_codes(
            StatusCode::BAD_REQUEST,
            None
        ));
        assert!(!is_retryable_status_with_codes(
            StatusCode::UNAUTHORIZED,
            None
        ));
        assert!(!is_retryable_status_with_codes(StatusCode::NOT_FOUND, None));
    }

    #[test]
    fn test_backoff_duration() {
        let retry = RetryConfig {
            max_attempts: 3,
            initial_backoff_secs: 1,
            max_backoff_secs: Some(30),
            multiplier: 2.0,
            jitter: None,
            retryable_status_codes: None,
        };
        assert_eq!(backoff_duration(&retry, 0), Duration::from_secs(1));
        assert_eq!(backoff_duration(&retry, 1), Duration::from_secs(2));
        assert_eq!(backoff_duration(&retry, 2), Duration::from_secs(4));
        assert_eq!(backoff_duration(&retry, 10), Duration::from_secs(30)); // capped
    }

    #[test]
    fn test_retry_after_delta_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "120".parse().unwrap());
        let d = retry_after_from_headers(&headers, None, None).unwrap();
        assert_eq!(d, Duration::from_secs(120));
        let d = retry_after_from_headers(&headers, Some(60), None).unwrap();
        assert_eq!(d, Duration::from_secs(60)); // capped
    }

    #[test]
    fn test_retry_after_x_ratelimit_reset() {
        let mut headers = HeaderMap::new();
        let reset_ts = chrono::Utc::now().timestamp() + 90;
        headers.insert("X-RateLimit-Reset", reset_ts.to_string().parse().unwrap());
        let d = retry_after_from_headers(&headers, None, None).unwrap();
        assert!(d.as_secs() >= 88 && d.as_secs() <= 92);
    }

    #[test]
    fn test_is_retryable_status_with_codes_custom_list() {
        use reqwest::StatusCode;
        let codes = [502u16, 503];
        assert!(is_retryable_status_with_codes(
            StatusCode::BAD_GATEWAY,
            Some(&codes)
        ));
        assert!(is_retryable_status_with_codes(
            StatusCode::SERVICE_UNAVAILABLE,
            Some(&codes)
        ));
        assert!(!is_retryable_status_with_codes(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(&codes)
        ));
        assert!(!is_retryable_status_with_codes(
            StatusCode::OK,
            Some(&codes)
        ));
    }

    #[test]
    fn test_backoff_duration_with_jitter_in_bounds() {
        let retry = RetryConfig {
            max_attempts: 3,
            initial_backoff_secs: 2,
            max_backoff_secs: Some(60),
            multiplier: 2.0,
            jitter: Some(0.1),
            retryable_status_codes: None,
        };
        for attempt in 0..5 {
            let d = backoff_duration(&retry, attempt);
            let base = 2.0 * 2.0f64.powi(attempt as i32);
            let min_secs = base * 0.9;
            let max_secs = base * 1.1;
            assert!(
                d.as_secs_f64() >= min_secs - 0.01 && d.as_secs_f64() <= max_secs + 0.01,
                "attempt {}: delay {} should be in [{}, {}]",
                attempt,
                d.as_secs_f64(),
                min_secs,
                max_secs
            );
        }
    }

    #[test]
    fn test_retry_after_x_rate_limit_reset_okta() {
        let mut headers = HeaderMap::new();
        let reset_ts = chrono::Utc::now().timestamp() + 45;
        headers.insert("X-Rate-Limit-Reset", reset_ts.to_string().parse().unwrap());
        let d = retry_after_from_headers(&headers, None, None).unwrap();
        assert!(d.as_secs() >= 43 && d.as_secs() <= 47);
    }

    #[test]
    fn test_retry_after_with_custom_reset_header() {
        let mut headers = HeaderMap::new();
        let reset_ts = chrono::Utc::now().timestamp() + 30;
        headers.insert("X-Rate-Limit-Reset", reset_ts.to_string().parse().unwrap());
        let mapping = RateLimitHeaderMapping {
            limit_header: None,
            remaining_header: None,
            reset_header: Some("X-Rate-Limit-Reset".to_string()),
        };
        let d = retry_after_from_headers(&headers, None, Some(&mapping)).unwrap();
        assert!(d.as_secs() >= 28 && d.as_secs() <= 32);
    }

    #[test]
    fn test_rate_limit_info_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("X-RateLimit-Limit", "100".parse().unwrap());
        headers.insert("X-RateLimit-Remaining", "42".parse().unwrap());
        headers.insert("X-RateLimit-Reset", "1700000000".parse().unwrap());
        let info = rate_limit_info_from_headers(&headers, None);
        assert_eq!(info.limit, Some(100));
        assert_eq!(info.remaining, Some(42));
        assert_eq!(info.reset_ts, Some(1700000000));
    }

    #[test]
    fn test_rate_limit_info_from_headers_custom_names() {
        let mut headers = HeaderMap::new();
        headers.insert("RateLimit-Limit", "50".parse().unwrap());
        headers.insert("RateLimit-Remaining", "0".parse().unwrap());
        headers.insert("RateLimit-Reset", "1700000100".parse().unwrap());
        let mapping = RateLimitHeaderMapping {
            limit_header: Some("RateLimit-Limit".to_string()),
            remaining_header: Some("RateLimit-Remaining".to_string()),
            reset_header: Some("RateLimit-Reset".to_string()),
        };
        let info = rate_limit_info_from_headers(&headers, Some(&mapping));
        assert_eq!(info.limit, Some(50));
        assert_eq!(info.remaining, Some(0));
        assert_eq!(info.reset_ts, Some(1700000100));
    }
}
