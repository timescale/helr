//! Retry layer for HTTP requests: exponential backoff, retryable status codes.

use crate::client::build_request;
use crate::config::{RetryConfig, SourceConfig};
use anyhow::Context;
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

/// Execute a GET request with optional retries. Uses source auth and headers.
/// Retries on: 408, 429, 5xx, and transport errors (timeouts, connection errors).
pub async fn execute_with_retry(
    client: &Client,
    source: &SourceConfig,
    url: &str,
    retry: Option<&RetryConfig>,
) -> anyhow::Result<Response> {
    let retry = match retry {
        Some(r) if r.max_attempts > 0 => r,
        _ => {
            let req = build_request(client, source, url)?;
            return client.execute(req).await.context("http request");
        }
    };

    let mut last_err = None;
    for attempt in 0..retry.max_attempts {
        let req = build_request(client, source, url)?;
        match client.execute(req).await {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                }
                if !is_retryable_status(response.status()) {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    anyhow::bail!("http {} {}", status, body);
                }
                // Retryable status: consume body and retry
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last_err = Some(anyhow::anyhow!("http {} {}", status, body));
                if attempt + 1 < retry.max_attempts {
                    let delay = backoff_duration(retry, attempt);
                    warn!(
                        status = %status,
                        attempt = attempt + 1,
                        max_attempts = retry.max_attempts,
                        delay_secs = delay.as_secs_f64(),
                        "retryable response, backing off"
                    );
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
}
