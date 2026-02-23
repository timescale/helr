use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub(crate) fn default_bearer_prefix() -> String {
    "Bearer".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum AuthConfig {
    Bearer {
        token_env: String,
        #[serde(default)]
        token_file: Option<String>,
        /// Authorization header prefix (default "Bearer"). Use "SSWS" for Okta API tokens.
        #[serde(default = "default_bearer_prefix")]
        prefix: String,
    },
    ApiKey {
        header: String,
        key_env: String,
        #[serde(default)]
        key_file: Option<String>,
    },
    Basic {
        user_env: String,
        #[serde(default)]
        user_file: Option<String>,
        password_env: String,
        #[serde(default)]
        password_file: Option<String>,
    },
    #[serde(rename = "oauth2")]
    OAuth2 {
        token_url: String,
        client_id_env: String,
        #[serde(default)]
        client_id_file: Option<String>,
        #[serde(default)]
        client_secret_env: Option<String>,
        #[serde(default)]
        client_secret_file: Option<String>,
        /// When set, use private_key_jwt for token endpoint client auth (e.g. Okta Org AS). PEM from env or file.
        #[serde(default)]
        client_private_key_env: Option<String>,
        #[serde(default)]
        client_private_key_file: Option<String>,
        /// When set, use refresh_token grant; when omitted, use client_credentials grant (any provider).
        #[serde(default)]
        refresh_token_env: Option<String>,
        #[serde(default)]
        refresh_token_file: Option<String>,
        #[serde(default)]
        scopes: Option<Vec<String>>,
        /// When true, send DPoP (Demonstrating Proof-of-Possession) header on token and API requests (e.g. Okta).
        #[serde(default)]
        dpop: bool,
    },
    /// Google Service Account (JWT bearer grant). For GWS Admin SDK use domain-wide delegation: set subject to admin user email.
    #[serde(rename = "google_service_account")]
    GoogleServiceAccount {
        /// Path to service account JSON key file (or use credentials_env).
        #[serde(default)]
        credentials_file: Option<String>,
        /// Env var containing full service account JSON string (or use credentials_file).
        #[serde(default)]
        credentials_env: Option<String>,
        /// Env var for delegated user email (domain-wide delegation). Required for Admin SDK Reports API.
        #[serde(default)]
        subject_env: Option<String>,
        /// File containing delegated user email (domain-wide delegation).
        #[serde(default)]
        subject_file: Option<String>,
        scopes: Vec<String>,
    },
}

/// Resolve a secret from file path (if set) or environment variable. File takes precedence.
pub fn read_secret(file_path: Option<&str>, env_var: &str) -> anyhow::Result<String> {
    if let Some(p) = file_path
        && !p.is_empty()
    {
        let s = std::fs::read_to_string(Path::new(p))
            .with_context(|| format!("read secret file {:?}", p))?;
        return Ok(s.trim().to_string());
    }
    std::env::var(env_var).with_context(|| format!("env {} not set", env_var))
}
