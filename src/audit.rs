//! Optional audit: log credential access, config reloads; redact secrets in logs.
//!
//! When `global.audit.enabled` is true:
//! - `log_credential_access`: log when secrets are read (source, kind); never log the value.
//! - `log_config_changes`: log when config is loaded or reloaded (e.g. SIGHUP).
//! - `redact_secrets`: use [REDACTED] instead of secret values in any log message.

use crate::config::AuditConfig;

/// Log that a credential was accessed for the given source and kind. Never logs the value.
pub fn log_credential_access(audit: Option<&AuditConfig>, source_id: &str, kind: &str) {
    let Some(a) = audit else { return };
    if !a.enabled || !a.log_credential_access {
        return;
    }
    tracing::info!(
        source = %source_id,
        kind = %kind,
        "audit: credential accessed"
    );
}

/// Log that config was loaded or reloaded (path only; no secret content).
pub fn log_config_change(audit: Option<&AuditConfig>, path: &std::path::Path, reload: bool) {
    let Some(a) = audit else { return };
    if !a.enabled || !a.log_config_changes {
        return;
    }
    if reload {
        tracing::info!(path = %path.display(), "audit: config reloaded");
    } else {
        tracing::info!(path = %path.display(), "audit: config loaded");
    }
}

/// Return value for logging: [REDACTED] when redact_secrets is true, else the actual value.
/// Use when a log message might contain a secret (e.g. token, password).
#[allow(dead_code)] // reserved for use when adding logs that might contain secrets
#[must_use]
pub fn redact_secret<'a>(audit: Option<&AuditConfig>, value: &'a str) -> std::borrow::Cow<'a, str> {
    match audit {
        Some(a) if a.enabled && a.redact_secrets => std::borrow::Cow::Borrowed("[REDACTED]"),
        _ => std::borrow::Cow::Borrowed(value),
    }
}
