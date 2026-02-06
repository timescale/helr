//! Optional audit: log credential access, config reloads.
//!
//! When `global.audit.enabled` is true:
//! - `log_credential_access`: log when secrets are read (source, kind); never log the value.
//! - `log_config_changes`: log when config is loaded or reloaded (e.g. SIGHUP).

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
