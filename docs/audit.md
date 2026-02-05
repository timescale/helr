# Audit

Optional audit logging for credential access and config changes. When enabled, Hel logs **when** secrets are read and when config is loaded or reloaded; it does **not** log secret values.

## Configuration

```yaml
global:
  audit:
    enabled: true
    log_credential_access: true   # log when secrets are read (env/file)
    log_config_changes: true      # log config load and reload (e.g. SIGHUP)
    redact_secrets: true          # never log actual secret values (use [REDACTED])
```

- **`enabled`** — Turn audit on. When false or omitted, no audit events are logged. Default: false (audit off).
- **`log_credential_access`** — When true, log an audit event each time a credential is read (bearer token, API key, basic auth, OAuth2 client_id/client_secret/refresh_token/private_key, TLS cert/key/CA, Google Service Account credentials). Log line includes `source` and `kind` (e.g. `bearer_token`, `oauth2_client_secret`); the value is never logged. Default: true when audit is enabled.
- **`log_config_changes`** — When true, log when config is loaded at startup and when config is reloaded (e.g. on SIGHUP). Log line includes config file path. Default: true when audit is enabled.
- **`redact_secrets`** — When true, any future log that might contain a secret should use the audit redaction helper so the value appears as `[REDACTED]`. Credential-access audit events never include values regardless. Default: true when audit is enabled.

## What gets logged

- **Credential access:** One `audit: credential accessed` log per read, with `source` (source id) and `kind` (e.g. `bearer_token`, `api_key`, `basic_user`, `basic_password`, `oauth2_client_id`, `oauth2_client_secret`, `oauth2_client_private_key`, `oauth2_refresh_token`, `tls_client_cert`, `tls_client_key`, `tls_ca`, `google_service_account_credentials`, `google_service_account_subject`). Values are never logged.
- **Config load:** `audit: config loaded` with config file path (at startup).
- **Config reload:** `audit: config reloaded` with config file path (on SIGHUP).

## Example

With `global.audit.enabled: true` and a bearer-token source `okta-audit`:

```
{"ts":"2026-02-05T12:00:00Z","level":"info","msg":"audit: config loaded","path":"/etc/hel/hel.yaml"}
{"ts":"2026-02-05T12:00:01Z","level":"info","msg":"audit: credential accessed","source":"okta-audit","kind":"bearer_token"}
```

After SIGHUP reload:

```
{"ts":"2026-02-05T12:05:00Z","level":"info","msg":"audit: config reloaded","path":"/etc/hel/hel.yaml"}
```

## Secret redaction elsewhere

Hel does **not** redact secrets from **event payloads** (NDJSON output). To detect or redact secrets in log lines or NDJSON, use downstream tooling (e.g. [Grafana Alloy `loki.secretfilter`](https://github.com/grafana/alloy/blob/main/docs/sources/reference/components/loki/loki.secretfilter.md), [Gitleaks](https://github.com/gitleaks/gitleaks), or [sensleak-rs](https://github.com/crates-pro/sensleak-rs)).
