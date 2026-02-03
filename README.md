# Hel

Hel is a generic HTTP API log collector. It polls audit-log and event APIs (Okta, Google Workspace, GitHub, Cloud Logging, and others), handles pagination and rate limits, keeps durable state, and emits **NDJSON** to stdout or to a file for downstream collectors (Grafana Alloy, Vector, Fluent Bit, Loki).

You configure one or more **sources** in YAML (URL, auth, pagination, schedule). Hel runs on an interval, fetches pages, checkpoints cursors, and writes one JSON object per event per line. Single binary, no runtime dependencies beyond the config and secrets.

## Supported features

- **Sources:** Okta System Log, Google Workspace (GWS) Admin SDK Reports API, GitHub organization audit log, Slack Enterprise audit logs, 1Password Events API (audit), Tailscale configuration audit and network flow logs, GWS via Cloud Logging (LogEntry format), and any HTTP API that returns a JSON array (items/events/entries/logs) with Link-header or cursor pagination
- **Auth:** Bearer (including SSWS for Okta), API key, Basic, OAuth2 (refresh token or client credentials; optional private_key_jwt, DPoP), Google Service Account (JWT, domain-wide delegation for GWS)
- **Pagination:** Link header (`rel=next`), cursor (query param or body), page/offset
- **Resilience:** Retries with backoff, circuit breaker, rate-limit handling (including Retry-After), optional per-page delay
- **State:** SQLite (or in-memory) for cursor/next_url; single-writer per store
- **Output:** NDJSON to stdout or file; optional rotation (daily or by size)
- **Session replay:** Record API responses to disk, replay without hitting the live API

## Install

### Cargo (from source)

You need Rust (e.g. [rustup](https://rustup.rs/)).

```bash
cargo install --path .
# or from a git checkout
cargo install --git https://github.com/your-org/hel.git
```

Binary will be `hel` in `~/.cargo/bin` (or your configured target dir).

### Build from source

```bash
git clone https://github.com/your-org/hel.git && cd hel
cargo build --release
./target/release/hel --help
```

## Quick start

```bash
# Validate config (fails if placeholders or secrets are missing)
hel validate

# One poll cycle (all sources)
hel run --once

# Continuous (NDJSON to stdout)
hel run

# Write to file with optional rotation
hel run --output /var/log/hel/events.ndjson
hel run --output /var/log/hel/events.ndjson --output-rotate daily
hel run --output /var/log/hel/events.ndjson --output-rotate size:100

# Test one source
hel test --source okta-audit
hel test --source gws-login
hel test --source github-audit
hel test --source slack-audit
hel test --source 1password-audit
hel test --source tailscale-audit
hel test --source tailscale-network

# State (inspect, reset, set cursor, export/import)
hel state show okta-audit
hel state reset okta-audit
hel state set okta-audit next_url "https://..."
hel state export
hel state import
```

Config path defaults to `hel.yaml`; override with `--config` per subcommand.

## Configuration

See [**`hel.yaml`**](./hel.yaml) in this repo for a minimal example. You define **sources** under `sources:`: each needs `url`, and usually `auth`, `pagination`, and `resilience`. Placeholders like `${OKTA_DOMAIN}` are expanded from the environment at load time.

### Config order of precedence

Configuration is merged in this order (later overrides earlier):

1. **Built-in defaults** (e.g. `log_level: info`, `schedule.interval_secs: 60`)
2. **Config file** (`hel.yaml` or path given by `--config`)
3. **Environment variables** — `HEL_LOG_LEVEL` and `HEL_LOG_FORMAT` override global log settings when set; placeholders like `${OKTA_DOMAIN}` are expanded from the environment at load time (no default; unset = error)
4. **CLI flags** — e.g. `--config` to choose the config file (no other config overrides via CLI today)

- **Auth:** `bearer` (with optional `prefix: SSWS` for Okta), `api_key`, `basic`, `oauth2` (refresh token or client credentials, e.g. Okta App Integration), `google_service_account` (GWS).
- **Pagination:** `link_header`, `cursor` (query or body), or `page_offset`. Cursor is merged into the request body for POST APIs (e.g. Cloud Logging `entries.list`).
- **Response array:** Hel looks for event arrays under `items`, `data`, `events`, `logs`, or `entries`.
- **Options:** `from` / `from_param`, `query_params`, `dedupe.id_path`, `rate_limit.page_delay_secs`, `max_pages`, and others - see `hel.yaml` comments and the manuals below.

**Output:** Each NDJSON line is one JSON object: `ts`, `source`, `endpoint`, `event` (raw payload), and `meta` (optional `cursor`, `request_id`). The producer label key defaults to `source`; value is the source id or `source_label_value`. With `log_format: json`, Hel’s own logs (stderr) use the same label key and value `hel`.

**State:** One writer per state store (e.g. one SQLite file). For multiple instances, use a shared backend (e.g. Redis/Postgres) when supported.

<details>
<summary><strong>Config reference (all options)</strong></summary>

#### Global (`global:`)

| Option | Description | Possible values | Default |
|--------|-------------|-----------------|---------|
| `log_level` | Logging level | `trace`, `debug`, `info`, `warn`, `error` | `info` |
| `log_format` | Hel log format (stderr) | `json`, `pretty` | — (none) |
| `source_label_key` | Key for producer label in NDJSON and Hel logs | string | — (effective: `source`) |
| `source_label_value` | Value for producer label in Hel’s own logs | string | — (effective: `hel`) |
| `state.backend` | State store backend | `sqlite`, `memory` | — |
| `state.path` | Path to state file (SQLite) or directory | string | — (e.g. `./hel-state.db`) |
| `health.enabled` | Enable health HTTP server | boolean | `false` |
| `health.address` | Health server bind address | string | `0.0.0.0` |
| `health.port` | Health server port | number | `8080` |

When `health.enabled` is true, GET `/healthz`, `/readyz`, and `/startupz` return detailed JSON (version, uptime, per-source status, circuit state, last_error). **Readyz semantics:** `/readyz` returns 200 only when (1) output path is writable (or stdout), (2) state store is connected (e.g. SQLite reachable), and (3) at least one source is healthy (circuit not open). The JSON includes `ready`, `output_writable`, `state_store_connected`, and `at_least_one_source_healthy` so you can see which condition failed.

| `metrics.enabled` | Enable Prometheus metrics server | boolean | `false` |
| `metrics.address` | Metrics server bind address | string | `0.0.0.0` |
| `metrics.port` | Metrics server port | number | `9090` |

Env overrides: `HEL_LOG_LEVEL`, `HEL_LOG_FORMAT` (or `RUST_LOG_JSON=1`) override global log settings when set.

---

#### Per-source (under `sources.<name>:`)

| Option | Description | Possible values | Default |
|--------|-------------|-----------------|---------|
| `url` | Request URL (GET or POST). Placeholders `${VAR}` expanded from env. | string | — (required) |
| `method` | HTTP method | `get`, `post` | `get` |
| `body` | Request body for POST (JSON). Cursor merged in when using cursor pagination. | object/array | — |
| `source_label_key` | Override producer label key for this source | string | — (use global) |
| `source_label_value` | Override producer label value for this source | string | source id |
| `schedule.interval_secs` | Poll interval in seconds | number | `60` |
| `schedule.jitter_secs` | Random jitter added to interval (seconds) | number | — |
| `auth` | Auth config; see Auth types below | object | — |
| `pagination` | Pagination config; see Pagination types below | object | — |
| `resilience` | Timeouts, retries, circuit breaker, rate limit; see Resilience below | object | — |
| `headers` | Extra HTTP headers (key: value) | map | — |
| `max_bytes` | Stop pagination when total response bytes exceed this (per poll) | number | — |
| `dedupe.id_path` | JSON path to event ID for deduplication (e.g. `uuid`, `id`, `event.id`) | string | — |
| `dedupe.capacity` | Max event IDs to keep (LRU) | number | `100000` |
| `on_cursor_error` | When API returns 4xx for cursor (e.g. expired) | `reset`, `fail` | — |
| `from` | Start of range for first request (e.g. ISO timestamp) | string | — |
| `from_param` | Query param name for `from` (e.g. `since`, `after`, `startTime`) | string | `since` (when `from` set) |
| `query_params` | Query params on first request only (e.g. `limit`, `filter`, `sortOrder`) | map (string or number values) | — |
| `on_parse_error` | When response parse or event extraction fails | `skip`, `fail` | `fail` |
| `max_response_bytes` | Fail if a single response body exceeds this (bytes) | number | — |
| `on_invalid_utf8` | When response body is not valid UTF-8 | `replace`, `escape`, `fail` | — |
| `max_line_bytes` | Max size of one emitted NDJSON line (bytes) | number | — |
| `max_line_bytes_behavior` | When a line exceeds `max_line_bytes` | `truncate`, `skip`, `fail` | — |
| `checkpoint` | When to write state | `end_of_tick`, `per_page` | — |
| `on_state_write_error` | When state store write fails (e.g. disk full) | `fail`, `skip_checkpoint` | `fail` |

---

#### Auth types (`auth.type`)

| Type | Required fields | Optional / notes |
|------|-----------------|------------------|
| `bearer` | `token_env` | `token_file`, `prefix` (default `Bearer`; use `SSWS` for Okta) |
| `api_key` | `header`, `key_env` | `key_file` |
| `basic` | `user_env`, `password_env` | `user_file`, `password_file` |
| `oauth2` | `token_url`, `client_id_env`; `client_secret_env` **or** `client_private_key_env` (PEM) | `refresh_token_env` (omit for client_credentials), `*_file` for each, `scopes`, `dpop` (true when server requires DPoP, e.g. Okta). Use `client_private_key_*` for Okta Org AS (private_key_jwt). Provider-agnostic. |
| `google_service_account` | `scopes` (list) | `credentials_file` or `credentials_env`; `subject_env` or `subject_file` (admin email for domain-wide delegation) |

Secrets can be read from env var or file; file takes precedence when set.

---

#### Pagination types (`pagination.strategy`)

| Strategy | Required fields | Optional | Defaults |
|----------|-----------------|----------|----------|
| `link_header` | — | `rel` (Link relation), `max_pages` | `rel: next` |
| `cursor` | `cursor_param`, `cursor_path` | `max_pages` | — |
| `page_offset` | `page_param`, `limit_param`, `limit` | `max_pages` | — |

- **link_header:** Next URL from `Link` header (e.g. `rel="next"`).
- **cursor:** Cursor from response JSON at `cursor_path`; sent as query param `cursor_param` (GET) or merged into body (POST).
- **page_offset:** Query params `page_param` (1-based page) and `limit_param` (page size); `limit` is the value.

---

#### Resilience (`resilience:`)

| Option | Description | Possible values | Default |
|--------|-------------|-----------------|---------|
| `timeout_secs` | HTTP request timeout (seconds) | number | `30` |
| `retries.max_attempts` | Max attempts per request (0 = no retries) | number | `3` |
| `retries.initial_backoff_secs` | Initial backoff (seconds) | number | `1` |
| `retries.max_backoff_secs` | Cap on backoff (seconds) | number | — |
| `retries.multiplier` | Backoff multiplier per attempt | number | `2.0` |
| `circuit_breaker.enabled` | Enable circuit breaker | boolean | `true` |
| `circuit_breaker.failure_threshold` | Failures before opening | number | `5` |
| `circuit_breaker.success_threshold` | Successes in half-open to close | number | `2` |
| `circuit_breaker.half_open_timeout_secs` | Seconds before half-open probe | number | `60` |
| `rate_limit.respect_headers` | Use Retry-After / X-RateLimit-Reset on 429 | boolean | `true` |
| `rate_limit.page_delay_secs` | Delay between pagination requests (seconds) | number | — |

</details>

## Documentation

| Doc | Description |
|-----|-------------|
| [hel.yaml](hel.yaml) | Example config with Okta, GWS, GitHub, Slack, 1Password, and Tailscale sources (commented where inactive). |
| [docs/okta.md](docs/okta.md) | Okta System Log: API token (SSWS) or OAuth2 App Integration; link-header pagination, replay. |
| [docs/gws-gcp.md](docs/gws-gcp.md) | GWS audit logs: OAuth2 refresh token or service account + domain-wide delegation. |
| [docs/github.md](docs/github.md) | GitHub organization audit log: PAT (classic) or GitHub App token; link-header pagination. |
| [docs/slack.md](docs/slack.md) | Slack Enterprise audit logs: user token (xoxp) with auditlogs:read; cursor pagination; Enterprise only. |
| [docs/1password.md](docs/1password.md) | 1Password Events API (audit): bearer token from Events Reporting; POST, cursor in body. |
| [docs/tailscale.md](docs/tailscale.md) | Tailscale configuration audit logs and network flow logs: API token (Basic auth); time-window GET; no pagination. |

## How to run with Okta

**API token (SSWS):** Create an API token in Okta Admin (**Security** → **API** → **Tokens**). In `hel.yaml`, add an Okta source; set `OKTA_DOMAIN` and `OKTA_API_TOKEN`. Run: `hel validate` then `hel test --source okta-audit` or `hel run`.

**OAuth2 (App Integration):** Use an API Services app with client credentials, private key (JWT), and optional DPoP — see [docs/okta.md](docs/okta.md).

Full steps and troubleshooting: **[docs/okta.md](docs/okta.md)**.

## How to run with Google Workspace (GWS)

**Option A (no service account):** Create an OAuth 2.0 Client ID in Google Cloud Console, get a refresh token once via [OAuth Playground](https://developers.google.com/oauthplayground/) (signed in as a Workspace admin), then use `auth.type: oauth2` with that token.

**Option B (service account):** Create a service account in GCP, enable domain-wide delegation in GWS Admin for the Admin SDK Reports API scope, download the JSON key, then use `auth.type: google_service_account` with `credentials_file` and `subject_env` (admin email).

Full steps: **[docs/gws-gcp.md](docs/gws-gcp.md)**.

## How to run with GitHub Enterprise

Create a personal access token (classic) with **read:audit_log**. You must be an **organization owner**. Set `GITHUB_ORG` (your org login) and `GITHUB_TOKEN`. Uncomment the `github-audit` source in `hel.yaml` and run: `hel validate` then `hel test --source github-audit` or `hel run`.

Full steps and troubleshooting: **[docs/github.md](docs/github.md)**.

## How to run with Slack Enterprise

Create a Slack app with the **auditlogs:read** user token scope. Install it on your **Enterprise organization** (not a single workspace) as the **Owner**. Copy the user OAuth token (`xoxp-...`) and set `SLACK_AUDIT_TOKEN`. Uncomment the `slack-audit` source in `hel.yaml` and run: `hel validate` then `hel test --source slack-audit` or `hel run`. The Audit Logs API is **only available for Slack Enterprise** workspaces.

Full steps and troubleshooting: **[docs/slack.md](docs/slack.md)**.

## How to run with 1Password (Business)

Create an **Events Reporting integration** in your 1Password Business account (**Integrations** → **Directory** → Events Reporting). Issue a bearer token with **Audit events** enabled and save it (e.g. as `ONEPASSWORD_EVENTS_TOKEN`). Uncomment the `1password-audit` source in `hel.yaml` and run: `hel validate` then `hel test --source 1password-audit` or `hel run`. The audit endpoint is **POST** with cursor-in-body pagination.

Full steps and troubleshooting: **[docs/1password.md](docs/1password.md)**.

## How to run with Tailscale

Create an **API access token** with **logs:configuration:read** (and optionally **logs:network:read** for network flow logs) in the Tailscale admin console (**Settings** → **Keys**). Set `TAILNET_ID` (your tailnet name, e.g. `example.com`), `TAILSCALE_START` and `TAILSCALE_END` (RFC3339 time window), `TAILSCALE_API_TOKEN` (the token), and `TAILSCALE_API_TOKEN_PASSWORD=""` (empty password for Basic auth). Uncomment the `tailscale-audit` source (and `tailscale-network` for [network flow logs](https://tailscale.com/kb/1219/network-flow-logs/) — Premium/Enterprise only; enable in admin console) in `hel.yaml` and run: `hel validate` then `hel test --source tailscale-audit` or `hel test --source tailscale-network` or `hel run`. Both APIs return all events in the window in one response (no pagination).

Full steps and troubleshooting: **[docs/tailscale.md](docs/tailscale.md)**.

## Session replay

Record API responses once, then replay from disk to test the pipeline without hitting the live API: `hel run --once --record-dir ./recordings` to save; `hel run --once --replay-dir ./recordings` to replay.

## License

MIT OR Apache-2.0
