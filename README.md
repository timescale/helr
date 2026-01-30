# Hel

**Hel** is a generic HTTP API log collector, which collects and feeds log streams into Loki (and other backends).

- Polls HTTP APIs (Okta, GitHub, Google Workspace, etc.)
- Handles pagination (Link header, cursor, page/offset), rate limits, and durable state
- Emits **NDJSON to stdout or to a file** (optional log rotation) for downstream collectors (Grafana Alloy, Vector, Fluent Bit)

## Quick start

```bash
# Validate config
hel validate

# Run one poll cycle
hel run --once

# Run continuously (NDJSON to stdout)
hel run

# Write to file (optional rotation: daily or size in MB)
hel run --output /var/log/hel/events.ndjson
hel run --output /var/log/hel/events.ndjson --output-rotate daily
hel run --output /var/log/hel/events.ndjson --output-rotate size:100

# Test a single source (one poll tick)
hel test --source okta-audit

# Session replay: record responses then replay without hitting the live API
hel run --once --record-dir ./recordings
hel run --once --replay-dir ./recordings

# Mock server: run a fake API from YAML (match query, serve fixture responses) for development
hel mock-server mocks/okta.yaml
# Then point hel.yaml sources at http://127.0.0.1:<port> and run hel run --once in another terminal

# Inspect or manage state
hel state show okta-audit
hel state reset okta-audit
hel state set okta-audit next_url "https://example.com/logs?after=xyz"
hel state export
hel state import
```

## Config

See `hel.yaml` for a minimal example. Config path is per subcommand (e.g. `hel run --config hel.yaml`; default `hel.yaml`). Required: `sources` with at least one source (`url`, optional `schedule`, `auth`, `pagination`, `resilience`). Placeholders like `${OKTA_DOMAIN}` are expanded at load time. **HTTP method:** use **`method: post`** and **`body`** (JSON) for POST-only APIs (e.g. [Cloud Logging `entries.list`](https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list)); with cursor pagination the cursor is merged into the body. Response arrays are read from top-level or from keys: **`items`**, **`data`**, **`events`**, **`logs`**, **`entries`** (Cloud Logging). **Google Workspace (GWS)** audit logs: use the Admin SDK Reports API (GET, cursor `pageToken`/`nextPageToken`) — see commented `gws-login-audit` in `hel.yaml`. Corner-case options: **`from`** / **`from_param`** (start of range for first request), **`query_params`** (first-request params: limit, until, filter, q, sortOrder), `on_cursor_error`, `on_parse_error`, `on_invalid_utf8`, `on_state_write_error`, `max_response_bytes`, `max_line_bytes` / `max_line_bytes_behavior`, `checkpoint`, `dedupe.id_path`, `rate_limit.page_delay_secs`. Unset config placeholders and missing auth secrets fail at startup.

**Output:** Each NDJSON line has a top-level **`source`** field (the config source key, e.g. `okta-audit`). Override with **`source_label`** per source for downstream labeling (e.g. Loki) or a stable name. Events are emitted in page order (order received). Timestamp order across pages is not guaranteed unless the API guarantees it; downstream can sort if needed.

**State:** Single-writer assumption — only one Hel process should use a given state store (e.g. one SQLite file). For multi-instance deployments use Redis or Postgres state backend.

## Status

- **v0.1 & v0.2:** CLI (run, validate, test, state), config load, SQLite + in-memory state store, HTTP client, **GET and POST** (optional `body`), link-header / cursor / page-offset pagination (cursor merged into body for POST), poll loop, retry, scheduler, health, metrics, graceful shutdown, dedupe, concurrent sources, file output with log rotation, **session replay** (`--record-dir`, `--replay-dir`), **mock server** (`hel mock-server <CONFIG>`). **`hel test --source NAME`** and **`hel state show/reset/set/export/import`** implemented.

## License

MIT OR Apache-2.0
