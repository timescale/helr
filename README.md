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
hel --mock-server --mock-config mocks/okta.yaml
# Then point hel.yaml sources at http://127.0.0.1:<port> and run hel run --once in another terminal

# Inspect or manage state
hel state show okta-audit
hel state reset okta-audit
hel state set okta-audit next_url "https://example.com/logs?after=xyz"
hel state export
hel state import
```

## Config

See `hel.yaml` for a minimal example. Required: `sources` with at least one source (`url`, optional `schedule`, `auth`, `pagination`, `resilience`). Placeholders like `${OKTA_DOMAIN}` are expanded at load time. Corner-case options: `cursor_expired`, `initial_since`/`since_param`, `on_parse_error`, `max_response_bytes`, `invalid_utf8`, `max_event_bytes`/`max_event_bytes_behavior`, `checkpoint`, `on_state_write_error`. Unset config placeholders and missing auth secrets fail at startup.

**Output:** Events are emitted in page order (order received). Timestamp order across pages is not guaranteed unless the API guarantees it; downstream can sort if needed.

**State:** Single-writer assumption — only one Hel process should use a given state store (e.g. one SQLite file). For multi-instance deployments use Redis or Postgres state backend.

## Status

- **v0.1 & v0.2:** CLI (run, validate, test, state), config load, SQLite + in-memory state store, HTTP client, link-header / cursor / page-offset pagination, poll loop, retry, scheduler, health, metrics, graceful shutdown, dedupe, concurrent sources, file output with log rotation, **session replay** (`--record-dir`, `--replay-dir`), **mock server** (`--mock-server --mock-config`). **`hel test --source NAME`** and **`hel state show/reset/set/export/import`** implemented.

## License

MIT OR Apache-2.0
