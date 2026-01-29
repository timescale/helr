# Hel

**Hel** is a generic HTTP API log collector, which collects and feeds log streams into Loki (and other backends).

- Polls HTTP APIs (Okta, GitHub, Google Workspace, etc.)
- Handles pagination (Link header, cursor, page/offset), rate limits, and durable state
- Emits **NDJSON to stdout** for downstream collectors (Grafana Alloy, Vector, Fluent Bit)

## Quick start

```bash
# Validate config
hel validate

# Run one poll cycle
hel run --once

# Run continuously
hel run

# Test a single source (one poll tick)
hel test --source okta-audit

# Inspect or manage state
hel state show okta-audit
hel state reset okta-audit
hel state export
hel state import
```

## Config

See `hel.yaml` for a minimal example. Required: `sources` with at least one source (`url`, optional `schedule`, `auth`, `pagination`, `resilience`). Placeholders like `${OKTA_DOMAIN}` are expanded at load time.

## Status

- **v0.1 & v0.2:** CLI (run, validate, test, state), config load, SQLite + in-memory state store, HTTP client, link-header / cursor / page-offset pagination, poll loop, retry, scheduler, health, metrics, graceful shutdown, dedupe, concurrent sources. **`hel test --source NAME`** and **`hel state show/reset/export/import`** implemented.

## License

MIT OR Apache-2.0
