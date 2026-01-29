# Hel

**Hel** is a generic HTTP API log collector, which collects and feeds log streams into Loki (and other backends).

- Polls HTTP APIs (Okta, GitHub, Google Workspace, etc.)
- Handles pagination (Link header, cursor, page/offset), rate limits, and durable state
- Emits **NDJSON to stdout** for downstream collectors (Grafana Alloy, Vector, Fluent Bit)

## Quick start

```bash
# Validate config
hel validate

# Run one poll cycle (when implemented)
hel run --once

# Run continuously (when implemented)
hel run
```

## Config

See `hel.yaml` for a minimal example. Required: `sources` with at least one source (`url`, optional `schedule`, `auth`, `pagination`, `resilience`). Env vars: `HEL_*` overrides; placeholders in config like `${OKTA_DOMAIN}` are expanded at load time.

## Status

- **v0.1 (in progress):** CLI, config load + validate, SQLite + in-memory state store, HTTP client (auth + timeouts), link-header pagination, **poll loop** (single source, link-header or single page, NDJSON to stdout, state commit). Next: retry layer, scheduler, health endpoint.

## License

MIT OR Apache-2.0
