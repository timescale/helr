# Helr REST API

When `global.api.enabled` is true, Helr serves health endpoints and a **REST API** on the same HTTP server (bind address and port from `global.api`). The API lets you list sources, read per-source state and config, read global config, trigger a one-off poll, and optionally reload config — suitable for scripts, operators, and a minimal web UI.

## Base path

All API routes are under **`/api/v1`**. Example base URL: `http://127.0.0.1:8080/api/v1`.

**Request logging:** Incoming HTTP requests (method, path) are logged at **debug** level. Set `HELR_LOG_LEVEL=debug` (or `RUST_LOG=helr=debug`) to see them in the log output.

## Endpoints

### GET /api/v1/sources

List all configured sources and their status (circuit state, last error).

**Response:** `200 OK`  
**Body (JSON):**

```json
{
  "version": "0.1.0",
  "uptime_secs": 3600.5,
  "sources": {
    "okta-audit": {
      "status": "ok",
      "circuit_state": {
        "state": "closed",
        "failures": 0
      },
      "last_error": null
    },
    "github-audit": {
      "status": "degraded",
      "circuit_state": { "state": "closed", "failures": 2 },
      "last_error": "rate limit exceeded"
    }
  }
}
```

- **status:** `ok` | `degraded` | `unhealthy` (unhealthy when circuit is open).
- **circuit_state.state:** `closed` | `open` | `half_open`.
- **last_error:** Last error message for that source, or null.

---

### GET /api/v1/sources/:id

Status for a single source. Same shape as one entry in `GET /api/v1/sources` (the object for that source).

**Response:** `200 OK` with source JSON, or `404 Not Found` if the source id is not in config.

---

### GET /api/v1/sources/:id/state

Read stored state for one source (cursor, watermark, next_url, etc.).

**Response:** `200 OK`  
**Body (JSON):**

```json
{
  "source_id": "okta-audit",
  "state": {
    "cursor": "abc123",
    "watermark_ts": "2026-01-15T12:00:00Z"
  }
}
```

Returns `404` if the source is not in config. If no state store is configured, `state` is an empty object.

---

### GET /api/v1/sources/:id/config

Config for a single source (URL, schedule, auth, pagination, resilience, etc.). Same shape as one entry under `sources` in `helr.yaml`. Auth fields expose env var names (e.g. `token_env`) and paths, not secret values.

**Response:** `200 OK` with source config JSON, or `404 Not Found` if the source id is not in config.

---

### GET /api/v1/config

Global config (log_level, log_format, state, health, metrics, backpressure, degradation, reload, bulkhead, load_shedding, hooks, audit, etc.). Same shape as `global` in `helr.yaml`.

**Response:** `200 OK` with global config JSON.

---

### POST /api/v1/sources/:id/poll

Run **one poll tick** for the given source (same as `helr test --source <id> --once` but via HTTP). The call blocks until the tick completes.

**Response:** `200 OK` (body describes outcome)

- **Success:** `{ "source_id": "okta-audit", "ok": true, "error": null }`
- **Poll failed:** `{ "source_id": "okta-audit", "ok": false, "error": "<message>" }`

**Errors:**

- **404 Not Found** — Source id not in config.
- **503 Service Unavailable** — Trigger poll not available (e.g. Helr was started with `--once` or replay mode; poll dependencies are not attached).

---

### POST /api/v1/reload

Reload config from the same file used at startup (same effect as SIGHUP). Optionally clears circuit breaker and OAuth2 token cache when `global.reload.restart_sources_on_sighup` is set.

**Response:** `200 OK` on success:

```json
{ "ok": true, "error": null }
```

**Errors:**

- **503 Service Unavailable** — Reload not available (no config path; e.g. `--once` or replay).
- **500 Internal Server Error** — Config file invalid or unreadable; body: `{ "ok": false, "error": "<message>" }`.

---

## Health endpoints (unchanged)

Same server also serves:

- **GET /healthz** — Liveness; JSON with version, uptime, and per-source status (circuit state, last_error). Use this for full diagnostic detail.
- **GET /readyz** — Readiness; JSON with `ready`, `output_writable`, `state_store_connected`, `at_least_one_source_healthy` (no per-source detail).
- **GET /startupz** — Startup; JSON with `started`, version, uptime (no per-source detail).

See the main docs and `global.api` for address and port.

## Enabling the API

The REST API is available when the API server is enabled:

```yaml
global:
  api:
    enabled: true
    address: "0.0.0.0"
    port: 8080
```

When `api.enabled` is false, the API and health endpoints are not served.

## Use with a minimal web UI

The API is designed to feed a minimal web UI (e.g. Alloy-style): use `GET /api/v1/sources` for the source list and status, `GET /api/v1/sources/:id/state` for state, `GET /api/v1/sources/:id/config` and `GET /api/v1/config` for config, and `POST /api/v1/sources/:id/poll` to trigger a poll from the UI. No auth in v1; put Helr behind a reverse proxy or restrict access by network.

## Examples

```bash
# List sources and status
curl -s http://127.0.0.1:8080/api/v1/sources | jq .

# State for one source
curl -s http://127.0.0.1:8080/api/v1/sources/okta-audit/state | jq .

# Config for one source
curl -s http://127.0.0.1:8080/api/v1/sources/okta-audit/config | jq .

# Global config
curl -s http://127.0.0.1:8080/api/v1/config | jq .

# Trigger one poll for a source
curl -s -X POST http://127.0.0.1:8080/api/v1/sources/okta-audit/poll | jq .

# Reload config
curl -s -X POST http://127.0.0.1:8080/api/v1/reload | jq .
```
