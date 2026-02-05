# Hel

Hel is a generic HTTP API log collector. It polls audit-log and event APIs (Okta, Google Workspace, GitHub, Cloud Logging, and others), handles pagination and rate limits, keeps durable state, and emits **NDJSON** to stdout or to a file for downstream collectors (Grafana Alloy, Vector, Fluent Bit, Loki).

You configure one or more **sources** in YAML (URL, auth, pagination, schedule). Hel runs on an interval, fetches pages, checkpoints cursors, and writes one JSON object per event per line. Single binary, no runtime dependencies beyond the config and secrets.

## Supported features

- **Sources:** Okta System Log, Google Workspace (GWS) Admin SDK Reports API, GitHub organization audit log, Slack Enterprise audit logs, 1Password Events API (audit), Tailscale configuration audit and network flow logs, GWS via Cloud Logging (LogEntry format), and any HTTP API that returns a JSON array (items/events/entries/logs) with Link-header or cursor pagination
- **Auth:** Bearer (including SSWS for Okta), API key, Basic, OAuth2 (refresh token or client credentials; optional private_key_jwt, DPoP), Google Service Account (JWT, domain-wide delegation for GWS)
- **Pagination:** Link header (`rel=next`), cursor (query param or body), page/offset; optional **incremental_from** (store latest event timestamp, send as query param on first request — e.g. Slack `oldest`); optional per-source **state.watermark_field** / **watermark_param** for APIs that derive "start from" from last event (e.g. GWS `startTime`)
- **Resilience:** Split timeouts (connect, request, read, idle, poll_tick), retries with backoff, circuit breaker, **rate limit** — header mapping (X-RateLimit-Limit/Remaining/Reset or custom names), client-side RPS/burst cap, optional adaptive rate limiting (throttle when remaining is low)
- **TLS:** Custom CA (file or env, merge or replace system roots), client certificate and key (mutual TLS), minimum TLS version (1.2 or 1.3)
- **State:** SQLite, Redis, or Postgres (or in-memory) for cursor/next_url; single-writer per SQLite file; Redis/Postgres for multi-instance
- **Output:** NDJSON to stdout or file; optional rotation (daily or by size)
- **Backpressure:** When the downstream consumer (stdout/file) can’t keep up: configurable detection (queue depth, RSS memory threshold) and strategies — **block** (pause poll until drain), **disk_buffer** (spill to disk when queue full, drain when consumer catches up), or **drop** (oldest_first / newest_first / random) with metrics; optional **max_queue_age_secs** to drop events that sit in the queue too long
- **Graceful degradation:** When the state store fails or is unavailable: optional **state_store_fallback** to memory (state not durable), **emit_without_checkpoint** to continue emitting events when state writes fail, and **reduced_frequency_multiplier** to poll less often when degraded; health JSON reports **state_store_fallback_active**
- **Session replay:** Record API responses to disk, replay without hitting the live API
- **Optional JS hooks (Boa):** Per-source scripts for `buildRequest`, `parseResponse`, `getNextPage`, `commitState`; sandbox (timeout, no network/fs). Build with `--features hooks`. See [**docs/hooks.md**](./docs/hooks.md) and the **GraphQL-via-hooks** pattern there.
- **Audit:** Optional `global.audit`: log credential access (when secrets are read), log config load/reload (e.g. SIGHUP); never log secret values when `redact_secrets` is true. See [**docs/audit.md**](./docs/audit.md) for config and behavior.

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
- **Options:** `from` / `from_param`, `query_params`, `incremental_from` (store latest event timestamp in state, send as query param on first request — e.g. Slack `oldest`), `dedupe.id_path`, `resilience.rate_limit` (header mapping, `max_requests_per_second`, `burst_size`, `adaptive`, `page_delay_secs`), `resilience.timeouts` (split: connect, request, read, idle, poll_tick), `resilience.tls` (custom CA, client cert/key, min TLS version), `resilience.retries.jitter` and `retryable_status_codes`, `transform.timestamp_field` and `transform.id_field`, `max_pages`, and others - see `hel.yaml` comments and the manuals below.

**Output:** Each NDJSON line is one JSON object: `ts`, `source`, `endpoint`, `event` (raw payload), and `meta` (optional `cursor`, `request_id`). The producer label key defaults to `source`; value is the source id or `source_label_value`. With `log_format: json`, Hel’s own logs (stderr) use the same label key and value `hel`.

**Broken pipe (SIGPIPE):** When stdout is a pipe and the consumer (e.g. Alloy, `hel run | alloy ...`) exits, writes return EPIPE. Hel treats this as **fatal**: the error is logged, `hel_output_errors_total` is incremented, and the process exits with a non-zero code so an orchestrator can restart. Keep the downstream process running, or use file output (`--output /path`) and have the collector tail the file instead.

**Backpressure:** When the downstream consumer (stdout or file) can’t keep up, enable `global.backpressure.enabled` so Hel uses a bounded queue and a writer thread. You can **block** (pause poll until the queue drains; no data loss), **disk_buffer** (spill to a file when the queue is full; requires `disk_buffer.path`), or **drop** (drop events with a policy: oldest_first, newest_first, random; tracked in `hel_events_dropped_total`). Optionally set `memory_threshold_mb` to apply the same strategy when process RSS exceeds the limit, and `max_queue_age_secs` to drop events that sit in the queue too long.

**Graceful degradation** (`global.degradation:`): When the primary state store (e.g. SQLite) fails to open or becomes unwritable, you can set `state_store_fallback: memory` so Hel falls back to an in-memory store (state is not durable across restarts). When degraded, set `reduced_frequency_multiplier` (e.g. `2.0`) to poll less often and reduce load. Set `emit_without_checkpoint: true` to continue emitting events when state writes fail (e.g. disk full); otherwise state write errors fail the tick. Health and readyz JSON include `state_store_fallback_active: true` when the fallback is in use.

**Rate limit** (`resilience.rate_limit:`): Rate limit headers are not standardized — APIs use different names (e.g. GitHub/Okta use `X-RateLimit-*`; others use `RateLimit-*` or custom names). You can map limit/remaining/reset via `headers` (defaults: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`). Set `max_requests_per_second` and optional `burst_size` to cap client-side request rate (token bucket) so you don’t exceed the API’s limit. Set `adaptive: true` to throttle proactively: when the response reports remaining ≤ 1, Hel waits until the reset window before the next request. On 429, Hel uses Retry-After or the configured reset header when `respect_headers` is true.

**State:** One writer per state store (e.g. one SQLite file). For multiple instances, use Redis or Postgres (`global.state.backend: redis` or `postgres` with `global.state.url`).

<details>
<summary><strong>Config reference (all options)</strong></summary>

#### Global (`global:`)

| Option | Description | Possible values | Default |
|--------|-------------|-----------------|---------|
| `log_level` | Logging level | `trace`, `debug`, `info`, `warn`, `error` | `info` |
| `log_format` | Hel log format (stderr) | `json`, `pretty` | — (none) |
| `source_label_key` | Key for producer label in NDJSON and Hel logs | string | — (effective: `source`) |
| `source_label_value` | Value for producer label in Hel’s own logs | string | — (effective: `hel`) |
| `state.backend` | State store backend | `sqlite`, `memory`, `redis`, `postgres` | — |
| `state.path` | Path to state file (SQLite) | string | `./hel-state.db` (when backend is sqlite) |
| `state.url` | Connection URL for Redis (`redis://...`) or Postgres (`postgres://...`) | string | — (required when backend is redis or postgres) |
| `health.enabled` | Enable health HTTP server | boolean | `false` |
| `health.address` | Health server bind address | string | `0.0.0.0` |
| `health.port` | Health server port | number | `8080` |
| `reload.restart_sources_on_sighup` | On SIGHUP, also clear circuit breaker and OAuth2 token cache so sources re-establish on next tick | boolean | `false` |
| `dump_on_sigusr1.destination` | Where to write SIGUSR1 dump: `log` (tracing at INFO) or `file` | string | `log` |
| `dump_on_sigusr1.path` | Path when destination is `file`; required when destination is `file` | string | — |
| `bulkhead.max_concurrent_sources` | Max number of sources that may poll concurrently (semaphore) | number | — (no limit) |
| `bulkhead.max_concurrent_requests` | Max concurrent HTTP requests per source (semaphore); overridable per source in `resilience.bulkhead` | number | — (no limit) |
| `load_shedding.skip_priority_below` | When set and backpressure is active, sources with priority below this (0–10) are not polled. Requires backpressure and per-source `priority`. | number | — (none) |

When `health.enabled` is true, GET `/healthz`, `/readyz`, and `/startupz` return detailed JSON (version, uptime, per-source status, circuit state, last_error). **Readyz semantics:** `/readyz` returns 200 only when (1) output path is writable (or stdout), (2) state store is connected (e.g. SQLite reachable), and (3) at least one source is healthy (circuit not open). The JSON includes `ready`, `output_writable`, `state_store_connected`, and `at_least_one_source_healthy` so you can see which condition failed. When graceful degradation is used (state store fallback to memory), the JSON includes `state_store_fallback_active: true`.

| `metrics.enabled` | Enable Prometheus metrics server | boolean | `false` |
| `metrics.address` | Metrics server bind address | string | `0.0.0.0` |
| `metrics.port` | Metrics server port | number | `9090` |

**Backpressure** (`global.backpressure:`): When the output queue is full or process RSS exceeds a threshold, apply a strategy so the poll loop doesn’t block indefinitely or OOM.

| Option | Description | Possible values | Default |
|--------|-------------|-----------------|---------|
| `backpressure.enabled` | Enable backpressure (bounded queue + writer thread) | boolean | `false` |
| `backpressure.detection.event_queue_size` | Max events in the internal queue before applying strategy | number | `10000` |
| `backpressure.detection.memory_threshold_mb` | Process RSS limit (MB); when exceeded, apply strategy (uses `sysinfo`; best-effort on supported platforms) | number | — (none) |
| `backpressure.detection.stdout_buffer_size` | Max total bytes of queued events; when queue byte size + next event would exceed this, apply strategy. 0 = disabled. | number | `65536` |
| `backpressure.strategy` | When queue is full (or over memory): **block** (pause until drain), **disk_buffer** (spill to file; requires `disk_buffer.path`), **drop** (drop with `drop_policy`) | `block`, `disk_buffer`, `drop` | `block` |
| `backpressure.drop_policy` | When strategy is **drop**: which event to drop | `oldest_first`, `newest_first`, `random` | `oldest_first` |
| `backpressure.max_queue_age_secs` | Max age (seconds) a queued event may sit; older events are dropped first (reason `max_queue_age` in metrics) | number | — (none) |
| `backpressure.disk_buffer` | Required when strategy is **disk_buffer** | object | — |
| `backpressure.disk_buffer.path` | Path to spill file (NDJSON lines appended when queue full; writer drains to inner sink) | string | — |
| `backpressure.disk_buffer.max_size_mb` | Max total spill size (MB); when current file + `.old` exceed this, producer blocks until writer drains | number | `1024` |
| `backpressure.disk_buffer.segment_size_mb` | When current spill file reaches this size (MB), it is rotated to `path.old` and a new file is created; writer drains `.old` then current | number | `64` |

Metrics: `hel_events_dropped_total{source, reason="backpressure"|"max_queue_age"}`, `hel_pending_events{source}`.

**SIGHUP** (Unix): When running continuously (not `--once`, not replay), sending SIGHUP to the process reloads the config from the same file. The next poll tick uses the new config (sources, schedule, auth, etc.). Set `global.reload.restart_sources_on_sighup: true` to also clear the circuit breaker and OAuth2 token cache so each source re-establishes connections and tokens on the next tick.

**SIGUSR1** (Unix): When `global.dump_on_sigusr1` is set, sending SIGUSR1 to the process dumps the current state (same shape as `hel state export`) and Prometheus metrics. Use `destination: log` to write the dump to the process log (INFO level), or `destination: file` with `path: /path/to/dump.txt` to write to a file.

**Bulkhead** (`global.bulkhead:`): Per-source and global concurrency caps using semaphores. Set `max_concurrent_sources` to limit how many sources poll at once (e.g. avoid overloading a shared API). Set `max_concurrent_requests` to limit concurrent HTTP requests per source (default no limit). Override per source with `resilience.bulkhead.max_concurrent_requests`.

**Load shedding** (`global.load_shedding:`): When backpressure is active (queue full or memory over `backpressure.detection` threshold), optionally skip polling low-priority sources. Set `skip_priority_below` (0–10); sources with `priority` below that value are not polled until the queue drains below 75% of cap. Per-source `priority` (0–10, default 10) tags sources for load shedding. Strategies: **drop oldest** and **pause polling** are already in backpressure (strategy **drop** with `drop_policy: oldest_first`, or **block**); **skip low-priority sources** is enabled when `load_shedding.skip_priority_below` is set.

**Graceful degradation** (`global.degradation:`): State store fallback, emit without checkpoint on state write failure, and reduced poll frequency when degraded.

| Option | Description | Possible values | Default |
|--------|-------------|-----------------|---------|
| `degradation.state_store_fallback` | When primary state store (SQLite, Redis, or Postgres) fails to open, fall back to this backend | `memory` | — (none; fail on error) |
| `degradation.emit_without_checkpoint` | When state store write fails, skip checkpoint and continue (log warning); same effect as per-source `on_state_write_error: skip_checkpoint` but global | boolean | — (none) |
| `degradation.reduced_frequency_multiplier` | When degraded (e.g. using state_store_fallback), multiply poll interval by this factor (e.g. `2.0` = double the delay) | number | `2.0` |

When `state_store_fallback: memory` is set and SQLite fails, Hel logs a warning and uses an in-memory store; health and readyz JSON include `state_store_fallback_active: true`.

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
| `priority` | Load-shedding priority (0–10, higher = higher priority). When under load and `load_shedding.skip_priority_below` is set, sources with priority below that threshold are not polled. | number | `10` (effective when unset) |
| `headers` | Extra HTTP headers (key: value) | map | — |
| `max_bytes` | Stop pagination when total response bytes exceed this (per poll) | number | — |
| `dedupe.id_path` | JSON path to event ID for deduplication (e.g. `uuid`, `id`, `event.id`) | string | — |
| `dedupe.capacity` | Max event IDs to keep (LRU) | number | `100000` |
| `transform` | Per-source field mapping for NDJSON envelope; see Transform below | object | — |
| `transform.timestamp_field` | Dotted path to event timestamp (e.g. `published`, `event.created_at`). Used for envelope `ts`. When unset: published, timestamp, ts, created_at, then now. | string | — |
| `transform.id_field` | Dotted path to event unique ID (e.g. `uuid`, `id`). When set, value is included in envelope `meta.id`. | string | — |
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
| `incremental_from` | Store latest event timestamp in state and send as query param on first request (e.g. Slack `oldest`); see below | object | — |
| `incremental_from.state_key` | State key to read/write the timestamp value | string | — |
| `incremental_from.event_timestamp_path` | Dotted JSON path in each event for the timestamp (e.g. `date_create`); max value is stored after each poll | string | — |
| `incremental_from.param_name` | Query param name for the state value on first request (e.g. `oldest`) | string | — |
| `state` | Per-source state: watermark field/param for APIs that derive "start from" from last event (e.g. GWS `startTime`); see below | object | — |
| `state.watermark_field` | Dotted JSON path in each event for the watermark value (e.g. `id.time`); max value is stored after each poll | string | — |
| `state.watermark_param` | Query param name for the stored watermark on first request (e.g. `startTime`) | string | — |
| `state.state_key` | State key to read/write the watermark | string | `watermark` |

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
| `timeout_secs` | HTTP request timeout (seconds). Fallback when `timeouts` is omitted. | number | `30` |
| `timeouts` | Split timeouts (optional). When set, overrides/supplements `timeout_secs` for the client. | object | — |
| `timeouts.connect_secs` | TCP connection establishment (seconds) | number | — (else min(10, timeout_secs)) |
| `timeouts.request_secs` | Entire request/response per request (seconds) | number | — (else timeout_secs) |
| `timeouts.read_secs` | Reading response body (seconds); should be ≤ request when both set | number | — |
| `timeouts.idle_secs` | Idle connection in pool (seconds) | number | — |
| `timeouts.poll_tick_secs` | Entire poll cycle (all pages) per source (seconds). Poll aborts with error when exceeded. | number | — |
| `retries.max_attempts` | Max attempts per request (0 = no retries) | number | `3` |
| `retries.initial_backoff_secs` | Initial backoff (seconds) | number | `1` |
| `retries.max_backoff_secs` | Cap on backoff (seconds) | number | — |
| `retries.multiplier` | Backoff multiplier per attempt | number | `2.0` |
| `retries.jitter` | Backoff jitter: delay × (1 + random(−jitter, +jitter)). e.g. `0.1` = ±10%. When unset, no jitter. | number (0–1) | — |
| `retries.retryable_status_codes` | HTTP status codes to retry. When unset: 408, 429, 5xx. | list of numbers | — |
| `circuit_breaker.enabled` | Enable circuit breaker | boolean | `true` |
| `circuit_breaker.failure_threshold` | Failures before opening | number | `5` |
| `circuit_breaker.success_threshold` | Successes in half-open to close | number | `2` |
| `circuit_breaker.half_open_timeout_secs` | Seconds before half-open probe | number | `60` |
| `circuit_breaker.reset_timeout_secs` | Max time in open state (open duration = min(half_open_timeout_secs, reset_timeout_secs)) | number | — |
| `circuit_breaker.failure_rate_threshold` | Optional: open when failure rate ≥ this (0.0–1.0); requires `minimum_requests` | number | — |
| `circuit_breaker.minimum_requests` | Minimum requests before evaluating `failure_rate_threshold` | number | — |
| `rate_limit.respect_headers` | Use Retry-After or reset header on 429 (see `headers.reset_header`) | boolean | `true` |
| `rate_limit.page_delay_secs` | Delay between pagination requests (seconds) | number | — |
| `rate_limit.headers` | Header names for limit/remaining/reset (when API uses different names) | object | — |
| `rate_limit.headers.limit_header` | Header for rate limit ceiling (e.g. `X-RateLimit-Limit`) | string | `X-RateLimit-Limit` |
| `rate_limit.headers.remaining_header` | Header for remaining requests in window | string | `X-RateLimit-Remaining` |
| `rate_limit.headers.reset_header` | Header for window reset (Unix timestamp); used on 429 and for adaptive | string | `X-RateLimit-Reset` |
| `rate_limit.max_requests_per_second` | Client-side RPS cap; requests are throttled before sending (token bucket) | number | — |
| `rate_limit.burst_size` | Client-side burst size (max requests in a burst). When unset with `max_requests_per_second`, defaults to ceil(rps) | number | — |
| `rate_limit.adaptive` | When true, use remaining/reset from response: if remaining ≤ 1, wait until reset before next request | boolean | — |

**TLS** (`resilience.tls:`): Custom CA, client cert/key (mutual TLS), and minimum TLS version for the reqwest client.

| Option | Description | Possible values | Default |
|--------|-------------|-----------------|---------|
| `tls.ca_file` | Path to PEM file (single cert or bundle) for custom CA. Merged with system roots unless `ca_only` is true. | string | — |
| `tls.ca_env` | Env var containing PEM for custom CA. Used when `ca_file` is unset. | string | — |
| `tls.ca_only` | When true and custom CA is set, use only the provided CA(s); otherwise merge with system roots. | boolean | `false` |
| `tls.client_cert_file` | Path to client certificate PEM (for mutual TLS). | string | — |
| `tls.client_cert_env` | Env var containing client certificate PEM. Used when `client_cert_file` is unset. | string | — |
| `tls.client_key_file` | Path to client private key PEM (required when client cert is set). | string | — |
| `tls.client_key_env` | Env var containing client private key PEM. Used when `client_key_file` is unset. | string | — |
| `tls.min_version` | Minimum TLS version for connections. | `"1.2"`, `"1.3"` | — (TLS backend default) |

Secrets can be read from file or env; file takes precedence when set. Client cert and key must both be set when using mutual TLS.

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

## Development / tests

- **Unit and integration tests:** `cargo test` (excludes testcontainers tests).
- **Testcontainers (Redis/Postgres state backends):** requires Docker. Run with: `cargo test --features testcontainers --test integration_testcontainers`.

## License

MIT OR Apache-2.0
