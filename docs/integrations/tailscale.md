# Tailscale

Tailscale exposes two logging APIs that Helr can poll. Both use the same **HTTP Basic auth** (API token as username, empty password) and **time-window** query params (`start` / `end`, RFC3339); neither uses pagination.

1. **Configuration audit logs** - who did what (policy, users, nodes, API keys). [Configuration audit logging](https://tailscale.com/kb/1203/audit-logging/).
2. **Network flow logs** - how and when nodes connect (traffic flow, not contents). [Network flow logs](https://tailscale.com/kb/1219/network-flow-logs/). Premium/Enterprise only; 30-day retention; must be enabled in the admin console.

---

## Configuration audit logging

**Configuration audit logs** record who did what in your tailnet—policy changes, user and node actions, API keys, and more. Endpoint: `GET https://api.tailscale.com/api/v2/tailnet/{tailnet_id}/logging/configuration`. You pass required **start** and **end** query parameters (RFC3339); the API returns all events in that time window in a single response (no pagination).

You need an **API access token** with the `logs:configuration:read` scope.

### Requirements (configuration audit)

- A **Tailscale** account (all plans). Configuration audit logs are available for all tailnets and enabled by default.
- An **API access token** with **logs:configuration:read** scope. Create one in the admin console (**Settings** → **Keys** → **Generate API key**).
- Logs are retained for **90 days**; you cannot configure retention.

### Step 1: Create an API access token (configuration audit)

1. Sign in to the [Tailscale admin console](https://login.tailscale.com/admin).
2. Go to **Settings** → **Keys** (or the **Keys** page).
3. Under **API access tokens**, generate a key with **Read** access to **Configuration logs** (or the scope that includes `logs:configuration:read`).
4. Copy the token (e.g. `tskey-api-k123456CNTRL-...`) and store it as `TAILSCALE_API_TOKEN`. **The token is shown only once.**

See [Tailscale API](https://tailscale.com/kb/1243/tailscale-api/) for details.

### Step 2: Configure Helr (configuration audit)

The configuration audit endpoint is **GET** with **required** query parameters **start** and **end** (RFC3339 timestamps, inclusive). There is **no pagination**; all events in the time window are returned in one response. Helr uses a single request per poll (no pagination config).

In `helr.yaml`, add a Tailscale audit source. The URL must include the tailnet ID and the time range; use environment variables so you can set the window per run or via a wrapper script.

```yaml
  tailscale-audit:
    url: "https://api.tailscale.com/api/v2/tailnet/${TAILNET_ID}/logging/configuration?start=${TAILSCALE_START}&end=${TAILSCALE_END}"
    schedule:
      interval_secs: 3600
      jitter_secs: 60
    auth:
      type: basic
      user_env: TAILSCALE_API_TOKEN
      password_env: TAILSCALE_API_TOKEN_PASSWORD
    headers:
      Accept: "application/json"
    resilience:
      timeout_secs: 30
      retries:
        max_attempts: 5
        initial_backoff_secs: 2
        max_backoff_secs: 60
        multiplier: 2.0
      circuit_breaker:
        enabled: true
        failure_threshold: 5
        success_threshold: 2
        half_open_timeout_secs: 60
```

- **`TAILNET_ID`**: Your tailnet name (e.g. `example.com`). Find it in the admin console under **General** (tailnet name).
- **`TAILSCALE_START`**, **`TAILSCALE_END`**: RFC3339 timestamps for the window (e.g. `2025-01-01T00:00:00Z`, `2025-01-01T23:59:59Z`). Start and end are inclusive (nanosecond resolution).
- **`TAILSCALE_API_TOKEN`**: Your API access token with `logs:configuration:read`.
- **`TAILSCALE_API_TOKEN_PASSWORD`**: For Tailscale Basic auth, the password is **empty**. Set this env var to an empty string, e.g. `export TAILSCALE_API_TOKEN_PASSWORD=""`.
- **No pagination**: Omit `pagination` so Helr performs one GET per poll and emits events from the `logs` array.

### Run (configuration audit)

```bash
export TAILNET_ID="example.com"
export TAILSCALE_START="2025-01-01T00:00:00Z"
export TAILSCALE_END="2025-01-01T23:59:59Z"
export TAILSCALE_API_TOKEN="tskey-api-k..."
export TAILSCALE_API_TOKEN_PASSWORD=""

helr validate
helr test --source tailscale-audit   # or: helr run --once
```

You should see NDJSON lines (one per audit event). Then run `helr run` for continuous collection.

### Time window and continuous ingestion (configuration audit)

The API returns **all** events in the [start, end] window in a single response; there is no cursor or page token. For **continuous ingestion** you must advance the time window yourself:

- **Option A (fixed window):** Set `TAILSCALE_START` and `TAILSCALE_END` to a fixed range (e.g. last 24 hours) and run Helr on a schedule. You may see overlapping events; use dedupe if needed.
- **Option B (sliding window):** Use a wrapper script or cron that sets `TAILSCALE_START` and `TAILSCALE_END` (e.g. end=now, start=now-1h) before each `helr run --once`, so each run fetches a new window.
- **Option C (one-off export):** Run once with a specific window to export a slice of logs.

Logs are kept for **90 days**; older events are not available.

### Optional: dedupe (configuration audit)

Events do not have a single global ID; they include `eventTime`, `eventGroupID`, `action`, and `target`. If you use overlapping windows or replay, you can dedupe by a composite path or skip dedupe. For non-overlapping windows, dedupe is usually unnecessary.

### Quick reference (configuration audit)

- **API:** [Configuration audit logging](https://tailscale.com/kb/1203/audit-logging/) — `GET https://api.tailscale.com/api/v2/tailnet/{tailnet_id}/logging/configuration?start=...&end=...`.
- **Auth:** HTTP Basic; username = API token, password = empty. Token scope: `logs:configuration:read`.
- **Pagination:** None. One GET per poll; all events in the time window in the `logs` array.
- **Env:** `TAILNET_ID`, `TAILSCALE_START`, `TAILSCALE_END`, `TAILSCALE_API_TOKEN`, `TAILSCALE_API_TOKEN_PASSWORD` (empty).

---

## Network flow logs

**Network flow logs** record how and when nodes on your tailnet connect—traffic flow between nodes, through subnet routers, and via exit nodes (not the contents of traffic). Endpoint: `GET https://api.tailscale.com/api/v2/tailnet/{tailnet_id}/logging/network`. Same pattern as configuration audit: required **start** and **end** (RFC3339), no pagination, response has a `logs` array.

You need an **API access token** with the `logs:network:read` scope. You can use the same token as for configuration audit if it has both scopes, or a separate token with only `logs:network:read`.

### Requirements (network flow)

- **Tailscale Premium or Enterprise**. Network flow logs are not available on free plans.
- **Network flow logs enabled** in the admin console: open the **Network flow logs** page and select **Start logging**.
- **API access token** with **logs:network:read** scope.
- **Nodes** must use Tailscale v1.34 or later to send flow telemetry.
- Logs are retained for **30 days** (not configurable).

### Configure Helr (network flow)

Add a second source for network flow. Same URL pattern and auth as configuration audit; only the path and scope differ (`/logging/network`, `logs:network:read`). Use the same env vars if your token has both scopes, or a separate token env for network-only.

```yaml
  tailscale-network:
    url: "https://api.tailscale.com/api/v2/tailnet/${TAILNET_ID}/logging/network?start=${TAILSCALE_START}&end=${TAILSCALE_END}"
    schedule:
      interval_secs: 3600
      jitter_secs: 60
    auth:
      type: basic
      user_env: TAILSCALE_API_TOKEN
      password_env: TAILSCALE_API_TOKEN_PASSWORD
    headers:
      Accept: "application/json"
    resilience:
      timeout_secs: 30
      retries:
        max_attempts: 5
        initial_backoff_secs: 2
        max_backoff_secs: 60
        multiplier: 2.0
      circuit_breaker:
        enabled: true
        failure_threshold: 5
        success_threshold: 2
        half_open_timeout_secs: 60
```

- **`TAILNET_ID`**, **`TAILSCALE_START`**, **`TAILSCALE_END`**: Same as configuration audit.
- **`TAILSCALE_API_TOKEN`**: Must have `logs:network:read` (and optionally `logs:configuration:read` if you use the same token for both sources).
- **No pagination**: Omit `pagination`; one GET per poll, events from `logs` array.

### Run (network flow)

```bash
# Same env as configuration audit if using one token for both
export TAILNET_ID="example.com"
export TAILSCALE_START="2025-01-01T00:00:00Z"
export TAILSCALE_END="2025-01-01T23:59:59Z"
export TAILSCALE_API_TOKEN="tskey-api-k..."
export TAILSCALE_API_TOKEN_PASSWORD=""

helr validate
helr test --source tailscale-network   # or: helr run --once
```

### Time window (network flow)

Same as configuration audit: advance `TAILSCALE_START` / `TAILSCALE_END` yourself for continuous ingestion (fixed window, sliding window via script, or one-off export). Network flow logs are available for the **last 30 days** only.

### Quick reference (network flow)

- **API:** [Network flow logs](https://tailscale.com/kb/1219/network-flow-logs/) — `GET https://api.tailscale.com/api/v2/tailnet/{tailnet_id}/logging/network?start=...&end=...`.
- **Auth:** Same Basic auth; token scope: `logs:network:read`.
- **Pagination:** None. One GET per poll; `logs` array.
- **Plans:** Premium or Enterprise; enable in admin console. Retention: 30 days.

---

## Other Tailscale logging and events

Tailscale has other logging/event features that **do not** expose a pull API Helr can use:

| Feature | Description | Why Helr doesn’t poll it |
|--------|-------------|--------------------------|
| **[Log streaming](https://tailscale.com/kb/1255/log-streaming)** | Tailscale **pushes** configuration audit and/or network flow logs to your SIEM, S3, or Vector endpoint (Splunk HEC, Elasticsearch, Datadog, etc.). | Push-only; you configure a destination URL and Tailscale sends logs there. No “list logs” API to poll. Use Helr’s **pull** APIs above, or use log streaming for push. |
| **[SSH session recording](https://tailscale.com/kb/1246/tailscale-ssh-session-recording)** | Tailscale SSH sessions are recorded (asciinema) and **streamed** to a recorder node in your tailnet (or to S3). | Recordings go to a node you run (tsrecorder) or S3; there is no central Tailscale API to fetch a list of recordings. |
| **[Client metrics](https://tailscale.com/kb/1482/client-metrics)** | Prometheus-style metrics (throughput, health, DERP, etc.) exposed **per client** at `http://100.100.100.100/metrics` or over Tailscale on port 5252. | Per-device scrape; not a single “list events” API. Use Prometheus/Grafana (or similar) to scrape each client. |
| **[Webhooks](https://tailscale.com/kb/1213/webhooks)** | Tailscale **pushes** HTTP requests to a URL you configure when certain events occur (e.g. device approved). | Push-only; no API to poll for webhook payloads. |

For **pull-based** log collection (poll an API, get a batch of events), the only Tailscale APIs are **configuration audit** and **network flow** documented above. For **push-based** delivery, use [Log streaming](https://tailscale.com/kb/1255/log-streaming) or [Webhooks](https://tailscale.com/kb/1213/webhooks).

---

## Testing with replay

Run once with `--record-dir ./recordings` (with valid credentials and a small time window) to save the response, then use `helr run --once --replay-dir ./recordings` to replay without calling the API. Works for both configuration audit and network flow sources.

## Troubleshooting

| Symptom | Check |
|--------|--------|
| `TAILNET_ID` / `TAILSCALE_START` / `TAILSCALE_END` unset | All three are required; use RFC3339 for start/end. |
| `TAILSCALE_API_TOKEN` unset | Create an API key with **logs:configuration:read** and/or **logs:network:read** in the admin console. |
| `TAILSCALE_API_TOKEN_PASSWORD` | Must be set (can be empty): `export TAILSCALE_API_TOKEN_PASSWORD=""`. |
| `401 Unauthorized` | Token valid and not revoked; correct scope for the endpoint (config vs network). |
| Empty or wrong window (config) | start/end inclusive; within last **90 days**. |
| Empty or wrong window (network) | start/end inclusive; within last **30 days**. Network flow logs must be **enabled** in admin console; **Premium/Enterprise** only. |
