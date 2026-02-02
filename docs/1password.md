# 1Password

**1Password** audit events record actions performed by team members in your 1Password Business account - account, vault, group, user, and app changes. Hel pulls these events via the **1Password Events API** ([Audit events](https://developer.1password.com/docs/events-api/audit-events/), [API reference](https://developer.1password.com/docs/events-api/reference/)): `POST https://events.1password.com/api/v2/auditevents`. Events are returned in an `items` array and paginated with a **cursor** in the response body; you send that cursor in the **request body** of the next POST.

You authenticate with a **bearer token** from an **Events Reporting integration** in your 1Password Business account. The token must have the **auditevents** feature (and optionally sign-in attempts and item usage).

## Requirements

- A **1Password Business** account. You must be an **owner** or **administrator** to create an Events Reporting integration.
- An **Events Reporting integration** with a bearer token that has **audit events** (and optionally sign-in attempts, item usage) enabled.

## Step 1: Set up an Events Reporting integration

1. Sign in to your account at [1Password.com](https://start.1password.com/signin).
2. Go to **Integrations** → **Directory**. In the **Events Reporting** section, choose your SIEM or **Other**.
3. Enter a name and select **Add Integration**.
4. Create a bearer token:
   - **Token Name**: e.g. `hel-audit`.
   - **Expires After**: Optional (default: Never).
   - **Events to Report**: Include **Audit events** (and sign-in attempts / item usage if you need those from other endpoints).
5. Select **Issue Token**, then save the token (e.g. in 1Password or as an env var). **The credential is shown only once.**
6. Store the token as `ONEPASSWORD_EVENTS_TOKEN` (or another env var you reference in `hel.yaml`).

See [Get started with the 1Password Events API](https://developer.1password.com/docs/events-api/get-started/) and [Authorization](https://developer.1password.com/docs/events-api/authorization/) for details.

## Step 2: Configure Hel

The audit events endpoint is **POST**; the first request uses a **ResetCursor** (empty object or with optional `limit`, `start_time`, `end_time`). Subsequent requests send the **cursor** from the previous response in the request body. Hel merges the cursor into the body when present.

In `hel.yaml`, add a 1Password audit source:

```yaml
  1password-audit:
    url: "https://events.1password.com/api/v2/auditevents"
    method: post
    body: {}
    schedule:
      interval_secs: 300
      jitter_secs: 30
    auth:
      type: bearer
      token_env: ONEPASSWORD_EVENTS_TOKEN
    headers:
      Accept: "application/json"
      Content-Type: "application/json"
    pagination:
      strategy: cursor
      cursor_param: cursor
      cursor_path: cursor
      max_pages: 50
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
      rate_limit:
        respect_headers: true
        page_delay_secs: 1
```

- **`ONEPASSWORD_EVENTS_TOKEN`**: Bearer token from your Events Reporting integration with **audit events** enabled.
- **`body: {}`**: First request uses an empty body (ResetCursor with defaults). Hel merges `cursor` into the body on subsequent requests.
- **Cursor**: Response has top-level `cursor` and `has_more`; Hel uses `cursor_path: cursor` and sends it as `cursor` in the body (`cursor_param: cursor`).
- **Events**: Response `items` array is used automatically by Hel.

## Optional: first-request time range and limit

The 1Password **ResetCursor** supports optional `limit` (1–1000, default 100), `start_time`, and `end_time` (RFC 3339). To use them, set them in the initial `body`; Hel will merge the cursor into that body on later requests (1Password expects only `cursor` for continuing requests, so the merged body is still valid).

Example: limit 500 and a start time:

```yaml
  1password-audit:
    url: "https://events.1password.com/api/v2/auditevents"
    method: post
    body:
      limit: 500
      start_time: "2025-01-01T00:00:00Z"
    # ... auth, pagination, resilience as above ...
```

For ongoing ingestion you typically use an empty body and rely on the saved cursor so each poll continues from the last position.

## Run

```bash
export ONEPASSWORD_EVENTS_TOKEN="ops_..."

hel validate
hel test --source 1password-audit   # or: hel run --once
```

You should see NDJSON lines (one per audit event). Then run `hel run` for continuous collection.

## Optional: dedupe

Events include a `uuid` field. To skip duplicates across polls or replay:

```yaml
  1password-audit:
    # ...
    dedupe:
      id_path: "uuid"
      capacity: 10000
```

## Other Events API endpoints

The same API has **sign-in attempts** (`POST /api/v2/signinattempts`) and **item usage** (`POST /api/v2/itemusages`), also cursor-in-body. Add separate sources with the same auth and pagination pattern, different URL and optional `source_label_value` to distinguish them.

## Testing with replay

Run once with `--record-dir ./recordings` (with valid credentials) to save responses, then use `hel run --once --replay-dir ./recordings` to replay without calling the API.

## Troubleshooting

| Symptom | Check |
|--------|--------|
| `ONEPASSWORD_EVENTS_TOKEN` unset | Export the bearer token from your Events Reporting integration. |
| `401 Unauthorized` | Token valid and not revoked; token must have **audit events** (auditevents) feature. |
| `400 Bad request` | Request body must be valid JSON; first request uses ResetCursor (e.g. `{}`) or cursor from previous response. |
| `500` / errors | See [Status codes](https://developer.1password.com/docs/events-api/status-codes/). |
| No events | Check optional `start_time` / `end_time` in body; cursor is persisted so next poll continues from last position. |

## Quick reference

- **API:** [POST /api/v2/auditevents](https://developer.1password.com/docs/events-api/reference#post-apiv2auditevents) — `POST https://events.1password.com/api/v2/auditevents`.
- **Auth:** Bearer token from Events Reporting integration with **audit events** (auditevents) feature.
- **Pagination:** Cursor in response body (`cursor`, `has_more`); send `cursor` in request body on next POST. Hel uses `strategy: cursor` with `cursor_param: cursor`, `cursor_path: cursor`; body is merged with cursor for POST.
- **Event array:** `items`.
- **Env:** `ONEPASSWORD_EVENTS_TOKEN`.
