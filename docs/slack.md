# Slack

**Slack** audit logs record who did what across your Enterprise organization - logins, channel and file actions, app installs, and more. Hel pulls these events via the **Slack Audit Logs API** ([Using the Audit Logs API](https://docs.slack.dev/admins/audit-logs-api/)): `GET https://api.slack.com/audit/v1/logs`. Events are returned in an `entries` array and paginated with a **cursor** in `response_metadata.next_cursor`; you pass it back as the `cursor` query parameter on the next request.

You authenticate with a **Slack user token** (prefix `xoxp-`) that has the `auditlogs:read` scope. The app must be **installed by the Owner** of an **Enterprise organization** (not a single workspace). The token must be associated with that Enterprise org owner.

## Requirements

- The Audit Logs API is **only available for Slack workspaces on an Enterprise plan**. Standard (Free, Pro, Business+) workspaces do not have access; the API returns errors such as `feature_not_enabled`, `invalid_workspace`, or `team_not_authorized`.
- An **Enterprise organization** and an app with the `auditlogs:read` **User Token Scope**, installed on the **organization** (not a workspace) by the **Owner**.
- Data is not available prior to **March 2018**.

## Step 1: Create an app and get a token

1. Go to [Slack API - Create app](https://api.slack.com/apps?new_app=1) and create an app (or use an existing one).
2. In the app, open **OAuth & Permissions**. Under **User Token Scopes**, add **auditlogs:read**.
3. Under **Manage Distribution**, enable distribution as needed, then use **Share Your App with Your Workspace** (or org). Copy the **Install to Workspace** (or **Install to Organization**) URL.
4. Open that URL in a browser and complete the OAuth flow. You must be logged in as the **Owner** of the **Enterprise organization**. In the install screen, choose the **organization** in the dropdown, not a single workspace.
5. After install, copy the **User OAuth Token** (starts with `xoxp-`). Store it as `SLACK_AUDIT_TOKEN` (or another env var you reference in `hel.yaml`).

**Note:** The token must be a **user token** (`xoxp-`) for an **Enterprise organization owner**. Enterprise org administrator tokens are not currently supported for the Audit Logs API.

## Step 2: Configure Hel

In `hel.yaml`, add a Slack audit log source. The API base URL is fixed; you can use query params for time range and filters.

```yaml
  slack-audit:
    url: "https://api.slack.com/audit/v1/logs"
    schedule:
      interval_secs: 300
      jitter_secs: 30
    auth:
      type: bearer
      token_env: SLACK_AUDIT_TOKEN
    headers:
      Accept: "application/json"
    query_params:
      limit: "200"
    pagination:
      strategy: cursor
      cursor_param: cursor
      cursor_path: response_metadata.next_cursor
      max_pages: 25
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
        page_delay_secs: 2
```

- **`SLACK_AUDIT_TOKEN`**: Your Slack user token (`xoxp-...`) with `auditlogs:read`, from an Enterprise org owner install.
- **`limit`**: Slack accepts up to **9,999** per request; 100–200 is recommended for steady pagination. Hel already supports the `entries` array and cursor from `response_metadata.next_cursor`.
- **Rate limit:** The Audit Logs API is **Tier 3**: up to **50 calls per minute** per organization (org-wide, not per-app). Use a conservative `interval_secs` (e.g. 300) and `page_delay_secs` (e.g. 2) to avoid `rate_limited` errors.

## Run

```bash
export SLACK_AUDIT_TOKEN="xoxp-..."

hel validate
hel test --source slack-audit   # or: hel run --once
```

You should see NDJSON lines (one per audit event). Then run `hel run` for continuous collection.

## Filters (query params)

You can narrow results with optional query parameters. Add them under `query_params` in `hel.yaml` (or use `first_request_query_params` if you only want them on the first request when there is no saved cursor).

| Parameter | Type   | Description |
|-----------|--------|-------------|
| `oldest`  | Integer | Unix timestamp of the least recent event to include (inclusive). Data not available before March 2018. |
| `latest`  | Integer | Unix timestamp of the most recent event to include (inclusive). |
| `limit`   | Integer | Max results per request (optimistic); maximum **9,999**. |
| `action`  | String | Comma-separated action names (e.g. `user_login,file_uploaded`), max 30. |
| `actor`   | String | User ID who initiated the action (e.g. `W123AB456`). |
| `entity`  | String | ID of the target entity (channel, workspace, file, etc.). |

Example: only logins for a specific user since a given time:

```yaml
  slack-audit:
    url: "https://api.slack.com/audit/v1/logs"
    # ...
    query_params:
      limit: "200"
      oldest: "1521214343"
      action: "user_login"
      actor: "W123AB456"
```

**Incremental from last event:** To resume from the latest event timestamp on each first request (no saved cursor), use `incremental_from`. Hel stores the max value of the given event field after each poll and sends it as the specified query param on the first request. Slack events use `date_create` (Unix timestamp); the API accepts `oldest` as that param:

```yaml
  slack-audit:
    url: "https://api.slack.com/audit/v1/logs"
    # ... auth, pagination, etc.
    incremental_from:
      state_key: "oldest_ts"
      event_timestamp_path: "date_create"
      param_name: "oldest"
```

On the first request (or after cursor reset), Hel will add `oldest=<last_stored_ts>` from state so you only fetch events newer than the last run. You can still set `oldest` / `latest` explicitly in `query_params` for a fixed window.

## Optional: dedupe

Events include an `id` field. To skip duplicates across polls or replay, add:

```yaml
  slack-audit:
    # ...
    dedupe:
      id_path: "id"
      capacity: 10000
```

## Testing with replay

To test without calling the API repeatedly: run once with `--record-dir ./recordings` (with valid credentials) to save responses, then use `hel run --once --replay-dir ./recordings` to replay from disk.

## Troubleshooting

| Symptom | Check |
|--------|--------|
| `SLACK_AUDIT_TOKEN` unset | Export the token; use a user token (`xoxp-`) with `auditlogs:read`. |
| `missing_authentication` / `invalid_authentication` | Token present and valid; must be for an **Enterprise organization owner** and have `auditlogs:read`. |
| `feature_not_enabled` / `invalid_workspace` / `team_not_authorized` | Audit Logs API is **only for Slack Enterprise**. Install the app on the **organization** (not a workspace) and ensure the workspace is part of an Enterprise org. |
| `user_not_authorized` | The user who installed the app must be an **Enterprise organization owner**. |
| `invalid_cursor` | Cursor expired or corrupted. With `on_cursor_error: reset`, Hel clears the cursor and starts from the first page on the next poll. |
| `rate_limited` | Tier 3 = 50 calls/min org-wide. Increase `interval_secs`, lower `max_pages`, set `page_delay_secs`; Hel uses `Retry-After` when `respect_headers: true`. |
| No events | Check `oldest` / `latest`; data not available before March 2018. |

## Quick reference

- **API:** [Audit Logs API methods & actions](https://docs.slack.dev/reference/audit-logs-api/methods-actions-reference) — `GET https://api.slack.com/audit/v1/logs`.
- **Auth:** User token (`xoxp-`) with `auditlogs:read`; `Authorization: Bearer <token>`; app installed by Enterprise org **Owner**.
- **Pagination:** Cursor in `response_metadata.next_cursor`; send as query param `cursor`. Hel uses `strategy: cursor` with `cursor_param: cursor`, `cursor_path: response_metadata.next_cursor`.
- **Rate limit:** 50 calls per minute (Tier 3), organization-wide.
- **Env:** `SLACK_AUDIT_TOKEN`.
