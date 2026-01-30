# Okta

**Okta** is an identity and access management platform (SSO, MFA, user lifecycle). The **System Log** records who did what in your org—logins, token usage, admin changes, policy events, and so on. Hel pulls these events via the **Okta Management API** ([System Log](https://developer.okta.com/docs/reference/api/system-log/)): `GET /api/v1/logs`. Events are paginated with a **Link** header (`rel=next`). You need an **API token** (admin) to call the API.

## Requirements

- An **Okta org** (e.g. `dev-12345.okta.com`).
- An **admin** (or account with sufficient API access) to create an API token (with sufficient permissions).

## Step 1: Create an API token

1. Sign in to Okta Admin Console (e.g. `https://your-domain.okta.com/admin` or `https://integrator-12345678-admin.okta.com`).
2. **Security** → **API** → **Tokens** tab → **Create Token**.
3. Name it (e.g. `hel-system-log` or whatever) and hit create.
4. Copy the token and store it securely. **It is shown only once.** You'll use it as `OKTA_API_TOKEN` environment variable.

Only certain admin roles can create tokens (e.g. Super Admin, Org Admin, Read-only Admin). Tokens inherit the permissions of the user who created them. For production, use a [dedicated admin, service account or OAuth 2.0](https://help.okta.com/en-us/content/topics/users-groups-profiles/service-accounts/service-accounts-alternatives.htm).

## Step 2: Configure Hel

In `hel.yaml`, add or uncomment an Okta source. Set **domain** via environment variables (no `https://`). To know more about configuration options, read [Configuration](../README.md#configuration).

```yaml
  okta-audit:
    url: "https://${OKTA_DOMAIN}/api/v1/logs"
    schedule:
      interval_secs: 60
      jitter_secs: 10
    auth:
      type: bearer
      token_env: OKTA_API_TOKEN
      prefix: SSWS
    pagination:
      strategy: link_header
      rel: next
      max_pages: 20
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

- **`OKTA_DOMAIN`**: Your Okta domain only (e.g. `integrator-12345678.okta.com`). No `https://`.
- **`prefix: SSWS`**: Okta expects `Authorization: SSWS <token>`.

## Step 3: Run

```bash
export OKTA_DOMAIN="dev-12345.okta.com"
export OKTA_API_TOKEN="your-api-token"

hel validate
hel test --source okta-audit  # or: hel run --once
```

You should see NDJSON lines (one per System Log event). Then execute `hel run` for continuous collection.

## Optional: time range and filters

- **First request only:** Use `from` and `from_param` to send a start time as a query param (e.g. `since`).
- **Query params:** Use `query_params` for `limit`, `since`, `until`, `filter`, `q`, `sortOrder`. See [System Log API](https://developer.okta.com/docs/reference/api/system-log/).

Example: only events after a date and a filter:

```yaml
  okta-audit:
    url: "https://${OKTA_DOMAIN}/api/v1/logs"
    # ...
    from: "2024-01-01T00:00:00Z"
    from_param: since
    query_params:
      limit: "100"
      sortOrder: "ASCENDING"
      # filter: 'eventType eq "user.session.start"'
```

## Optional: dedupe and rate limiting

- **Dedupe:** To skip duplicate events by ID (e.g. after replay or overlapping polls), add `dedupe.id_path: "uuid"` (or `"id"`) and `capacity`.
- **Rate limit:** Okta allows about 60 requests/min. One poll can request many pages in a row. Use `max_pages` and `rate_limit.page_delay_secs` to cap burst; Hel uses `Retry-After` on 429 when `respect_headers: true`.

## Testing with replay

To test the pipeline without calling Okta repeatedly: run once with `--record-dir ./recordings` (with valid credentials) to save responses, then use `hel run --once --replay-dir ./recordings` to replay from disk. No live API calls during replay.

## Troubleshooting

| Symptom | Check |
|--------|--------|
| `OKTA_DOMAIN` / `OKTA_API_TOKEN` unset | Export both before running; no `https://` in domain. |
| `401 Unauthorized` | Token valid and not revoked; correct `prefix: SSWS`. |
| `429 Too Many Requests` | Rate limit; Hel retries with `Retry-After`. Lower `max_pages` or set `page_delay_secs`. |
| Config placeholder unset | `${OKTA_DOMAIN}` in `hel.yaml` requires `OKTA_DOMAIN` in the environment. |
| No events | Time range or filter may exclude events; try without `from`/`filter` first. |

## Quick reference

- **API:** [System Log](https://developer.okta.com/docs/reference/api/system-log/) — `GET https://<domain>/api/v1/logs`.
- **Auth:** API token in **Security → API → Tokens**; use `Authorization: SSWS <token>` (`auth.prefix: SSWS`).
- **Pagination:** Link header `rel=next`; Hel uses `link_header` strategy.
- **Env:** `OKTA_DOMAIN` (domain only), `OKTA_API_TOKEN`.
