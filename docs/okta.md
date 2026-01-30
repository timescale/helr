# Okta

**Okta** is an identity and access management platform (SSO, MFA, user lifecycle). The **System Log** records who did what in your org—logins, token usage, admin changes, policy events, and so on. Hel pulls these events via the **Okta Management API** ([System Log](https://developer.okta.com/docs/reference/api/system-log/)): `GET /api/v1/logs`. Events are paginated with a **Link** header (`rel=next`).

You can authenticate in one of two ways:

- **Option A (SSWS):** Create an API token in Okta Admin (**Security → API → Tokens**) and use `Authorization: SSWS <token>`. Easiest for a single admin or testing.
- **Option B (OAuth 2.0):** Create an API service integration (app with **Public key / Private key** client auth), grant **okta.logs.read**, assign an **Admin role** (e.g. Read-only Admin), and use client_credentials + DPoP. Better for production and least-privilege.

## Requirements

- An **Okta org** (e.g. `dev-12345.okta.com`).
- An **admin** (or account with sufficient API access) to create an API token or an OAuth 2.0 app integration.

---

## Option A: API token (SSWS)

### Step 1: Create an API token

1. Sign in to Okta Admin Console (e.g. `https://your-domain.okta.com/admin` or `https://integrator-12345678-admin.okta.com`).
2. **Security** → **API** → **Tokens** tab → **Create Token**.
3. Name it (e.g. `hel-system-log` or whatever) and hit create.
4. Copy the token and store it securely. **It is shown only once.** You'll use it as `OKTA_API_TOKEN` environment variable.

Only certain admin roles can create tokens (e.g. Super Admin, Org Admin, Read-only Admin). Tokens inherit the permissions of the user who created them. For production, use a [dedicated admin, service account or OAuth 2.0](https://help.okta.com/en-us/content/topics/users-groups-profiles/service-accounts/service-accounts-alternatives.htm).

### Step 2: Configure Hel

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

---

## Option B: OAuth 2.0 (API Service Integration)

Use an **API service integration** with **client_credentials** and **DPoP** so Hel can call Okta APIs (e.g. System Log) without a long-lived API token. The Org authorization server requires **private_key_jwt** for client auth and (when DPoP is enabled) a **DPoP** proof on token and API requests.

### Step 1: Create the app in Okta

1. Sign in to Okta Admin Console (e.g. `https://your-domain.okta.com/admin`).
2. Go to **Applications** → **Applications** (or **API Service Integrations** if your org has that section).
3. Click **Create App Integration**.
4. Choose **API Services** (or the option that gives **Client Credentials** and **Okta API Scopes**). Create the app (e.g. name it "Hel").
5. On the app’s **General** tab:
   - Note the **Client ID**.
   - Set **Client authentication** to **Public key / Private key**.
   - Generate or upload an RSA key pair; **save the private key (PEM)** for Hel—Okta only stores the public key.
6. If the app has a **Require DPoP header** (or similar) option and you enable it, set `dpop: true` in Hel (see below).

### Step 2: Grant Okta API scope and assign an Admin role

1. Open the **Okta API Scopes** tab for the app.
2. Click **Grant** for **okta.logs.read** (required for System Log). Save if needed.
3. Open the **Admin roles** tab.
4. Click **Assign Admin Role** and assign at least one role that can read the System Log (e.g. **Read-only Admin**). Without an assigned admin role, Okta returns **403 Forbidden** on `/api/v1/logs` even when `okta.logs.read` is granted. Save.

### Step 3: Configure Hel

In `hel.yaml`, add a source that uses the **org** token URL, client ID, and **client_private_key** (no client_secret). Use the same domain as for SSWS (e.g. `your-org.okta.com`, not the `-admin` URL).

```yaml
  okta-audit-oauth2:
    url: "https://${OKTA_DOMAIN}/api/v1/logs"
    schedule:
      interval_secs: 60
      jitter_secs: 10
    auth:
      type: oauth2
      token_url: "https://${OKTA_DOMAIN}/oauth2/v1/token"
      client_id_env: OKTA_CLIENT_ID
      client_private_key_env: OKTA_CLIENT_PRIVATE_KEY   # PEM (RS256); required for Org AS
      scopes:
        - "okta.logs.read"
      dpop: true   # when Okta app has "Require DPoP header" enabled
      # no refresh_token_env → client_credentials grant; scope required for API service integration
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

- **Token URL:** For **API service integration** (client_credentials for Okta APIs): `https://${OKTA_DOMAIN}/oauth2/v1/token` — [Build an API service integration](https://developer.okta.com/docs/guides/build-api-integration/main/#request-an-access-token). For **custom** server only: `https://${OKTA_DOMAIN}/oauth2/default/v1/token` (tokens cannot call Okta APIs).
- **Scopes:** For API service integration, **scope is required** (Okta rejects empty scope). Use `scopes: ["okta.logs.read"]` for System Log. See [OAuth 2.0 Scopes for Okta Admin Management](https://developer.okta.com/docs/reference/api/oauth2/#scopes).
- **DPoP:** When your Okta app has **Require DPoP header** enabled, set `dpop: true` under `auth`; Hel sends a DPoP proof (RFC 9449) on token and API requests.
- **Private key JWT:** The **Org** authorization server requires **private_key_jwt** for client_credentials. Use `client_private_key_env` or `client_private_key_file` (RSA PEM). For **custom** authorization server, client_secret is still supported.

## Step 3: Run

```bash
export OKTA_DOMAIN="dev-12345.okta.com"
export OKTA_API_TOKEN="your-api-token"

hel validate
hel test --source okta-audit  # or: hel run --once
```

With OAuth2 (App Integration):

```bash
export OKTA_DOMAIN="dev-12345.okta.com"
export OKTA_CLIENT_ID="your-client-id"
export OKTA_CLIENT_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----"

hel validate
hel test --source okta-audit-oauth2  # or: hel run --once
```

Alternatively, put the private key in a file and set `client_private_key_file: "./priv.pem"` in `hel.yaml` instead of `client_private_key_env`.

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
| OAuth2: **404 on token** | **Use the API domain, not the Admin domain.** If you use an Integrator org, the Admin Console is often `https://your-org-admin.okta.com`, but OAuth and API live on `https://your-org.okta.com` (no `-admin`). Set `OKTA_DOMAIN=your-org.okta.com` (e.g. `tigerdata-integrator-8290068.okta.com`). To confirm: open `https://${OKTA_DOMAIN}/.well-known/openid-configuration` in a browser; if you see JSON with `token_endpoint`, that domain is correct. |
| OAuth2: **invalid_dpop_proof** / "DPoP proof JWT header is missing" | Set `dpop: true` under `auth` in `hel.yaml` so Hel sends a DPoP proof on token and API requests. See [Okta DPoP](https://developer.okta.com/docs/guides/dpop/nonoktaresourceserver/main/). Or in Okta Admin uncheck **Require DPoP header** for the app. |
| OAuth2: other token error | Use org server `/oauth2/v1/token` for Okta API scopes; use `/oauth2/default/v1/token` for custom server. See [Build an API service integration](https://developer.okta.com/docs/guides/build-api-integration/main/#request-an-access-token). |
| OAuth2: 403 on `/api/v1/logs` | Grant **okta.logs.read** on the app’s **Okta API Scopes** tab **and** assign an **Admin role** (e.g. Read-only Admin) on the **Admin roles** tab. Without an admin role, Okta returns 403 even when the scope is granted. If using the custom server, use org server instead (tokens from `/oauth2/default` cannot call Okta APIs). |
| `401 Unauthorized` | Token valid and not revoked; correct `prefix: SSWS` for API token. |
| `429 Too Many Requests` | Rate limit; Hel retries with `Retry-After`. Lower `max_pages` or set `page_delay_secs`. |
| Config placeholder unset | `${OKTA_DOMAIN}` in `hel.yaml` requires `OKTA_DOMAIN` in the environment. |
| No events | Time range or filter may exclude events; try without `from`/`filter` first. |

## Quick reference

- **API:** [System Log](https://developer.okta.com/docs/reference/api/system-log/) — `GET https://<domain>/api/v1/logs`.
- **Auth (SSWS):** API token in **Security → API → Tokens**; use `Authorization: SSWS <token>` (`auth.prefix: SSWS`).
- **Auth (OAuth2):** App Integration with **Public key / Private key** client auth; grant **okta.logs.read** and assign an **Admin role** (e.g. Read-only Admin). Use `auth.type: oauth2`, org token URL, `client_private_key_*` (PEM), `scopes: ["okta.logs.read"]`, and `dpop: true` if the app requires DPoP.
- **Pagination:** Link header `rel=next`; Hel uses `link_header` strategy.
- **Env:** `OKTA_DOMAIN` (domain only); for SSWS: `OKTA_API_TOKEN`; for OAuth2: `OKTA_CLIENT_ID` and `OKTA_CLIENT_PRIVATE_KEY` (PEM) or `client_private_key_file` in config.
