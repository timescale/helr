# GitHub

**GitHub** organization audit logs record who did what in your org—repo changes, team events, user actions, and so on. Hel pulls these events via the **GitHub REST API** ([Get the audit log for an organization](https://docs.github.com/en/rest/orgs/audit-log#get-the-audit-log-for-an-organization)): `GET /orgs/{org}/audit-log`. Events are paginated with a **Link** header (`rel=next`); the API uses cursor-based pagination via `after`/`before` in the link URLs.

You authenticate with a **personal access token (classic)** or a **GitHub App** token that has the `read:audit_log` scope. The authenticated user must be an **organization owner** to use this endpoint.

## Requirements

- The audit log **API is only available for organizations on GitHub Enterprise Cloud or GitHub Enterprise Server.** Standard GitHub.com organizations (Free, Team without an Enterprise account) do not have access; the API returns **404 Not Found** for them.
- An **organization** you own (or have owner access to), **or** an **Enterprise** you own (for the enterprise-level endpoint below).
- A **personal access token (classic)** or **GitHub App** token with `read:audit_log` (org) or **Enterprise administration (read)** (enterprise).

## Option A: Personal access token (classic)

### Step 1: Create a classic PAT with audit log scope

1. Sign in to GitHub.com.
2. **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)** → **Generate new token (classic)**.
3. Name it (e.g. `hel-audit-log`). Set expiration as needed.
4. Under **Scopes**, enable **read:audit_log** (under "Admin: Organization").
5. Generate the token and copy it. **It is shown only once.** Store it as `GITHUB_TOKEN` (or another env var you reference in `hel.yaml`).

**Note:** The audit log API supports classic PATs with `read:audit_log`. Fine-grained PATs are not supported for this endpoint. GitHub App user/installation tokens with the appropriate org permissions also work.

### Step 2: Configure Hel

In `hel.yaml`, add a GitHub audit log source. Set the **organization name** (login) via environment variable. See [Configuration](../../README.md#configuration) for more options.

```yaml
  github-audit:
    url: "https://api.github.com/orgs/${GITHUB_ORG}/audit-log"
    schedule:
      interval_secs: 300
      jitter_secs: 30
    auth:
      type: bearer
      token_env: GITHUB_TOKEN
    headers:
      Accept: "application/vnd.github+json"
      X-GitHub-Api-Version: "2022-11-28"
      User-Agent: "Hel"
    query_params:
      per_page: "100"
      order: "asc"
    pagination:
      strategy: link_header
      rel: next
      max_pages: 15
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

- **`GITHUB_ORG`**: The organization **login** (slug), e.g. `my-company`, not the display name.
- **`GITHUB_TOKEN`**: Your classic PAT (or App token) with `read:audit_log`.
- **`order: asc`**: Fetches oldest-first so you can ingest in chronological order; use `desc` for newest-first.
- **Rate limit:** The audit log API allows **1,750 queries per hour** per user and IP. Use a conservative `interval_secs` (e.g. 300) and `max_pages` (e.g. 15); `page_delay_secs` helps spread pages.

## Option B: GitHub App (installation or user token)

For automation, you can use a **GitHub App** with permission to read the organization audit log. Install the app on the org, then use an **installation access token** or **user-to-server token**. Configure Hel the same way as Option A: `auth.type: bearer` and `token_env` pointing at the token your app obtains.

## Run

```bash
export GITHUB_ORG="your-org-login"
export GITHUB_TOKEN="ghp_..."

hel validate
hel test --source github-audit   # or: hel run --once
```

You should see NDJSON lines (one per audit event). Then run `hel run` for continuous collection.

## If you get 404: Enterprise-level audit log

If your org is not on GitHub Enterprise, the **organization** endpoint (`/orgs/{org}/audit-log`) returns 404. If you have an **Enterprise** (GitHub Enterprise Cloud or Server), use the **enterprise** endpoint instead: `GET /enterprises/{enterprise}/audit-log`. You must be an **enterprise owner**.

In `hel.yaml`, point the source at the enterprise URL and set the enterprise slug (and, for GitHub Enterprise Server, the API host):

```yaml
  github-audit:
    url: "https://${GITHUB_API_HOST}/enterprises/${GITHUB_ENTERPRISE}/audit-log"
    # ... same auth, headers, query_params, pagination, resilience as above ...
```

- **`GITHUB_API_HOST`**: Use `api.github.com` for GitHub Enterprise Cloud. For **GitHub Enterprise Server**, use your instance (e.g. `api.mycompany.ghe.com`).
- **`GITHUB_ENTERPRISE`**: The **enterprise slug** (e.g. `my-enterprise`), not the org name. Find it in your Enterprise settings URL.
- **`GITHUB_TOKEN`**: Classic PAT with **read:audit_log** (under Admin: Enterprise) or GitHub App with **Enterprise administration (read)**.

Then run:

```bash
export GITHUB_API_HOST="api.github.com"   # or api.yourcompany.ghe.com for GHE
export GITHUB_ENTERPRISE="your-enterprise-slug"
export GITHUB_TOKEN="ghp_..."

hel validate
hel test --source github-audit
```

## Optional: time range and filters

- **First request only:** Use `from` and `from_param` to send a start time. GitHub uses a `phrase` query for search; see [Searching the audit log](https://docs.github.com/en/organizations/keeping-your-organization-secure/managing-security-settings-for-your-organization/reviewing-the-audit-log-for-your-organization#searching-the-audit-log).
- **Query params:** Use `query_params` for `phrase`, `include` (`web` | `git` | `all`), `order`, `per_page` (max 100). The audit log retains events for **180 days** (Git events **7 days**). By default only the past three months are considered; use `phrase=created:>=YYYY-MM-DD` to narrow.

Example: only web events and a date phrase:

```yaml
  github-audit:
    url: "https://api.github.com/orgs/${GITHUB_ORG}/audit-log"
    # ...
    query_params:
      per_page: "100"
      order: "asc"
      include: "web"
      phrase: "created:>=2024-01-01"
```

## Optional: dedupe

Events include a `_document_id` field. To skip duplicates across polls or replay, add:

```yaml
  github-audit:
    # ...
    dedupe:
      id_path: "_document_id"
      capacity: 10000
```

## Testing with replay

To test without calling the API repeatedly: run once with `--record-dir ./recordings` (with valid credentials) to save responses, then use `hel run --once --replay-dir ./recordings` to replay from disk.

## Troubleshooting

| Symptom | Check |
|--------|--------|
| `GITHUB_ORG` / `GITHUB_TOKEN` unset | Export both; use org **login**, not display name. |
| `401 Unauthorized` | Token valid, not revoked, and has `read:audit_log`. |
| `403 Forbidden` (administrative rules / User-Agent) | GitHub requires a **User-Agent** header. Add `User-Agent: "Hel"` (or your app name) under `headers` in `hel.yaml`. |
| `403 Forbidden` (access) | Authenticated user must be an **organization owner** (or have audit log access). |
| `403` / `429` rate limit | Audit log API limit is 1,750 queries/hour. Increase `interval_secs`, lower `max_pages`, set `page_delay_secs`; Hel uses `Retry-After` when `respect_headers: true`. |
| `404 Not Found` | The audit log API is **only available for GitHub Enterprise**. Standard (Free/Team) orgs get 404. Use the **enterprise** endpoint if you have an Enterprise: `url: "https://${GITHUB_API_HOST}/enterprises/${GITHUB_ENTERPRISE}/audit-log"` with `GITHUB_ENTERPRISE` (enterprise slug) and `GITHUB_API_HOST` (e.g. `api.github.com` or `api.yourcompany.ghe.com`). See "If you get 404: Enterprise-level audit log" above. |
| Config placeholder unset | `${GITHUB_ORG}` (or `${GITHUB_ENTERPRISE}` / `${GITHUB_API_HOST}` for enterprise) in `hel.yaml` requires that env var. |
| No events | Check `phrase` and `include`; default view is last three months. |

## Quick reference

- **API:** [Get the audit log for an organization](https://docs.github.com/en/rest/orgs/audit-log#get-the-audit-log-for-an-organization) — `GET https://api.github.com/orgs/{org}/audit-log`.
- **Auth:** Classic PAT or GitHub App token with `read:audit_log`; `Authorization: Bearer <token>`.
- **Pagination:** Link header `rel=next`; Hel uses `link_header` strategy.
- **Rate limit:** 1,750 queries per hour; use conservative interval and `page_delay_secs`.
- **Env:** `GITHUB_ORG` (organization login), `GITHUB_TOKEN`.
