# GWS & GCP

**Google Workspace (GWS)** (formerly G Suite) is Google's productivity suite for organizations: Gmail, Drive, Calendar, Meet, Chat, Admin, and related services. **GWS audit logs** record who did what across your domain—logins, file access, admin changes, and so on. Hel pulls these logs via the **Admin SDK Reports API** (`activities.list`), one stream per application (login, drive, admin, user_accounts, chat, calendar, token, etc.). You need a **Workspace admin** (or Admin console access) to grant access.

## Auth options

| | Option A: OAuth2 refresh token | Option B: Service account |
|--------|--------|--------|
| **GCP** | OAuth Client ID only (Cloud Console) | Project, Admin SDK API, service account + JSON key |
| **GWS Admin** | Not needed | Domain-wide delegation required |
| **Setup** | One-time browser sign-in to get refresh token | No interactive sign-in after setup |
| **Hel auth** | `type: oauth2` | `type: google_service_account` |

Both options use OAuth credentials from [Google Cloud Console](https://console.cloud.google.com/). Option A avoids service accounts and domain-wide delegation; Option B (like [sansfor509 GWS log collection](https://github.com/dlcowen/sansfor509/tree/main/GWS/gws-log-collection)) is better for unattended automation.

## Option A: OAuth2 with refresh token

**Requires:** Workspace admin account, one-time browser access to get a refresh token.

### A1. Create OAuth credentials (Cloud Console)

1. [Google Cloud Console](https://console.cloud.google.com/) → create or select a project.
2. **APIs & Services** → **Credentials** → **Create credentials** → **OAuth client ID**.
3. If prompted, configure the **OAuth consent screen** (Internal or External).
4. **Application type:** Desktop app. Name it (e.g. `hel-gws`). Create.
5. Note **Client ID** and **Client Secret**.

### A2. Get a refresh token (one-time, as admin)

1. Open [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/).
2. Gear (⚙) → **Use your own OAuth credentials** → enter Client ID and Client Secret.
3. **Step 1:** Find **Admin SDK API v1** → select scope  
   `https://www.googleapis.com/auth/admin.reports.audit.readonly`  
   → **Authorize APIs** → sign in with a **Workspace admin**.
4. **Step 2:** **Exchange authorization code for tokens** → copy the **Refresh token**.

### A3. Configure Hel

In `hel.yaml`, add a GWS source with `auth.type: oauth2`:

```yaml
  gws-login:
    url: "https://admin.googleapis.com/admin/reports/v1/activity/users/all/applications/login"
    schedule:
      interval_secs: 300
      jitter_secs: 30
    auth:
      type: oauth2
      token_url: "https://oauth2.googleapis.com/token"
      client_id_env: GWS_CLIENT_ID
      client_secret_env: GWS_CLIENT_SECRET
      refresh_token_env: GWS_REFRESH_TOKEN
      scopes:
        - "https://www.googleapis.com/auth/admin.reports.audit.readonly"
    pagination:
      strategy: cursor
      cursor_param: pageToken
      cursor_path: nextPageToken
      max_pages: 50
    query_params:
      maxResults: "500"
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

### A4. Run

```bash
export GWS_CLIENT_ID="your-client-id.apps.googleusercontent.com"
export GWS_CLIENT_SECRET="your-client-secret"
export GWS_REFRESH_TOKEN="your-refresh-token"

hel validate
hel test --source gws-login
```

To add more apps (drive, admin, etc.), duplicate the source and change the URL path to `.../applications/drive`, `.../applications/admin`, and so on.

## Option B: Service account with domain-wide delegation

**Requires:** GCP project, service account JSON key, and GWS Admin access to enable domain-wide delegation.

### B1. Create a GCP project

1. [Google Cloud Console](https://console.cloud.google.com/) → create or select a project.

### B2. Enable Admin SDK API

1. **APIs & Services** → **Library** → search **Admin SDK API** → **Enable**.

### B3. Create service account and key

1. **APIs & Services** → **Credentials** → **Create credentials** → **Service account**.
2. Name (e.g. `hel-gws-audit`) → **Create and continue** → **Done**.
3. Open the service account → **Keys** → **Add key** → **Create new key** → **JSON** → **Create**.
4. Save the JSON file (e.g. `./credentials.json`). **Do not commit it.**

### B4. Domain-wide delegation (GWS Admin)

1. In Cloud Console, open the service account and copy its **Client ID** (numeric).
2. [admin.google.com](https://admin.google.com) → **Security** → **Access and data control** → **API Controls** → **Manage Domain Wide Delegation** → **Add new**.
3. Paste **Client ID**. **OAuth Scopes:**  
   `https://www.googleapis.com/auth/admin.reports.audit.readonly`  
   → **Authorize**.

### B5. Configure Hel

In `hel.yaml`, uncomment the `gws-login` source that uses **`auth.type: google_service_account`** (see `hel.yaml` comment block “Google Workspace (GWS) audit logs — Admin SDK Reports API”). Set:

- `credentials_file: "./credentials.json"` (or `credentials_env: GWS_CREDENTIALS_JSON` with the JSON string in that env var).
- Keep `subject_env: GWS_DELEGATED_USER`.

### B6. Run

```bash
export GWS_DELEGATED_USER="admin@yourdomain.com"   # Workspace admin email

hel validate
hel test --source gws-login
```

Optional: `hel run --once` or `hel run` for continuous collection.

### B7. Add more applications

Duplicate the `gws-login` source; change the source key and URL path:

| Source key | URL path |
|--------|--------|
| `gws-drive` | `.../applications/drive` |
| `gws-admin` | `.../applications/admin` |
| `gws-user_accounts` | `.../applications/user_accounts` |
| `gws-chat` | `.../applications/chat` |
| `gws-calendar` | `.../applications/calendar` |
| `gws-token` | `.../applications/token` |

## Troubleshooting

| Symptom | Check |
|--------|--------|
| **A:** `oauth2 token error` | Refresh token revoked or wrong; get a new one from OAuth Playground (signed in as Workspace admin). |
| **A:** `401 Unauthorized` | `GWS_CLIENT_ID`, `GWS_CLIENT_SECRET`, `GWS_REFRESH_TOKEN` set and correct. |
| **B:** `credentials missing client_email` / `private_key` | Service account JSON is valid and contains both fields. |
| **B:** `403 Forbidden` | Domain-wide delegation in GWS Admin with scope `admin.reports.audit.readonly`; Client ID matches the service account. |
| **B:** `401 Unauthorized` | `GWS_DELEGATED_USER` is a **Workspace admin** email (same domain). |
| No events | No activity in that app recently; try another app or check Admin SDK quota. |
| Config placeholder unset | Any `${VAR}` in `hel.yaml` must have that env var set. |

## Quick reference

- **Option A:** OAuth Client ID in Cloud Console → refresh token via [OAuth Playground](https://developers.google.com/oauthplayground/) (admin) → `auth.type: oauth2` + `GWS_CLIENT_ID`, `GWS_CLIENT_SECRET`, `GWS_REFRESH_TOKEN`.
- **Option B:** GCP project → Admin SDK API → service account + JSON key → domain-wide delegation in GWS Admin → `auth.type: google_service_account` + `credentials_file` (or `credentials_env`) + `GWS_DELEGATED_USER`.
