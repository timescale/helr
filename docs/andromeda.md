# Andromeda Security

**Andromeda Security** provides identity and access management; its **audit trail** records user and system events (logins, access requests, configuration changes, etc.). Hel pulls these events via the **Andromeda GraphQL API**: `POST https://api.live.andromedasecurity.com/graphql`. The API uses the operation **AndromedaEventsList** and returns events in `data.AndromedaEvents.edges[].node`. Pagination is **offset-based** (`pageArgs: { skip, pageSize }`).

Hel supports this API via **hooks** for **cookie auth** (PAT → cookie) and **backfill** by advancing `skip` each poll. See [hooks](hooks.md).

## Requirements

- An **Andromeda** tenant (e.g. `app.live.andromedasecurity.com`).
- **Authentication:** **PAT** (Personal Access Token) — set `ANDROMEDA_PAT` and `allow_network: true`; the hook's **getAuth** uses **fetch()** to exchange the PAT for a session cookie and returns `{ cookie }`. Or set **ANDROMEDA_COOKIE** (full Cookie header from the browser) for short-lived runs; **buildRequest** reads it from ctx.env.

## API overview (from HAR)

- **Endpoint:** `https://api.live.andromedasecurity.com/graphql`
- **Method:** POST
- **Request:** `Content-Type: application/json`; body is JSON with `operationName`, `variables` (including `pageArgs: { skip, pageSize }`), and `query`.
- **Response:** `data.AndromedaEvents.edges[]`; each `edge.node` has: `id`, `type`, `name`, `time`, `actor`, `level`, `subtype`, `eventPrimaryKey`, `__typename`.

Events are returned **newest first**. The hook uses **`getNextPage(ctx, request, response)`**: `request` is the request that was sent (url, body), so the hook can advance `pageArgs.skip` and return the next body — **multiple pages per poll** until a page returns fewer than `pageSize` events. **`commitState`** saves `skip` (so the next poll continues from the right offset) and `watermark`. After backfill, reset `skip` to `0` (or clear state) and use **dedupe** for continuous collection of new events.

## Step 1: Enable hooks and obtain credentials

1. **Enable hooks** in `hel.yaml`:

```yaml
global:
  hooks:
    enabled: true
    path: "./hooks"
    timeout_secs: 5
    allow_network: true   # required for PAT → cookie via fetch() in getAuth
```

2. **Credentials** (one of):
   - **PAT (recommended):** Create a Personal Access Token in Andromeda and set `ANDROMEDA_PAT`. Enable **`allow_network: true`** in `global.hooks`; the hook’s **getAuth** uses **fetch()** to exchange the PAT for a session cookie and returns `{ cookie }` (same flow as Andromeda’s official Python script). See [hooks.md](hooks.md#using-fetch).
   - **Cookie:** From the browser (e.g. DevTools → Application → Cookies for `app.live.andromedasecurity.com`), copy the full `Cookie` header (e.g. `DS=...; DSR=...`) and set `ANDROMEDA_COOKIE`. **buildRequest** reads it from ctx.env. Session cookies expire; use for testing or short-lived runs.

## Step 2: Add the Andromeda source (hooks)

In `hel.yaml`, add the source with `method: post`, no body (the hook builds it), and the hook script. Set `ANDROMEDA_PAT` (or `ANDROMEDA_COOKIE` for manual cookie). The hook’s **getAuth** handles PAT → cookie. The API expects browser-like headers (User-Agent, Referer, Origin) to avoid WAF/blocking; set them under the source's **headers**. The hook reuses them for the login fetch via **ctx.headers**.

```yaml
  andromeda-audit:
    url: "https://api.live.andromedasecurity.com/graphql"
    method: post
    headers:
      User-Agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0"
      Referer: "https://app.live.andromedasecurity.com/"
      Origin: "https://app.live.andromedasecurity.com"
      Accept: "*/*"
      Accept-Language: "en-US,en;q=0.9"
    schedule:
      interval_secs: 300
      jitter_secs: 30
    hooks:
      script: "andromeda-audit.js"
    dedupe:
      id_path: "id"
      capacity: 100000
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

- **`hooks.script`**: Uses `./hooks/andromeda-audit.js` (under `global.hooks.path`). The script builds the GraphQL body with `pageArgs.skip` from state and advances `skip` in `commitState`.
- **Backfill:** Each poll requests one page at `state.skip` (starts at 0), then saves `skip += events.length`. Run `hel run` (or `hel test --source andromeda-audit` repeatedly) to walk through pages. When a page returns fewer than `pageSize` events, you've reached the end.
- **Continuous (after backfill):** Reset state so `skip` is 0 (e.g. delete or clear the source’s state key `skip`), then keep running; dedupe by `id` avoids re-emitting old events.

## Hook script (andromeda-audit.js)

The script in `hooks/andromeda-audit.js`:

- **getAuth:** When `ANDROMEDA_PAT` is set (and `allow_network: true`), uses **fetch()** to POST to the login URL with `{ code: ctx.env.ANDROMEDA_PAT }`, reusing **ctx.headers** (source-configured) so the login request sends the same User-Agent, Referer, Origin, etc. Reads `Set-Cookie` from the response and returns `{ cookie }`. When only `ANDROMEDA_COOKIE` is set, returns `null`; **buildRequest** reads the cookie from ctx.env.
- **buildRequest:** Sets `Content-Type`, then **Cookie** (from ctx.env.ANDROMEDA_COOKIE when set). Builds the GraphQL body with `pageArgs: { skip: state.skip || 0, pageSize: 50 }`.
- **parseResponse:** Reads `data.AndromedaEvents.edges`, maps each `node` to `{ ts: node.time, source, event: node, meta: { id: node.id } }`.
- **getNextPage(ctx, request, response):** Uses `request.body.variables.pageArgs.skip` from the request that was sent. If the response has a full page (`edges.length === PAGE_SIZE`), returns the next body with `skip: currentSkip + PAGE_SIZE` so Hel fetches the next page in the same poll. Returns `null` when no more pages.
- **commitState:** Returns `{ skip: state.skip + events.length, watermark: lastTs }` (events here are all events from the poll, so skip advances correctly for the next poll).

You can change `PAGE_SIZE` at the top of the script if you want more or fewer events per request.

## Run

Build with the `hooks` feature, set credentials, then validate and run:

```bash
cargo build --features hooks
export ANDROMEDA_PAT="your-personal-access-token"   # or ANDROMEDA_COOKIE (from browser)
hel validate
hel test --source andromeda-audit
hel run
```

For **backfill**, run until you see fewer than 50 events in a poll (or run a fixed number of times). For **continuous** collection after that, reset `skip` to 0 and rely on dedupe.

## Optional: Declarative (no hooks)

If you only need **continuous** collection of the latest page (no backfill), you can use the declarative config: `response_events_path: "data.AndromedaEvents.edges"`, `response_event_object_path: "node"`, static `body` with `skip: 0`, and cookie or other auth as needed.
