# JS Hooks (Boa)

Optional JavaScript hooks let you customize request building, response parsing, pagination, and state commit per source. Hooks run in a **sandbox**: timeout per call, no network (`fetch`) or file system (`require`) — Boa does not expose them by default. **`console.log`, `console.warn`, and `console.error`** are available; output is forwarded to Hel's logger (tracing), so it appears as JSON like other Hel logs, with field `hook_console`.

**Requires:** Build with `--features hooks` and enable hooks in config.

## Configuration

```yaml
global:
  hooks:
    enabled: true
    path: "./hooks"       # directory for scripts (or base path)
    timeout_secs: 5       # max execution time per hook call
    memory_limit_mb: 64   # optional; not all Boa builds support it
    allow_network: false # sandbox: no fetch()
    allow_fs: false      # sandbox: no file access

sources:
  my-source:
    url: "https://api.example.com/events"
    hooks:
      script: "my-source.js"   # under global.hooks.path, or absolute path
```

You can pass the script **inline** in YAML instead of a file path:

```yaml
sources:
  my-source:
    url: "https://api.example.com/events"
    hooks:
      script_inline: |
        function buildRequest(ctx) { return { query: { limit: "100" } }; }
        function parseResponse(ctx, response) {
          var body = typeof response.body === "string" ? JSON.parse(response.body) : response.body;
          return (body.items || []).map(function(e) {
            return { ts: e.published || "", source: ctx.sourceId, event: e, meta: {} };
          });
        }
```

Use a YAML block scalar (`|` or `>`) for multi-line JS. **One of `script` or `script_inline` must be set.**

### Inline vs file: when to use which?

| Use inline (`script_inline`) when… | Use a file (`script`) when… |
|-----------------------------------|-----------------------------|
| Single-file deployment: config + script in one YAML | Script is long or shared across sources |
| Tiny one-off hooks or examples | You want JS syntax highlighting and tooling |
| You’re generating config (e.g. from a template) | You prefer to keep config and code separate |

**Trade-offs:** Inline avoids file I/O and keeps everything in one place, but multi-line JS in YAML needs careful indentation and escaping. For anything beyond a few lines, a separate `.js` file is usually easier to maintain.

## Hook boundaries

Each hook is a JavaScript function in your script. You can define one or more; undefined hooks are skipped and default behavior is used.

| Hook | When called | Return value |
|------|-------------|--------------|
| **buildRequest(ctx)** | Before each HTTP request | `{ url?, headers?, query?, body? }` or `null` to use default |
| **parseResponse(ctx, response)** | After each response | Array of `{ ts, source, event, meta? }` |
| **getNextPage(ctx, request, response)** | After parsing; decide next page | `{ url?, body? }` or `null` when no more pages |
| **commitState(ctx, events)** | After a successful poll tick | Object of key-value pairs to write to the state store |

### Context (`ctx`)

- **ctx.env** — Environment variables (read-only snapshot).
- **ctx.state** — State store snapshot for this source (e.g. `cursor`, `next_url`, `watermark`).
- **ctx.requestId** — Unique ID for this request.
- **ctx.sourceId** — Source key from config.
- **ctx.defaultSince** — Value of `from` from config (if set).
- **ctx.pagination** — `{ lastCursor }` when applicable.

### Response (`response`)

- **response.status** — HTTP status code (number).
- **response.headers** — Object of header name → value.
- **response.body** — Parsed JSON (or string if not JSON).

### Request (`request`), passed to getNextPage

- **request.url** — URL that was sent.
- **request.body** — Request body that was sent (if POST), so you can derive the next page (e.g. offset or cursor from the previous request).

## Login-for-cookie auth

Many APIs (e.g. Andromeda Security, other SaaS) exchange a **token/code for a session cookie**: you POST the credential to a login URL and get back `Set-Cookie`; you then send that cookie on API requests. Hel supports this generically so any integration can reuse the same pattern.

**Config:** Use `auth.type: login_for_cookie` with hooks:

```yaml
sources:
  my-api:
    url: "https://api.example.com/graphql"
    method: post
    auth:
      type: login_for_cookie
      login_url: "https://api.example.com/login/access-key"
      credential_env: MY_API_PAT
      cookie_env: MY_API_COOKIE   # Hel injects the cookie into ctx.env under this key
      body_key: "code"            # optional; default "code". Request body is { body_key: credential }
    hooks:
      script: "my-api.js"
```

**Flow:** Each poll, Hel POSTs `{ body_key: credential }` to `login_url`, reads `Set-Cookie` from the response, and injects it into `ctx.env[cookie_env]`. Your hook’s **buildRequest** reads `ctx.env[cookie_env]` and sets the `Cookie` header. The same cookie is reused for all pages in that poll.

**Hook pattern:** In `buildRequest`, set the Cookie header from the env key you configured:

```javascript
function buildRequest(ctx) {
  var headers = { "Content-Type": "application/json" };
  var cookie = (ctx.env && ctx.env.MY_API_COOKIE) || "";
  if (cookie) headers["Cookie"] = cookie;
  return { headers: headers, body: { ... } };
}
```

**Convenience:** For Andromeda Security you can use `auth.type: andromeda_pat` (same flow with Andromeda’s default login URL and `cookie_env: ANDROMEDA_COOKIE`). Other integrations use `login_for_cookie` with their own URL and env names.

## Example script (Okta-style)

```javascript
// hooks/okta.js

function buildRequest(ctx) {
  return {
    headers: {
      "Authorization": "SSWS " + (ctx.env.OKTA_TOKEN || ""),
      "X-Request-ID": ctx.requestId
    },
    query: {
      since: ctx.state.watermark || ctx.defaultSince || "",
      limit: 1000
    }
  };
}

function parseResponse(ctx, response) {
  const body = typeof response.body === "string" ? JSON.parse(response.body) : response.body;
  const items = body.items || body.data || body.events || [];
  return items.map(function (event) {
    return {
      ts: event.published || event.timestamp || new Date().toISOString(),
      source: ctx.sourceId,
      event: event,
      meta: { cursor: ctx.state.cursor, request_id: ctx.requestId }
    };
  });
}

function getNextPage(ctx, request, response) {
  const linkHeader = response.headers["link"] || response.headers["Link"];
  if (!linkHeader) return null;
  const nextMatch = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
  if (!nextMatch) return null;
  return { url: nextMatch[1] };
}

function commitState(ctx, events) {
  const last = events[events.length - 1];
  return {
    watermark: last ? last.ts : (ctx.state.watermark || ""),
    cursor: ctx.pagination && ctx.pagination.lastCursor ? ctx.pagination.lastCursor : (ctx.state.cursor || "")
  };
}
```

## GraphQL via hooks

GraphQL APIs use a single POST endpoint with `query` and `variables` in the body. Response shape is often `{ data: { <queryName>: { edges: [ { node: {...} } ], pageInfo: { endCursor, hasNextPage } } } }`. You can support this entirely with hooks.

### 1. buildRequest

Build the GraphQL request body and optional headers:

```javascript
function buildRequest(ctx) {
  return {
    headers: {
      "Content-Type": "application/json",
      "Authorization": "Bearer " + (ctx.env.API_TOKEN || "")
    },
    body: {
      query: "query AuditLog($after: String) { auditLog(first: 100, after: $after) { edges { node { id createdAt action } } pageInfo { endCursor hasNextPage } } }",
      variables: {
        after: ctx.state.cursor || null
      }
    }
  };
}
```
(HTTP method is taken from source config: `method: post` for GraphQL.)

### 2. parseResponse

Parse the GraphQL JSON and map `data.<queryName>.edges` to events:

```javascript
function parseResponse(ctx, response) {
  const body = typeof response.body === "string" ? JSON.parse(response.body) : response.body;
  if (body.errors && body.errors.length > 0) {
    throw new Error("GraphQL errors: " + JSON.stringify(body.errors));
  }
  const edges = body.data && body.data.auditLog && body.data.auditLog.edges ? body.data.auditLog.edges : [];
  return edges.map(function (edge) {
    var node = edge.node || edge;
    return {
      ts: node.createdAt || new Date().toISOString(),
      source: ctx.sourceId,
      event: node,
      meta: { cursor: body.data.auditLog.pageInfo && body.data.auditLog.pageInfo.endCursor }
    };
  });
}
```

### 3. getNextPage

Return the next request body with updated `variables.after`:

```javascript
function getNextPage(ctx, request, response) {
  const body = typeof response.body === "string" ? JSON.parse(response.body) : response.body;
  const pageInfo = body.data && body.data.auditLog && body.data.auditLog.pageInfo;
  if (!pageInfo || !pageInfo.hasNextPage || !pageInfo.endCursor) return null;
  return {
    body: {
      query: "query AuditLog($after: String) { auditLog(first: 100, after: $after) { edges { node { id createdAt action } } pageInfo { endCursor hasNextPage } } }",
      variables: { after: pageInfo.endCursor }
    }
  };
}
```

### 4. commitState

Persist cursor for the next poll:

```javascript
function commitState(ctx, events) {
  var cursor = ctx.state.cursor || "";
  if (ctx.pagination && ctx.pagination.lastCursor) cursor = ctx.pagination.lastCursor;
  return { cursor: cursor };
}
```

### Config for GraphQL source

```yaml
sources:
  graphql-audit:
    url: "https://api.example.com/graphql"
    method: post
    hooks:
      script: "graphql-audit.js"
    auth: {}  # or use buildRequest to set Authorization from env
```

When using hooks, the **hook** is responsible for auth (e.g. in `buildRequest` headers). Declarative auth is still used when `buildRequest` returns `null` (default request).

## Sandbox

- **Timeout:** Each hook call is limited to `hooks.timeout_secs` (default 5). Exceeding it fails the call and the poll tick.
- **No network:** Boa does not expose `fetch` or `XMLHttpRequest` by default; hooks cannot make HTTP calls.
- **No file system:** No `require` or Node-style `fs`; hooks cannot read or write files.
- **Console:** `console.log`, `console.warn`, and `console.error` are available for debugging; they are forwarded to Hel's logger (tracing), so output is JSON with field `hook_console`.

## When to use hooks vs declarative config

- **Declarative (no hooks):** Use built-in pagination (link-header, cursor, page-offset), transform, and auth. Easiest to maintain.
- **Hooks:** Use for GraphQL, custom response shapes, per-request header mutation, or one-off APIs that don’t fit the declarative model.
