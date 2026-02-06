// Andromeda Security audit trail: GraphQL API with cookie auth, skip-based backfill.
// Auth: ANDROMEDA_PAT → getAuth uses fetch() to exchange for cookie (set allow_network: true); or ANDROMEDA_COOKIE (full Cookie header, e.g. from browser). State.skip advances each poll for backfill.

var PAGE_SIZE = 50;
var ANDROMEDA_LOGIN_URL = "https://api.live.andromedasecurity.com/login/access-key";

function andromedaEventsListBody(skip) {
  return {
    operationName: "AndromedaEventsList",
    variables: {
      nameFilter: null,
      typeFilter: null,
      subtypeFilter: null,
      levelFilter: null,
      search: "",
      pageArgs: { skip: skip, pageSize: PAGE_SIZE }
    },
    query: `
      query AndromedaEventsList(
        $nameFilter: AndromedaEventNameFilter,
        $typeFilter: AndromedaEventTypeFilter,
        $subtypeFilter: AndromedaEventSubTypeFilter,
        $levelFilter: AndromedaEventLevelFilter,
        $search: String,
        $pageArgs: PageArgs!
      ) {
        AndromedaEvents(
          filters: {
            name: $nameFilter
            eventType: $typeFilter
            eventSubtype: $subtypeFilter
            level: $levelFilter
            eventPrimaryKey: { icontains: $search }
            or: {
              name: $nameFilter
              eventType: $typeFilter
              eventSubtype: $subtypeFilter
              level: $levelFilter
              actor: { icontains: $search }
            }
          }
          pageArgs: $pageArgs
        ) {
          edges {
            node {
              id type name time actor level subtype eventPrimaryKey __typename
            }
            __typename
          }
          __typename
        }
      }
    `.trim()
  };
}

async function getAuth(ctx) {
  if (ctx.env && ctx.env.ANDROMEDA_PAT) {
    var headers = ctx.headers ? Object.assign({}, ctx.headers) : {};
    headers["Content-Type"] = "application/json";
    var res = await fetch(ANDROMEDA_LOGIN_URL, {
      method: "POST",
      headers: headers,
      body: JSON.stringify({ code: ctx.env.ANDROMEDA_PAT })
    });
    if (res.status < 200 || res.status >= 300) throw new Error("Andromeda login failed: " + res.status);
    var setCookie = res.headers.get("set-cookie") || res.headers.get("Set-Cookie") || "";
    var cookie = setCookie.split(";")[0].trim();
    if (!cookie) throw new Error("Andromeda login response missing set-cookie (status " + res.status + ")");
    return { cookie: cookie };
  }
  return null;
}

function buildRequest(ctx) {
  var skip = parseInt(ctx.state.skip || "0", 10);
  if (Number.isNaN(skip)) skip = 0;
  var headers = { "Content-Type": "application/json" };
  var cookie = (ctx.env && ctx.env.ANDROMEDA_COOKIE) || "";
  if (cookie) headers["Cookie"] = cookie;
  return {
    headers: headers,
    body: andromedaEventsListBody(skip)
  };
}

function parseResponse(ctx, response) {
  var body;
  if (typeof response.body === "string") {
    try {
      body = JSON.parse(response.body);
    } catch (e) {
      var snippet = response.body.length > 200 ? response.body.slice(0, 200) + "..." : response.body;
      throw new Error("parseResponse: HTTP " + (response.status || "") + ", body is not JSON: " + snippet);
    }
  } else {
    body = response.body;
  }
  if (!body || typeof body !== "object") {
    throw new Error("parseResponse: HTTP " + (response.status || "") + ", body is not an object");
  }
  if (body.errors && body.errors.length > 0) {
    throw new Error("parseResponse: HTTP " + (response.status || "") + ", GraphQL errors: " + JSON.stringify(body.errors));
  }
  var connection = body.data && body.data.AndromedaEvents;
  var edges = connection && connection.edges ? connection.edges : [];
  return edges.map(function (edge) {
    var node = edge.node || edge;
    return {
      ts: node.time || new Date().toISOString(),
      source: ctx.sourceId,
      event: node,
      meta: { id: node.id }
    };
  });
}

function getNextPage(ctx, request, response) {
  if (!request || !request.body || !request.body.variables || !request.body.variables.pageArgs) return null;
  var body;
  if (typeof response.body === "string") {
    try {
      body = JSON.parse(response.body);
    } catch (e) {
      return null;
    }
  } else {
    body = response.body;
  }
  if (!body || typeof body !== "object") return null;
  var connection = body.data && body.data.AndromedaEvents;
  var edges = connection && connection.edges ? connection.edges : [];
  if (edges.length < PAGE_SIZE) return null;
  var currentSkip = request.body.variables.pageArgs.skip || 0;
  var nextSkip = currentSkip + PAGE_SIZE;
  return { body: andromedaEventsListBody(nextSkip) };
}

function commitState(ctx, events) {
  var skip = parseInt(ctx.state.skip || "0", 10);
  if (Number.isNaN(skip)) skip = 0;
  var nextSkip = skip + events.length;
  var last = events.length > 0 ? events[events.length - 1] : null;
  return {
    skip: String(nextSkip),
    watermark: last ? last.ts : (ctx.state.watermark || "")
  };
}
