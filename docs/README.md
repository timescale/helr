# Hel documentation

Table of contents for Hel docs.

## Core & features

| Doc | Description |
|-----|-------------|
| [hooks.md](hooks.md) | **JS Hooks (Boa)** — Customize request building, response parsing, pagination, and state per source. Optional `fetch()` for auth (e.g. PAT → cookie). |
| [audit.md](audit.md) | **Audit** — Optional logging for credential access and config changes (no secret values). |
| [rest-api.md](rest-api.md) | **REST API** — List sources, read state/config, trigger poll, reload config (when `global.api.enabled` is true). |

## Integrations

Step-by-step setup and troubleshooting for each supported API:

| Integration | Doc |
|-------------|-----|
| Okta | [integrations/okta.md](integrations/okta.md) |
| Google Workspace (GWS) | [integrations/gws-gcp.md](integrations/gws-gcp.md) |
| GitHub Enterprise | [integrations/github.md](integrations/github.md) |
| Slack Enterprise | [integrations/slack.md](integrations/slack.md) |
| 1Password (Business) | [integrations/1password.md](integrations/1password.md) |
| Tailscale | [integrations/tailscale.md](integrations/tailscale.md) |
| Andromeda Security | [integrations/andromeda.md](integrations/andromeda.md) |

## See also

- **[../README.md](../README.md)** — Project overview, quick start, and “How to run with…” summaries.
