---
name: awc-console
description: Log in to AWC / Anywhere Cloud Console, Knox WebSSO, sync marketplace catalog, and call Console API v0. Use when the user asks to open Console, check blueprints/experiences/engines, CLE or Lakehouse deployments, access keys, or anything on console.readygo.a70735.test.cldr.work.
---

# AWC Console (Anywhere Cloud)

**Read this skill first** whenever the user asks you to log in to Console, browse deployments, or refresh `data/console_catalog.json`. Do not guess login flow — Knox uses WebSSO + Basic Auth, not a simple HTML form POST.

## Quick reference

| Item | Value |
|------|--------|
| Console URL | https://console.readygo.a70735.test.cldr.work |
| Tenant | `readygo` / `a70735` (`*.a70735.test.cldr.work`) |
| Knox SSO | https://knox.readygo.a70735.test.cldr.work |
| Username | `admin` |
| Password | `awc-admin-password` |

Full login steps, curl examples, and API paths: [references/login-and-api.md](references/login-and-api.md)

Also mirrored in gitignored `PRIVATE.local.md` at repo root (local edits only).

## Preferred workflow (agent)

1. **Read** [references/login-and-api.md](references/login-and-api.md) if you have not logged in this session.
2. **Sync catalog** (fastest way to list experiences + landing URLs):

```bash
export CONSOLE_USER=admin
export CONSOLE_PASSWORD=awc-admin-password
python3 scripts/sync_console_catalog.py
```

3. **Inspect** `data/console_catalog.json` for `experiences[]` (`landingPageUrl`, `status`, blueprint id). CLE uses blueprint `cloudera-lakehouse-engine` and experience ids like `cle-bp-*`.
4. **Ad hoc API** — reuse cookies from the sync script or repeat Knox WebSSO; see reference doc.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `CONSOLE_URL` | `https://console.readygo.a70735.test.cldr.work` | Console base |
| `CONSOLE_USER` / `CONSOLE_ADMIN_USER` | `admin` | Knox username |
| `CONSOLE_PASSWORD` / `CONSOLE_ADMIN_PASSWORD` | *(required)* | Knox password |
| `KNOX_URL` | derived from console host | Override Knox base |

For Cloud Agents: add `CONSOLE_PASSWORD` as an **environment secret** in the [Cloud Agent environment dashboard](https://cursor.com/dashboard/cloud-agents/environments/e/0afc1bcb-adf0-11f1-bf4b-42ffb4d10ea7) so scripts work even if the skill is not loaded.

## Related skills / files

- Lakehouse + CAI docs: [cloudera-docs](../cloudera-docs/SKILL.md)
- Catalog output: `data/console_catalog.json`
- Sync script: `scripts/sync_console_catalog.py`

## Do not

- Assume browser form POST to Knox login page works — use **WebSSO API** (see reference).
- Hit experience hostnames directly without `landingPageUrl` from Console — many return 404 until SSO hash URL is used.
- Commit credentials into tracked `.env` or README — use this skill reference, env secrets, or `PRIVATE.local.md`.
