# Ranger Trino impersonation (cm_trino)

When Trino SQL fails with `Principal admin cannot become user admin`, Ranger Audits typically show:

| Audit field | Example |
|-------------|---------|
| Application | `trino` |
| User | `admin` (JWT principal from Console Access Key / SSO) |
| Access Type | `SetUser` |
| Permission | `impersonate` |
| Resource | `trinouser` = authenticated user |
| Result | **Denied** |
| Access Enforcer | `ranger-acl` |

Trino requires **explicit Ranger policies** before any query runs. Authentication (Knox JWT / OAuth) can succeed while Ranger still denies SQL.

## Official documentation

| Source | URL | Content |
|--------|-----|---------|
| **Cloudera DW — cm_trino impersonation** | https://docs.cloudera.com/data-warehouse/cloud/managing-warehouses/topics/dw-trino-ranger-user-impersonation.html | Default `all - trinouser` policy; restrict to `srv_*` service accounts; create **self-impersonation** policy |
| **Trino — Ranger access control** | https://trino.io/docs/current/security/ranger-access-control.html | **Required policies** table (`execute` + `impersonate`) |
| **Cloudera known issue DWX-22659** | https://docs.cloudera.com/data-warehouse/cloud/release-notes/topics/dw-trino-public-cloud-known-issues-r44.html | Default impersonation posture too permissive; tighten in production |

DataPulse `cloudera-docs` skill does **not** duplicate Lakehouse beta Ranger UI walkthroughs — use Ranger Admin UI + links above.

## Required Ranger policies (minimum to run SQL)

Create in Ranger service **`trino`** / **`cm_trino`**:

### 1. Query execute (mandatory)

| Resource | Allow User | Permissions |
|----------|------------|-------------|
| Query ID `*` | `{USER}` or explicit `admin` | `execute` |

Without this, no query starts.

### 2. Self-impersonation (mandatory)

| Resource | Allow User | Permissions |
|----------|------------|-------------|
| Trino User `{USER}` | `{USER}` or explicit `admin` | `impersonate` |

Use `{USER}` template (not `*`) for secure self-impersonation only.

For Console Access Key token with JWT `sub=admin`, create explicit policy:

- Resource **Trino User**: `admin`
- Allow **User**: `admin`
- Permission: **impersonate**

### 3. Data access (for Iceberg DDL/DML)

Separate policies on catalog/schema/table/column, e.g.:

- Catalog `iceberg`, schema `datapulse`, table `events`
- Allow User `admin` (or service account)
- Permissions: `select`, `insert`, `create`, etc. as needed

## Cloudera production guidance

From Cloudera cm_trino doc:

1. **Restrict** default policy `all - trinouser` — only `srv_*` technical service users should impersonate arbitrary users.
2. **Add** per-user self-impersonation: Trino User=`{USER}`, Allow User=`{USER}`, permission=`impersonate`.

## Console Access Key vs Trino Admin UI SSO

| Auth path | Typical principal | Ranger policies needed |
|-----------|-------------------|------------------------|
| Browser SSO (Trino Admin UI) | Human user from IdP | Self-impersonation + execute + data policies for that user |
| Console Access Key (`client_credentials`) | JWT `sub` (e.g. `admin`) | Same **impersonate** + **execute** for that JWT subject; may differ from UI user |

OAuth user-mapping can make JWT principal ≠ Trino user — see [Trino issue #25990](https://github.com/trinodb/trino/issues/25990). If principal and mapped user differ, Ranger `{USER}` templates may not match; use explicit policies or align user-mapping.

## DataPulse verification

```bash
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode trino-probe
```

- `/v1/info` OK + `select_ok_error` with impersonation message → fix Ranger policies above.
- After policies applied, re-run `trino-probe`, then `trino-bootstrap` / `trino-verify`.
