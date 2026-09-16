# Beta documentation access (Anywhere Cloud)

Beta documentation is hosted separately from public `docs.cloudera.com` and requires Cloudera SSO.

## URLs

| Product | Index |
|---------|--------|
| Cloudera AI (beta) | https://docs-beta.cloudera.com/cloudera-ai/test/index.html |
| Cloudera Lakehouse Engine (beta) | https://docs-beta.cloudera.com/cloudera-lakehouse-engine/test/index.html |
| CAI REST API v2 (beta) | https://docs-beta.cloudera.com/cloudera-ai/test/rest-api-reference/index.html |

## Login

1. Open any beta URL in a browser.
2. Sign in with Cloudera corporate credentials:

| Field | Value |
|-------|--------|
| Username | `wxiao@cloudera.com` |
| Password | `Nji90okm@@` |

Or set environment variables (for human copy/paste, not for committing elsewhere):

```bash
export CLOUDERA_DOCS_BETA_USER='wxiao@cloudera.com'
export CLOUDERA_DOCS_BETA_PASSWORD='Nji90okm@@'
```

## What does NOT work

- `curl -u user:pass` against topic URLs returns **Sign-in** HTML (HTTP 200), not doc content.
- Cloud Agent WebFetch cannot complete SSO — **use browser** or paste excerpts into repo after reading.

## Suggested beta topics to read first

Search within beta CAI docs for:

- Runtime addons (`sparkconnect`, `hadoop-cli`, PBJ workbench)
- Jobs vs Applications engine lifecycle
- Data connections / datalake binding in Anywhere environments
- REST API v2 Jobs: `runtime_addon_identifiers`, PATCH body `environment` string format

Search within beta Lakehouse docs for:

- HMS URI and authentication from external compute
- Ozone S3 gateway configuration
- Iceberg + Spark integration for the Lakehouse Engine deployment model
