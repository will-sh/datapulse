# AWC Console — login and API

## Credentials (readygo test environment)

| Field | Value |
|-------|--------|
| Username | `admin` |
| Password | `awc-admin-password` |
| Console | https://console.readygo.a70735.test.cldr.work |
| Knox | https://knox.readygo.a70735.test.cldr.work |

User provided these credentials in a prior session; WebSSO login was verified (HTTP 303 + `hadoop-jwt` cookie, Console title **Anywhere Cloud Console UI**).

## Knox WebSSO login (required pattern)

The Knox **login HTML page is not a simple form POST**. Use the WebSSO endpoint with **HTTP Basic Auth**:

```http
POST https://knox.readygo.a70735.test.cldr.work/gateway/knox-cdpsso/api/v1/websso?originalUrl=https://console.readygo.a70735.test.cldr.work/
Authorization: Basic <base64(admin:awc-admin-password)>
```

Success:

- HTTP **303** (or 302)
- `Set-Cookie: hadoop-jwt=...` (domain `.a70735.test.cldr.work`)
- Follow redirect / reuse cookie for Console and experience URLs

### curl example

```bash
CONSOLE_URL='https://console.readygo.a70735.test.cldr.work'
KNOX='https://knox.readygo.a70735.test.cldr.work'
AUTH=$(printf 'admin:awc-admin-password' | base64 -w0)
COOKIEJAR=/tmp/console-cookies.txt

curl -sk -c "$COOKIEJAR" -X POST \
  -H "Authorization: Basic $AUTH" \
  "${KNOX}/gateway/knox-cdpsso/api/v1/websso?originalUrl=${CONSOLE_URL}/"

curl -sk -b "$COOKIEJAR" "${CONSOLE_URL}/" | grep -i '<title'
```

### Python (same as sync script)

See `scripts/sync_console_catalog.py` → `login()`. It POSTs to WebSSO, stores cookies in `/tmp/console-catalog-cookies.txt`, then calls JSON APIs.

## Sync marketplace catalog

```bash
export CONSOLE_USER=admin
export CONSOLE_PASSWORD=awc-admin-password
python3 scripts/sync_console_catalog.py
```

Writes `data/console_catalog.json` with:

- `experiences` — deployed stacks (`landingPageUrl`, status, blueprint)
- `engines` — engine definitions
- `blueprints` — marketplace templates (CAI, CSM, CLE, etc.)

## Useful Console API v0 paths

All require authenticated session cookie from Knox WebSSO:

| Path | Content |
|------|---------|
| `/api/v0/console/experiences` | Deployed experiences + landing URLs |
| `/api/v0/console/blueprints` | Marketplace blueprints |
| `/api/v0/console/engines` | Engine catalog |
| `/api/v0/auth/access-keys/credentials` | Create Kafka/API access keys (POST) |
| `/api/v0/auth/access-keys/token` | OAuth token for Kafka SASL |

## Experience URLs (CLE / Lakehouse)

Direct DNS names like `cle-bp-trino.cldr-csk-lakehouse.a70735.test.cldr.work` often **404** without the Console-issued `landingPageUrl` (hash-based Knox path). Always prefer `landingPageUrl` from synced experiences.

Legacy Lakehouse experience ids used `lakehouse-bp-*`; newer CLE deployments use `cle-bp-*` on cluster `cldr-csk-lakehouse`.

## Bastion access (optional)

If running from outside the test VPC, prior agents used AWS EC2 Instance Connect + SSH port 2222 to a bastion, then curl from inside the network. Cloud Agent pods in this environment can usually reach Console/Knox directly.

## CSA (Streaming Analytics / Flink SQL)

| Item | Value |
|------|--------|
| Experience | `deploy-018` / `csa-bp` on cluster `cldr-csk-csa-1` |
| UI (SSB) | https://csa-bp-csa-ssb-sse.cldr-csk-csa-1.a70735.test.cldr.work/ui/console |
| Login | Knox WebSSO first (`admin` / `awc-admin-password`), then open UI URL |

**NXDOMAIN / UI unreachable:** CSA cluster Istio ingress was **internal-only** and Route53 had no records. Fix pattern (2026-09-17): create **internet-facing** NLB alias records in Route53 for `cldr-csk-csa-1.a70735.test.cldr.work`, `*.cldr-csk-csa-1...`, and `csa-bp-csa-ssb-sse.cldr-csk-csa-1...` pointing at the cluster Istio targets. Long-term: patch `istio-ingress/default-awc-istio` Service on CSA cluster to `internet-facing` + public subnet so `external-dns` manages records.

### CSA Flink job API (programmatic)

Knox WebSSO cookie auth, then SSB API on the CSA host. If local DNS for the SSB hostname fails, use curl `--resolve csa-bp-csa-ssb-sse.cldr-csk-csa-1.a70735.test.cldr.work:443:<istio-ip>`.

| Step | Method | Path | Body |
|------|--------|------|------|
| Create/update job | POST / PUT | `/api/v2/projects/{projectId}/jobs` | `SqlExecuteRequest`: `{ sql, job_config: { job_name, runtime_config, checkpoint_config, kubernetes_config, ... }, mv_endpoints }` |
| Execute job | POST | `/internal/job/execute?jobId={id}` | same `SqlExecuteRequest` |
| List jobs | GET | `/internal/job/projects/{projectId}` | — |
| Create table DDL template | POST | `/internal/ddl/create-table` | `{ connector_type, format_type, sql: null }` |

**Job config enums:** `runtime_config.execution_mode` ∈ `APPLICATION` \| `PER_JOB` \| `SESSION`; `runtime_config.runtime_mode` ∈ `STREAMING` \| `BATCH` \| `AUTOMATIC`. Job names must match `[A-Za-z_][A-Za-z0-9_]*` (no hyphens).

**Kafka OAuth in Flink:** avoid `sasl.login.callback.handler.class=OAuthBearerLoginCallbackHandler` (class missing in CSA Flink image). Use `properties.sasl.jaas.config` + embedded `properties.ssl.truststore.certificates` (PEM with literal `\n`, not raw newlines). See `scripts/csa_flink_kafka_iceberg.py`. As of 2026-09-17, Flink’s shaded Kafka connector still auto-resolves the non-shaded `OAuthBearerLoginCallbackHandler` and fails at runtime (`Class ... could not be found`) — requires CSA image / classpath fix (kafka-clients in Flink lib).

**Iceberg sink (2026-09-17 probes):**

| HMS URI | Result from CSA Flink pods |
|---------|----------------------------|
| `thrift://hivemetastore.cldr-csk-lakehouse...:9083` (public) | TCP/HMS connect OK; sink fails: `Failed to list namespace under namespace: datapulse` (also `default`) — typical Ranger `USE` denial for the Flink HMS principal |
| `thrift://metastore-service.lakehouse-bp-*.svc.cluster.local:9083` | `Failed to connect to Hive Metastore` (cross-cluster; CSA `cldr-csk-csa-1` cannot reach Lakehouse in-cluster services) |

Trino on `:443` works (`iceberg.datapulse.events` has data). Flink Iceberg connector needs HMS Thrift **plus** Ranger policies for the CSA Flink service user on target databases, or a platform-provided Lakehouse catalog/data connection (like CAI `lakehouse-integrated`). Until fixed, use CAI/Trino ingest (`python3 scripts/cai_submit_lakehouse_job.py --mode trino-ingest`).

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `CONSOLE_PASSWORD is required` | Export password or read this file |
| Knox login page loop | Use WebSSO POST + Basic Auth, not form fields |
| Experience 404 | Fetch `landingPageUrl` from `/api/v0/console/experiences` |
| CSA / experience NXDOMAIN | See CSA section above; verify Route53 + public Istio NLB |
| Stale deployment list | Re-run `sync_console_catalog.py` |
