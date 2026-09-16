---
name: cloudera-docs
description: Look up Cloudera AI (CAI), Lakehouse, and API v2 documentation — DS 1.0 public docs and Anywhere Cloud beta docs. Use when configuring Spark/Iceberg/HMS in CAI Jobs, runtime addons, data connections, or REST API v2 jobs.
---

# Cloudera Documentation (CAI + Lakehouse)

Use this skill before guessing Spark/Iceberg/HMS wiring in DataPulse CAI Jobs.

## Which doc set to use

| Era | Base URL | Auth | When |
|-----|----------|------|------|
| **DS 1.0 / public CAI on Cloud** | https://docs.cloudera.com/machine-learning/cloud/ | None | Stable patterns: Jobs, runtime addons, Iceberg data connections |
| **Anywhere Cloud beta (CAI)** | https://docs-beta.cloudera.com/cloudera-ai/test/ | Browser SSO — see [beta-docs-access.md](references/beta-docs-access.md) | PBJ workbench, new API/job semantics |
| **Anywhere Cloud beta (Lakehouse)** | https://docs-beta.cloudera.com/cloudera-lakehouse-engine/test/ | Browser SSO | Lakehouse engine, HMS/Ozone from product perspective |
| **CAI REST API v2 (beta)** | https://docs-beta.cloudera.com/cloudera-ai/test/rest-api-reference/ | Browser SSO | `runtime_addon_identifiers`, Jobs PATCH, Applications |

Public Runtime / Iceberg (applies to catalog config):

- Spark + Iceberg catalog: https://docs.cloudera.com/runtime/7.3.2/spark-iceberg/topics/cde-iceberg-configure-catalog.html
- CDE Iceberg jobs: https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-iceberg-library-dependency.html

## How to search

1. Read [doc-catalog.md](references/doc-catalog.md) for curated links by topic.
2. For **Iceberg + HMS from CAI**, read [lakehouse-hms-from-cai.md](references/lakehouse-hms-from-cai.md) first — it maps official guidance to DataPulse job code.
3. For **Ranger Trino impersonation denials** (`SetUser` / `impersonate` in audit), read [ranger-trino-impersonation.md](references/ranger-trino-impersonation.md).
4. Public pages: fetch with WebFetch/curl (no auth).
5. Beta pages: **HTTP Basic Auth does not work** (returns Sign-in HTML). Open in browser after SSO login, or copy relevant sections into repo notes.

## Beta doc access (required for test docs)

Credentials and login steps: [references/beta-docs-access.md](references/beta-docs-access.md)

Prefer env vars when scripting:

- `CLOUDERA_DOCS_BETA_USER`
- `CLOUDERA_DOCS_BETA_PASSWORD`

## Topics most relevant to DataPulse lakehouse Job

### 1. CAI → Iceberg / Data Lake (official pattern)

Cloudera documents **Project Data Connections** + **Enable Spark in Session**, not raw `thrift://` env vars alone.

Key pages:

- https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-iceberg-connection.html
- https://docs.cloudera.com/machine-learning/cloud/mlde/topics/ml-mlde-spark-data-connection.html
- https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-pvc-iceberg-connection.html

Official manual Spark snippet highlights:

- `DATALAKE_DIRECTORY` — warehouse path (e.g. `s3a://...`)
- `spark.sql.catalog.*.type=hive` + `SparkCatalog` or `SparkSessionCatalog`
- `spark.executorEnv.HADOOP_CONF_DIR=/home/cdsw/hadoop_config_dir` — **critical; we did not set this**
- `spark.kerberos.access.hadoopFileSystems` or `spark.yarn.access.hadoopFileSystems`
- Iceberg jars from `/opt/spark/optional-11b/` (runtime-provided), not only Maven `spark.jars.packages`
- Same-environment data lake: Spark Data Lake connection auto-discovered from Environment → Data Lake → Cloud Storage

### 2. Runtime addons (CAI)

- https://docs.cloudera.com/machine-learning/cloud/managing-runtimes/topics/ml-using-runtimes-addons.html
- https://docs.cloudera.com/machine-learning/cloud/managing-runtimes/topics/ml-managing-ml-runtimes-add-ons.html
- https://docs.cloudera.com/machine-learning/cloud/runtimes/topics/ml-custom-runtime-addons.html

Addons mount Spark/Hadoop binaries from NFS; enable Spark **and** Hadoop CLI together in classic Session UI. Custom addons via `POST /api/v2/runtimeaddons/custom`.

### 3. CAI Jobs

- https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-job-c.html
- PDF: https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/ml-jobs-pipelines.pdf

Notes:

- PBJ runtime runs via ipykernel; avoid `sys.exit()` / `raise SystemExit` on success.
- Use `JOB_ARGUMENTS` env for script args; filter ipykernel `-f kernel-*.json` from argv.
- API v2: `runtime_identifier`, `runtime_addon_identifiers`, `environment` as JSON string on PATCH.

### 4. Iceberg catalog naming (Runtime doc)

For atomic CTAS/RTAS, use dedicated `SparkCatalog`:

```
spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type=hive
```

(DataPulse `jobs/kafka_to_iceberg.py` already follows this pattern.)

### 5. CDE contrast (not CAI, but HMS/Iceberg reference)

- Jobs need `cde.iceberg.enabled=true` on VC
- CDE adds default DataLake filesystem to Spark config automatically
- HMS/Iceberg libraries on classpath by default for Spark SQL

## When docs suggest our HMS failure root cause

Official CAI Iceberg snippets assume:

1. **Hadoop conf dir** populated under project (`hadoop_config_dir`) with hive/core-site
2. **Kerberos / filesystem access** lists for HDFS/Ozone/S3A
3. **Data Connection** or Environment-linked datalake — not manual hostname only
4. Spark enabled via **Session/Job runtime path** with platform jars

Our Job used env-based `thrift://hivemetastore...:9083` without `HADOOP_CONF_DIR`, Kerberos, or Project Data Connection — consistent with `Broken pipe` on Thrift `get_database`.

## Agent workflow

When user asks about CAI + Lakehouse:

1. Check [lakehouse-hms-from-cai.md](references/lakehouse-hms-from-cai.md) for gap analysis vs official snippet.
2. Prefer **Data Connection + hadoop_config_dir** over bare HMS URI if docs apply to this deployment.
3. For API/job issues, cross-check beta REST API v2 (browser) and public `ml-creating-a-job-c.html`.
4. Document new findings back into `references/` — keep SKILL.md as index only.
