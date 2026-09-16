# Lakehouse HMS + Iceberg from CAI — official docs vs DataPulse

This note summarizes what Cloudera documents for **CAI → Iceberg/HMS**, and how it compares to the DataPulse isolated Job (`jobs/kafka_to_iceberg.py`).

## Official connection model (CAI public docs)

Cloudera expects CAI workloads to reach a datalake through **platform integration**, not only a hard-coded Thrift URI.

### Recommended path: Data Connection

1. Environment UI → **Data Lake → Cloud Storage** → copy **Hive Metastore External Warehouse Directory**
2. Project Settings → **Data Connections** → New → type **Spark Data Lake**
3. Start Session with **Enable Spark** (Spark 3)
4. Use UI **Connection Code Snippet** or equivalent generated config

Same-environment Spark datalakes may be **auto-discovered** (`ml-mlde-spark-data-connection.html`).

### Manual Spark snippet (from `ml-iceberg-connection.html`)

Required concepts:

| Config / env | Purpose |
|--------------|---------|
| `DATALAKE_DIRECTORY` | Warehouse root (`s3a://...`) |
| `spark.sql.extensions` | Iceberg session extensions |
| `spark.sql.catalog.spark_catalog.type=hive` | Hive-type Iceberg catalog |
| `spark.sql.catalog.spark_catalog` | `SparkCatalog` or `SparkSessionCatalog` |
| `spark.executorEnv.HADOOP_CONF_DIR` | **`/home/cdsw/hadoop_config_dir`** — hive-site, core-site |
| `spark.kerberos.access.hadoopFileSystems` | Authorized filesystems |
| `spark.hadoop.iceberg.engine.hive.enabled=true` | Hive engine integration |
| `spark.jars` | Platform paths e.g. `/opt/spark/optional-11b/iceberg-spark-runtime.jar` |

Validation commands in docs:

```python
spark.sql("show databases").show()
spark.sql("describe formatted <db>.<table>").show()
```

### Iceberg catalog naming (Runtime 7.3.2 doc)

- Default `spark_catalog` → `SparkSessionCatalog` (mixed Iceberg + non-Iceberg)
- Dedicated catalog for atomic CTAS → `SparkCatalog`:

```
spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type=hive
```

DataPulse uses `iceberg_catalog` + `SparkCatalog` — **aligned with docs**.

### HMS role (Lakehouse / Iceberg overview)

- Iceberg uses **HiveCatalog**; HMS stores table metadata location
- Partition stats live in Iceberg metadata files (reduces HMS load)
- `CREATE DATABASE` / `CREATE TABLE` still invoke HMS Thrift APIs

## What DataPulse Job did

| Aspect | DataPulse | Official CAI pattern |
|--------|-----------|----------------------|
| HMS URI | Env `HIVE_METASTORE_HOST/PORT` → `thrift://...:9083` | From datalake binding + `hadoop_config_dir` |
| Warehouse | Env `s3a://s3v/warehouse/datapulse` | `DATALAKE_DIRECTORY` from Environment |
| Hadoop conf | **Not set** (`hive_conf_exists: false`) | **`HADOOP_CONF_DIR=/home/cdsw/hadoop_config_dir`** |
| Kerberos / FS ACL | Partial `spark.yarn.access.hadoopFileSystems` | `spark.kerberos.access.hadoopFileSystems` |
| Iceberg jars | Maven `spark.jars.packages` | `/opt/spark/optional-11b/*.jar` when using platform Spark |
| Spark runtime | `local[2]` + pip PySpark + `hadoop-cli` addon | Enable Spark via Session/addon; Data Connection snippet |
| Entry | CAI Job subprocess | Session or Job with generated snippet |

## Errors observed in DataPulse (after Spark path fixed)

### Bootstrap HMS failure

```
pyspark...AnalysisException
Caused by: org.apache.thrift.transport.TTransportException: Broken pipe
  at ThriftHiveMetastore$Client.send_get_database
  at HiveMetaStoreClient.getDatabase
  at org.apache.iceberg.hive.HiveCatalog.loadNamespaceMetadata
```

Triggered by:

```sql
CREATE DATABASE IF NOT EXISTS iceberg_catalog.datapulse
```

### Network checks

| From | TCP :9083 | Thrift RPC |
|------|-----------|------------|
| Cloud Agent VM | timeout | — |
| CAI Application pod (consumer) | OK | not fully tested |
| CAI Job pod (bootstrap) | likely OK | **Broken pipe** on `get_database` |

Interpretation vs docs:

- Docs assume **authenticated, configured** HMS client via Hadoop conf + datalake binding
- Bare `thrift://hostname:9083` without `HADOOP_CONF_DIR`/Kerberos may connect then fail on RPC

## Doc-backed next steps for DataPulse

1. **Add Project Data Connection** (Spark Data Lake) and copy UI snippet settings into Job env — or populate `/home/cdsw/hadoop_config_dir` from Environment datalake config
2. Set `spark.executorEnv.HADOOP_CONF_DIR` and driver equivalent per official snippet
3. Use platform Iceberg jars if Spark addon provides `/opt/spark/optional-*` paths
4. Confirm with Lakehouse team: HMS URI + auth for **CAI Job pods** (may differ from Application pods)
5. Read **beta** Lakehouse + CAI docs (SSO) for Anywhere-specific datalake binding — public cloud docs may not cover ReadyGo/AWC topology

## Trino via Console OAuth (same token as Kafka)

DataPulse can reach Trino with the **same Console Access Key** used for Kafka OAuth:

| Step | Detail |
|------|--------|
| Token URL | `KAFKA_TOKEN_URL` → `POST grant_type=client_credentials` + `KAFKA_CLIENT_ID` / `KAFKA_CLIENT_SECRET` |
| Trino auth | `Authorization: Bearer <access_token>` (Knox JWT gateway in front of Trino) |
| `/v1/info` | **Works** with Console OAuth token |
| SQL (`/v1/statement`) | May fail with `Principal admin cannot become user admin` — Ranger impersonation policy for access-key principals |

Job modes:

```bash
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode trino-probe
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode trino-bootstrap
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode trino-verify
```

Implementation: `app/console_oauth.py`, `app/trino_lakehouse.py`, `jobs/kafka_to_iceberg.py` (`trino-*` modes).

Trino Admin UI datasource tests use browser SSO — not identical to headless Access Key tokens, even when both are "OAuth".

## Related DataPulse files

- `app/lakehouse_settings.py` — URI/warehouse resolution
- `jobs/kafka_to_iceberg.py` — Iceberg catalog builder + DDL
- `scripts/cai_submit_lakehouse_job.py` — Job env defaults
- `scripts/cai_lakehouse_discover_only.py` — Job entry, subprocess isolation
