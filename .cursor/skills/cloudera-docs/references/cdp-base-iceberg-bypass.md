# CDP Base Spark + Iceberg (bypass Lakehouse)

Use an on-prem **CDP Base 7.3.2** cluster with Spark 3.5 + Iceberg **Hadoop catalog** when Lakehouse HMS is not ready.

## Constraints (your cluster)

| Item | Value |
|------|-------|
| Hive | **Not installed** → `catalog.type=hadoop`, not `hive` |
| Warehouse | `hdfs:///user/systest/iceberg-warehouse` |
| Job user | `systest` (not root) |
| Auth | Simple (no Kerberos) |
| Iceberg jar | parcel `iceberg-spark-runtime-3.5_2.12-1.10.1.*.jar` |

## DataPulse files

| File | Purpose |
|------|---------|
| `app/cdp_base_settings.py` | env defaults for CDP Base |
| `jobs/cdp_base_iceberg_smoke.py` | `discover` / `smoke` modes |
| `conf/cdp-base/spark-iceberg.conf` | 4 core Spark Iceberg configs |
| `scripts/cdp_base_submit_smoke.sh` | run **on cluster node** |
| `scripts/cdp_base_remote_submit.sh` | rsync + SSH from dev laptop |

## On cluster (recommended)

```bash
# SSH
ssh root@ccycloud-1.wxiao-732.root.comops.site   # password: cloudera

# clone/copy repo to /tmp/datapulse-cdp-base or sync from laptop
cd /path/to/datapulse
chmod +x scripts/cdp_base_submit_smoke.sh
./scripts/cdp_base_submit_smoke.sh discover
./scripts/cdp_base_submit_smoke.sh smoke
```

Expected smoke output: `row_count=2`, two rows login/logout.

Monitor: http://ccycloud-1.wxiao-732.root.comops.site:8088

## From dev machine (VPN required)

```bash
export CDP_BASE_HOST=ccycloud-1.wxiao-732.root.comops.site
chmod +x scripts/cdp_base_remote_submit.sh
./scripts/cdp_base_remote_submit.sh smoke
```

Cloud Agent VMs **cannot** reach `*.wxiao-732.root.comops.site` without VPN — run submit from your lab network.

## vs Lakehouse path

| | CDP Base (this) | Lakehouse Integrated |
|--|-----------------|----------------------|
| Catalog | Hadoop (`local.*`) | HiveCatalog + HMS |
| Storage | HDFS | Ozone/S3A |
| CAI Job today | SSH/YARN submit | CAI Job + Data Connection |
| Trino SQL | not included | `lakehouse-bp-trino` |
| Kafka ingest | future: YARN job on Base or Connect | Spark Job → Iceberg |

## Env overrides

```bash
export CDP_BASE_HOST=ccycloud-1.wxiao-732.root.comops.site
export CDP_BASE_HDFS_USER=systest
export CDP_BASE_ICEBERG_WAREHOUSE=hdfs:///user/systest/iceberg-warehouse
export CDP_BASE_ICEBERG_CATALOG=local
export CDP_BASE_ICEBERG_DATABASE=datapulse
export CDP_BASE_ICEBERG_TABLE=events
```
