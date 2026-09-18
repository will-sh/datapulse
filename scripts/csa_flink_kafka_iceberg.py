#!/usr/bin/env python3
"""Create and run a CSA (SSB) Flink SQL job: Kafka OAuth -> Iceberg."""

from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

CSA_HOST = os.getenv(
    "CSA_HOST",
    "csa-bp-csa-ssb-sse.cldr-csk-csa-1.a70735.test.cldr.work",
)
CSA_BASE = os.getenv("CSA_BASE", f"https://{CSA_HOST}").rstrip("/")
KNOX_BASE = os.getenv("KNOX_BASE", "https://knox.readygo.a70735.test.cldr.work").rstrip("/")
CONSOLE_USER = os.getenv("CONSOLE_USER", "admin")
CONSOLE_PASSWORD = os.getenv("CONSOLE_PASSWORD", "awc-admin-password")
CSA_PROJECT_ID = os.getenv("CSA_PROJECT_ID", "637cbade")
CSA_JOB_ID = os.getenv("CSA_JOB_ID", "")
CSA_JOB_NAME = os.getenv("CSA_JOB_NAME", "datapulse_kafka_iceberg")
COOKIEJAR = os.getenv("CSA_COOKIEJAR", "/tmp/csa-cookies.txt")
KAFKA_CONFIG_DIR = Path(os.getenv("KAFKA_CONFIG_DIR", "config/kafka"))
# Public Route53 HMS (often blocked off-cluster); prefer in-cluster URI from Lakehouse UI template:
# thrift://metastore-service.<warehouseId>.svc.cluster.local:9083
# warehouseId for this env matches ozone release hash lakehouse-bp-b05257 (see console_catalog.json).
# Public Route53 HMS is reachable from CSA Flink pods; in-cluster svc URIs are not (cross-cluster).
# Public HMS (CSA cluster). In-cluster URI for reference only (lakehouse-bp-ccbe9d, not reachable from CSA):
# thrift://lakehouse-bp-hms-hms-service.lakehouse-bp-ccbe9d.svc.cluster.local:9083
HMS_URI_DEFAULT = "thrift://hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work:9083"
HMS_URI = os.getenv("HMS_URI", HMS_URI_DEFAULT)
HMS_URI_CANDIDATES = [
    uri.strip()
    for uri in os.getenv("HMS_URI_CANDIDATES", HMS_URI_DEFAULT).split(";")
    if uri.strip()
]
# Populated by scripts/csa_patch_flink_lakehouse_conf.sh from lakehouse-bp-310fe0-cfg.
FLINK_LAKEHOUSE_CONF_DIR = os.getenv("FLINK_LAKEHOUSE_CONF_DIR", "/opt/flink/lakehouse-conf")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://hive-warehouse/external")
ICEBERG_DATABASE = os.getenv("ICEBERG_DATABASE", "datapulse")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "events")
OZONE_S3_ENDPOINT = os.getenv(
    "OZONE_S3_ENDPOINT",
    "https://lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work",
)
OZONE_S3_CA_PATH = os.getenv("OZONE_S3_CA_PATH", "/opt/flink/certs/ozone-s3-ca.crt")
FLINK_SHADED_KAFKA = "org.apache.flink.kafka.shaded.org.apache.kafka"
KAFKA_CALLBACK_HANDLER = os.getenv(
    "KAFKA_CALLBACK_HANDLER",
    f"{FLINK_SHADED_KAFKA}.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
)
KAFKA_LOGIN_MODULE = os.getenv(
    "KAFKA_LOGIN_MODULE",
    f"{FLINK_SHADED_KAFKA}.common.security.oauthbearer.OAuthBearerLoginModule",
)
# TLS truststores mounted on Flink pods via scripts/csa_patch_flink_kafka_oauth.sh (CAI config/kafka/*.crt).
KAFKA_OAUTH_CA_PATH = os.getenv("KAFKA_OAUTH_CA_PATH", "/opt/flink/certs/oauth-ca.crt")
KAFKA_BROKER_CA_PATH = os.getenv("KAFKA_BROKER_CA_PATH", "/opt/flink/certs/kafka-ca.crt")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "datapulse-events")
POLL_INTERVAL = int(os.getenv("CSA_POLL_INTERVAL", "10"))
WAIT_TIMEOUT = int(os.getenv("CSA_WAIT_TIMEOUT", "600"))
PROBE_JOB_NAME = os.getenv("CSA_PROBE_JOB_NAME", "datapulse_hms_probe")
PROBE_CONF_JOB_NAME = os.getenv("CSA_PROBE_CONF_JOB_NAME", "datapulse_hms_conf_probe")


def _resolve_ip(host: str) -> str:
    try:
        out = subprocess.check_output(["dig", "+short", host, "@8.8.8.8"], text=True)
        return out.strip().split("\n")[0]
    except Exception:
        return ""


def _curl_resolve_args() -> list[str]:
    ip = _resolve_ip(CSA_HOST)
    if not ip:
        return []
    return ["--resolve", f"{CSA_HOST}:443:{ip}"]


def login() -> None:
    auth = base64.b64encode(f"{CONSOLE_USER}:{CONSOLE_PASSWORD}".encode()).decode()
    original = f"{CSA_BASE}/"
    url = (
        f"{KNOX_BASE}/gateway/knox-cdpsso/api/v1/websso"
        f"?originalUrl={urllib.request.quote(original, safe='')}"
    )
    cmd = [
        "curl",
        "-sk",
        "-c",
        COOKIEJAR,
        "-X",
        "POST",
        "-H",
        f"Authorization: Basic {auth}",
        url,
    ]
    subprocess.check_call(cmd)


def _load_kafka_props() -> dict[str, str]:
    props: dict[str, str] = {}
    path = KAFKA_CONFIG_DIR / "external.properties"
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            props[key.strip()] = value.strip()
    return props


def _pem_certificates() -> str:
    chunks: list[str] = []
    for name in ("kafka-ca.crt", "oauth-ca.crt"):
        path = KAFKA_CONFIG_DIR / name
        if path.is_file():
            # PEM files contain real newlines; Flink SQL expects literal \n in the property value.
            chunks.append(path.read_text(encoding="utf-8").strip().replace("\n", "\\n"))
    return "\\n".join(chunks)


def _jaas_module_line(props: dict[str, str]) -> str:
    """OAuth login module for Flink's shaded Kafka connector (CAI creds, Flink class names)."""
    client_id = props.get("sasl.oauthbearer.client.id") or os.getenv("KAFKA_CLIENT_ID", "")
    client_secret = props.get("sasl.oauthbearer.client.secret") or os.getenv("KAFKA_CLIENT_SECRET", "")
    return (
        f'{KAFKA_LOGIN_MODULE} required clientId="{client_id}" clientSecret="{client_secret}" '
        f'ssl.truststore.location="{KAFKA_OAUTH_CA_PATH}" ssl.truststore.type=PEM'
    )


def _jaas_config(props: dict[str, str]) -> str:
    """Module-only JAAS for Kafka sasl.jaas.config (CAI external.properties format)."""
    return f"{_jaas_module_line(props)};"


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _lakehouse_conf_props() -> str:
    if os.getenv("FLINK_USE_LAKEHOUSE_CONF", "1").strip().lower() in {"0", "false", "no"}:
        return ""
    conf = _sql_string(FLINK_LAKEHOUSE_CONF_DIR)
    return (
        f"  'hive-conf-dir' = '{conf}',\n"
        f"  'hadoop-conf-dir' = '{conf}',\n"
    )


def _iceberg_s3_props() -> str:
    """Forward Ozone S3A settings (incl. TLS truststore) into Iceberg Hadoop conf."""
    ca = _sql_string(OZONE_S3_CA_PATH)
    endpoint = _sql_string(OZONE_S3_ENDPOINT)
    return (
        f"  'iceberg.hadoop.fs.s3a.endpoint' = '{endpoint}',\n"
        "  'iceberg.hadoop.fs.s3a.path.style.access' = 'true',\n"
        "  'iceberg.hadoop.fs.s3a.connection.ssl.enabled' = 'true',\n"
        "  'iceberg.hadoop.fs.s3a.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',\n"
        "  'iceberg.hadoop.fs.s3a.aws.credentials.provider' = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',\n"
        "  'iceberg.hadoop.fs.s3a.access.key' = 'ozone',\n"
        "  'iceberg.hadoop.fs.s3a.secret.key' = 'ozone',\n"
        f"  'iceberg.hadoop.fs.s3a.ssl.truststore.location' = '{ca}',\n"
        "  'iceberg.hadoop.fs.s3a.ssl.truststore.type' = 'PEM'\n"
    )


def _iceberg_hms_hadoop_props() -> str:
    """Forward HMS HTTP Thrift + client timeouts to Iceberg's Hadoop Configuration."""
    user = os.getenv("HADOOP_USER_NAME", "admin")
    user_sql = _sql_string(user)
    return (
        "  'iceberg.hadoop.hive.metastore.client.thrift.transport.mode' = 'http',\n"
        "  'iceberg.hadoop.hive.metastore.client.socket.timeout' = '1800',\n"
        f"  'iceberg.hadoop.metastore.client.plain.username' = '{user_sql}',\n"
        f"  'iceberg.hadoop.hive.metastore.client.plain.username' = '{user_sql}',\n"
    )


def _iceberg_sink_ddl(
    *,
    hms_uri: str,
    table_name: str = "iceberg_events_sink",
    catalog_database: str | None = None,
    catalog_table: str | None = None,
) -> str:
    lakehouse_conf = _lakehouse_conf_props()
    db = catalog_database or ICEBERG_DATABASE
    table = catalog_table or ICEBERG_TABLE
    return f"""
CREATE TABLE {table_name} (
  event_id STRING,
  project_id STRING,
  event_name STRING,
  properties_json STRING,
  event_timestamp BIGINT,
  source STRING,
  user_id STRING,
  anonymous_id STRING,
  session_id STRING,
  page_path STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP(3),
  ingested_at TIMESTAMP(3),
  raw_payload STRING
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'lakehouse_hive',
  'catalog-type' = 'hive',
{lakehouse_conf}  'uri' = '{_sql_string(hms_uri)}',
  'warehouse' = '{_sql_string(ICEBERG_WAREHOUSE)}',
  'catalog-database' = '{_sql_string(db)}',
  'catalog-table' = '{_sql_string(table)}',
{_iceberg_hms_hadoop_props()}{_iceberg_s3_props()})
""".strip()


def build_hms_conf_probe_sql(*, hms_uri: str) -> str:
    """Lightweight probe: requires hive-site.xml on pod at FLINK_LAKEHOUSE_CONF_DIR."""
    conf = _sql_string(FLINK_LAKEHOUSE_CONF_DIR)
    catalog = f"lh_conf_probe_{abs(hash(hms_uri)) % 100000}"
    return f"""
CREATE CATALOG {catalog} WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hive',
  'hive-conf-dir' = '{conf}',
  'hadoop-conf-dir' = '{conf}',
  'uri' = '{_sql_string(hms_uri)}',
  'warehouse' = '{_sql_string(ICEBERG_WAREHOUSE)}',
{_iceberg_hms_hadoop_props()}{_iceberg_s3_props()});
USE CATALOG {catalog};
SHOW DATABASES
""".strip()


def build_hms_probe_sql(*, hms_uri: str) -> str:
    suffix = abs(hash(hms_uri)) % 100000
    src = f"hms_probe_src_{suffix}"
    sink = f"hms_probe_sink_{suffix}"
    return f"""
DROP TABLE IF EXISTS {sink};
DROP TABLE IF EXISTS {src};

CREATE TABLE {src} (
  event_id STRING,
  event_name STRING
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'number-of-rows' = '5',
  'fields.event_id.length' = '10',
  'fields.event_name.length' = '8'
);

{_iceberg_sink_ddl(hms_uri=hms_uri, table_name=sink, catalog_database=os.getenv("ICEBERG_PROBE_DATABASE", "datapulse"), catalog_table=os.getenv("ICEBERG_PROBE_TABLE", "flink_hms_probe"))};

INSERT INTO {sink}
SELECT
  event_id,
  CAST(NULL AS STRING) AS project_id,
  event_name,
  CAST(NULL AS STRING) AS properties_json,
  CAST(NULL AS BIGINT) AS event_timestamp,
  CAST(NULL AS STRING) AS source,
  CAST(NULL AS STRING) AS user_id,
  CAST(NULL AS STRING) AS anonymous_id,
  CAST(NULL AS STRING) AS session_id,
  CAST(NULL AS STRING) AS page_path,
  CAST(0 AS INT) AS kafka_partition,
  CAST(0 AS BIGINT) AS kafka_offset,
  CAST(NULL AS TIMESTAMP(3)) AS kafka_timestamp,
  CURRENT_TIMESTAMP AS ingested_at,
  CAST(NULL AS STRING) AS raw_payload
FROM {src}
""".strip()


def build_job_sql(*, kafka_only: bool = False, hms_uri: str | None = None) -> str:
    props = _load_kafka_props()
    bootstrap = props.get("bootstrap.servers", "")
    token_url = props.get("sasl.oauthbearer.token.endpoint.url", "")
    jaas = _sql_string(_jaas_config(props))
    hms = hms_uri or HMS_URI
    broker_truststore = (
        f"'properties.ssl.truststore.location' = '{_sql_string(KAFKA_BROKER_CA_PATH)}',\n"
        "  'properties.ssl.truststore.type' = 'PEM'"
    )
    if not os.getenv("KAFKA_USE_MOUNTED_CERTS", "1").strip().lower() in {"0", "false", "no"}:
        truststore_props = broker_truststore
    else:
        certs = _sql_string(_pem_certificates())
        truststore_props = (
            "  'properties.ssl.truststore.type' = 'PEM',\n"
            f"  'properties.ssl.truststore.certificates' = '{certs}'"
        )
    oauth_client_id = props.get("sasl.oauthbearer.client.id") or os.getenv("KAFKA_CLIENT_ID", "")
    oauth_client_secret = props.get("sasl.oauthbearer.client.secret") or os.getenv("KAFKA_CLIENT_SECRET", "")

    kafka_ddl = f"""
DROP TABLE IF EXISTS bh_sink;
DROP TABLE IF EXISTS kafka_datapulse_events;

CREATE TABLE kafka_datapulse_events (
  event_id STRING,
  project_id STRING,
  name STRING,
  properties STRING,
  `timestamp` BIGINT,
  source STRING,
  user_id STRING,
  anonymous_id STRING,
  session_id STRING,
  page_path STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_ts TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = '{KAFKA_TOPIC}',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'json.ignore-parse-errors' = 'true',
  'properties.bootstrap.servers' = '{bootstrap}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'OAUTHBEARER',
  'properties.sasl.login.callback.handler.class' = '{_sql_string(KAFKA_CALLBACK_HANDLER)}',
  'properties.sasl.jaas.config' = '{jaas}',
  'properties.sasl.oauthbearer.client.id' = '{_sql_string(oauth_client_id)}',
  'properties.sasl.oauthbearer.client.secret' = '{_sql_string(oauth_client_secret)}',
  'properties.sasl.oauthbearer.client.credentials.client.id' = '{_sql_string(oauth_client_id)}',
  'properties.sasl.oauthbearer.client.credentials.client.secret' = '{_sql_string(oauth_client_secret)}',
  'properties.sasl.oauthbearer.token.endpoint.url' = '{token_url}',
  {truststore_props},
  'properties.ssl.endpoint.identification.algorithm' = ''
)
""".strip()

    if kafka_only:
        return (
            kafka_ddl
            + ";\n\n"
            + """
CREATE TABLE bh_sink (
  event_id STRING,
  name STRING
) WITH ('connector' = 'blackhole');

INSERT INTO bh_sink
SELECT event_id, name FROM kafka_datapulse_events
""".strip()
        )

    iceberg_ddl = "DROP TABLE IF EXISTS iceberg_events_sink;\n\n" + _iceberg_sink_ddl(hms_uri=hms)

    insert_sql = """
INSERT INTO iceberg_events_sink
SELECT
  event_id,
  project_id,
  name AS event_name,
  properties AS properties_json,
  `timestamp` AS event_timestamp,
  source,
  user_id,
  anonymous_id,
  session_id,
  page_path,
  kafka_partition,
  kafka_offset,
  kafka_ts AS kafka_timestamp,
  CURRENT_TIMESTAMP AS ingested_at,
  CAST(NULL AS STRING) AS raw_payload
FROM kafka_datapulse_events
""".strip()

    return ";\n\n".join([kafka_ddl, iceberg_ddl, insert_sql]) + ";"


def _job_payload(
    sql: str,
    *,
    job_name: str | None = None,
    kafka_only: bool = False,
    runtime_mode: str = "STREAMING",
) -> dict[str, Any]:
    resolved_name = job_name or (CSA_JOB_NAME if not kafka_only else f"{CSA_JOB_NAME}_probe")
    return {
        "sql": sql,
        "job_config": {
            "job_name": resolved_name,
            "runtime_config": {
                "execution_mode": "APPLICATION",
                "runtime_mode": runtime_mode,
                "parallelism": 1,
                "start_with_savepoint": False,
                "sample_interval": 1000,
                "sample_count": 100,
                "window_size": 100,
            },
            "checkpoint_config": {"enable_checkpointing": False},
            "kubernetes_config": {
                "kubernetes_deployment_mode": "NATIVE",
                "restart_failed_job": False,
                "job_manager_replicas": 1,
            },
            "autoscaler_config": {"enabled": False},
            "mv_config": None,
        },
        "mv_endpoints": [],
    }


def api_request(method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, object]:
    cmd = [
        "curl",
        "-sk",
        "-b",
        COOKIEJAR,
        *_curl_resolve_args(),
        "-X",
        method,
        f"{CSA_BASE}{path}",
        "-w",
        "\n__HTTP__:%{http_code}\n",
    ]
    if payload is not None:
        cmd.extend(["-H", "Content-Type: application/json", "-d", json.dumps(payload)])
    raw = subprocess.check_output(cmd, text=True)
    if "__HTTP__:" not in raw:
        return 0, raw
    body, _, meta = raw.rpartition("__HTTP__:")
    status = int(meta.strip().removeprefix(":"))
    body = body.strip()
    try:
        parsed: object = json.loads(body) if body else {}
    except json.JSONDecodeError:
        parsed = {"raw": body}
    return status, parsed


def _resolved_job_name(*, kafka_only: bool) -> str:
    return f"{CSA_JOB_NAME}_probe" if kafka_only else CSA_JOB_NAME


def update_job(
    job_id: int,
    sql: str,
    *,
    job_name: str | None = None,
    kafka_only: bool = False,
    runtime_mode: str = "STREAMING",
) -> None:
    api_request(
        "PUT",
        f"/api/v2/projects/{CSA_PROJECT_ID}/jobs/{job_id}",
        _job_payload(sql, job_name=job_name, kafka_only=kafka_only, runtime_mode=runtime_mode),
    )


def ensure_job(
    sql: str,
    *,
    job_name: str | None = None,
    kafka_only: bool = False,
    runtime_mode: str = "STREAMING",
) -> int:
    name = job_name or CSA_JOB_NAME
    status, jobs_raw = api_request("GET", f"/internal/job/projects/{CSA_PROJECT_ID}")
    if status != 200 or not isinstance(jobs_raw, list):
        raise RuntimeError(f"list jobs failed: HTTP {status} {jobs_raw}")
    for job in jobs_raw:
        if isinstance(job, dict) and job.get("name") == name:
            job_id = int(job["job_id"])
            api_request(
                "PUT",
                f"/api/v2/projects/{CSA_PROJECT_ID}/jobs/{job_id}",
                _job_payload(sql, job_name=name, kafka_only=kafka_only, runtime_mode=runtime_mode),
            )
            return job_id

    status, created = api_request(
        "POST",
        f"/api/v2/projects/{CSA_PROJECT_ID}/jobs",
        _job_payload(sql, job_name=name, kafka_only=kafka_only, runtime_mode=runtime_mode),
    )
    if status != 200 or not isinstance(created, dict):
        raise RuntimeError(f"create job failed: HTTP {status} {created}")
    return int(created["job_id"])


def execute_job(
    job_id: int,
    sql: str,
    *,
    job_name: str | None = None,
    kafka_only: bool = False,
    runtime_mode: str = "STREAMING",
) -> dict[str, Any]:
    status, response = api_request(
        "POST",
        f"/internal/job/execute?jobId={job_id}",
        _job_payload(sql, job_name=job_name, kafka_only=kafka_only, runtime_mode=runtime_mode),
    )
    if status >= 400:
        raise RuntimeError(f"execute failed: HTTP {status} {response}")
    if not isinstance(response, dict):
        return {"response": response}
    return response


def wait_for_job(job_id: int) -> dict[str, Any]:
    deadline = time.time() + WAIT_TIMEOUT
    last: dict[str, Any] = {}
    while time.time() < deadline:
        status, jobs_raw = api_request("GET", f"/internal/job/projects/{CSA_PROJECT_ID}")
        if status == 200 and isinstance(jobs_raw, list):
            for job in jobs_raw:
                if isinstance(job, dict) and int(job.get("job_id", -1)) == job_id:
                    last = job
                    kind = (job.get("status") or {}).get("kind")
                    terminal = (job.get("status") or {}).get("terminal_state")
                    if kind == "RUNNING" and job.get("flink_job_id"):
                        return job
                    if kind == "FINISHED" and terminal:
                        return job
                    if kind in {"FAILED", "CANCELED"}:
                        return job
        time.sleep(POLL_INTERVAL)
    return last


def _classify_hms_error(message: str) -> str:
    text = message or ""
    if "hive-site.xml" in text and FLINK_LAKEHOUSE_CONF_DIR in text:
        return "lakehouse_conf_mount_missing"
    if "Could not find Hadoop configuration" in text or "Unexpected EOF in prolog" in text:
        return "lakehouse_conf_invalid"
    if "Failed to list namespace" in text or "Failed to list all namespace" in text:
        return "hms_metadata_denied"
    if "Failed to connect to Hive Metastore" in text:
        return "hms_unreachable"
    return "unknown"


def probe_hms_conf(job_id: int, *, hms_uri: str) -> dict[str, Any]:
    sql = build_hms_conf_probe_sql(hms_uri=hms_uri)
    entry: dict[str, Any] = {"hms_uri": hms_uri, "conf_dir": FLINK_LAKEHOUSE_CONF_DIR}
    try:
        api_request(
            "PUT",
            f"/api/v2/projects/{CSA_PROJECT_ID}/jobs/{job_id}",
            _job_payload(sql, job_name=PROBE_CONF_JOB_NAME, runtime_mode="BATCH"),
        )
        execute_job(job_id, sql, job_name=PROBE_CONF_JOB_NAME, runtime_mode="BATCH")
        final = wait_for_job(job_id)
        entry["status"] = final.get("status")
        entry["flink_job_id"] = final.get("flink_job_id")
        error = (final.get("status") or {}).get("error") or ""
        entry["error_class"] = _classify_hms_error(str(error))
        entry["error"] = error or None
        kind = (final.get("status") or {}).get("kind")
        terminal = (final.get("status") or {}).get("terminal_state")
        # BATCH SHOW DATABASES validates on SSB and finishes without a Flink job id.
        entry["ok"] = (
            not error
            and (
                (kind == "RUNNING" and bool(final.get("flink_job_id")))
                or (kind == "FINISHED" and terminal)
            )
        )
        if entry["ok"] and kind == "FINISHED":
            entry["conf_mount_ok"] = True
        if entry["error_class"] == "hms_metadata_denied":
            entry["conf_mount_ok"] = True
            entry["ok"] = True
            entry["note"] = "hive-conf-dir readable; HMS list denied (Ranger/UGI — run probe-hms or fix auth)"
    except RuntimeError as exc:
        message = str(exc)
        entry["error_class"] = _classify_hms_error(message)
        entry["error"] = message
        entry["ok"] = entry["error_class"] == "hms_metadata_denied"
        if entry["ok"]:
            entry["conf_mount_ok"] = True
            entry["note"] = "hive-conf-dir readable; HMS list denied (Ranger/UGI — run probe-hms or fix auth)"
    return entry


def probe_hms_candidates(job_id: int) -> tuple[str | None, list[dict[str, Any]]]:
    results: list[dict[str, Any]] = []
    working_uri: str | None = None
    for uri in HMS_URI_CANDIDATES:
        sql = build_hms_probe_sql(hms_uri=uri)
        entry: dict[str, Any] = {"hms_uri": uri}
        try:
            api_request(
                "PUT",
                f"/api/v2/projects/{CSA_PROJECT_ID}/jobs/{job_id}",
                _job_payload(sql, job_name=PROBE_JOB_NAME),
            )
            execute_job(job_id, sql, job_name=PROBE_JOB_NAME)
            final = wait_for_job(job_id)
            entry["status"] = final.get("status")
            entry["flink_job_id"] = final.get("flink_job_id")
            kind = (final.get("status") or {}).get("kind")
            terminal = (final.get("status") or {}).get("terminal_state")
            error = (final.get("status") or {}).get("error") or ""
            entry["error"] = error or None
            entry["error_class"] = _classify_hms_error(str(error))
            # Datagen probe (5 rows) may FINISH before we observe RUNNING.
            sink_ok = (
                not error
                and final.get("flink_job_id")
                and (
                    (kind == "RUNNING")
                    or (kind == "FINISHED" and terminal)
                )
            )
            if sink_ok:
                entry["ok"] = True
                working_uri = uri
                results.append(entry)
                break
        except RuntimeError as exc:
            message = str(exc)
            entry["error"] = message
            entry["error_class"] = _classify_hms_error(message)
        results.append(entry)
    return working_uri, results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=["create", "execute", "run", "status", "probe-kafka", "probe-hms-conf", "probe-hms"],
        help="create/update job SQL, execute, run end-to-end, print status, or probes",
    )
    parser.add_argument("--job-id", type=int, default=int(CSA_JOB_ID) if CSA_JOB_ID else 0)
    parser.add_argument("--hms-uri", default="", help="override HMS URI for run/create")
    args = parser.parse_args()

    login()
    kafka_only = args.action == "probe-kafka"
    hms_override = args.hms_uri.strip() or None

    if args.action == "probe-hms-conf":
        uri = hms_override or HMS_URI_CANDIDATES[0]
        sql = build_hms_conf_probe_sql(hms_uri=uri)
        job_id = args.job_id or ensure_job(
            sql,
            job_name=PROBE_CONF_JOB_NAME,
            runtime_mode="BATCH",
        )
        result = probe_hms_conf(job_id, hms_uri=uri)
        print(json.dumps(result, indent=2, default=str))
        if result.get("error_class") == "lakehouse_conf_mount_missing":
            print(
                "\nRun on readygo bastion: bash scripts/csa_patch_flink_lakehouse_conf.sh",
                file=sys.stderr,
            )
        return 0 if result.get("ok") else 1

    if args.action == "probe-hms":
        uri = hms_override or HMS_URI_CANDIDATES[0]
        conf_job_id = args.job_id or ensure_job(
            build_hms_conf_probe_sql(hms_uri=uri),
            job_name=PROBE_CONF_JOB_NAME,
            runtime_mode="BATCH",
        )
        conf_result = probe_hms_conf(conf_job_id, hms_uri=uri)
        if not conf_result.get("ok"):
            print(
                json.dumps(
                    {
                        "stage": "conf_probe",
                        "conf_result": conf_result,
                        "hint": "Deploy lakehouse conf mount before sink probe",
                    },
                    indent=2,
                    default=str,
                )
            )
            if conf_result.get("error_class") == "lakehouse_conf_mount_missing":
                print(
                    "Run on readygo bastion: bash scripts/csa_patch_flink_lakehouse_conf.sh",
                    file=sys.stderr,
                )
            return 1
        sql = build_hms_probe_sql(hms_uri=uri)
        job_id = args.job_id or ensure_job(sql, job_name=PROBE_JOB_NAME)
        working_uri, results = probe_hms_candidates(job_id)
        print(
            json.dumps(
                {"stage": "sink_probe", "conf_result": conf_result, "working_hms_uri": working_uri, "results": results},
                indent=2,
                default=str,
            )
        )
        return 0 if working_uri else 1

    sql = build_job_sql(kafka_only=kafka_only, hms_uri=hms_override)
    job_name = _resolved_job_name(kafka_only=kafka_only)
    if args.job_id:
        job_id = args.job_id
        update_job(job_id, sql, job_name=job_name, kafka_only=kafka_only)
    else:
        job_id = ensure_job(sql, job_name=job_name, kafka_only=kafka_only)

    if args.action == "create":
        print(json.dumps({"job_id": job_id, "job_name": CSA_JOB_NAME, "hms_uri": hms_override or HMS_URI}, indent=2))
        return 0

    if args.action == "status":
        final = wait_for_job(job_id)
        print(json.dumps({"job": final}, indent=2, default=str))
        kind = (final.get("status") or {}).get("kind")
        return 0 if kind == "RUNNING" else 1

    if args.action in {"execute", "run", "probe-kafka"}:
        result = execute_job(job_id, sql, job_name=job_name, kafka_only=kafka_only)
        print(json.dumps({"execute": result, "job_id": job_id, "hms_uri": hms_override or HMS_URI}, indent=2))
        if args.action == "execute":
            return 0

    if args.action in {"run", "probe-kafka"}:
        final = wait_for_job(job_id)
        print(json.dumps({"job": final}, indent=2, default=str))
        kind = (final.get("status") or {}).get("kind")
        return 0 if kind == "RUNNING" else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
