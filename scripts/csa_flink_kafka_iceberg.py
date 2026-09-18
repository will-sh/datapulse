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
HMS_URI_DEFAULT = "thrift://hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work:9083"
HMS_URI = os.getenv("HMS_URI", HMS_URI_DEFAULT)
HMS_URI_CANDIDATES = [
    uri.strip()
    for uri in os.getenv(
        "HMS_URI_CANDIDATES",
        ";".join(
            [
                HMS_URI_DEFAULT,
                "thrift://metastore-service.lakehouse-bp-b05257.svc.cluster.local:9083",
                "thrift://metastore-service.lakehouse-bp-556b64.svc.cluster.local:9083",
                "thrift://hivemetastore.lakehouse-bp-b05257.svc.cluster.local:9083",
            ]
        ),
    ).split(";")
    if uri.strip()
]
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://hive-warehouse/external")
ICEBERG_DATABASE = os.getenv("ICEBERG_DATABASE", "datapulse")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "events")
OZONE_S3_ENDPOINT = os.getenv(
    "OZONE_S3_ENDPOINT",
    "lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work",
)
KAFKA_CALLBACK_HANDLER = os.getenv(
    "KAFKA_CALLBACK_HANDLER",
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "datapulse-events")
POLL_INTERVAL = int(os.getenv("CSA_POLL_INTERVAL", "10"))
WAIT_TIMEOUT = int(os.getenv("CSA_WAIT_TIMEOUT", "600"))
PROBE_JOB_NAME = os.getenv("CSA_PROBE_JOB_NAME", "datapulse_hms_probe")


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
    """OAuth login module line aligned with CAI config/kafka/external.properties."""
    raw = (props.get("sasl.jaas.config") or "").strip()
    if raw:
        # CAI template embeds oauth-ca.crt for token-endpoint TLS; Flink pods use inline PEM instead.
        return raw.replace(" ssl.truststore.location=oauth-ca.crt ssl.truststore.type=PEM", "").rstrip(";")
    client_id = props.get("sasl.oauthbearer.client.id") or os.getenv("KAFKA_CLIENT_ID", "")
    client_secret = props.get("sasl.oauthbearer.client.secret") or os.getenv("KAFKA_CLIENT_SECRET", "")
    return (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
        f'clientId="{client_id}" clientSecret="{client_secret}"'
    )


def _jaas_config(props: dict[str, str]) -> str:
    """Flink writes JAAS to java.security.auth.login.config; requires a KafkaClient stanza."""
    module = _jaas_module_line(props)
    return f"KafkaClient {{\n  {module};\n}};"


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _iceberg_sink_ddl(*, hms_uri: str, table_name: str = "iceberg_events_sink") -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
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
  'uri' = '{_sql_string(hms_uri)}',
  'warehouse' = '{_sql_string(ICEBERG_WAREHOUSE)}',
  'catalog-database' = '{_sql_string(ICEBERG_DATABASE)}',
  'catalog-table' = '{_sql_string(ICEBERG_TABLE)}',
  'iceberg.hadoop.fs.s3a.endpoint' = '{_sql_string(OZONE_S3_ENDPOINT)}',
  'iceberg.hadoop.fs.s3a.path.style.access' = 'true',
  'iceberg.hadoop.fs.s3a.connection.ssl.enabled' = 'true',
  'iceberg.hadoop.fs.s3a.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem'
)
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

{_iceberg_sink_ddl(hms_uri=hms_uri, table_name=sink)};

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
    certs = _sql_string(_pem_certificates())
    hms = hms_uri or HMS_URI
    oauth_client_id = props.get("sasl.oauthbearer.client.id") or os.getenv("KAFKA_CLIENT_ID", "")
    oauth_client_secret = props.get("sasl.oauthbearer.client.secret") or os.getenv("KAFKA_CLIENT_SECRET", "")

    kafka_ddl = f"""
CREATE TABLE IF NOT EXISTS kafka_datapulse_events (
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
  'properties.ssl.truststore.type' = 'PEM',
  'properties.ssl.truststore.certificates' = '{certs}',
  'properties.ssl.endpoint.identification.algorithm' = ''
)
""".strip()

    if kafka_only:
        return (
            kafka_ddl
            + ";\n\n"
            + """
CREATE TABLE IF NOT EXISTS bh_sink (
  event_id STRING,
  name STRING
) WITH ('connector' = 'blackhole');

INSERT INTO bh_sink
SELECT event_id, name FROM kafka_datapulse_events
""".strip()
        )

    iceberg_ddl = _iceberg_sink_ddl(hms_uri=hms)

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
) -> dict[str, Any]:
    resolved_name = job_name or (CSA_JOB_NAME if not kafka_only else f"{CSA_JOB_NAME}_probe")
    return {
        "sql": sql,
        "job_config": {
            "job_name": resolved_name,
            "runtime_config": {
                "execution_mode": "APPLICATION",
                "runtime_mode": "STREAMING",
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


def ensure_job(sql: str, *, job_name: str | None = None, kafka_only: bool = False) -> int:
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
                _job_payload(sql, job_name=name, kafka_only=kafka_only),
            )
            return job_id

    status, created = api_request(
        "POST",
        f"/api/v2/projects/{CSA_PROJECT_ID}/jobs",
        _job_payload(sql, job_name=name, kafka_only=kafka_only),
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
) -> dict[str, Any]:
    status, response = api_request(
        "POST",
        f"/internal/job/execute?jobId={job_id}",
        _job_payload(sql, job_name=job_name, kafka_only=kafka_only),
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
                    if kind == "RUNNING" and job.get("flink_job_id"):
                        return job
                    if kind in {"FAILED", "CANCELED"}:
                        return job
        time.sleep(POLL_INTERVAL)
    return last


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
            if kind == "RUNNING" and final.get("flink_job_id"):
                working_uri = uri
                results.append(entry)
                break
            entry["error"] = (final.get("status") or {}).get("error")
        except RuntimeError as exc:
            entry["error"] = str(exc)
        results.append(entry)
    return working_uri, results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=["create", "execute", "run", "status", "probe-kafka", "probe-hms"],
        help="create/update job SQL, execute, run end-to-end, print status, or probes",
    )
    parser.add_argument("--job-id", type=int, default=int(CSA_JOB_ID) if CSA_JOB_ID else 0)
    parser.add_argument("--hms-uri", default="", help="override HMS URI for run/create")
    args = parser.parse_args()

    login()
    kafka_only = args.action == "probe-kafka"
    hms_override = args.hms_uri.strip() or None

    if args.action == "probe-hms":
        sql = build_hms_probe_sql(hms_uri=hms_override or HMS_URI_CANDIDATES[0])
        job_id = args.job_id or ensure_job(sql, job_name=PROBE_JOB_NAME)
        working_uri, results = probe_hms_candidates(job_id)
        print(json.dumps({"working_hms_uri": working_uri, "results": results}, indent=2, default=str))
        return 0 if working_uri else 1

    sql = build_job_sql(kafka_only=kafka_only, hms_uri=hms_override)
    job_id = args.job_id or ensure_job(sql, kafka_only=kafka_only)

    if args.action == "create":
        print(json.dumps({"job_id": job_id, "job_name": CSA_JOB_NAME, "hms_uri": hms_override or HMS_URI}, indent=2))
        return 0

    if args.action == "status":
        final = wait_for_job(job_id)
        print(json.dumps({"job": final}, indent=2, default=str))
        kind = (final.get("status") or {}).get("kind")
        return 0 if kind == "RUNNING" else 1

    if args.action in {"execute", "run", "probe-kafka"}:
        result = execute_job(job_id, sql, kafka_only=kafka_only)
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
