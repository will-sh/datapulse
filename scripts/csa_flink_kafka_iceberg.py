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
HMS_URI = os.getenv(
    "HMS_URI",
    "thrift://hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work:9083",
)
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://hive-warehouse/external")
ICEBERG_DATABASE = os.getenv("ICEBERG_DATABASE", "datapulse")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "events")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "datapulse-events")
POLL_INTERVAL = int(os.getenv("CSA_POLL_INTERVAL", "10"))
WAIT_TIMEOUT = int(os.getenv("CSA_WAIT_TIMEOUT", "600"))


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
            chunks.append(path.read_text(encoding="utf-8").strip())
    return "\\n".join(chunks)


def _jaas_config(props: dict[str, str]) -> str:
    client_id = props.get("sasl.oauthbearer.client.id") or os.getenv("KAFKA_CLIENT_ID", "")
    client_secret = props.get("sasl.oauthbearer.client.secret") or os.getenv("KAFKA_CLIENT_SECRET", "")
    if props.get("sasl.jaas.config"):
        return props["sasl.jaas.config"]
    return (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
        f'clientId="{client_id}" clientSecret="{client_secret}";'
    )


def build_job_sql(*, kafka_only: bool = False) -> str:
    props = _load_kafka_props()
    bootstrap = props.get("bootstrap.servers", "")
    token_url = props.get("sasl.oauthbearer.token.endpoint.url", "")
    jaas = _jaas_config(props)
    certs = _pem_certificates()

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
  'properties.sasl.jaas.config' = '{jaas}',
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

    iceberg_ddl = f"""
CREATE TABLE IF NOT EXISTS iceberg_events_sink (
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
  'uri' = '{HMS_URI}',
  'warehouse' = '{ICEBERG_WAREHOUSE}',
  'catalog-database' = '{ICEBERG_DATABASE}',
  'catalog-table' = '{ICEBERG_TABLE}'
)
""".strip()

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


def _job_payload(sql: str, *, kafka_only: bool = False) -> dict[str, Any]:
    return {
        "sql": sql,
        "job_config": {
            "job_name": CSA_JOB_NAME if not kafka_only else f"{CSA_JOB_NAME}_probe",
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


def ensure_job(sql: str) -> int:
    status, jobs_raw = api_request("GET", f"/internal/job/projects/{CSA_PROJECT_ID}")
    if status != 200 or not isinstance(jobs_raw, list):
        raise RuntimeError(f"list jobs failed: HTTP {status} {jobs_raw}")
    for job in jobs_raw:
        if isinstance(job, dict) and job.get("name") == CSA_JOB_NAME:
            job_id = int(job["job_id"])
            api_request("PUT", f"/api/v2/projects/{CSA_PROJECT_ID}/jobs/{job_id}", _job_payload(sql))
            return job_id

    status, created = api_request(
        "POST",
        f"/api/v2/projects/{CSA_PROJECT_ID}/jobs",
        _job_payload(sql),
    )
    if status != 200 or not isinstance(created, dict):
        raise RuntimeError(f"create job failed: HTTP {status} {created}")
    return int(created["job_id"])


def execute_job(job_id: int, sql: str, *, kafka_only: bool = False) -> dict[str, Any]:
    status, response = api_request(
        "POST",
        f"/internal/job/execute?jobId={job_id}",
        _job_payload(sql, kafka_only=kafka_only),
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=["create", "execute", "run", "status", "probe-kafka"],
        help="create/update job SQL, execute, run end-to-end, print status, or kafka-only probe",
    )
    parser.add_argument("--job-id", type=int, default=int(CSA_JOB_ID) if CSA_JOB_ID else 0)
    args = parser.parse_args()

    login()
    kafka_only = args.action == "probe-kafka"
    sql = build_job_sql(kafka_only=kafka_only)
    job_id = args.job_id or ensure_job(sql)

    if args.action == "create":
        print(json.dumps({"job_id": job_id, "job_name": CSA_JOB_NAME}, indent=2))
        return 0

    if args.action in {"execute", "run", "probe-kafka"}:
        result = execute_job(job_id, sql, kafka_only=kafka_only)
        print(json.dumps({"execute": result, "job_id": job_id}, indent=2))
        if args.action == "execute":
            return 0

    if args.action in {"run", "probe-kafka", "status"}:
        final = wait_for_job(job_id)
        print(json.dumps({"job": final}, indent=2, default=str))
        kind = (final.get("status") or {}).get("kind")
        return 0 if kind == "RUNNING" else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
