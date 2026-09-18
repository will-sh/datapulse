#!/usr/bin/env python3
"""Create or run the isolated datapulse Lakehouse ingest CAI Job.

This script only touches Jobs — it does not recreate or restart Applications.
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from typing import Any

CAI_BASE = os.getenv("CAI_BASE", "https://ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work").rstrip("/")
CAI_PID = os.getenv("CAI_PID", "k87h-zej9-dugs-473y")
CAI_PROJECT_NUMERIC_ID = os.getenv("CAI_PROJECT_NUMERIC_ID", "3")
CAI_KEY = os.getenv("CAI_KEY", os.getenv("CDSW_APIV2_KEY", ""))

DEFAULT_RUNTIME = (
    "container.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-workbench-python3.11-hardened:2026.04.2-b16"
)
DEFAULT_ADDONS = ["hadoop-cli-7.3.1.709-1"]
# sparkconnect354 combined with hadoop-cli breaks CAI Job engine startup; local Spark uses hadoop-cli only.
SPARK_ADDONS = ["hadoop-cli-7.3.1.709-1"]
SPARK_DATA_CONNECTION_ADDONS = ["sparkconnect354-731-26"]
SPARK_MODES = {"bootstrap", "batch", "stream", "verify", "spark-probe", "spark-layout", "spark-pi"}
TRINO_MODES = {"trino-probe", "trino-bootstrap", "trino-verify", "trino-ingest"}
JOB_NAME = "datapulse-lakehouse-kafka-ingest"
JOB_ID = os.getenv("CAI_LAKEHOUSE_JOB_ID", "4h98-444z-iu1n-5x3m")
JOB_SCRIPT = "scripts/cai_lakehouse_discover_only.py"

DEFAULT_ENV = {
    "KAFKA_CONFIG_DIR": ".",
    "KAFKA_TOPIC": "datapulse-events",
    "KAFKA_BOOTSTRAP_SERVERS": "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
    "KAFKA_TOKEN_URL": "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
    "KAFKA_STARTING_OFFSETS": "earliest",
    "HIVE_METASTORE_HOST": "hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work",
    "HIVE_METASTORE_PORT": "9083",
    "ICEBERG_CATALOG": "iceberg_catalog",
    "ICEBERG_DATABASE": "datapulse",
    "ICEBERG_TABLE": "events",
    "OZONE_HOST": "lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work",
    "OZONE_VOLUME": "s3v",
    "OZONE_BUCKET": "warehouse",
    "OZONE_WAREHOUSE_PREFIX": "datapulse",
    "ICEBERG_WAREHOUSE": "s3a://hive-warehouse/external",
    "CAI_SPARK_DATA_CONNECTION": "lakehouse-integrated",
    "CAI_SPARK_DATA_CONNECTION_INFO": json.dumps(
        {
            "id": 1,
            "workspaceConnectionId": "1",
            "projectId": 3,
            "name": "lakehouse-integrated",
            "type": "SPARK",
            "connectionInfo": {"dataLakeExternalDir": "s3a://hive-warehouse/external"},
            "availability": True,
        }
    ),
    "CAI_PROJECT_NUMERIC_ID": CAI_PROJECT_NUMERIC_ID,
    "CAI_BASE": CAI_BASE,
    "HADOOP_CONF_DIR": "/home/cdsw/hadoop_config_dir",
    "PYTHONHTTPSVERIFY": "0",
    "LAKEHOUSE_CHECKPOINT_DIR": "config/lakehouse/.checkpoints/kafka-to-iceberg",
    "SPARK_SESSION_TIMEOUT_SEC": "300",
    "TRINO_HOST": "lakehouse-bp-556b64.cldr-csk-lakehouse.a70735.test.cldr.work",
    "TRINO_PORT": "443",
    "TRINO_CATALOG": "iceberg",
    "TRINO_VERIFY_SSL": "false",
    "TRINO_INGEST_BATCH_SIZE": "100",
    "TRINO_INGEST_MAX_BATCHES": "30",
    "TRINO_INGEST_POLL_INTERVAL_SEC": "2",
    "TRINO_INGEST_TIMEOUT_MS": "10000",
    "TRINO_INGEST_CHECKPOINT": "config/lakehouse/.checkpoints/trino-ingest-offset.json",
    "TRINO_INGEST_ENSURE_TABLE": "true",
    "TRINO_INGEST_STOP_ON_EMPTY": "true",
    # Use checkpoint on CAI project when present; earliest only for empty checkpoint.
    "TRINO_INGEST_START_MODE": "checkpoint",
}

POLL_INTERVAL = int(os.getenv("CAI_JOB_POLL_INTERVAL", "10"))
WAIT_TIMEOUT = int(os.getenv("WAIT_TIMEOUT", "1800"))


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def api_request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 60,
) -> tuple[int, object]:
    url = f"{CAI_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {CAI_KEY}",
        "Accept": "application/json",
    }
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=ssl_context()) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed: object = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"raw": body[:1000]}
        return exc.code, parsed


def list_jobs() -> list[dict[str, Any]]:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/jobs?page_size=100")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"list jobs failed: status={status} payload={payload}")
    jobs = payload.get("jobs", [])
    return jobs if isinstance(jobs, list) else []


def find_job(name: str) -> dict[str, Any] | None:
    preferred_id = os.getenv("CAI_LAKEHOUSE_JOB_ID", JOB_ID)
    preferred: dict[str, Any] | None = None
    fallback: dict[str, Any] | None = None
    for job in list_jobs():
        if job.get("name") != name:
            continue
        if str(job.get("id")) == preferred_id:
            preferred = job
        elif fallback is None:
            fallback = job
    return preferred or fallback


def parse_environment(raw: Any) -> dict[str, str]:
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, str) and raw.strip():
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    return {}


def fetch_kafka_env_from_consumer() -> dict[str, str]:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/applications")
    if status != 200 or not isinstance(payload, dict):
        return {}
    for app in payload.get("applications", []):
        if app.get("name") != "datapulse-spark-consumer":
            continue
        env = parse_environment(app.get("environment"))
        keys = (
            "KAFKA_CLIENT_ID",
            "KAFKA_CLIENT_SECRET",
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOKEN_URL",
            "KAFKA_TOPIC",
        )
        return {key: env[key] for key in keys if env.get(key)}
    return {}


def addons_for_mode(mode: str, environment: dict[str, str] | None = None) -> list[str]:
    env = environment or DEFAULT_ENV
    if env.get("CAI_SPARK_DATA_CONNECTION") or env.get("CDSW_DATA_CONNECTION"):
        return SPARK_DATA_CONNECTION_ADDONS
    return SPARK_ADDONS if mode in SPARK_MODES else DEFAULT_ADDONS


def build_job_payload(mode: str, extra_env: dict[str, str] | None = None, *, stringify_env: bool = False) -> dict[str, Any]:
    environment = dict(DEFAULT_ENV)
    environment.update(fetch_kafka_env_from_consumer())
    if CAI_KEY:
        environment.setdefault("CDSW_APIV2_KEY", CAI_KEY)
    if extra_env:
        environment.update(extra_env)
    environment["LAKEHOUSE_JOB_MODE"] = mode
    environment["LAKEHOUSE_JOB_ARGS"] = mode
    addons = addons_for_mode(mode, environment)

    payload = {
        "name": JOB_NAME,
        "script": JOB_SCRIPT,
        "arguments": mode,
        "cpu": 2 if mode in SPARK_MODES else 1,
        "memory": 4 if mode in SPARK_MODES else 2,
        "type": "manual",
        "timeout": str(WAIT_TIMEOUT),
        "runtime_identifier": DEFAULT_RUNTIME,
        "runtime_addon_identifiers": addons,
        "environment": json.dumps(environment) if stringify_env else environment,
    }
    return payload


def create_job(mode: str, extra_env: dict[str, str] | None = None) -> dict[str, Any]:
    payload = build_job_payload(mode, extra_env, stringify_env=False)
    status, response = api_request("POST", f"/api/v2/projects/{CAI_PID}/jobs", payload, timeout=120)
    if status not in {200, 201} or not isinstance(response, dict):
        raise RuntimeError(f"create job failed: status={status} payload={response}")
    return response


def update_job(job_id: str, mode: str, extra_env: dict[str, str] | None = None) -> dict[str, Any]:
    payload = build_job_payload(mode, extra_env, stringify_env=True)
    status, response = api_request(
        "PATCH",
        f"/api/v2/projects/{CAI_PID}/jobs/{job_id}",
        payload,
        timeout=120,
    )
    if status not in {200, 201} or not isinstance(response, dict):
        raise RuntimeError(f"update job failed: status={status} payload={response}")
    return response


def ensure_job(mode: str, extra_env: dict[str, str] | None = None) -> dict[str, Any]:
    existing = find_job(JOB_NAME)
    if existing:
        job_id = str(existing["id"])
        print(f"Updating existing job {JOB_NAME} ({job_id}) mode={mode}")
        return update_job(job_id, mode, extra_env)
    print(f"Creating job {JOB_NAME} mode={mode}")
    return create_job(mode, extra_env)


def start_job_run(job_id: str) -> dict[str, Any]:
    status, response = api_request(
        "POST",
        f"/api/v2/projects/{CAI_PID}/jobs/{job_id}/runs",
        {},
        timeout=120,
    )
    if status not in {200, 201} or not isinstance(response, dict):
        raise RuntimeError(f"start job run failed: status={status} payload={response}")
    return response


def get_job_run(job_id: str, run_id: str) -> dict[str, Any]:
    status, payload = api_request(
        "GET",
        f"/api/v2/projects/{CAI_PID}/jobs/{job_id}/runs/{run_id}",
        timeout=60,
    )
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"get job run failed: status={status} payload={payload}")
    return payload


TERMINAL_STATUSES = {
    "succeeded",
    "failed",
    "stopped",
    "timed out",
    "skipped",
    "ENGINE_SUCCEEDED",
    "ENGINE_FAILED",
}


def wait_for_job_run(job_id: str, run_id: str) -> dict[str, Any]:
    deadline = time.time() + WAIT_TIMEOUT
    last_status = "unknown"
    while time.time() < deadline:
        run = get_job_run(job_id, run_id)
        last_status = str(run.get("status", "unknown"))
        print(f"  run {run_id}: {last_status}")
        if last_status in TERMINAL_STATUSES:
            return run
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"job run {run_id} did not finish within {WAIT_TIMEOUT}s (last={last_status})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("ensure", "run", "ensure-and-run"),
        help="ensure job exists, run existing job, or ensure then run",
    )
    parser.add_argument(
        "--mode",
        default="discover",
        choices=(
            "discover",
            "bootstrap",
            "batch",
            "stream",
            "verify",
            "spark-probe",
            "spark-layout",
            "spark-pi",
            "trino-probe",
            "trino-bootstrap",
            "trino-verify",
            "trino-ingest",
        ),
        help="passed to jobs/kafka_to_iceberg.py via LAKEHOUSE_JOB_MODE",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Extra KEY=VALUE env overrides for the job (repeatable)",
    )
    parser.add_argument("--no-wait", action="store_true", help="Start run and return without waiting")
    return parser.parse_args()


def parse_env_overrides(pairs: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"invalid --env {pair!r}; expected KEY=VALUE")
        key, value = pair.split("=", 1)
        overrides[key.strip()] = value.strip()
    return overrides


def main() -> int:
    if not CAI_KEY:
        print("CAI_KEY (or CDSW_APIV2_KEY) is required", file=sys.stderr)
        return 1

    args = parse_args()
    extra_env = parse_env_overrides(args.env)

    job = None
    if args.action in {"ensure", "ensure-and-run"}:
        job = ensure_job(args.mode, extra_env)
    else:
        job = find_job(JOB_NAME)
        if not job:
            print(f"Job {JOB_NAME!r} not found; run with action ensure-and-run first", file=sys.stderr)
            return 1

    if args.action == "ensure":
        print(json.dumps({"job_id": job.get("id"), "name": job.get("name"), "mode": args.mode}, indent=2))
        return 0

    job_id = str(job["id"])
    run = start_job_run(job_id)
    run_id = str(run.get("id") or run.get("run_id") or "")
    print(json.dumps({"job_id": job_id, "run_id": run_id, "mode": args.mode}, indent=2))

    if args.no_wait or not run_id:
        return 0

    final = wait_for_job_run(job_id, run_id)
    print("\nFinal run status:")
    print(json.dumps(final, indent=2, default=str))
    return 0 if final.get("status") in {"succeeded", "ENGINE_SUCCEEDED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
