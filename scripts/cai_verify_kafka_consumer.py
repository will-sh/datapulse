#!/usr/bin/env python3
"""Run verify_kafka_cli_consumer.py as a CAI Job on the project."""

from __future__ import annotations

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
CAI_KEY = os.getenv("CAI_KEY", os.getenv("CDSW_APIV2_KEY", ""))
JOB_NAME = "datapulse-kafka-cli-verify"
JOB_SCRIPT = "scripts/verify_kafka_cli_consumer.py"
DEFAULT_RUNTIME = (
    "container.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-workbench-python3.11-hardened:2026.04.2-b16"
)
DEFAULT_ADDONS = ["hadoop-cli-7.3.1.709-1"]
POLL_INTERVAL = int(os.getenv("CAI_JOB_POLL_INTERVAL", "10"))
WAIT_TIMEOUT = int(os.getenv("WAIT_TIMEOUT", "600"))


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
    headers = {"Authorization": f"Bearer {CAI_KEY}", "Accept": "application/json"}
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


def parse_environment(raw: Any) -> dict[str, str]:
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, str) and raw.strip():
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    return {}


def fetch_consumer_env() -> dict[str, str]:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/applications")
    if status != 200 or not isinstance(payload, dict):
        return {}
    defaults = {
        "KAFKA_CONFIG_DIR": "config/kafka",
        "KAFKA_ENABLED": "true",
        "KAFKA_TOPIC": "datapulse-events",
        "KAFKA_BOOTSTRAP_SERVERS": "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
        "KAFKA_TOKEN_URL": "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
    }
    for app in payload.get("applications", []):
        if app.get("name") != "datapulse-spark-consumer":
            continue
        env = parse_environment(app.get("environment"))
        merged = {**defaults, **env}
        return merged
    return defaults


def find_job() -> dict[str, Any] | None:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/jobs?page_size=100")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"list jobs failed: status={status} payload={payload}")
    for job in payload.get("jobs", []):
        if job.get("name") == JOB_NAME:
            return job
    return None


def ensure_job() -> dict[str, Any]:
    environment = fetch_consumer_env()
    payload = {
        "name": JOB_NAME,
        "script": JOB_SCRIPT,
        "arguments": "--max-messages 1 --timeout 45",
        "cpu": 2,
        "memory": 4,
        "type": "manual",
        "timeout": str(WAIT_TIMEOUT),
        "runtime_identifier": DEFAULT_RUNTIME,
        "runtime_addon_identifiers": DEFAULT_ADDONS,
        "environment": environment,
    }
    existing = find_job()
    if existing:
        status, response = api_request(
            "PATCH",
            f"/api/v2/projects/{CAI_PID}/jobs/{existing['id']}",
            {**payload, "environment": json.dumps(environment)},
            timeout=120,
        )
        if status not in {200, 201} or not isinstance(response, dict):
            raise RuntimeError(f"update job failed: status={status} payload={response}")
        return response
    status, response = api_request("POST", f"/api/v2/projects/{CAI_PID}/jobs", payload, timeout=120)
    if status not in {200, 201} or not isinstance(response, dict):
        raise RuntimeError(f"create job failed: status={status} payload={response}")
    return response


def start_run(job_id: str) -> dict[str, Any]:
    status, response = api_request("POST", f"/api/v2/projects/{CAI_PID}/jobs/{job_id}/runs", {}, timeout=120)
    if status not in {200, 201} or not isinstance(response, dict):
        raise RuntimeError(f"start run failed: status={status} payload={response}")
    return response


def get_run(job_id: str, run_id: str) -> dict[str, Any]:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/jobs/{job_id}/runs/{run_id}", timeout=60)
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"get run failed: status={status} payload={payload}")
    return payload


def wait_run(job_id: str, run_id: str) -> dict[str, Any]:
    deadline = time.time() + WAIT_TIMEOUT
    last = "unknown"
    while time.time() < deadline:
        run = get_run(job_id, run_id)
        last = str(run.get("status", "unknown"))
        print(f"  run {run_id}: {last}")
        if last in {"succeeded", "failed", "stopped", "timed out", "skipped", "ENGINE_SUCCEEDED", "ENGINE_FAILED"}:
            return run
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"run {run_id} did not finish within {WAIT_TIMEOUT}s (last={last})")


def main() -> int:
    if not CAI_KEY:
        print("CAI_KEY is required", file=sys.stderr)
        return 1
    job = ensure_job()
    job_id = str(job["id"])
    run = start_run(job_id)
    run_id = str(run.get("id") or run.get("run_id") or "")
    print(json.dumps({"job_id": job_id, "run_id": run_id}, indent=2))
    if not run_id:
        return 1
    final = wait_run(job_id, run_id)
    print(json.dumps(final, indent=2, default=str))
    return 0 if final.get("status") in {"succeeded", "ENGINE_SUCCEEDED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
