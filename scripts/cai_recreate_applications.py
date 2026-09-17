#!/usr/bin/env python3
"""Recreate DataPulse CAI applications via delete + create.

Preferred deploy path after `cai_upload_files.py`. Avoid `:restart` — it can leave
orphan engine sessions when apps are stuck in APPLICATION_STARTING.
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
CAI_KEY = os.getenv("CAI_KEY", os.getenv("CDSW_APIV2_KEY", ""))

DEFAULT_RUNTIME = (
    "container.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-workbench-python3.11-hardened:2026.04.2-b16"
)
DEFAULT_ADDONS = ["hadoop-cli-7.3.1.709-1"]

TARGET_ORDER = [
    "datapulse-app",
    "datapulse-spark-consumer",
    "datapulse-monitoring",
]

DEFAULT_SPECS: dict[str, dict[str, Any]] = {
    "datapulse-app": {
        "name": "datapulse-app",
        "subdomain": "datapulse-app",
        "script": "scripts/cai_start_application.py",
        "cpu": 2,
        "memory": 4,
        "environment": {
            "CDSW_APP_POLLING_ENDPOINT": "/",
            "KAFKA_ENABLED": "true",
            "KAFKA_CONFIG_DIR": "config/kafka",
            "METRICS_RELAY_DIR": "monitoring/relay",
        },
    },
    "datapulse-spark-consumer": {
        "name": "datapulse-spark-consumer",
        "subdomain": "datapulse-spark-consumer",
        "script": "scripts/cai_spark_kafka_stream.py",
        "cpu": 2,
        "memory": 4,
        "environment": {
            "CDSW_APP_POLLING_ENDPOINT": "/",
            "KAFKA_ENABLED": "true",
            "KAFKA_CONFIG_DIR": "config/kafka",
            "KAFKA_CONSUMER_MODE": "cli",
            "KAFKA_CLI_FROM_BEGINNING": "true",
            "KAFKA_STARTING_OFFSETS": "earliest",
            "KAFKA_POLL_INTERVAL": "5 seconds",
            "METRICS_RELAY_DIR": "monitoring/relay",
        },
    },
    "datapulse-monitoring": {
        "name": "datapulse-monitoring",
        "subdomain": "datapulse-mon-7rrlwp",
        "script": "scripts/cai_start_monitoring.py",
        "cpu": 2,
        "memory": 4,
        "environment": {
            "CDSW_APP_POLLING_ENDPOINT": "/",
            "MONITORING_VERIFY_SSL": "false",
        },
    },
}

WAIT_TIMEOUT = int(os.getenv("WAIT_TIMEOUT", "600"))
POLL_INTERVAL = int(os.getenv("CAI_RECREATE_POLL_INTERVAL", "10"))
DELETE_TIMEOUT = int(os.getenv("CAI_DELETE_TIMEOUT", "180"))


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
            parsed = {"raw": body[:500]}
        return exc.code, parsed
    except TimeoutError as exc:
        return 0, {"error": f"timeout: {exc}"}


def list_applications() -> list[dict[str, Any]]:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/applications")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"list applications failed: status={status} payload={payload}")
    apps = payload.get("applications", [])
    return apps if isinstance(apps, list) else []


def find_application(name: str) -> dict[str, Any] | None:
    for app in list_applications():
        if app.get("name") == name:
            return app
    return None


def get_application(app_id: str) -> dict[str, Any]:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/applications/{app_id}")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"get_application {app_id} failed: status={status} payload={payload}")
    return payload


def parse_environment(raw: Any) -> dict[str, str]:
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    return {}


def snapshot_spec(app: dict[str, Any]) -> dict[str, Any]:
    defaults = DEFAULT_SPECS.get(app["name"], {})
    environment = parse_environment(app.get("environment"))
    default_env = defaults.get("environment", {})
    if isinstance(default_env, dict):
        merged_env = {**environment, **default_env}
    else:
        merged_env = environment

    spec: dict[str, Any] = {
        "name": app.get("name") or defaults.get("name"),
        "subdomain": app.get("subdomain") or defaults.get("subdomain"),
        "script": app.get("script") or defaults.get("script"),
        "cpu": app.get("cpu", defaults.get("cpu", 2)),
        "memory": app.get("memory", defaults.get("memory", 4)),
        "nvidia_gpu": app.get("nvidia_gpu", 0),
        "kernel": app.get("kernel") or "",
        "description": app.get("description") or "",
        "environment": merged_env,
        "runtime_identifier": app.get("runtime_identifier") or DEFAULT_RUNTIME,
        "runtime_addon_identifiers": app.get("runtime_addon_identifiers") or DEFAULT_ADDONS,
    }
    if app.get("bypass_authentication") is not None:
        spec["bypass_authentication"] = app["bypass_authentication"]
    return spec


def build_create_payload(spec: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "project_id": CAI_PID,
        "name": spec["name"],
        "subdomain": spec["subdomain"],
        "script": spec["script"],
        "cpu": spec.get("cpu", 2),
        "memory": spec.get("memory", 4),
        "nvidia_gpu": spec.get("nvidia_gpu", 0),
        "kernel": spec.get("kernel", ""),
        "description": spec.get("description", ""),
        "environment": spec.get("environment", {}),
        "runtime_identifier": spec.get("runtime_identifier", DEFAULT_RUNTIME),
        "runtime_addon_identifiers": spec.get("runtime_addon_identifiers", DEFAULT_ADDONS),
    }
    if "bypass_authentication" in spec:
        payload["bypass_authentication"] = spec["bypass_authentication"]
    return payload


def stop_application(app_id: str) -> None:
    status, payload = api_request(
        "POST",
        f"/api/v2/projects/{CAI_PID}/applications/{app_id}:stop",
        timeout=30,
    )
    if status == 0:
        print(f"  stop request timed out for {app_id}; continuing")
        return
    if status not in {200, 201}:
        raise RuntimeError(f"stop {app_id} failed: status={status} payload={payload}")


def wait_until_gone(name: str, app_id: str) -> None:
    deadline = time.time() + DELETE_TIMEOUT
    while time.time() < deadline:
        existing = find_application(name)
        if existing is None or existing.get("id") != app_id:
            return
        status = existing.get("status", "unknown")
        print(f"  waiting for delete: {name} ({app_id}) status={status}")
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{name} ({app_id}) still present after delete")


def delete_application(name: str, app: dict[str, Any]) -> None:
    app_id = app["id"]
    status = app.get("status", "")
    if status in {"APPLICATION_RUNNING", "APPLICATION_STARTING", "APPLICATION_STOPPING"}:
        print(f"  stopping {name} before delete (was {status}) ...")
        stop_application(app_id)
        deadline = time.time() + DELETE_TIMEOUT
        while time.time() < deadline:
            current = get_application(app_id)
            current_status = current.get("status", "unknown")
            print(f"  {name}: {current_status}")
            if current_status in {"APPLICATION_STOPPED", "APPLICATION_FAILED", "APPLICATION_KILLED"}:
                break
            time.sleep(POLL_INTERVAL)
        else:
            print(f"  warning: {name} did not stop cleanly before delete; attempting delete anyway")

    print(f"  deleting {name} ({app_id}) ...")
    code, payload = api_request(
        "DELETE",
        f"/api/v2/projects/{CAI_PID}/applications/{app_id}",
        timeout=30,
    )
    if code not in {200, 204}:
        raise RuntimeError(f"delete {name} failed: status={code} payload={payload}")
    wait_until_gone(name, app_id)


def create_application(spec: dict[str, Any]) -> dict[str, Any]:
    payload = build_create_payload(spec)
    status, response = api_request(
        "POST",
        f"/api/v2/projects/{CAI_PID}/applications",
        payload,
        timeout=120,
    )
    if status not in {200, 201} or not isinstance(response, dict):
        raise RuntimeError(f"create {spec['name']} failed: status={status} payload={response}")
    return response


def wait_for_running(app_id: str, name: str) -> dict[str, Any]:
    deadline = time.time() + WAIT_TIMEOUT
    last_status = "unknown"
    while time.time() < deadline:
        app = get_application(app_id)
        last_status = app.get("status", "unknown")
        print(f"  {name}: {last_status}")
        if last_status == "APPLICATION_RUNNING":
            return app
        if last_status in {"APPLICATION_FAILED", "APPLICATION_STOPPED", "APPLICATION_KILLED"}:
            raise RuntimeError(f"{name} entered terminal state {last_status}")
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(
        f"{name} did not reach APPLICATION_RUNNING within {WAIT_TIMEOUT}s (last={last_status})"
    )


def recreate_application(name: str) -> dict[str, Any]:
    print(f"Recreating {name} ...")
    existing = find_application(name)
    if existing:
        spec = snapshot_spec(existing)
        delete_application(name, existing)
    else:
        defaults = DEFAULT_SPECS.get(name)
        if not defaults:
            raise RuntimeError(f"no default spec for unknown application {name!r}")
        spec = defaults.copy()
        spec["environment"] = dict(defaults.get("environment", {}))
        spec["runtime_identifier"] = DEFAULT_RUNTIME
        spec["runtime_addon_identifiers"] = list(DEFAULT_ADDONS)
        print(f"  no existing app; creating from defaults")

    created = create_application(spec)
    app_id = created.get("id")
    if not app_id:
        raise RuntimeError(f"create {name} returned no id: {created}")
    print(f"  created {name} id={app_id} subdomain={created.get('subdomain')}")
    running = wait_for_running(str(app_id), name)
    return {
        "id": running.get("id"),
        "name": name,
        "status": running.get("status"),
        "subdomain": running.get("subdomain"),
        "updated_at": running.get("updated_at"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apps",
        default=",".join(TARGET_ORDER),
        help=f"Comma-separated app names (default: {','.join(TARGET_ORDER)})",
    )
    return parser.parse_args()


def main() -> int:
    if not CAI_KEY:
        print("CAI_KEY (or CDSW_APIV2_KEY) is required", file=sys.stderr)
        return 1

    args = parse_args()
    names = [part.strip() for part in args.apps.split(",") if part.strip()]
    unknown = [name for name in names if name not in DEFAULT_SPECS]
    if unknown:
        print(f"Unknown app names: {', '.join(unknown)}", file=sys.stderr)
        return 1

    results = []
    for name in names:
        try:
            results.append(recreate_application(name))
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR recreating {name}: {exc}", file=sys.stderr)
            return 1

    print("\nRecreate complete:")
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
