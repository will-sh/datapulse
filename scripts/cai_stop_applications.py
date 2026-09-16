#!/usr/bin/env python3
"""Stop DataPulse CAI applications via API and wait for APPLICATION_STOPPED."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request

CAI_BASE = os.getenv("CAI_BASE", "https://ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work").rstrip("/")
CAI_PID = os.getenv("CAI_PID", "k87h-zej9-dugs-473y")
CAI_KEY = os.getenv("CAI_KEY", os.getenv("CDSW_APIV2_KEY", ""))

DEFAULT_NAMES = [
    "datapulse-app",
    "datapulse-spark-consumer",
    "datapulse-monitoring",
]

POLL_INTERVAL = int(os.getenv("CAI_STOP_POLL_INTERVAL", "10"))
WAIT_TIMEOUT = int(os.getenv("WAIT_TIMEOUT", "600"))


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def api_request(method: str, path: str, timeout: int = 60) -> tuple[int, object]:
    request = urllib.request.Request(
        f"{CAI_BASE}{path}",
        headers={
            "Authorization": f"Bearer {CAI_KEY}",
            "Accept": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=ssl_context()) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"raw": body[:500]}
        return exc.code, parsed
    except TimeoutError as exc:
        return 0, {"error": f"timeout: {exc}"}


def list_applications() -> list[dict]:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/applications")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"list applications failed: status={status} payload={payload}")
    apps = payload.get("applications", [])
    return apps if isinstance(apps, list) else []


def find_application(name: str) -> dict | None:
    for app in list_applications():
        if app.get("name") == name:
            return app
    return None


def get_application(app_id: str) -> dict:
    status, payload = api_request("GET", f"/api/v2/projects/{CAI_PID}/applications/{app_id}")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"get_application {app_id} failed: status={status} payload={payload}")
    return payload


def stop_application(app_id: str) -> tuple[int, object]:
    return api_request(
        "POST",
        f"/api/v2/projects/{CAI_PID}/applications/{app_id}:stop",
        timeout=30,
    )


def ensure_stopped(app_id: str, name: str) -> dict:
    app = get_application(app_id)
    status = app.get("status", "")
    if status in {"APPLICATION_STOPPED", "APPLICATION_FAILED", "APPLICATION_KILLED"}:
        return app
    if status in {"APPLICATION_RUNNING", "APPLICATION_STARTING", "APPLICATION_STOPPING"}:
        print(f"Stopping {name} (was {status}) ...")
        code, payload = stop_application(app_id)
        print(f"  stop accepted: HTTP {code} status={payload.get('status') if isinstance(payload, dict) else payload}")
    deadline = time.time() + WAIT_TIMEOUT
    last_status = status
    while time.time() < deadline:
        app = get_application(app_id)
        last_status = app.get("status", "unknown")
        print(f"  {name}: {last_status}")
        if last_status in {"APPLICATION_STOPPED", "APPLICATION_FAILED", "APPLICATION_KILLED"}:
            return app
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{name} did not stop within {WAIT_TIMEOUT}s (last={last_status})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apps",
        default=",".join(DEFAULT_NAMES),
        help=f"Comma-separated app names (default: {','.join(DEFAULT_NAMES)})",
    )
    return parser.parse_args()


def main() -> int:
    if not CAI_KEY:
        print("CAI_KEY (or CDSW_APIV2_KEY) is required", file=sys.stderr)
        return 1

    names = [part.strip() for part in parse_args().apps.split(",") if part.strip()]
    results = []
    for name in names:
        app = find_application(name)
        if not app:
            print(f"Skipping {name}: not found")
            results.append({"name": name, "status": "not_found"})
            continue
        app_id = app["id"]
        print(f"Ensuring {name} ({app_id}) is stopped ...")
        try:
            stopped = ensure_stopped(app_id, name)
        except TimeoutError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            print(
                f"  {name} may be stuck in a transition state; use cai_recreate_applications.py instead of :restart.",
                file=sys.stderr,
            )
            results.append({"id": app_id, "name": name, "status": get_application(app_id).get("status")})
            continue
        results.append(
            {
                "id": app_id,
                "name": name,
                "status": stopped.get("status"),
                "updated_at": stopped.get("updated_at"),
            }
        )

    print("\nStop results:")
    print(json.dumps(results, indent=2))
    stuck = [
        r
        for r in results
        if r.get("status") not in {
            "APPLICATION_STOPPED",
            "APPLICATION_FAILED",
            "APPLICATION_KILLED",
            "not_found",
        }
    ]
    return 1 if stuck else 0


if __name__ == "__main__":
    raise SystemExit(main())
