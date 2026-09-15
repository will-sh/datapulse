#!/usr/bin/env python3
"""Import the DataPulse Overview dashboard into Grafana via API."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

GRAFANA_URL = os.getenv("GRAFANA_URL", "http://127.0.0.1:8100").rstrip("/")
DASHBOARD_PATH = Path(
    os.getenv(
        "DASHBOARD_PATH",
        Path(__file__).resolve().parent / "dashboards" / "datapulse.json",
    )
)
FOLDER_TITLE = "DataPulse"
TIMEOUT = int(os.getenv("GRAFANA_IMPORT_TIMEOUT", "120"))


def request(method: str, path: str, payload: dict | None = None) -> tuple[int, str]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(f"{GRAFANA_URL}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def wait_for_grafana() -> None:
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        status, _ = request("GET", "/api/health")
        if status == 200:
            return
        time.sleep(2)
    raise SystemExit(f"Grafana not ready at {GRAFANA_URL}")


def ensure_folder() -> str | None:
    status, body = request("GET", "/api/folders")
    if status != 200:
        return None
    folders = json.loads(body)
    for folder in folders:
        if folder.get("title") == FOLDER_TITLE:
            return folder.get("uid")
    status, body = request("POST", "/api/folders", {"title": FOLDER_TITLE})
    if status not in {200, 409}:
        print(f"Failed to create folder: {status} {body}", file=sys.stderr)
        return None
    if status == 409:
        return ensure_folder()
    return json.loads(body).get("uid")


def import_dashboard() -> None:
    dashboard = json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))
    folder_uid = ensure_folder()
    payload: dict[str, object] = {
        "dashboard": dashboard,
        "overwrite": True,
        "message": "Imported by monitoring/import_dashboard.py",
    }
    if folder_uid:
        payload["folderUid"] = folder_uid
    status, body = request("POST", "/api/dashboards/db", payload)
    if status != 200:
        raise SystemExit(f"Dashboard import failed: {status} {body}")
    result = json.loads(body)
    print(f"Imported dashboard uid={result.get('uid')} url={result.get('url')}")


def main() -> None:
    if not DASHBOARD_PATH.is_file():
        raise SystemExit(f"Dashboard file not found: {DASHBOARD_PATH}")
    wait_for_grafana()
    import_dashboard()


if __name__ == "__main__":
    main()
