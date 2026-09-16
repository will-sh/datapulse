#!/usr/bin/env python3
"""Upload changed project files to CAI Workbench via multipart API."""

from __future__ import annotations

import mimetypes
import os
import ssl
import sys
import urllib.request
from pathlib import Path

CAI_BASE = os.getenv("CAI_BASE", "").rstrip("/")
CAI_PID = os.getenv("CAI_PID", "")
CAI_KEY = os.getenv("CAI_KEY", os.getenv("CDSW_APIV2_KEY", ""))
ROOT = Path(__file__).resolve().parent.parent

FILES = [
    "app/metrics_relay.py",
    "app/main.py",
    "app/config.py",
    "app/spark_stream/web.py",
    "app/spark_stream/store.py",
    "app/spark_stream/kafka_stream.py",
    "app/live/routes.py",
    "app/live/service.py",
    "app/live/schemas.py",
    "app/live/templates/live.html",
    "app/live/templates/legacy.html",
    "app/live/static/live.css",
    "app/live/static/live.js",
    "static/css/styles.css",
    "static/js/analytics.js",
    "templates/base.html",
    "templates/index.html",
    "templates/features.html",
    "templates/pricing.html",
    "templates/playground.html",
    "monitoring/DataPulseExporter.py",
    "monitoring/relay/placeholder.txt",
    "datapulse.env",
    "scripts/e2e_playground_metrics.py",
    "scripts/cai_restart_applications.py",
    "scripts/cai_upload_files.py",
]


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def upload_file(rel_path: str) -> None:
    local_path = ROOT / rel_path
    if not local_path.is_file():
        raise FileNotFoundError(local_path)

    delete_url = f"{CAI_BASE}/api/v2/projects/{CAI_PID}/files/{rel_path}"
    delete_req = urllib.request.Request(
        delete_url,
        headers={"Authorization": f"Bearer {CAI_KEY}"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(delete_req, timeout=60, context=ssl_context()) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        if exc.code not in {404, 400}:
            raise

    boundary = "----datapulse-upload"
    body_parts: list[bytes] = []
    filename = local_path.name
    content = local_path.read_bytes()
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    body_parts.append(f"--{boundary}\r\n".encode())
    body_parts.append(
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
    )
    body_parts.append(f"Content-Type: {content_type}\r\n\r\n".encode())
    body_parts.append(content)
    body_parts.append(b"\r\n")
    body_parts.append(f"--{boundary}--\r\n".encode())
    payload = b"".join(body_parts)

    url = f"{CAI_BASE}/api/v2/projects/{CAI_PID}/files/{rel_path}"
    request = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {CAI_KEY}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120, context=ssl_context()) as response:
        result = response.read().decode("utf-8", errors="replace")
        print(f"uploaded {rel_path}: {response.status} {result[:120]}")


def main() -> int:
    if not CAI_BASE or not CAI_PID or not CAI_KEY:
        print("CAI_BASE, CAI_PID, and CAI_KEY are required", file=sys.stderr)
        return 1
    for rel_path in FILES:
        upload_file(rel_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
