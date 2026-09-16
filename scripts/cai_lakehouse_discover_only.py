#!/usr/bin/env python3
"""CAI Job entry for Kafka -> Iceberg (isolated from Applications)."""

from __future__ import annotations

import json
import os
import sys
import traceback
import zipfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(os.getcwd())
OUT = ROOT / "config" / "lakehouse" / "last-job-run.json"
SPARK_CONNECT_ZIP = Path("/opt/spark-connect/spark_connect.zip")
SPARK_CONNECT_NATIVE = Path("/opt/spark-connect/native")
SPARK_CONNECT_DIR = Path("/tmp/spark-connect-unpack")
SPARK_MODES = {"bootstrap", "batch", "stream", "verify"}


def configure_spark_connect_python() -> None:
    if not SPARK_CONNECT_ZIP.is_file():
        print("Spark Connect runtime addon not found at /opt/spark-connect/spark_connect.zip")
        return

    SPARK_CONNECT_DIR.mkdir(parents=True, exist_ok=True)
    marker = SPARK_CONNECT_DIR / ".extracted"
    if not marker.is_file():
        print(f"Extracting Spark Connect client from {SPARK_CONNECT_ZIP} ...")
        with zipfile.ZipFile(SPARK_CONNECT_ZIP) as archive:
            archive.extractall(SPARK_CONNECT_DIR)
        marker.write_text("ok", encoding="utf-8")

    pythonpath_parts: list[str] = []
    if SPARK_CONNECT_NATIVE.is_dir():
        pythonpath_parts.append(str(SPARK_CONNECT_NATIVE))
    pythonpath_parts.append(str(SPARK_CONNECT_DIR))
    existing = os.environ.get("PYTHONPATH", "")
    merged = ":".join([*pythonpath_parts, existing]) if existing else ":".join(pythonpath_parts)
    os.environ["PYTHONPATH"] = merged
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    print(
        f"PySpark configured: zip={SPARK_CONNECT_ZIP.is_file()} "
        f"native={SPARK_CONNECT_NATIVE.is_dir()} client={SPARK_CONNECT_DIR}"
    )


def detect_spark_connect_url() -> None:
    if os.getenv("SPARK_REMOTE") or os.getenv("SPARK_CONNECT_URL"):
        return

    host = os.getenv("SPARK_CONNECT_HOST") or os.getenv("CDSW_IP_ADDRESS") or "127.0.0.1"
    engine_id = os.getenv("CDSW_ENGINE_ID", "").upper()
    port = os.getenv(f"DS_RUNTIME_{engine_id}_SERVICE_PORT_SPARK") if engine_id else None
    if not port:
        for key, value in os.environ.items():
            if key.startswith("DS_RUNTIME_") and key.endswith("_SERVICE_PORT_SPARK"):
                port = value
                break
    port = port or os.getenv("SPARK_CONNECT_PORT", "20049")
    os.environ["SPARK_CONNECT_URL"] = f"sc://{host}:{port}"
    print(f"Spark Connect URL: sc://{host}:{port}")


def ensure_kafka_config_dir() -> None:
    os.environ.setdefault("KAFKA_CONFIG_DIR", ".")
    config_dir = Path(os.environ["KAFKA_CONFIG_DIR"])
    for name in ("kafka-ca.crt", "oauth-ca.crt"):
        for src in (ROOT / "config" / "kafka" / name, ROOT / name):
            dst = config_dir / name
            if src.is_file() and not dst.is_file():
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(src.read_bytes())
                print(f"copied {src} -> {dst}")


if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

mode = os.getenv("LAKEHOUSE_JOB_MODE") or (sys.argv[1] if len(sys.argv) > 1 else "discover")
extra_args: list[str] = []
raw_args = os.getenv("LAKEHOUSE_JOB_EXTRA_ARGS", "").strip()
if raw_args:
    extra_args.extend(raw_args.split())
if len(sys.argv) > 2:
    extra_args.extend(sys.argv[2:])

OUT.parent.mkdir(parents=True, exist_ok=True)
payload: dict[str, object] = {
    "timestamp": datetime.now(UTC).isoformat(),
    "mode": mode,
    "steps": [],
}

try:
    if mode in SPARK_MODES:
        configure_spark_connect_python()
        detect_spark_connect_url()
    ensure_kafka_config_dir()
    payload["steps"].append("preflight_ok")

    jobs_dir = ROOT / "jobs"
    if str(jobs_dir) not in sys.path:
        sys.path.insert(0, str(jobs_dir))

    from kafka_to_iceberg import main as job_main

    payload["steps"].append("import_ok")
    exit_code = job_main([mode, *extra_args])
    payload["status"] = "ok" if exit_code == 0 else "failed"
    payload["exit_code"] = exit_code
except Exception as exc:  # noqa: BLE001
    payload["status"] = "error"
    payload["error"] = str(exc)
    payload["traceback"] = traceback.format_exc()

OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
print(json.dumps(payload, indent=2, default=str))
