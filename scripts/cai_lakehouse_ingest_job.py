#!/usr/bin/env python3
"""CAI Job entry for Kafka -> Iceberg lakehouse ingest (does not start Applications)."""

from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path

ROOT = Path(os.getcwd())
SPARK_CONNECT_ZIP = Path("/opt/spark-connect/spark_connect.zip")
SPARK_CONNECT_NATIVE = Path("/opt/spark-connect/native")
SPARK_CONNECT_DIR = Path("/tmp/spark-connect-unpack")


def _prepend_sys_path() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


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

    pythonpath_parts = []
    if SPARK_CONNECT_NATIVE.is_dir():
        pythonpath_parts.append(str(SPARK_CONNECT_NATIVE))
    pythonpath_parts.append(str(SPARK_CONNECT_DIR))
    existing = os.environ.get("PYTHONPATH", "")
    merged = ":".join([*pythonpath_parts, existing]) if existing else ":".join(pythonpath_parts)
    os.environ["PYTHONPATH"] = merged
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    print(f"PySpark paths configured: native={SPARK_CONNECT_NATIVE.is_dir()} client={SPARK_CONNECT_DIR}")


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
    url = f"sc://{host}:{port}"
    os.environ["SPARK_CONNECT_URL"] = url
    print(f"Spark Connect URL: {url}")


def ensure_kafka_config_dir() -> None:
    os.environ.setdefault("KAFKA_CONFIG_DIR", ".")
    config_dir = Path(os.environ["KAFKA_CONFIG_DIR"])
    for name in ("kafka-ca.crt", "oauth-ca.crt"):
        src = ROOT / "config" / "kafka" / name
        dst = config_dir / name
        if src.is_file() and not dst.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())
            print(f"copied {src} -> {dst}")


def main() -> int:
    _prepend_sys_path()
    configure_spark_connect_python()
    detect_spark_connect_url()
    ensure_kafka_config_dir()

    mode = os.getenv("LAKEHOUSE_JOB_MODE") or (sys.argv[1] if len(sys.argv) > 1 else "discover")
    extra_args = []
    raw_args = os.getenv("LAKEHOUSE_JOB_EXTRA_ARGS", "").strip()
    if raw_args:
        extra_args.extend(raw_args.split())
    if len(sys.argv) > 2:
        extra_args.extend(sys.argv[2:])

    jobs_dir = ROOT / "jobs"
    if str(jobs_dir) not in sys.path:
        sys.path.insert(0, str(jobs_dir))

    from kafka_to_iceberg import main as job_main

    return job_main([mode, *extra_args])


if __name__ == "__main__":
    raise SystemExit(main())
