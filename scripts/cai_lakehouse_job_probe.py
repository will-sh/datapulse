#!/usr/bin/env python3
"""Minimal CAI Job probe — writes engine diagnostics to config/lakehouse/last-job-run.json."""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(os.getcwd())
OUT = ROOT / "config" / "lakehouse" / "last-job-run.json"


def collect() -> dict[str, object]:
    spark_zip = Path("/opt/spark-connect/spark_connect.zip")
    spark_native = Path("/opt/spark-connect/native")
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "cwd": str(ROOT),
        "argv": sys.argv,
        "python": sys.version,
        "spark_connect_zip": spark_zip.is_file(),
        "spark_connect_native": spark_native.is_dir(),
        "runtime_addon_hint": os.getenv("RUNTIME_ADDON_IDENTIFIERS", ""),
        "lakehouse_job_mode": os.getenv("LAKEHOUSE_JOB_MODE", ""),
        "ds_runtime_keys": sorted(k for k in os.environ if k.startswith("DS_RUNTIME_")),
        "hadoop_conf_dir": os.getenv("HADOOP_CONF_DIR", ""),
        "hive_conf_exists": Path("/etc/hadoop/conf/hive-site.xml").is_file(),
    }


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = {"status": "ok", **collect()}
        OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0
    except Exception as exc:  # noqa: BLE001
        payload = {"status": "error", "error": str(exc), "traceback": traceback.format_exc(), **collect()}
        OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 1


exit_code = main()
if exit_code != 0:
    raise RuntimeError(f"probe failed with exit code {exit_code}")
