#!/usr/bin/env python3
import json
import os
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(os.getcwd())
OUT = ROOT / "config" / "lakehouse" / "last-job-run.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

payload = {"timestamp": datetime.now(UTC).isoformat(), "steps": []}

try:
    sys.path.insert(0, str(ROOT))
    payload["steps"].append("root_path_ok")

    from app.lakehouse_settings import get_lakehouse_settings, discover_hive_site

    payload["steps"].append("lakehouse_settings_ok")
    settings = get_lakehouse_settings()
    payload["lakehouse"] = {
        "qualified_table": settings.qualified_table,
        "hive_metastore_uri": settings.hive_metastore_uri,
        "warehouse": settings.warehouse,
    }

    sys.path.insert(0, str(ROOT / "jobs"))
    from kafka_to_iceberg import discover_environment

    payload["steps"].append("kafka_to_iceberg_ok")
    payload["discover_exit_code"] = discover_environment()
    payload["status"] = "ok"
except Exception as exc:  # noqa: BLE001
    payload["status"] = "error"
    payload["error"] = str(exc)
    payload["traceback"] = traceback.format_exc()

OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(json.dumps(payload, indent=2))
