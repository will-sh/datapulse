#!/usr/bin/env python3
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path.cwd()
OUT = ROOT / "config" / "lakehouse" / "last-job-run.json"
OUT.parent.mkdir(parents=True, exist_ok=True)
payload = {"timestamp": datetime.now(UTC).isoformat(), "steps": []}
try:
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "jobs"))
    import kafka_to_iceberg

    payload["steps"] = ["import_ok", kafka_to_iceberg.__name__]
    payload["status"] = "ok"
except Exception as exc:  # noqa: BLE001
    payload["status"] = "error"
    payload["error"] = str(exc)
OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(json.dumps(payload, indent=2))
