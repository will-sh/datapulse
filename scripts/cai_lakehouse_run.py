#!/usr/bin/env python3
import json
from datetime import UTC, datetime
from pathlib import Path

out = Path("config/lakehouse/last-job-run.json")
out.parent.mkdir(parents=True, exist_ok=True)
payload = {"status": "ok", "marker": "minimal-run-script", "timestamp": datetime.now(UTC).isoformat()}
out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(json.dumps(payload, indent=2))
