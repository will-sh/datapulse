#!/usr/bin/env python3
"""CAI Job probe: verify Insights Trino path on project filesystem."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.live.insights import compute_insights  # noqa: E402


def main() -> int:
    os.environ.setdefault("TRINO_VERIFY_SSL", "false")
    os.environ.setdefault("INSIGHTS_ICEBERG_DATABASE", "datapulse")
    os.environ.setdefault("INSIGHTS_ICEBERG_TABLE", "events")
    result = compute_insights(source="auto", window_days=int(os.getenv("INSIGHTS_WINDOW_DAYS", "7")))
    summary = {
        "data_source": result.get("data_source"),
        "window_days": result.get("window_days"),
        "sessions": result.get("kpis", {}).get("sessions"),
        "form_submissions": result.get("kpis", {}).get("form_submissions"),
        "events_analyzed": (result.get("warehouse") or {}).get("events_after_dedupe"),
        "qualified_table": (result.get("warehouse") or {}).get("qualified_table"),
        "note": result.get("note"),
        "recent_converter": (result.get("recent_converters") or [None])[0],
    }
    print(json.dumps(summary, indent=2, default=str))
    if result.get("data_source") != "trino":
        print("WARN: expected data_source=trino", file=sys.stderr)
        return 1
    if not summary.get("events_analyzed"):
        print("WARN: no events analyzed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
