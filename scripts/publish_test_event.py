#!/usr/bin/env python3
"""Publish a test event to datapulse-events (same path as producer /api/events)."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.kafka_settings import get_kafka_settings
from app.services.kafka_producer import publish_event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default="e2e_probe")
    parser.add_argument("--source", default="verify-script")
    args = parser.parse_args()

    settings = get_kafka_settings()
    if not settings.is_ready():
        print("Producer not ready:", "; ".join(settings.readiness_issues()), file=sys.stderr)
        return 1

    event = {
        "name": args.name,
        "properties": {"source": args.source, "ts": int(time.time())},
        "timestamp": int(time.time() * 1000),
        "source": "datapulse-web",
    }
    publish_event(event, settings)
    print(json.dumps({"status": "published", "topic": settings.topic, "event": event}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
