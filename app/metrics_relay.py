"""Write Prometheus metrics and health snapshots to project-shared files."""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

from prometheus_client import generate_latest

RELAY_DIR = Path(os.getenv("METRICS_RELAY_DIR", "monitoring/relay"))
RELAY_INTERVAL = float(os.getenv("METRICS_RELAY_INTERVAL", "5"))

ROLE_FILES = {
    "producer": {
        "metrics": RELAY_DIR / "producer.prom",
        "health": RELAY_DIR / "producer.health.json",
    },
    "consumer": {
        "metrics": RELAY_DIR / "consumer.prom",
        "health": RELAY_DIR / "consumer.health.json",
    },
}


def _write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def write_metrics_snapshot(role: str) -> None:
    path = ROLE_FILES[role]["metrics"]
    _write_atomic(path, generate_latest().decode("utf-8"))


def write_health_snapshot(role: str, payload: dict[str, object]) -> None:
    path = ROLE_FILES[role]["health"]
    enriched = {
        **payload,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    _write_atomic(path, json.dumps(enriched, indent=2))


def relay_once(role: str, health_fn: Callable[[], dict[str, object]]) -> None:
    write_metrics_snapshot(role)
    write_health_snapshot(role, health_fn())


def start_metrics_relay(role: str, health_fn: Callable[[], dict[str, object]]) -> None:
    def loop() -> None:
        while True:
            try:
                relay_once(role, health_fn)
            except Exception:
                pass
            time.sleep(RELAY_INTERVAL)

    relay_once(role, health_fn)
    thread = threading.Thread(target=loop, name=f"metrics-relay-{role}", daemon=True)
    thread.start()
