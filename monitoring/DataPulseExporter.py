#!/usr/bin/env python3
"""Aggregate DataPulse producer/consumer health into Prometheus metrics."""

from __future__ import annotations

import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://127.0.0.1:8080").rstrip("/")
CONSUMER_URL = os.getenv("CONSUMER_URL", "http://127.0.0.1:8081").rstrip("/")
SCRAPE_INTERVAL = float(os.getenv("EXPORTER_SCRAPE_INTERVAL", "10"))
EXPORTER_HOST = os.getenv("EXPORTER_HOST", "127.0.0.1")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9191"))
AUTH_TOKEN = os.getenv(
    "MONITORING_BEARER_TOKEN",
    os.getenv("CDSW_APIV2_KEY", os.getenv("WORKBENCH_API_KEY", "")),
)
VERIFY_SSL = os.getenv("MONITORING_VERIFY_SSL", "false").lower() in {"1", "true", "yes"}
STATUS_PATH = Path(os.getenv("MONITORING_STATUS_PATH", Path.cwd() / "monitoring" / "status.json"))

PRODUCER_UP = Gauge("datapulse_exporter_producer_up", "Producer health endpoint reachable")
CONSUMER_UP = Gauge("datapulse_exporter_consumer_up", "Consumer health endpoint reachable")
PRODUCER_KAFKA_READY = Gauge(
    "datapulse_exporter_producer_kafka_ready",
    "Producer Kafka readiness from /health",
)
CONSUMER_STREAM_ACTIVE = Gauge(
    "datapulse_exporter_consumer_stream_active",
    "Consumer stream active flag from /health",
)
CONSUMER_TOTAL_RECEIVED = Gauge(
    "datapulse_exporter_consumer_total_received",
    "Consumer total_received from /health",
)


def _ssl_context() -> ssl.SSLContext | None:
    if VERIFY_SSL:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _fetch_json(url: str) -> dict | None:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    if AUTH_TOKEN:
        request.add_header("Authorization", f"Bearer {AUTH_TOKEN}")
    try:
        with urllib.request.urlopen(request, timeout=15, context=_ssl_context()) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None


def scrape_once() -> None:
    producer_health = _fetch_json(f"{PRODUCER_URL}/health")
    consumer_health = _fetch_json(f"{CONSUMER_URL}/health")

    PRODUCER_UP.set(1 if producer_health is not None else 0)
    CONSUMER_UP.set(1 if consumer_health is not None else 0)

    if producer_health is not None:
        PRODUCER_KAFKA_READY.set(1 if producer_health.get("kafka_ready") else 0)
    else:
        PRODUCER_KAFKA_READY.set(0)

    if consumer_health is not None:
        CONSUMER_STREAM_ACTIVE.set(1 if consumer_health.get("stream_active") else 0)
        CONSUMER_TOTAL_RECEIVED.set(float(consumer_health.get("total_received") or 0))
    else:
        CONSUMER_STREAM_ACTIVE.set(0)

    try:
        STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATUS_PATH.write_text(
            json.dumps(
                {
                    "producer_url": PRODUCER_URL,
                    "consumer_url": CONSUMER_URL,
                    "producer_up": producer_health is not None,
                    "consumer_up": consumer_health is not None,
                    "producer_health": producer_health,
                    "consumer_health": consumer_health,
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    except OSError:
        pass


def scrape_loop() -> None:
    while True:
        scrape_once()
        time.sleep(SCRAPE_INTERVAL)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {"/", "/metrics"}:
            self.send_response(404)
            self.end_headers()
            return
        payload = generate_latest()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def main() -> None:
    scrape_once()
    thread = threading.Thread(target=scrape_loop, name="datapulse-exporter-scraper", daemon=True)
    thread.start()
    server = ThreadingHTTPServer((EXPORTER_HOST, EXPORTER_PORT), MetricsHandler)
    print(
        f"DataPulse exporter listening on http://{EXPORTER_HOST}:{EXPORTER_PORT}/metrics "
        f"(producer={PRODUCER_URL}, consumer={CONSUMER_URL})"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
