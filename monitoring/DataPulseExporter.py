#!/usr/bin/env python3
"""Aggregate DataPulse producer/consumer health and relay app metrics for Prometheus."""

from __future__ import annotations

import base64
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from prometheus_client import CONTENT_TYPE_LATEST, Gauge, REGISTRY, generate_latest
from prometheus_client.parser import text_string_to_metric_families

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://127.0.0.1:8080").rstrip("/")
CONSUMER_URL = os.getenv("CONSUMER_URL", "http://127.0.0.1:8081").rstrip("/")
READONLY_PORT = os.getenv("CDSW_READONLY_PORT", "8100")
SCRAPE_INTERVAL = float(os.getenv("EXPORTER_SCRAPE_INTERVAL", "10"))
EXPORTER_HOST = os.getenv("EXPORTER_HOST", "127.0.0.1")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9191"))
AUTH_TOKEN = os.getenv(
    "MONITORING_BEARER_TOKEN",
    os.getenv("CDSW_APIV2_KEY", os.getenv("WORKBENCH_API_KEY", "")),
)
CDSW_API_KEY = os.getenv("CDSW_API_KEY", "")
VERIFY_SSL = os.getenv("MONITORING_VERIFY_SSL", "false").lower() in {"1", "true", "yes"}
STATUS_PATH = Path(os.getenv("MONITORING_STATUS_PATH", Path.cwd() / "monitoring" / "status.json"))
RELAY_DIR = Path(os.getenv("METRICS_RELAY_DIR", Path.cwd() / "monitoring" / "relay"))
RELAY_MAX_AGE = float(os.getenv("METRICS_RELAY_MAX_AGE", "30"))
PRODUCER_METRICS_FILE = RELAY_DIR / "producer.prom"
CONSUMER_METRICS_FILE = RELAY_DIR / "consumer.prom"
PRODUCER_HEALTH_FILE = RELAY_DIR / "producer.health.json"
CONSUMER_HEALTH_FILE = RELAY_DIR / "consumer.health.json"
RELAY_PREFIX = "datapulse_"
EXCLUDE_PREFIX = "datapulse_exporter_"
LAST_SCRAPE_ERROR = ""

PRODUCER_UP = Gauge("datapulse_exporter_producer_up", "Producer health endpoint reachable")
CONSUMER_UP = Gauge("datapulse_exporter_consumer_up", "Consumer health endpoint reachable")
PRODUCER_METRICS_UP = Gauge(
    "datapulse_exporter_producer_metrics_up",
    "Producer /metrics endpoint reachable",
)
CONSUMER_METRICS_UP = Gauge(
    "datapulse_exporter_consumer_metrics_up",
    "Consumer /metrics endpoint reachable",
)
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


def _family_sample_count(family) -> int:
    return sum(1 for _ in family.samples)


def _merge_metric_families(existing, incoming):
    """Merge samples from two parsed metric families with the same name."""
    if existing is None:
        return incoming
    if _family_sample_count(incoming) == 0:
        return existing
    if _family_sample_count(existing) == 0:
        return incoming

    samples_by_key: dict[tuple[str, tuple[tuple[str, str], ...]], object] = {}
    for sample in existing.samples:
        samples_by_key[(sample.name, tuple(sorted(sample.labels.items())))] = sample
    for sample in incoming.samples:
        samples_by_key[(sample.name, tuple(sorted(sample.labels.items())))] = sample

    merged = type(existing)(existing.name, existing.documentation, existing.type)
    for sample in samples_by_key.values():
        merged.add_sample(
            sample.name,
            dict(sample.labels),
            sample.value,
            sample.timestamp,
            sample.exemplar,
        )
    return merged


class RemoteMetricsCollector:
    """Re-expose producer/consumer Prometheus text metrics on the local exporter."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._families: list = []

    def collect(self):
        with self._lock:
            return list(self._families)

    def update(self, producer_body: str | None, consumer_body: str | None) -> dict[str, object]:
        families_by_name: dict[str, object] = {}
        result = {
            "producer_metrics_up": producer_body is not None,
            "consumer_metrics_up": consumer_body is not None,
            "relayed_metric_count": 0,
        }

        for body in (producer_body, consumer_body):
            if not body:
                continue
            for family in text_string_to_metric_families(body):
                if not family.name.startswith(RELAY_PREFIX):
                    continue
                if family.name.startswith(EXCLUDE_PREFIX):
                    continue
                families_by_name[family.name] = _merge_metric_families(
                    families_by_name.get(family.name),
                    family,
                )

        families = list(families_by_name.values())
        result["relayed_metric_count"] = len(families)
        with self._lock:
            self._families = families
        return result


REMOTE_METRICS = RemoteMetricsCollector()
REGISTRY.register(REMOTE_METRICS)

_resolved_targets: dict[str, str] = {
    "producer": PRODUCER_URL,
    "consumer": CONSUMER_URL,
}


def _internal_readonly_hosts() -> list[str]:
    hosts: set[str] = set()
    for key, value in os.environ.items():
        if key.startswith("DS_RUNTIME_") and key.endswith("_PORT_8100_TCP_ADDR") and value:
            hosts.add(value)
    return sorted(hosts)


def _fetch_json_direct(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None


def _read_relay_text(path: Path) -> tuple[str | None, float | None]:
    if not path.is_file():
        return None, None
    try:
        age = time.time() - path.stat().st_mtime
        if age > RELAY_MAX_AGE:
            return None, age
        return path.read_text(encoding="utf-8"), age
    except OSError:
        return None, None


def _read_relay_health(path: Path) -> tuple[dict | None, float | None]:
    body, age = _read_relay_text(path)
    if body is None:
        return None, age
    try:
        payload = json.loads(body)
        return payload if isinstance(payload, dict) else None, age
    except json.JSONDecodeError:
        return None, age


def _resolve_scrape_targets() -> dict[str, str]:
    producer = os.getenv("PRODUCER_INTERNAL_URL", "").rstrip("/") or PRODUCER_URL
    consumer = os.getenv("CONSUMER_INTERNAL_URL", "").rstrip("/") or CONSUMER_URL

    discovered_producer = None
    discovered_consumer = None
    probe_results: list[dict[str, object]] = []
    for host in _internal_readonly_hosts():
        base = f"http://{host}:{READONLY_PORT}"
        health = _fetch_json_direct(f"{base}/health")
        probe_results.append(
            {
                "host": host,
                "url": f"{base}/health",
                "ok": health is not None,
                "kafka_ready": bool(health and "kafka_ready" in health),
                "stream_active": bool(health and "stream_active" in health),
            }
        )
        if not health:
            continue
        if "kafka_ready" in health:
            discovered_producer = base
        if "stream_active" in health:
            discovered_consumer = base

    if discovered_producer:
        producer = discovered_producer
    if discovered_consumer:
        consumer = discovered_consumer

    _resolved_targets["producer"] = producer
    _resolved_targets["consumer"] = consumer
    _resolved_targets["probe_results"] = probe_results
    return _resolved_targets


def _is_internal_url(url: str) -> bool:
    return url.startswith("http://") and not url.startswith("http://127.0.0.1")


def _ssl_context() -> ssl.SSLContext | None:
    if VERIFY_SSL:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def _urlopen(request: urllib.request.Request, timeout: int = 15):
    handlers = [_NoRedirect()]
    ctx = _ssl_context()
    if ctx is not None:
        handlers.append(urllib.request.HTTPSHandler(context=ctx))
    opener = urllib.request.build_opener(*handlers)
    return opener.open(request, timeout=timeout)


def _auth_strategies() -> list[tuple[str, dict[str, str]]]:
    strategies: list[tuple[str, dict[str, str]]] = []
    if AUTH_TOKEN:
        strategies.append(("bearer", {"Authorization": f"Bearer {AUTH_TOKEN}"}))
    if CDSW_API_KEY:
        token = base64.b64encode(f"{CDSW_API_KEY}:".encode()).decode()
        strategies.append(("basic", {"Authorization": f"Basic {token}"}))
    strategies.append(("none", {}))
    return strategies


def _fetch_bytes(url: str, accept: str) -> tuple[bytes | None, str | None]:
    global LAST_SCRAPE_ERROR
    if _is_internal_url(url) or url.startswith("http://127.0.0.1"):
        try:
            request = urllib.request.Request(url, headers={"Accept": accept})
            with urllib.request.urlopen(request, timeout=10) as response:
                body = response.read()
                if response.status >= 300:
                    LAST_SCRAPE_ERROR = f"{url} internal status={response.status}"
                    return None, None
                LAST_SCRAPE_ERROR = ""
                return body, "internal"
        except (urllib.error.URLError, TimeoutError, urllib.error.HTTPError) as exc:
            LAST_SCRAPE_ERROR = f"{url} internal error={exc}"
            return None, None

    last_error = ""
    for name, headers in _auth_strategies():
        request = urllib.request.Request(url, headers={"Accept": accept, **headers})
        try:
            with _urlopen(request) as response:
                body = response.read()
                content_type = response.headers.get("Content-Type", "")
                if response.status >= 300:
                    last_error = f"{url} auth={name} status={response.status}"
                    continue
                if "text/html" in content_type.lower() or body.lstrip().startswith(b"<!"):
                    last_error = f"{url} auth={name} got HTML (Knox login page?)"
                    continue
                LAST_SCRAPE_ERROR = ""
                return body, name
        except urllib.error.HTTPError as exc:
            last_error = f"{url} auth={name} http={exc.code}"
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = f"{url} auth={name} error={exc}"
    LAST_SCRAPE_ERROR = last_error
    return None, None


def _fetch_json(url: str) -> dict | None:
    body, _ = _fetch_bytes(url, "application/json")
    if body is None:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, ValueError):
        LAST_SCRAPE_ERROR = f"{url} invalid JSON"
        return None


def _fetch_text(url: str) -> str | None:
    body, _ = _fetch_bytes(url, "text/plain; version=0.0.4")
    if body is None:
        return None
    return body.decode("utf-8", errors="replace")


def scrape_once() -> None:
    targets = _resolve_scrape_targets()
    producer_base = targets["producer"]
    consumer_base = targets["consumer"]

    producer_metrics, producer_metrics_age = _read_relay_text(PRODUCER_METRICS_FILE)
    consumer_metrics, consumer_metrics_age = _read_relay_text(CONSUMER_METRICS_FILE)
    producer_health, producer_health_age = _read_relay_health(PRODUCER_HEALTH_FILE)
    consumer_health, consumer_health_age = _read_relay_health(CONSUMER_HEALTH_FILE)

    health_source = {
        "producer": "relay" if producer_health is not None else "http",
        "consumer": "relay" if consumer_health is not None else "http",
    }
    metrics_source = {
        "producer": "relay" if producer_metrics is not None else "http",
        "consumer": "relay" if consumer_metrics is not None else "http",
    }

    if producer_health is None:
        producer_health = _fetch_json(f"{producer_base}/health")
    if consumer_health is None:
        consumer_health = _fetch_json(f"{consumer_base}/health")
    if producer_metrics is None:
        producer_metrics = _fetch_text(f"{producer_base}/metrics")
    if consumer_metrics is None:
        consumer_metrics = _fetch_text(f"{consumer_base}/metrics")
    relay = REMOTE_METRICS.update(producer_metrics, consumer_metrics)

    PRODUCER_UP.set(1 if producer_health is not None else 0)
    CONSUMER_UP.set(1 if consumer_health is not None else 0)
    PRODUCER_METRICS_UP.set(1 if relay["producer_metrics_up"] else 0)
    CONSUMER_METRICS_UP.set(1 if relay["consumer_metrics_up"] else 0)

    if producer_health is not None:
        PRODUCER_KAFKA_READY.set(1 if producer_health.get("kafka_ready") else 0)
    else:
        PRODUCER_KAFKA_READY.set(0)

    if consumer_health is not None:
        CONSUMER_STREAM_ACTIVE.set(1 if consumer_health.get("stream_active") else 0)
        CONSUMER_TOTAL_RECEIVED.set(float(consumer_health.get("total_received") or 0))
    else:
        CONSUMER_STREAM_ACTIVE.set(0)

    status_payload: dict[str, object] = {
        "producer_url": PRODUCER_URL,
        "consumer_url": CONSUMER_URL,
        "producer_scrape_url": producer_base,
        "consumer_scrape_url": consumer_base,
        "relay_dir": str(RELAY_DIR),
        "relay_max_age_seconds": RELAY_MAX_AGE,
        "health_source": health_source,
        "metrics_source": metrics_source,
        "relay_file_ages_seconds": {
            "producer_metrics": producer_metrics_age,
            "consumer_metrics": consumer_metrics_age,
            "producer_health": producer_health_age,
            "consumer_health": consumer_health_age,
        },
        "internal_hosts_seen": _internal_readonly_hosts(),
        "internal_probe_results": targets.get("probe_results", []),
        "producer_up": producer_health is not None,
        "consumer_up": consumer_health is not None,
        "producer_metrics_up": relay["producer_metrics_up"],
        "consumer_metrics_up": relay["consumer_metrics_up"],
        "relayed_metric_count": relay["relayed_metric_count"],
        "producer_health": producer_health,
        "consumer_health": consumer_health,
        "auth_token_configured": bool(AUTH_TOKEN or CDSW_API_KEY),
        "last_scrape_error": LAST_SCRAPE_ERROR,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if STATUS_PATH.is_file():
        try:
            existing = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                for key in ("stack_ready", "exporter_ready", "prometheus_ready", "grafana_ready"):
                    if key in existing:
                        status_payload[key] = existing[key]
        except (OSError, json.JSONDecodeError):
            pass

    try:
        STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATUS_PATH.write_text(json.dumps(status_payload, indent=2), encoding="utf-8")
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
        f"(producer={PRODUCER_URL}, consumer={CONSUMER_URL}, relay=enabled)"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
