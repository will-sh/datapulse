#!/usr/bin/env python3
"""End-to-end test: Playground events -> NFS relay -> Prometheus -> Grafana queries."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RELAY_DIR = ROOT / "monitoring" / "relay"
RESULTS_PATH = ROOT / "monitoring" / "e2e-results.json"

PLAYGROUND_EVENTS = [
    {"name": "button_clicked", "properties": {"button": "primary_action"}},
    {"name": "button_clicked", "properties": {"button": "secondary_action"}},
    {"name": "feature_used", "properties": {"feature": "export", "format": "csv"}},
    {"name": "demo_form_submitted", "properties": {"email": "test@example.com"}},
]

DASHBOARD_QUERIES = {
    "events_accepted_rate": "sum(rate(datapulse_events_accepted_total[1m]))",
    "events_consumed_rate": "sum(rate(datapulse_events_consumed_total[1m]))",
    "consumer_total_exporter": "datapulse_exporter_consumer_total_received",
    "consumer_total_metrics": "datapulse_consumer_total_received",
    "producer_up": "datapulse_exporter_producer_up",
    "consumer_up": "datapulse_exporter_consumer_up",
}


def http_json(url: str, method: str = "GET", payload: dict | None = None, timeout: int = 15) -> tuple[int, object]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"raw": body[:500]}
        return exc.code, parsed
    except Exception as exc:  # noqa: BLE001
        return 0, {"error": str(exc)}


def prom_query(base: str, query: str) -> dict:
    url = f"{base.rstrip('/')}/api/v1/query?{urllib.parse.urlencode({'query': query})}"
    status, payload = http_json(url)
    return {"status": status, "query": query, "response": payload}


def prom_value(result: dict) -> float | None:
    try:
        data = result["response"]["data"]
        if data.get("resultType") != "vector":
            return None
        results = data.get("result") or []
        if not results:
            return 0.0
        return float(results[0]["value"][1])
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def wait_until(name: str, url: str, timeout: int = 120) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, _ = http_json(url)
        if status == 200:
            print(f"  ready: {name}")
            return True
        time.sleep(1)
    print(f"  timeout: {name} ({url})")
    return False


def post_playground_events(producer_url: str, count_per_event: int = 2) -> dict:
    posted = []
    for event in PLAYGROUND_EVENTS:
        for _ in range(count_per_event):
            status, body = http_json(
                f"{producer_url.rstrip('/')}/api/events",
                method="POST",
                payload={
                    **event,
                    "timestamp": int(time.time() * 1000),
                    "source": "e2e-test",
                    "page_path": "/playground",
                },
            )
            posted.append({"event": event["name"], "status": status, "body": body})
            time.sleep(0.2)
    return {"posted": posted, "success_count": sum(1 for p in posted if p["status"] == 202)}


def check_relay_files() -> dict:
    files = {
        "producer.prom": RELAY_DIR / "producer.prom",
        "consumer.prom": RELAY_DIR / "consumer.prom",
        "producer.health.json": RELAY_DIR / "producer.health.json",
        "consumer.health.json": RELAY_DIR / "consumer.health.json",
    }
    result = {}
    now = time.time()
    for name, path in files.items():
        entry = {"path": str(path), "exists": path.is_file()}
        if path.is_file():
            stat = path.stat()
            entry["age_seconds"] = round(now - stat.st_mtime, 1)
            entry["size_bytes"] = stat.st_size
            if name.endswith(".prom"):
                lines = [
                    line
                    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()
                    if line.startswith("datapulse_") and not line.startswith("#")
                ]
                entry["metric_lines"] = len(lines)
                entry["sample"] = lines[:5]
            else:
                try:
                    entry["json"] = json.loads(path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    entry["json"] = None
        result[name] = entry
    return result


def load_status_json() -> dict | None:
    path = ROOT / "monitoring" / "status.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def evaluate_dashboard_queries(prom_url: str) -> dict:
    checks = {}
    for name, query in DASHBOARD_QUERIES.items():
        result = prom_query(prom_url, query)
        value = prom_value(result)
        checks[name] = {
            "query": query,
            "status": result["status"],
            "value": value,
            "ok": result["status"] == 200 and value is not None,
        }
    return checks


def run(args: argparse.Namespace) -> int:
    producer_url = args.producer_url.rstrip("/")
    consumer_url = args.consumer_url.rstrip("/")
    prom_url = args.prometheus_url.rstrip("/")
    grafana_url = args.grafana_url.rstrip("/")

    results: dict[str, object] = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "targets": {
            "producer_url": producer_url,
            "consumer_url": consumer_url,
            "prometheus_url": prom_url,
            "grafana_url": grafana_url,
        },
        "ok": True,
        "steps": {},
    }

    print("== Step 1: Wait for services ==")
    ready = {
        "producer_health": wait_until("producer", f"{producer_url}/health", args.timeout),
        "consumer_health": wait_until("consumer", f"{consumer_url}/health", args.timeout),
        "prometheus": wait_until("prometheus", f"{prom_url}/-/ready", args.timeout),
        "exporter": wait_until("exporter", "http://127.0.0.1:9191/metrics", args.timeout),
    }
    if args.check_grafana:
        ready["grafana"] = wait_until("grafana", f"{grafana_url}/api/health", args.timeout)
    results["steps"]["readiness"] = ready
    if not all(ready.values()):
        results["ok"] = False

    print("== Step 2: Post Playground-like events ==")
    if args.post_events:
        post_result = post_playground_events(producer_url, args.events_per_type)
        results["steps"]["post_events"] = post_result
        print(f"  posted {post_result['success_count']} / {len(post_result['posted'])} accepted")
        if post_result["success_count"] == 0:
            results["ok"] = False
        time.sleep(max(args.settle_seconds, 15))
    else:
        results["steps"]["post_events"] = {"skipped": True}

    print("== Step 3: Check NFS relay files ==")
    relay = check_relay_files()
    results["steps"]["relay_files"] = relay
    relay_ok = all(relay[name]["exists"] for name in ("producer.prom", "consumer.prom"))
    if relay_ok:
        for name in ("producer.prom", "consumer.prom"):
            age = relay[name].get("age_seconds", 999)
            if age > 30:
                relay_ok = False
    results["steps"]["relay_ok"] = relay_ok
    if not relay_ok:
        results["ok"] = False

    print("== Step 4: Check exporter status.json ==")
    status = load_status_json()
    results["steps"]["monitoring_status"] = status
    if status:
        print(
            "  relayed_metric_count=",
            status.get("relayed_metric_count"),
            "metrics_source=",
            status.get("metrics_source"),
        )
        if status.get("relayed_metric_count", 0) == 0:
            results["ok"] = False

    print("== Step 5: Query Prometheus (Grafana dashboard PromQL) ==")
    # Give Prometheus a scrape or two after metrics change.
    time.sleep(12)
    dashboard = evaluate_dashboard_queries(prom_url)
    results["steps"]["dashboard_queries"] = dashboard

    accepted = dashboard["events_accepted_rate"]["value"] or 0
    consumed = dashboard["events_consumed_rate"]["value"] or 0
    consumer_total = dashboard["consumer_total_metrics"]["value"] or 0
    charts_ok = accepted > 0 or consumed > 0 or consumer_total > 0
    results["steps"]["charts_have_data"] = charts_ok
    print(f"  accepted_rate={accepted} consumed_rate={consumed} consumer_total={consumer_total}")
    if args.require_chart_data and not charts_ok:
        results["ok"] = False

    if args.check_grafana:
        print("== Step 6: Grafana datasource query ==")
        ds_status, ds_payload = http_json(f"{grafana_url}/api/datasources")
        results["steps"]["grafana_datasources"] = {"status": ds_status, "payload": ds_payload}

    results["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nWrote {RESULTS_PATH}")
    print(json.dumps(results, indent=2))
    return 0 if results["ok"] else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-url", default=os.getenv("PRODUCER_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--consumer-url", default=os.getenv("CONSUMER_URL", "http://127.0.0.1:8081"))
    parser.add_argument("--prometheus-url", default=os.getenv("PROMETHEUS_URL", "http://127.0.0.1:9090"))
    parser.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "http://127.0.0.1:8100"))
    parser.add_argument("--timeout", type=int, default=int(os.getenv("WAIT_TIMEOUT", "120")))
    parser.add_argument("--settle_seconds", type=int, default=20)
    parser.add_argument("--events-per-type", type=int, default=2)
    parser.add_argument("--no-post-events", action="store_true")
    parser.add_argument("--no-grafana", action="store_true")
    parser.add_argument("--no-require-chart-data", action="store_true")
    args = parser.parse_args()
    args.post_events = not args.no_post_events
    args.check_grafana = not args.no_grafana
    args.require_chart_data = not args.no_require_chart_data
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
