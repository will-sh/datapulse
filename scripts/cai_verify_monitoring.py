#!/usr/bin/env python3
"""Run inside CAI to verify monitoring stack end-to-end."""

from __future__ import annotations

import json
import os
import ssl
import sys
import urllib.error
import urllib.request

ROOT = os.getcwd()
OUT = os.path.join(ROOT, "monitoring-e2e-results.json")

PUBLIC_BASE = os.getenv("CDSW_DOMAIN", "ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work")
PRODUCER_URL = os.getenv(
    "PRODUCER_URL",
    f"https://datapulse-app.{PUBLIC_BASE}",
).rstrip("/")
CONSUMER_URL = os.getenv(
    "CONSUMER_URL",
    f"https://datapulse-spark-consumer.{PUBLIC_BASE}",
).rstrip("/")
MONITORING_URL = os.getenv(
    "MONITORING_URL",
    f"https://datapulse-monitoring.{PUBLIC_BASE}",
).rstrip("/")
VERIFY_SSL = os.getenv("MONITORING_VERIFY_SSL", "false").lower() in {"1", "true", "yes"}

TOKENS = {
    "cdsw_apiv2_key": os.getenv("CDSW_APIV2_KEY", ""),
    "cdsw_api_key": os.getenv("CDSW_API_KEY", ""),
    "monitoring_bearer": os.getenv("MONITORING_BEARER_TOKEN", os.getenv("WORKBENCH_API_KEY", "")),
}

URL_CANDIDATES = {
    "producer_health": [f"{PRODUCER_URL}/health"],
    "consumer_health": [f"{CONSUMER_URL}/health"],
    "producer_metrics": [f"{PRODUCER_URL}/metrics"],
    "consumer_metrics": [f"{CONSUMER_URL}/metrics"],
    "grafana_health": [f"{MONITORING_URL}/api/health"],
}


def ssl_context() -> ssl.SSLContext | None:
    if VERIFY_SSL:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def fetch(url: str, accept: str = "application/json", token: str = "") -> tuple[int, str]:
    request = urllib.request.Request(url, headers={"Accept": accept})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=20, context=ssl_context()) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def metric_lines(body: str, prefix: str = "datapulse_") -> list[str]:
    return [line for line in body.splitlines() if line.startswith(prefix) and not line.startswith("#")]


results: dict[str, object] = {
    "ok": True,
    "checks": {},
    "env": {k: v for k, v in os.environ.items() if k.startswith(("CDSW", "PRODUCER", "CONSUMER", "MONITORING"))},
}

for name, urls in URL_CANDIDATES.items():
    accept = "text/plain" if name.endswith("_metrics") else "application/json"
    entry: dict[str, object] = {"tried": []}
    success = False
    for token_name, token in TOKENS.items():
        if not token:
            continue
        for url in urls:
            status, body = fetch(url, accept=accept, token=token)
            entry["tried"].append({"url": url, "token": token_name, "status": status})
            if status != 200:
                continue
            entry["status"] = status
            entry["url"] = url
            entry["token"] = token_name
            if name.endswith("_metrics"):
                lines = metric_lines(body)
                entry["metric_count"] = len(lines)
                entry["sample"] = lines[:8]
            else:
                try:
                    entry["json"] = json.loads(body)
                except json.JSONDecodeError:
                    entry["body_preview"] = body[:200]
            success = True
            break
        if success:
            break
    if not success:
        entry["status"] = entry["tried"][-1]["status"] if entry["tried"] else 0
        results["ok"] = False
    results["checks"][name] = entry

status_path = os.path.join(ROOT, "monitoring", "status.json")
if os.path.isfile(status_path):
    with open(status_path, encoding="utf-8") as handle:
        results["monitoring_status"] = json.load(handle)

with open(OUT, "w", encoding="utf-8") as handle:
    json.dump(results, handle, indent=2)

print(json.dumps(results, indent=2))
sys.exit(0)
