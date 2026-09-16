#!/usr/bin/env python3
"""Sync AWC Console marketplace catalog (engines, blueprints, experiences) to data/console_catalog.json."""

from __future__ import annotations

import base64
import http.cookiejar
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "console_catalog.json"

CONSOLE_URL = os.getenv("CONSOLE_URL", "https://console.readygo.a70735.test.cldr.work").rstrip("/")
CONSOLE_USER = os.getenv("CONSOLE_USER", os.getenv("CONSOLE_ADMIN_USER", "admin"))
CONSOLE_PASSWORD = os.getenv("CONSOLE_PASSWORD", os.getenv("CONSOLE_ADMIN_PASSWORD", ""))
KNOX_URL = os.getenv("KNOX_URL", "").rstrip("/")


def knox_base() -> str:
    if KNOX_URL:
        return KNOX_URL
    host = urlparse(CONSOLE_URL).hostname or ""
    parts = host.split(".", 1)
    if len(parts) == 2:
        return f"https://knox.{parts[1]}"
    raise RuntimeError(f"Cannot derive Knox URL from CONSOLE_URL={CONSOLE_URL}")


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def login(session_path: Path) -> None:
    if not CONSOLE_PASSWORD:
        raise RuntimeError("CONSOLE_PASSWORD (or CONSOLE_ADMIN_PASSWORD) is required")

    original_url = f"{CONSOLE_URL}/marketplace"
    auth = base64.b64encode(f"{CONSOLE_USER}:{CONSOLE_PASSWORD}".encode()).decode()
    login_url = f"{knox_base()}/gateway/knox-cdpsso/api/v1/websso?originalUrl={original_url}"
    request = urllib.request.Request(
        login_url,
        method="POST",
        headers={"Authorization": f"Basic {auth}"},
    )
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookie_jar),
        urllib.request.HTTPSHandler(context=ssl_context()),
    )
    response = opener.open(request, timeout=60)
    response.read()
    jar_text = [f"{cookie.name}={cookie.value}" for cookie in cookie_jar]
    session_path.write_text("\n".join(jar_text), encoding="utf-8")


def fetch_json(path: str, cookie_header: str) -> object:
    url = f"{CONSOLE_URL}{path}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Cookie": cookie_header,
        },
    )
    with urllib.request.urlopen(request, timeout=60, context=ssl_context()) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    session_path = Path("/tmp/console-catalog-cookies.txt")
    login(session_path)
    cookie_header = session_path.read_text(encoding="utf-8").replace("\n", "; ")

    catalog = {
        "synced_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "console_url": CONSOLE_URL,
        "experiences": fetch_json("/api/v0/console/experiences", cookie_header),
        "engines": fetch_json("/api/v0/console/engines", cookie_header),
        "blueprints": fetch_json("/api/v0/console/blueprints", cookie_header),
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "path": str(OUT_PATH),
                "experiences": len(catalog["experiences"]),
                "engines": len(catalog["engines"]),
                "blueprints": len(catalog["blueprints"]),
                "synced_at": catalog["synced_at"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
