"""Fetch Console Access Key OAuth tokens (same flow as Kafka OAUTHBEARER)."""

from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from functools import lru_cache

DEFAULT_TOKEN_URL = (
    "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token"
)


@dataclass(frozen=True)
class ConsoleOAuthSettings:
    client_id: str
    client_secret: str
    token_url: str


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@lru_cache
def get_console_oauth_settings() -> ConsoleOAuthSettings:
    return ConsoleOAuthSettings(
        client_id=_env("KAFKA_CLIENT_ID"),
        client_secret=_env("KAFKA_CLIENT_SECRET"),
        token_url=_env("KAFKA_TOKEN_URL", DEFAULT_TOKEN_URL),
    )


def _ssl_context() -> ssl.SSLContext:
    verify = _env("CONSOLE_OAUTH_VERIFY_SSL", "false").lower() in {"1", "true", "yes"}
    if verify:
        return ssl.create_default_context()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def fetch_access_token(settings: ConsoleOAuthSettings | None = None) -> dict[str, object]:
    settings = settings or get_console_oauth_settings()
    if not settings.client_id or not settings.client_secret:
        raise RuntimeError("KAFKA_CLIENT_ID and KAFKA_CLIENT_SECRET are required for Console OAuth")

    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": settings.client_id,
            "client_secret": settings.client_secret,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        settings.token_url,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30, context=_ssl_context()) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Console OAuth token request failed: HTTP {exc.code} {detail}") from exc

    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError(f"Console OAuth token response missing access_token: {payload}")
    return payload
