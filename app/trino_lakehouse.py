"""Trino access for Lakehouse using Console OAuth (same token as Kafka)."""

from __future__ import annotations

import json
import os
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from app.console_oauth import fetch_access_token


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class TrinoSettings:
    host: str
    port: int
    catalog: str
    schema: str
    database: str
    table: str
    verify_ssl: bool
    source: str

    @property
    def qualified_table(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"


def get_trino_settings() -> TrinoSettings:
    catalog = _env("TRINO_CATALOG", _env("ICEBERG_CATALOG", "iceberg"))
    database = _env("TRINO_SCHEMA", _env("ICEBERG_DATABASE", "datapulse"))
    table = _env("TRINO_TABLE", _env("ICEBERG_TABLE", "events"))
    return TrinoSettings(
        host=_env(
            "TRINO_HOST",
            "lakehouse-bp-556b64.cldr-csk-lakehouse.a70735.test.cldr.work",
        ),
        port=int(_env("TRINO_PORT", "443")),
        catalog=catalog,
        schema=_env("TRINO_DEFAULT_SCHEMA", "information_schema"),
        database=database,
        table=table,
        verify_ssl=_env("TRINO_VERIFY_SSL", "false").lower() in {"1", "true", "yes"},
        source=_env("TRINO_SOURCE", "datapulse-lakehouse-job"),
    )


def _ssl_context(verify_ssl: bool) -> ssl.SSLContext | None:
    if verify_ssl:
        return ssl.create_default_context()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class TrinoOAuthClient:
    """Minimal Trino REST client using Console OAuth Bearer token."""

    def __init__(self, settings: TrinoSettings | None = None, access_token: str | None = None):
        self.settings = settings or get_trino_settings()
        token_payload = fetch_access_token() if access_token is None else {"access_token": access_token}
        self.access_token = str(token_payload["access_token"])
        self.token_payload = token_payload

    def _base_url(self) -> str:
        scheme = "https" if self.settings.port == 443 else "http"
        return f"{scheme}://{self.settings.host}:{self.settings.port}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, object]:
        url = path if path.startswith("http") else f"{self._base_url()}{path}"
        merged = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "User-Agent": self.settings.source,
        }
        if headers:
            merged.update(headers)
        request = urllib.request.Request(url, data=data, headers=merged, method=method)
        try:
            with urllib.request.urlopen(
                request,
                timeout=60,
                context=_ssl_context(self.settings.verify_ssl),
            ) as response:
                body = response.read().decode("utf-8", errors="replace")
                return response.status, json.loads(body) if body else {}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                parsed: object = json.loads(body)
            except json.JSONDecodeError:
                parsed = {"raw": body[:1000]}
            return exc.code, parsed

    def info(self) -> dict[str, Any]:
        status, payload = self._request("GET", "/v1/info")
        if status != 200 or not isinstance(payload, dict):
            raise RuntimeError(f"Trino /v1/info failed: HTTP {status} {payload}")
        return payload

    def execute(
        self,
        sql: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
            "X-Trino-Source": self.settings.source,
            "X-Trino-Catalog": catalog or self.settings.catalog,
            "X-Trino-Schema": schema or self.settings.schema,
        }
        status, payload = self._request(
            "POST",
            "/v1/statement",
            data=sql.encode("utf-8"),
            headers=headers,
        )
        if status != 200 or not isinstance(payload, dict):
            raise RuntimeError(f"Trino statement submit failed: HTTP {status} {payload}")

        state: dict[str, Any] = payload
        rows: list[list[Any]] = []
        columns: list[dict[str, Any]] = []
        for _ in range(60):
            if state.get("error"):
                err = state["error"]
                raise RuntimeError(
                    f"Trino query failed: {err.get('message') or err.get('errorName') or err}"
                )
            if state.get("columns") and not columns:
                columns = state["columns"]
            if state.get("data"):
                rows.extend(state["data"])
            stats = state.get("stats") or {}
            if stats.get("state") == "FINISHED":
                break
            next_uri = state.get("nextUri")
            if not next_uri:
                break
            _, state_raw = self._request("GET", next_uri)
            if not isinstance(state_raw, dict):
                raise RuntimeError(f"Unexpected Trino poll payload: {state_raw}")
            state = state_raw
            time.sleep(0.15)

        return {
            "columns": [column.get("name") for column in columns],
            "rows": rows,
            "stats": state.get("stats"),
        }


def probe_trino() -> dict[str, Any]:
    settings = get_trino_settings()
    client = TrinoOAuthClient(settings)
    info = client.info()
    probe: dict[str, Any] = {
        "trino_host": settings.host,
        "trino_port": settings.port,
        "catalog": settings.catalog,
        "database": settings.database,
        "table": settings.table,
        "oauth_token_url": _env("KAFKA_TOKEN_URL"),
        "oauth_client_id_set": bool(_env("KAFKA_CLIENT_ID")),
        "info": {
            "nodeId": info.get("nodeId"),
            "state": info.get("state"),
            "nodeVersion": info.get("nodeVersion"),
            "coordinator": info.get("coordinator"),
        },
    }
    try:
        result = client.execute("SELECT 1 AS ok")
        probe["select_ok"] = result
    except Exception as exc:  # noqa: BLE001
        probe["select_ok_error"] = str(exc)
    return probe


def bootstrap_via_trino() -> dict[str, Any]:
    settings = get_trino_settings()
    client = TrinoOAuthClient(settings)
    client.info()
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {settings.qualified_table} (
      event_id varchar,
      project_id varchar,
      event_name varchar,
      properties_json varchar,
      event_timestamp bigint,
      source varchar,
      user_id varchar,
      anonymous_id varchar,
      session_id varchar,
      page_path varchar,
      kafka_partition integer,
      kafka_offset bigint,
      kafka_timestamp timestamp(6),
      ingested_at timestamp(6),
      raw_payload varchar
    )
    """
    steps: list[dict[str, Any]] = []
    for sql in (
        f"CREATE SCHEMA IF NOT EXISTS {settings.catalog}.{settings.database}",
        ddl,
    ):
        steps.append({"sql": sql.strip(), "result": client.execute(sql)})
    return {"qualified_table": settings.qualified_table, "steps": steps}


def verify_via_trino(limit: int = 10) -> dict[str, Any]:
    settings = get_trino_settings()
    client = TrinoOAuthClient(settings)
    count = client.execute(f"SELECT count(*) AS row_count FROM {settings.qualified_table}")
    sample = client.execute(
        f"""
        SELECT event_name, user_id, anonymous_id, event_timestamp, ingested_at
        FROM {settings.qualified_table}
        ORDER BY ingested_at DESC
        LIMIT {limit}
        """
    )
    return {
        "qualified_table": settings.qualified_table,
        "count": count,
        "sample": sample,
    }
