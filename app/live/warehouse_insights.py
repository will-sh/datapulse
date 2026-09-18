"""Load Insights events from Iceberg via Trino (with in-process cache)."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from app.console_oauth import get_console_oauth_settings
from app.event_labels import event_component
from app.spark_stream.store import StreamEvent
from app.trino_lakehouse import TrinoOAuthClient, TrinoSettings, get_trino_settings


def get_insights_trino_settings() -> TrinoSettings:
    """Lakehouse target for Insights — defaults to datapulse.events (not probe ICEBERG_*)."""
    base = get_trino_settings()
    return TrinoSettings(
        host=base.host,
        port=base.port,
        catalog=os.getenv("INSIGHTS_TRINO_CATALOG", base.catalog).strip() or base.catalog,
        schema=base.schema,
        database=os.getenv("INSIGHTS_ICEBERG_DATABASE", "datapulse").strip() or "datapulse",
        table=os.getenv("INSIGHTS_ICEBERG_TABLE", "events").strip() or "events",
        verify_ssl=base.verify_ssl,
        source=os.getenv("INSIGHTS_TRINO_SOURCE", "datapulse-consumer-insights").strip()
        or "datapulse-consumer-insights",
    )


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


INSIGHTS_CACHE_TTL_SEC = _env_int("INSIGHTS_TRINO_CACHE_SEC", 60)
INSIGHTS_TRINO_MAX_ROWS = _env_int("INSIGHTS_TRINO_MAX_ROWS", 10000)
INSIGHTS_DEFAULT_WINDOW_DAYS = _env_int("INSIGHTS_WINDOW_DAYS", 7)

_cache: dict[str, tuple[float, list[StreamEvent], dict[str, Any]]] = {}


def trino_oauth_configured() -> bool:
    settings = get_console_oauth_settings()
    return bool(settings.client_id and settings.client_secret)


def _parse_properties(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _parse_ingested_at(value: Any) -> float:
    if value is None:
        return time.time()
    if isinstance(value, (int, float)):
        # Trino may return epoch millis in some drivers; treat large ints as ms.
        numeric = float(value)
        if numeric > 1_000_000_000_000:
            return numeric / 1000.0
        if numeric > 1_000_000_000:
            return numeric
    text = str(value).strip()
    if not text:
        return time.time()
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(text.replace("Z", ""), fmt.replace("Z", ""))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            continue
    return time.time()


def _row_to_stream_event(row: dict[str, Any]) -> StreamEvent:
    properties = _parse_properties(row.get("properties_json"))
    event_name = str(row.get("event_name") or "unknown")
    page_path = row.get("page_path")
    if page_path is not None:
        page_path = str(page_path)
    elif event_name == "$pageview":
        path = properties.get("path")
        if path:
            page_path = str(path)

    timestamp = row.get("event_timestamp")
    if timestamp is not None:
        try:
            timestamp = int(timestamp)
        except (TypeError, ValueError):
            timestamp = None

    def _optional_str(key: str) -> str | None:
        value = row.get(key)
        return str(value) if value is not None else None

    received_at = _parse_ingested_at(row.get("ingested_at"))
    if timestamp and received_at == time.time():
        received_at = timestamp / 1000.0

    raw_payload = {
        "name": event_name,
        "properties": properties,
        "timestamp": timestamp,
        "user_id": row.get("user_id"),
        "anonymous_id": row.get("anonymous_id"),
        "session_id": row.get("session_id"),
        "page_path": page_path,
        "event_id": row.get("event_id"),
        "project_id": row.get("project_id"),
        "source": row.get("source"),
    }

    return StreamEvent(
        raw=json.dumps(raw_payload, ensure_ascii=False),
        name=event_name,
        timestamp=timestamp,
        user_id=_optional_str("user_id"),
        page_path=page_path,
        component=event_component(event_name, properties),
        properties=properties,
        event_id=_optional_str("event_id"),
        project_id=_optional_str("project_id"),
        anonymous_id=_optional_str("anonymous_id"),
        session_id=_optional_str("session_id"),
        source=_optional_str("source"),
        context=None,
        kafka_timestamp=None,
        received_at=received_at,
    )


def _dedupe_events(events: list[StreamEvent]) -> list[StreamEvent]:
    seen: set[str] = set()
    deduped: list[StreamEvent] = []
    for event in sorted(events, key=lambda item: item.received_at):
        event_id = event.event_id
        if event_id:
            if event_id in seen:
                continue
            seen.add(event_id)
        deduped.append(event)
    return deduped


def _fetch_trino_events(*, window_days: int) -> tuple[list[StreamEvent], dict[str, Any]]:
    window_days = max(1, min(window_days, 30))
    cache_key = f"trino:{window_days}"
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and now - cached[0] < INSIGHTS_CACHE_TTL_SEC:
        return cached[1], cached[2]

    settings = get_insights_trino_settings()
    client = TrinoOAuthClient(settings, access_token=None)
    sql = f"""
    SELECT
      event_id,
      project_id,
      event_name,
      properties_json,
      event_timestamp,
      source,
      user_id,
      anonymous_id,
      session_id,
      page_path,
      ingested_at
    FROM {settings.qualified_table}
    WHERE ingested_at >= current_timestamp - interval '{window_days}' day
    ORDER BY ingested_at ASC
    LIMIT {INSIGHTS_TRINO_MAX_ROWS}
    """
    result = client.execute(sql, catalog=settings.catalog, schema=settings.database)
    columns = [str(name) for name in (result.get("columns") or [])]
    rows: list[StreamEvent] = []
    for raw_row in result.get("rows") or []:
        if not isinstance(raw_row, list):
            continue
        row = {columns[i]: raw_row[i] for i in range(min(len(columns), len(raw_row)))}
        rows.append(_row_to_stream_event(row))

    events = _dedupe_events(rows)
    meta = {
        "qualified_table": settings.qualified_table,
        "rows_fetched": len(result.get("rows") or []),
        "events_after_dedupe": len(events),
        "window_days": window_days,
        "max_rows": INSIGHTS_TRINO_MAX_ROWS,
        "cache_ttl_sec": INSIGHTS_CACHE_TTL_SEC,
    }
    _cache[cache_key] = (now, events, meta)
    return events, meta


def load_insights_events(
    *,
    source: str = "auto",
    window_seconds: int | None = None,
    window_days: int | None = None,
    comparison: bool = False,
) -> tuple[list[StreamEvent], str, int, str, dict[str, Any] | None]:
    """Return events, resolved source, effective window seconds, note, warehouse meta."""
    from app.live.service import pipeline_out
    from app.spark_stream.store import STORE

    normalized = (source or "auto").strip().lower()
    days = window_days if window_days is not None else INSIGHTS_DEFAULT_WINDOW_DAYS
    days = max(1, min(days, 30))

    if normalized == "buffer":
        window = window_seconds if window_seconds is not None else _env_int("LIVE_STATS_WINDOW", 300)
        window = max(60, min(window, 3600))
        events = STORE.events_in_window(window)
        note = "Insights from the in-memory Kafka consumer buffer (real-time window)."
        return events, "buffer", window, note, None

    use_trino = normalized == "trino"
    if normalized == "auto":
        use_trino = trino_oauth_configured()

    if use_trino:
        try:
            fetch_days = min(days * 2, 30) if comparison else days
            events, meta = _fetch_trino_events(window_days=fetch_days)
            meta = {
                **meta,
                "fetch_days": fetch_days,
                "comparison_enabled": comparison and fetch_days > days,
            }
            window = days * 86400
            note = (
                f"Insights from Lakehouse table {meta['qualified_table']} "
                f"(last {days} day(s), deduped by event_id)."
            )
            if meta.get("comparison_enabled"):
                note += f" Comparison uses the prior {days} day(s)."
            elif comparison and fetch_days == days:
                note += " Period comparison uses the first vs second half of the window."
            return events, "trino", window, note, meta
        except Exception as exc:  # noqa: BLE001
            if normalized == "trino":
                raise
            window = window_seconds if window_seconds is not None else _env_int("LIVE_STATS_WINDOW", 300)
            window = max(60, min(window, 3600))
            events = STORE.events_in_window(window)
            note = (
                f"Lakehouse query unavailable ({exc}); showing in-memory buffer instead."
            )
            meta = {"fallback_error": str(exc), "pipeline": pipeline_out().model_dump()}
            return events, "buffer", window, note, meta

    window = window_seconds if window_seconds is not None else _env_int("LIVE_STATS_WINDOW", 300)
    window = max(60, min(window, 3600))
    events = STORE.events_in_window(window)
    note = "Insights from the in-memory Kafka consumer buffer (OAuth not configured for Trino)."
    return events, "buffer", window, note, None
