from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any

from app.event_labels import event_component
from app.metrics import (
    CONSUMER_ERRORS,
    CONSUMER_STREAM_ACTIVE,
    CONSUMER_TOTAL_RECEIVED,
    EVENTS_CONSUMED,
)

LIVE_BUFFER_SIZE = int(os.getenv("LIVE_BUFFER_SIZE", "200"))
LIVE_STATS_WINDOW = int(os.getenv("LIVE_STATS_WINDOW", "300"))


@dataclass(frozen=True)
class StreamEvent:
    raw: str
    name: str
    timestamp: int | None
    user_id: str | None
    page_path: str | None
    component: str
    properties: dict[str, Any] = field(default_factory=dict)
    event_id: str | None = None
    project_id: str | None = None
    anonymous_id: str | None = None
    session_id: str | None = None
    source: str | None = None
    context: dict[str, Any] | None = None
    kafka_timestamp: str | None = None
    received_at: float = 0.0

    def to_legacy_dict(self) -> dict[str, Any]:
        """Shape used by deprecated /api/messages clients."""
        return {
            "message": self.raw,
            "name": self.name,
            "kafka_timestamp": self.kafka_timestamp,
            "received_at": self.received_at,
        }

    def to_live_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "timestamp": self.timestamp,
            "user_id": self.user_id,
            "page_path": self.page_path,
            "component": self.component,
            "properties": self.properties,
            "kafka_timestamp": self.kafka_timestamp,
            "received_at": self.received_at,
        }
        if self.event_id:
            payload["event_id"] = self.event_id
        if self.project_id:
            payload["project_id"] = self.project_id
        if self.anonymous_id:
            payload["anonymous_id"] = self.anonymous_id
        if self.session_id:
            payload["session_id"] = self.session_id
        if self.source:
            payload["source"] = self.source
        if self.context:
            payload["context"] = self.context
        return payload


def _coerce_properties(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _parse_stream_event(raw: str, kafka_timestamp: str | None = None) -> StreamEvent:
    received_at = time.time()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return StreamEvent(
            raw=raw,
            name="unknown",
            timestamp=None,
            user_id=None,
            page_path=None,
            component="-",
            properties={},
            kafka_timestamp=kafka_timestamp,
            received_at=received_at,
        )

    if not isinstance(payload, dict):
        return StreamEvent(
            raw=raw,
            name="unknown",
            timestamp=None,
            user_id=None,
            page_path=None,
            component="-",
            properties={},
            kafka_timestamp=kafka_timestamp,
            received_at=received_at,
        )

    name = str(payload.get("name") or "unknown")
    properties = _coerce_properties(payload.get("properties"))
    timestamp = payload.get("timestamp")
    if timestamp is not None:
        try:
            timestamp = int(timestamp)
        except (TypeError, ValueError):
            timestamp = None

    user_id = payload.get("user_id")
    if user_id is not None:
        user_id = str(user_id)

    page_path = payload.get("page_path")
    if page_path is not None:
        page_path = str(page_path)

    def _optional_str(key: str) -> str | None:
        value = payload.get(key)
        return str(value) if value is not None else None

    context = payload.get("context")
    if context is not None and not isinstance(context, dict):
        context = None

    return StreamEvent(
        raw=raw,
        name=name,
        timestamp=timestamp,
        user_id=user_id,
        page_path=page_path,
        component=event_component(name, properties),
        properties=properties,
        event_id=_optional_str("event_id"),
        project_id=_optional_str("project_id"),
        anonymous_id=_optional_str("anonymous_id"),
        session_id=_optional_str("session_id"),
        source=_optional_str("source"),
        context=context,
        kafka_timestamp=kafka_timestamp,
        received_at=received_at,
    )


class EventStore:
    def __init__(self, max_events: int = LIVE_BUFFER_SIZE) -> None:
        self._max_events = max_events
        self._events: deque[StreamEvent] = deque(maxlen=max_events)
        self._lock = threading.Lock()
        self.total_received = 0
        self.last_error: str | None = None
        self.stream_active = False
        self.spark_status = "initializing"

    @property
    def buffer_size(self) -> int:
        return self._max_events

    def ingest(self, raw: str, kafka_timestamp: str | None = None) -> StreamEvent:
        event = _parse_stream_event(raw, kafka_timestamp)
        with self._lock:
            self._events.appendleft(event)
            self.total_received += 1
            EVENTS_CONSUMED.labels(
                event_name=event.name,
                component=event.component,
            ).inc()
            CONSUMER_TOTAL_RECEIVED.set(self.total_received)
            CONSUMER_STREAM_ACTIVE.set(1 if self.stream_active else 0)
        return event

    def set_error(self, message: str) -> None:
        with self._lock:
            self.last_error = message
            self.stream_active = False
            self.spark_status = "error"
            CONSUMER_ERRORS.labels(source="stream").inc()
            CONSUMER_STREAM_ACTIVE.set(0)

    def set_status(self, status: str, active: bool | None = None) -> None:
        with self._lock:
            self.spark_status = status
            if active is not None:
                self.stream_active = active
            CONSUMER_STREAM_ACTIVE.set(1 if self.stream_active else 0)
            CONSUMER_TOTAL_RECEIVED.set(self.total_received)

    def _filtered_events(
        self,
        *,
        name: str | None = None,
        user_id: str | None = None,
    ) -> list[StreamEvent]:
        with self._lock:
            events = list(self._events)
        if name:
            events = [event for event in events if event.name == name]
        if user_id:
            events = [event for event in events if event.user_id == user_id]
        return events

    def query(
        self,
        *,
        name: str | None = None,
        user_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[StreamEvent], int]:
        limit = max(1, min(limit, self._max_events))
        offset = max(0, offset)
        filtered = self._filtered_events(name=name, user_id=user_id)
        return filtered[offset : offset + limit], len(filtered)

    def distinct_names(self) -> list[str]:
        with self._lock:
            names = {event.name for event in self._events if event.name}
        return sorted(names)

    def stats(self, window_seconds: int | None = None) -> dict[str, Any]:
        window = window_seconds if window_seconds is not None else LIVE_STATS_WINDOW
        window = max(1, window)
        cutoff = time.time() - window
        with self._lock:
            recent = [event for event in self._events if event.received_at >= cutoff]
        by_name = Counter(event.name for event in recent)
        rate_per_minute = (len(recent) / window) * 60 if recent else 0.0
        return {
            "window_seconds": window,
            "events_in_window": len(recent),
            "rate_per_minute": round(rate_per_minute, 2),
            "by_name": [
                {"name": name, "count": count}
                for name, count in by_name.most_common()
            ],
            "note": "Stats reflect in-memory buffer only, not historical data.",
        }

    def pipeline_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "total_received": self.total_received,
                "last_error": self.last_error,
                "stream_active": self.stream_active,
                "spark_status": self.spark_status,
                "buffer_size": self._max_events,
                "buffer_used": len(self._events),
            }

    def snapshot(self) -> dict[str, Any]:
        pipeline = self.pipeline_snapshot()
        with self._lock:
            events = [event.to_legacy_dict() for event in self._events]
        return {
            **pipeline,
            "events": events,
        }


STORE = EventStore()
