from __future__ import annotations

import os
from typing import Any

from app.live.schemas import (
    LiveEventOut,
    LiveEventsResponse,
    LiveMetaOut,
    LivePipelineOut,
    LiveSnapshotResponse,
    LiveStatsResponse,
)
from app.spark_stream.store import STORE, StreamEvent


def _topic() -> str:
    return os.getenv("KAFKA_TOPIC", "datapulse-events")


def _consumer_mode() -> str:
    return os.getenv("KAFKA_CONSUMER_MODE", "auto")


def pipeline_out() -> LivePipelineOut:
    snapshot = STORE.pipeline_snapshot()
    return LivePipelineOut(
        stream_active=snapshot["stream_active"],
        spark_status=str(snapshot["spark_status"]),
        total_received=int(snapshot["total_received"]),
        last_error=snapshot["last_error"],
        topic=_topic(),
        buffer_size=int(snapshot["buffer_size"]),
        buffer_used=int(snapshot["buffer_used"]),
        consumer_mode=_consumer_mode(),
    )


def _event_out(event: StreamEvent) -> LiveEventOut:
    payload = event.to_live_dict()
    return LiveEventOut(**payload)


def live_events(
    *,
    name: str | None = None,
    user_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> LiveEventsResponse:
    events, total = STORE.query(name=name, user_id=user_id, limit=limit, offset=offset)
    return LiveEventsResponse(
        pipeline=pipeline_out(),
        events=[_event_out(event) for event in events],
        meta=LiveMetaOut(
            limit=limit,
            offset=offset,
            returned=len(events),
            total_matched=total,
        ),
    )


def live_snapshot(
    *,
    name: str | None = None,
    user_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> LiveSnapshotResponse:
    events_response = live_events(
        name=name,
        user_id=user_id,
        limit=limit,
        offset=offset,
    )
    return LiveSnapshotResponse(
        pipeline=events_response.pipeline,
        stats=STORE.stats(),
        events=events_response.events,
        meta=events_response.meta,
    )


def live_stats(window_seconds: int | None = None) -> LiveStatsResponse:
    return LiveStatsResponse(
        stats=STORE.stats(window_seconds=window_seconds),
        pipeline=pipeline_out(),
    )


def legacy_messages_snapshot() -> dict[str, Any]:
    return STORE.snapshot()
