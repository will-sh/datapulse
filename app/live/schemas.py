from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class LiveEventOut(BaseModel):
    name: str
    timestamp: int | None = None
    user_id: str | None = None
    page_path: str | None = None
    component: str
    properties: dict[str, Any] = Field(default_factory=dict)
    kafka_timestamp: str | None = None
    received_at: float


class LiveMetaOut(BaseModel):
    limit: int
    offset: int
    returned: int
    total_matched: int


class LivePipelineOut(BaseModel):
    stream_active: bool
    spark_status: str
    total_received: int
    last_error: str | None = None
    topic: str
    buffer_size: int
    buffer_used: int
    consumer_mode: str


class LiveEventsResponse(BaseModel):
    pipeline: LivePipelineOut
    events: list[LiveEventOut]
    meta: LiveMetaOut


class LiveSnapshotResponse(BaseModel):
    pipeline: LivePipelineOut
    stats: dict[str, Any]
    events: list[LiveEventOut]
    meta: LiveMetaOut


class LiveStatsResponse(BaseModel):
    stats: dict[str, Any]
    pipeline: LivePipelineOut
