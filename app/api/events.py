from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.event_labels import event_component
from app.kafka_settings import get_kafka_settings
from app.metrics import EVENTS_ACCEPTED, KAFKA_PUBLISH
from app.services.kafka_producer import publish_event

router = APIRouter(prefix="/api", tags=["events"])


class EventIn(BaseModel):
    event_id: str | None = Field(default=None, max_length=128)
    project_id: str | None = Field(default=None, max_length=128)
    name: str = Field(min_length=1, max_length=200)
    properties: dict[str, Any] = Field(default_factory=dict)
    timestamp: int | None = None
    source: str | None = None
    user_id: str | None = None
    anonymous_id: str | None = Field(default=None, max_length=128)
    session_id: str | None = Field(default=None, max_length=128)
    page_path: str | None = None
    context: dict[str, Any] | None = None


class EventAccepted(BaseModel):
    status: str
    topic: str
    event: dict[str, Any]


def _build_event_payload(payload: EventIn) -> dict[str, Any]:
    event: dict[str, Any] = {
        "name": payload.name,
        "properties": payload.properties,
        "timestamp": payload.timestamp or int(datetime.now(UTC).timestamp() * 1000),
        "source": payload.source or "datapulse-web",
        "user_id": payload.user_id,
        "page_path": payload.page_path,
    }
    if payload.event_id:
        event["event_id"] = payload.event_id
    if payload.project_id:
        event["project_id"] = payload.project_id
    if payload.anonymous_id:
        event["anonymous_id"] = payload.anonymous_id
    if payload.session_id:
        event["session_id"] = payload.session_id
    if payload.context:
        event["context"] = payload.context
    return event


async def _ingest_event(payload: EventIn) -> EventAccepted:
    settings = get_kafka_settings()
    if not settings.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka publishing is disabled (set KAFKA_ENABLED=true)",
        )
    if not settings.is_ready():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Kafka is not configured", "issues": settings.readiness_issues()},
        )

    event = _build_event_payload(payload)

    try:
        publish_event(event, settings)
    except RuntimeError as exc:
        KAFKA_PUBLISH.labels(status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc

    EVENTS_ACCEPTED.labels(
        event_name=payload.name,
        component=event_component(payload.name, payload.properties),
    ).inc()
    KAFKA_PUBLISH.labels(status="success").inc()
    return EventAccepted(status="accepted", topic=settings.topic, event=event)


@router.post("/events", status_code=status.HTTP_202_ACCEPTED, response_model=EventAccepted)
async def ingest_event(payload: EventIn) -> EventAccepted:
    return await _ingest_event(payload)


capture_router = APIRouter(prefix="/v1", tags=["capture"])


@capture_router.post("/capture", status_code=status.HTTP_202_ACCEPTED, response_model=EventAccepted)
async def capture_event(payload: EventIn) -> EventAccepted:
    return await _ingest_event(payload)
