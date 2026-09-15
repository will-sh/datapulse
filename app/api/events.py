from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.kafka_settings import get_kafka_settings
from app.metrics import EVENTS_ACCEPTED, KAFKA_PUBLISH
from app.services.kafka_producer import publish_event

router = APIRouter(prefix="/api", tags=["events"])


class EventIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    properties: dict[str, Any] = Field(default_factory=dict)
    timestamp: int | None = None
    source: str | None = None
    user_id: str | None = None
    page_path: str | None = None


class EventAccepted(BaseModel):
    status: str
    topic: str
    event: dict[str, Any]


@router.post("/events", status_code=status.HTTP_202_ACCEPTED, response_model=EventAccepted)
async def ingest_event(payload: EventIn) -> EventAccepted:
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

    event = {
        "name": payload.name,
        "properties": payload.properties,
        "timestamp": payload.timestamp or int(datetime.now(UTC).timestamp() * 1000),
        "source": payload.source or "datapulse-web",
        "user_id": payload.user_id,
        "page_path": payload.page_path,
    }

    try:
        publish_event(event, settings)
    except RuntimeError as exc:
        KAFKA_PUBLISH.labels(status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc

    EVENTS_ACCEPTED.labels(event_name=payload.name).inc()
    KAFKA_PUBLISH.labels(status="success").inc()
    return EventAccepted(status="accepted", topic=settings.topic, event=event)
