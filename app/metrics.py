from __future__ import annotations

import time
from typing import Callable

from fastapi import Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

EVENTS_ACCEPTED = Counter(
    "datapulse_events_accepted_total",
    "Events accepted by the producer API",
    ["event_name"],
)
KAFKA_PUBLISH = Counter(
    "datapulse_kafka_publish_total",
    "Kafka publish attempts from the producer",
    ["status"],
)
KAFKA_PUBLISH_DURATION = Histogram(
    "datapulse_kafka_publish_duration_seconds",
    "Kafka publish latency from the producer",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 45.0),
)
KAFKA_READY = Gauge(
    "datapulse_kafka_ready",
    "Whether Kafka OAuth producer configuration is ready (1=yes, 0=no)",
)
HTTP_REQUESTS = Counter(
    "datapulse_http_requests_total",
    "HTTP requests handled by the application",
    ["method", "path", "status"],
)
HTTP_REQUEST_DURATION = Histogram(
    "datapulse_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

EVENTS_CONSUMED = Counter(
    "datapulse_events_consumed_total",
    "Events consumed from Kafka",
    ["event_name"],
)
CONSUMER_STREAM_ACTIVE = Gauge(
    "datapulse_consumer_stream_active",
    "Whether the consumer stream loop is active (1=yes, 0=no)",
)
CONSUMER_TOTAL_RECEIVED = Gauge(
    "datapulse_consumer_total_received",
    "Total events received by the in-memory consumer store",
)
CONSUMER_ERRORS = Counter(
    "datapulse_consumer_errors_total",
    "Consumer-side errors",
    ["source"],
)


def metrics_response() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def register_metrics_middleware(app) -> None:
    @app.middleware("http")
    async def observe_requests(request: Request, call_next: Callable):
        if request.url.path == "/metrics":
            return await call_next(request)

        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            path = request.url.path
            method = request.method
            HTTP_REQUESTS.labels(method=method, path=path, status=str(status_code)).inc()
            HTTP_REQUEST_DURATION.labels(method=method, path=path).observe(time.perf_counter() - start)


def register_metrics_route(app) -> None:
    @app.get("/metrics")
    async def metrics() -> Response:
        return metrics_response()
