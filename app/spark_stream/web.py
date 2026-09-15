from __future__ import annotations

import os

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.live.routes import router as live_router
from app.metrics import register_metrics_middleware, register_metrics_route
from app.metrics_relay import start_metrics_relay
from app.spark_stream.kafka_stream import launch_streaming_thread
from app.spark_stream.store import STORE

LIVE_STATIC_DIR = Path(__file__).resolve().parent.parent / "live" / "static"


def _consumer_health_snapshot() -> dict[str, object]:
    snapshot = STORE.snapshot()
    healthy = snapshot["stream_active"] and not snapshot["last_error"]
    return {
        "status": "ok" if healthy else snapshot["spark_status"],
        "spark_status": snapshot["spark_status"],
        "stream_active": snapshot["stream_active"],
        "total_received": snapshot["total_received"],
        "last_error": snapshot["last_error"],
        "topic": os.getenv("KAFKA_TOPIC", "datapulse-events"),
        "consumer_mode": os.getenv("KAFKA_CONSUMER_MODE", "auto"),
        "live_version": "1.0.0",
        "buffer_size": snapshot.get("buffer_size", STORE.buffer_size),
        "buffer_used": snapshot.get("buffer_used", 0),
    }


@asynccontextmanager
async def lifespan(_app: FastAPI):
    launch_streaming_thread()
    start_metrics_relay("consumer", _consumer_health_snapshot)
    yield


app = FastAPI(title="DataPulse Live Events", version="1.0.0", lifespan=lifespan)
register_metrics_middleware(app)
register_metrics_route(app)
app.include_router(live_router)

if LIVE_STATIC_DIR.is_dir():
    app.mount("/live/static", StaticFiles(directory=str(LIVE_STATIC_DIR)), name="live-static")


@app.get("/health")
async def health() -> dict:
    return _consumer_health_snapshot()
