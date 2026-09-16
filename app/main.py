from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.events import capture_router, router as events_router
from app.console_catalog import (
    blueprint_plans,
    catalog_meta,
    marketplace_engines,
    marketplace_experiences,
    playground_blueprints,
    playground_deploy_targets,
)
from app.config import get_settings
from app.kafka_settings import get_kafka_settings
from app.metrics import KAFKA_READY, register_metrics_middleware, register_metrics_route
from app.metrics_relay import start_metrics_relay

BASE_DIR = Path(__file__).resolve().parent.parent


def _producer_health_snapshot() -> dict[str, object]:
    kafka = get_kafka_settings()
    kafka_ready = kafka.is_ready()
    KAFKA_READY.set(1 if kafka_ready else 0)
    return {
        "status": "ok",
        "kafka_enabled": kafka.enabled,
        "kafka_ready": kafka_ready,
        "kafka_issues": kafka.readiness_issues() if kafka.enabled else [],
    }


app = FastAPI(title="DataPulse", version="1.0.0")
app.include_router(events_router)
app.include_router(capture_router)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
register_metrics_middleware(app)
register_metrics_route(app)


@app.on_event("startup")
async def _start_metrics_relay() -> None:
    start_metrics_relay("producer", _producer_health_snapshot)


templates = Jinja2Templates(directory=BASE_DIR / "templates")


def template_context(request: Request, **extra: object) -> dict:
    settings = get_settings()
    return {
        "request": request,
        "settings": settings,
        "active_path": request.url.path,
        **extra,
    }


@app.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        template_context(request),
    )


@app.get("/features", response_class=HTMLResponse)
async def features(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "features.html",
        template_context(
            request,
            catalog=catalog_meta(),
            experiences=marketplace_experiences(),
            engines=marketplace_engines(),
        ),
    )


@app.get("/pricing", response_class=HTMLResponse)
async def pricing(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "pricing.html",
        template_context(
            request,
            catalog=catalog_meta(),
            plans=blueprint_plans(),
        ),
    )


@app.get("/playground", response_class=HTMLResponse)
async def playground(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "playground.html",
        template_context(
            request,
            catalog=catalog_meta(),
            deploy_targets=playground_deploy_targets(),
            blueprint_options=playground_blueprints(),
        ),
    )


@app.get("/health")
async def health() -> dict[str, str | bool | list[str]]:
    return _producer_health_snapshot()  # type: ignore[return-value]
