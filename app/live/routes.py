from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.spark_stream.store import STORE
from app.live import service
from app.live.insights import compute_insights

LIVE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(LIVE_DIR / "templates"))

router = APIRouter(tags=["live"])


@router.get("/", include_in_schema=False)
async def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/live", status_code=302)


@router.get("/live", response_class=HTMLResponse)
async def live_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="live.html",
        context={
            "topic": service._topic(),  # noqa: SLF001
            "console_url": "",
            "active_tab": "live",
        },
    )


@router.get("/insights", response_class=HTMLResponse)
async def insights_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="insights.html",
        context={
            "topic": service._topic(),  # noqa: SLF001
            "active_tab": "insights",
        },
    )


@router.get("/api/insights/summary")
async def insights_summary(
    window_seconds: int | None = Query(default=None, ge=60, le=3600),
):
    return compute_insights(window_seconds=window_seconds)


@router.get("/live/legacy", response_class=HTMLResponse, include_in_schema=False)
async def live_legacy_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="legacy.html",
        context={
            "topic": service._topic(),  # noqa: SLF001
        },
    )


@router.get("/api/live/snapshot")
async def live_snapshot(
    name: str | None = None,
    user_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    return service.live_snapshot(name=name, user_id=user_id, limit=limit, offset=offset)


@router.get("/api/live/events")
async def live_events(
    name: str | None = None,
    user_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    return service.live_events(name=name, user_id=user_id, limit=limit, offset=offset)


@router.get("/api/live/stats")
async def live_stats(window_seconds: int | None = Query(default=None, ge=1, le=3600)):
    return service.live_stats(window_seconds=window_seconds)


@router.get("/api/live/names")
async def live_names():
    return {"names": STORE.distinct_names()}


@router.get("/api/messages")
async def legacy_messages() -> JSONResponse:
    """Deprecated: use /api/live/snapshot instead."""
    return JSONResponse(service.legacy_messages_snapshot())
