from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.events import router as events_router
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
            features=[
                {
                    "id": "realtime",
                    "title": "实时事件流",
                    "description": "毫秒级采集用户点击、浏览与转化行为，右侧面板即时展示。",
                },
                {
                    "id": "funnels",
                    "title": "漏斗分析",
                    "description": "追踪用户从访问到转化的完整路径，发现流失节点。",
                },
                {
                    "id": "cohorts",
                    "title": "用户分群",
                    "description": "按行为特征划分用户群体，精准定位高价值用户。",
                },
                {
                    "id": "alerts",
                    "title": "异常告警",
                    "description": "关键指标波动时自动通知，快速响应业务变化。",
                },
                {
                    "id": "privacy",
                    "title": "隐私合规",
                    "description": "支持数据脱敏与 GDPR 合规配置，保护用户隐私。",
                },
                {
                    "id": "export",
                    "title": "数据导出",
                    "description": "一键导出事件数据，对接 BI 工具与数据仓库。",
                },
            ],
        ),
    )


@app.get("/pricing", response_class=HTMLResponse)
async def pricing(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "pricing.html",
        template_context(
            request,
            plans=[
                {
                    "id": "starter",
                    "name": "入门版",
                    "price": "免费",
                    "description": "适合个人项目与小型 Demo",
                    "features": ["每月 1 万事件", "基础事件面板", "7 天数据保留"],
                    "popular": False,
                },
                {
                    "id": "pro",
                    "name": "专业版",
                    "price": "¥299/月",
                    "description": "适合成长型团队",
                    "features": [
                        "每月 100 万事件",
                        "漏斗与留存分析",
                        "90 天数据保留",
                        "邮件告警",
                    ],
                    "popular": True,
                },
                {
                    "id": "enterprise",
                    "name": "企业版",
                    "price": "联系销售",
                    "description": "适合大规模生产环境",
                    "features": ["无限事件", "专属技术支持", "SLA 保障", "私有化部署"],
                    "popular": False,
                },
            ],
        ),
    )


@app.get("/playground", response_class=HTMLResponse)
async def playground(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "playground.html",
        template_context(request),
    )


@app.get("/health")
async def health() -> dict[str, str | bool | list[str]]:
    return _producer_health_snapshot()  # type: ignore[return-value]
