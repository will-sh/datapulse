from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.events import capture_router, router as events_router
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
            features=[
                {
                    "id": "streams_messaging",
                    "icon": "KM",
                    "category": "Streaming",
                    "title": "Streams Messaging",
                    "description": "企业级 Kafka 事件流骨干 — 本 Demo 的 DataPulse 事件总线即基于此服务。",
                },
                {
                    "id": "streaming_analytics",
                    "icon": "SA",
                    "category": "Streaming",
                    "title": "Streaming Analytics",
                    "description": "实时事件处理与连续分析，驱动 Live Events 与运营看板。",
                },
                {
                    "id": "lakehouse_engine",
                    "icon": "LH",
                    "category": "Analytics",
                    "title": "Lakehouse Engine",
                    "description": "Apache Iceberg + Trino 零拷贝联邦分析，承载漏斗与留存查询。",
                },
                {
                    "id": "cloudera_ai",
                    "icon": "AI",
                    "category": "AI",
                    "title": "Cloudera AI",
                    "description": "企业级 AI Workbench — 私有化 LLM 开发、调优与部署。",
                },
                {
                    "id": "data_engineering",
                    "icon": "DE",
                    "category": "Data Flow",
                    "title": "Data Engineering",
                    "description": "Spark 批/流处理与数据转换 pipeline，连接 Kafka 与 Iceberg 入湖。",
                },
                {
                    "id": "data_visualization",
                    "icon": "DV",
                    "category": "Analytics",
                    "title": "Data Visualization",
                    "description": "自助 BI 与业务仪表盘 — 对应 Grafana / CDV 行为分析视图。",
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
                    "id": "connected_enterprise_intelligence",
                    "name": "Connected Enterprise Intelligence",
                    "description": "统一实时流与核心企业系统",
                    "outcome": "连接 Kafka 实时事件与业务上下文，驱动 AI Agent 与实时分析。",
                    "features": [
                        "Streams Messaging + Lakehouse",
                        "实时 AI Agent 上下文层",
                        "DataPulse 行为事件入湖",
                    ],
                    "popular": False,
                },
                {
                    "id": "sovereign_ai",
                    "name": "Sovereign AI",
                    "description": "私有化 AI 与零外部暴露",
                    "outcome": "在主权环境部署 NL Search 与 AI Agent，敏感数据不出域。",
                    "features": [
                        "Cloudera AI Workbench",
                        "Vector DB + RAG",
                        "统一治理与审计血缘",
                    ],
                    "popular": True,
                },
                {
                    "id": "multi_cloud_lakehouse",
                    "name": "Multi-cloud Lakehouse",
                    "description": "跨云零拷贝湖仓",
                    "outcome": "Iceberg 开放标准，跨环境就地查询，避免冗余存储与 egress 成本。",
                    "features": [
                        "Iceberg 表 + Trino SQL",
                        "Multi-cloud 联邦查询",
                        "CDV 自助分析",
                    ],
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
