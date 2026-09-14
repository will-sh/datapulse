from __future__ import annotations

import os

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from app.spark_stream.kafka_stream import launch_streaming_thread
from app.spark_stream.store import STORE


@asynccontextmanager
async def lifespan(_app: FastAPI):
    launch_streaming_thread()
    yield


app = FastAPI(title="DataPulse Spark Kafka Consumer", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
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
    }


@app.get("/api/messages")
async def messages() -> JSONResponse:
    return JSONResponse(STORE.snapshot())


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    topic = os.getenv("KAFKA_TOPIC", "datapulse-events")
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Spark Kafka Stream Viewer</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 24px; background: #0b1020; color: #e5e7eb; }}
    h1 {{ margin-bottom: 8px; }}
    .meta {{ color: #94a3b8; margin-bottom: 20px; }}
    .status {{ padding: 12px 16px; border-radius: 10px; background: #111827; margin-bottom: 16px; }}
    .ok {{ color: #34d399; }}
    .err {{ color: #f87171; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border-bottom: 1px solid #1f2937; padding: 10px 8px; text-align: left; vertical-align: top; }}
    th {{ color: #93c5fd; }}
    code {{ white-space: pre-wrap; word-break: break-word; }}
    .empty {{ color: #64748b; padding: 24px 0; }}
  </style>
</head>
<body>
  <h1>Spark Structured Streaming → Kafka</h1>
  <p class="meta">Topic: <strong>{topic}</strong> · 每 3 秒自动刷新</p>
  <div id="status" class="status">加载中…</div>
  <table>
    <thead>
      <tr><th>收到时间</th><th>Kafka 时间</th><th>内容</th></tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
  <script>
    function fmt(ts) {{
      if (!ts) return "-";
      return new Date(ts * 1000).toLocaleString("zh-CN");
    }}
    async function refresh() {{
      const res = await fetch("/api/messages");
      const data = await res.json();
      const status = document.getElementById("status");
      const rows = document.getElementById("rows");
      const active = data.stream_active ? "ok" : "err";
      status.innerHTML = `
        <div>Spark 状态: <span class="${{active}}">${{data.spark_status}}</span></div>
        <div>已消费: <strong>${{data.total_received}}</strong> 条</div>
        ${{data.last_error ? `<div class="err">错误: ${{data.last_error}}</div>` : ""}}
      `;
      if (!data.events.length) {{
        rows.innerHTML = '<tr><td colspan="3" class="empty">暂无消息。请在 datapulse-app 行为实验室触发事件。</td></tr>';
        return;
      }}
      rows.innerHTML = data.events.map((event) => {{
        let pretty = event.message;
        try {{ pretty = JSON.stringify(JSON.parse(event.message), null, 2); }} catch (_) {{}}
        return `<tr>
          <td>${{fmt(event.received_at)}}</td>
          <td>${{event.kafka_timestamp || "-"}}</td>
          <td><code>${{pretty}}</code></td>
        </tr>`;
      }}).join("");
    }}
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""
    return HTMLResponse(html)
