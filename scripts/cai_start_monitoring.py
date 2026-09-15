import asyncio
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path

ROOT = Path(os.getcwd())
MONITORING = ROOT / "monitoring"
PORT = int(os.environ["CDSW_READONLY_PORT"])
GRAFANA_INTERNAL_PORT = int(os.getenv("GRAFANA_INTERNAL_PORT", str(PORT + 1)))
DOMAIN = os.environ.get("CDSW_DOMAIN", "ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work")

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}
STRIP_RESPONSE_HEADERS = HOP_BY_HOP_HEADERS | {
    "content-encoding",
    "content-length",
    "transfer-encoding",
}

grafana_ready = False


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def binaries_ready() -> bool:
    return (
        (MONITORING / "prometheus" / "prometheus").is_file()
        and (MONITORING / "grafana" / "bin" / "grafana").is_file()
    )


def wait_for_grafana(timeout: int = 300) -> bool:
    deadline = time.time() + timeout
    url = f"http://127.0.0.1:{GRAFANA_INTERNAL_PORT}/api/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if response.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(2)
    return False


def proxy_to_grafana(method: str, path: str, headers: list[tuple[str, str]], body: bytes | None) -> tuple[int, list[tuple[str, str]], bytes]:
    url = f"http://127.0.0.1:{GRAFANA_INTERNAL_PORT}{path}"
    forward_headers = {"Accept-Encoding": "identity"}
    for key, value in headers:
        lower = key.lower()
        if lower in HOP_BY_HOP_HEADERS or lower in {"host", "accept-encoding"}:
            continue
        forward_headers[key] = value

    request = urllib.request.Request(
        url,
        data=body,
        headers=forward_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = response.read()
            out_headers = [
                (key, value)
                for key, value in response.headers.items()
                if key.lower() not in STRIP_RESPONSE_HEADERS
            ]
            return response.status, out_headers, payload
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        out_headers = [
            (key, value)
            for key, value in exc.headers.items()
            if key.lower() not in STRIP_RESPONSE_HEADERS
        ]
        return exc.code, out_headers, payload
    except Exception:
        return 502, [], b""


def launch_stack() -> None:
    global grafana_ready

    start_script = MONITORING / "start.sh"
    download_script = MONITORING / "download.sh"
    if not start_script.is_file():
        print(f"Missing monitoring start script: {start_script}", file=sys.stderr)
        return

    if not binaries_ready():
        if not download_script.is_file():
            print(f"Missing monitoring download script: {download_script}", file=sys.stderr)
            return
        print("Downloading Prometheus and Grafana binaries ...")
        subprocess.check_call(["bash", str(download_script)], cwd=str(ROOT))

    print("Launching monitoring/start.sh ...")
    stack = subprocess.Popen(["bash", str(start_script)], cwd=str(ROOT))
    if wait_for_grafana():
        grafana_ready = True
        print("Grafana ready; gateway proxy enabled with gzip-safe forwarding.")
    else:
        print("Timed out waiting for Grafana; gateway stays in bootstrap mode.", file=sys.stderr)
    stack.wait()
    if stack.returncode:
        print(f"monitoring/start.sh exited with code {stack.returncode}", file=sys.stderr)


load_env_file(ROOT / "datapulse.env")

os.environ.setdefault("PRODUCER_URL", f"https://datapulse-app.{DOMAIN}")
os.environ.setdefault("CONSUMER_URL", f"https://datapulse-spark-consumer.{DOMAIN}")
os.environ.setdefault("MONITORING_VERIFY_SSL", "false")
os.environ.setdefault("GRAFANA_ROOT_URL", f"https://datapulse-monitoring.{DOMAIN}/")
os.environ["GRAFANA_PORT"] = str(GRAFANA_INTERNAL_PORT)
os.environ["GRAFANA_ADDR"] = "127.0.0.1"

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "-r", str(ROOT / "requirements.txt")],
    env={**os.environ, "PIP_USER": "1"},
)

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response


@asynccontextmanager
async def lifespan(_app: FastAPI):
    thread = threading.Thread(target=launch_stack, daemon=True)
    thread.start()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
@app.get("/api/health")
async def health() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok" if grafana_ready else "starting",
            "grafana_ready": grafana_ready,
        }
    )


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def gateway(full_path: str, request: Request) -> Response:
    path = f"/{full_path}" if full_path else "/"
    if not grafana_ready:
        if request.method == "GET" and path == "/":
            return PlainTextResponse("DataPulse monitoring stack is starting...", status_code=200)
        return Response(status_code=503)

    body = await request.body()
    raw_headers = list(request.headers.items())
    status, headers, payload = await asyncio.to_thread(
        proxy_to_grafana,
        request.method,
        path,
        raw_headers,
        body or None,
    )
    return Response(content=payload, status_code=status, headers=dict(headers))


print(f"Starting uvicorn gateway on 127.0.0.1:{PORT} (Grafana internal :{GRAFANA_INTERNAL_PORT}) ...")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=PORT,
        log_level="info",
        access_log=False,
    )
