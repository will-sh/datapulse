import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


class GatewayHandler(BaseHTTPRequestHandler):
    grafana_ready = False
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        self._handle_request()

    def do_POST(self) -> None:
        self._handle_request()

    def do_PUT(self) -> None:
        self._handle_request()

    def do_PATCH(self) -> None:
        self._handle_request()

    def do_DELETE(self) -> None:
        self._handle_request()

    def do_OPTIONS(self) -> None:
        self._handle_request()

    def _handle_request(self) -> None:
        # CAI readiness probes hit /health (and sometimes /) on CDSW_READONLY_PORT.
        # Never proxy these — Grafana may redirect or 404 and the app stays STARTING.
        if self.command == "GET" and self.path in {"/health", "/api/health"}:
            self._send_health()
            return
        if self.grafana_ready:
            self._proxy_to_grafana()
            return
        if self.command == "GET" and self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"DataPulse monitoring stack is starting...")
            return
        self.send_response(503)
        self.end_headers()

    def _send_health(self) -> None:
        if self.grafana_ready:
            body = b'{"status":"ok","grafana_ready":true}'
            content_type = "application/json; charset=utf-8"
        else:
            body = b'{"status":"starting","grafana_ready":false}'
            content_type = "application/json; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy_to_grafana(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(content_length) if content_length > 0 else None
        url = f"http://127.0.0.1:{GRAFANA_INTERNAL_PORT}{self.path}"

        headers = {"Accept-Encoding": "identity"}
        for key, value in self.headers.items():
            lower = key.lower()
            if lower in HOP_BY_HOP_HEADERS or lower in {"host", "accept-encoding"}:
                continue
            headers[key] = value

        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=self.command,
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = response.read()
                self.send_response(response.status)
                for key, value in response.headers.items():
                    if key.lower() in STRIP_RESPONSE_HEADERS:
                        continue
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            self.send_response(exc.code)
            for key, value in exc.headers.items():
                if key.lower() in STRIP_RESPONSE_HEADERS:
                    continue
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception:
            self.send_response(502)
            self.end_headers()

    def log_message(self, _format: str, *args) -> None:
        return


def start_gateway() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", PORT), GatewayHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


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


load_env_file(ROOT / "datapulse.env")

os.environ.setdefault("PRODUCER_URL", f"https://datapulse-app.{DOMAIN}")
os.environ.setdefault("CONSUMER_URL", f"https://datapulse-spark-consumer.{DOMAIN}")
os.environ.setdefault("MONITORING_VERIFY_SSL", "false")
os.environ.setdefault("GRAFANA_ROOT_URL", f"https://datapulse-monitoring.{DOMAIN}/")
os.environ["GRAFANA_PORT"] = str(GRAFANA_INTERNAL_PORT)
os.environ["GRAFANA_ADDR"] = "127.0.0.1"

print(f"Gateway listening on 127.0.0.1:{PORT} (Grafana internal :{GRAFANA_INTERNAL_PORT}) ...")
gateway = start_gateway()
time.sleep(0.5)

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "--user", "prometheus-client>=0.21.0"],
    env={**os.environ, "PIP_USER": "1"},
)

start_script = MONITORING / "start.sh"
download_script = MONITORING / "download.sh"
if not start_script.is_file():
    raise SystemExit(f"Missing monitoring start script: {start_script}")

if not binaries_ready():
    if not download_script.is_file():
        raise SystemExit(f"Missing monitoring download script: {download_script}")
    print("Downloading Prometheus and Grafana binaries ...")
    subprocess.check_call(["bash", str(download_script)], cwd=str(ROOT))

print("Launching monitoring/start.sh ...")
stack = subprocess.Popen(["bash", str(start_script)], cwd=str(ROOT))

if wait_for_grafana():
    GatewayHandler.grafana_ready = True
    print("Grafana ready; gateway proxy enabled with gzip-safe forwarding.")
else:
    print("Timed out waiting for Grafana; gateway stays in bootstrap mode.", file=sys.stderr)

stack.wait()
raise SystemExit(stack.returncode or 0)
