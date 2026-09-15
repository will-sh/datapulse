import os
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(os.getcwd())
MONITORING = ROOT / "monitoring"
PORT = int(os.environ["CDSW_READONLY_PORT"])
GRAFANA_INTERNAL_PORT = int(os.getenv("GRAFANA_INTERNAL_PORT", str(PORT + 1)))


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

    def do_GET(self) -> None:
        if self.grafana_ready:
            self._proxy_to_grafana()
            return
        if self.path in {"/", "/health", "/api/health"}:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"DataPulse monitoring stack is starting...")
            return
        self.send_response(503)
        self.end_headers()

    def do_POST(self) -> None:
        if self.grafana_ready:
            self._proxy_to_grafana()
            return
        self.send_response(503)
        self.end_headers()

    def _proxy_to_grafana(self) -> None:
        url = f"http://127.0.0.1:{GRAFANA_INTERNAL_PORT}{self.path}"
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                body = response.read()
                self.send_response(response.status)
                for key, value in response.headers.items():
                    if key.lower() in {"transfer-encoding", "connection"}:
                        continue
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)
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

os.environ.setdefault(
    "PRODUCER_URL",
    "https://datapulse-app.ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work",
)
os.environ.setdefault(
    "CONSUMER_URL",
    "https://datapulse-spark-consumer.ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work",
)
os.environ.setdefault("MONITORING_VERIFY_SSL", "false")
os.environ["GRAFANA_PORT"] = str(GRAFANA_INTERNAL_PORT)
os.environ["GRAFANA_ADDR"] = "127.0.0.1"

print(f"Starting gateway listener on 127.0.0.1:{PORT} ...")
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
    print(f"Grafana ready; gateway now proxying to 127.0.0.1:{GRAFANA_INTERNAL_PORT}")
else:
    print("Timed out waiting for Grafana; gateway remains in bootstrap mode", file=sys.stderr)

stack.wait()
raise SystemExit(stack.returncode or 0)
