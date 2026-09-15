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
DOMAIN = os.environ.get("CDSW_DOMAIN", "ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work")


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


class BootstrapHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path in {"/", "/health", "/api/health"}:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"DataPulse monitoring stack is starting...")
            return
        self.send_response(503)
        self.end_headers()

    def log_message(self, _format: str, *args) -> None:
        return


def start_bootstrap() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", PORT), BootstrapHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def binaries_ready() -> bool:
    return (
        (MONITORING / "prometheus" / "prometheus").is_file()
        and (MONITORING / "grafana" / "bin" / "grafana").is_file()
    )


load_env_file(ROOT / "datapulse.env")

os.environ.setdefault(
    "PRODUCER_URL",
    f"https://datapulse-app.{DOMAIN}",
)
os.environ.setdefault(
    "CONSUMER_URL",
    f"https://datapulse-spark-consumer.{DOMAIN}",
)
os.environ.setdefault("MONITORING_VERIFY_SSL", "false")
os.environ.setdefault(
    "GRAFANA_ROOT_URL",
    f"https://datapulse-monitoring.{DOMAIN}/",
)
os.environ["GRAFANA_PORT"] = str(PORT)
os.environ["GRAFANA_ADDR"] = "127.0.0.1"

print(f"Bootstrap listener on 127.0.0.1:{PORT} while dependencies start ...")
bootstrap = start_bootstrap()
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

print("Stopping bootstrap; starting Grafana directly on readonly port ...")
bootstrap.shutdown()
bootstrap.server_close()
time.sleep(0.2)

os.execvp("bash", ["bash", str(start_script)])
