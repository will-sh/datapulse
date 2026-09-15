import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.getcwd())
MONITORING = ROOT / "monitoring"
PORT = os.environ["CDSW_READONLY_PORT"]
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


def binaries_ready() -> bool:
    return (
        (MONITORING / "prometheus" / "prometheus").is_file()
        and (MONITORING / "grafana" / "bin" / "grafana").is_file()
    )


load_env_file(ROOT / "datapulse.env")

os.environ.setdefault("CDSW_APP_POLLING_ENDPOINT", "/")
os.environ.setdefault("PRODUCER_URL", f"https://datapulse-app.{DOMAIN}")
os.environ.setdefault("CONSUMER_URL", f"https://datapulse-spark-consumer.{DOMAIN}")
os.environ.setdefault("MONITORING_VERIFY_SSL", "false")
os.environ["GRAFANA_PORT"] = PORT
os.environ["GRAFANA_ADDR"] = "127.0.0.1"

print(
    "Starting DataPulse monitoring "
    f"(CDSW_APP_POLLING_ENDPOINT={os.environ.get('CDSW_APP_POLLING_ENDPOINT')}, "
    f"GRAFANA_PORT={PORT}) ..."
)

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "--user", "prometheus-client>=0.21.0"],
    env={**os.environ, "PIP_USER": "1"},
)

start_script = MONITORING / "start.sh"
stop_script = MONITORING / "stop.sh"
download_script = MONITORING / "download.sh"
if not start_script.is_file():
    raise SystemExit(f"Missing monitoring start script: {start_script}")

if stop_script.is_file():
    subprocess.call(["bash", str(stop_script)], cwd=str(ROOT))

if not binaries_ready():
    if not download_script.is_file():
        raise SystemExit(f"Missing monitoring download script: {download_script}")
    print("Downloading Prometheus and Grafana binaries ...")
    subprocess.check_call(["bash", str(download_script)], cwd=str(ROOT))

print("Launching monitoring/start.sh (Grafana on CDSW_READONLY_PORT) ...")
sys.exit(subprocess.call(["bash", str(start_script)], cwd=str(ROOT)))
