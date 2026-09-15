import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.getcwd())
MONITORING = ROOT / "monitoring"


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


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
os.environ.setdefault("GRAFANA_PORT", os.environ["CDSW_READONLY_PORT"])

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "--user", "prometheus-client>=0.21.0"],
    env={**os.environ, "PIP_USER": "1"},
)

start_script = MONITORING / "start.sh"
if not start_script.is_file():
    raise SystemExit(f"Missing monitoring start script: {start_script}")

os.execvp("bash", ["bash", str(start_script)])
