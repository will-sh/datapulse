import os
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(os.getcwd())


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

os.environ.setdefault("CDSW_APP_POLLING_ENDPOINT", "/")
PORT = os.environ["CDSW_READONLY_PORT"]
KAFKA_VERSION = os.getenv("KAFKA_VERSION", "3.9.0")
KAFKA_SCALA = os.getenv("KAFKA_SCALA", "2.13")
KAFKA_CACHE_DIR = Path(os.getenv("KAFKA_CACHE_DIR", str(ROOT / ".cache" / "kafka")))
KAFKA_HOME = Path(
    os.getenv(
        "KAFKA_HOME",
        str(KAFKA_CACHE_DIR / f"kafka_{KAFKA_SCALA}-{KAFKA_VERSION}"),
    ),
)


def ensure_kafka_cli() -> None:
    producer = KAFKA_HOME / "bin" / "kafka-console-producer.sh"
    if producer.is_file():
        os.environ.setdefault("KAFKA_HOME", str(KAFKA_HOME))
        return

    archive_url = (
        f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/"
        f"kafka_{KAFKA_SCALA}-{KAFKA_VERSION}.tgz"
    )
    print(f"Downloading Kafka {KAFKA_VERSION} CLI to {KAFKA_HOME}...")
    KAFKA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        urllib.request.urlretrieve(archive_url, tmp_path)
        with tarfile.open(tmp_path) as archive:
            archive.extractall(KAFKA_CACHE_DIR)
        tmp_path.unlink(missing_ok=True)

    os.environ["KAFKA_HOME"] = str(KAFKA_HOME)
    print(f"Kafka CLI ready at {KAFKA_HOME}")


if os.getenv("KAFKA_ENABLED", "").lower() in {"1", "true", "yes", "on"}:
    ensure_kafka_cli()

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "-r", str(ROOT / "requirements.txt")],
    env={**os.environ, "PIP_USER": "1"},
)

# Bind loopback only: engine-init already listens on the pod IP:CDSW_READONLY_PORT.
subprocess.call(
    [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        PORT,
    ],
    cwd=str(ROOT),
)
