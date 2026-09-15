import os
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(os.getcwd())
PORT = os.environ["CDSW_READONLY_PORT"]
SPARK_CONNECT_ZIP = Path("/opt/spark-connect/spark_connect.zip")
SPARK_CONNECT_NATIVE = Path("/opt/spark-connect/native")
SPARK_CONNECT_DIR = Path("/tmp/spark-connect-unpack")
KAFKA_VERSION = os.getenv("KAFKA_VERSION", "3.9.0")
KAFKA_SCALA = os.getenv("KAFKA_SCALA", "2.13")
KAFKA_HOME = Path(
    os.getenv("KAFKA_HOME", str(Path.home() / ".cache" / "kafka" / f"kafka_{KAFKA_SCALA}-{KAFKA_VERSION}")),
)


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def ensure_kafka_cli() -> None:
    consumer = KAFKA_HOME / "bin" / "kafka-console-consumer.sh"
    if consumer.is_file():
        os.environ.setdefault("KAFKA_HOME", str(KAFKA_HOME))
        return

    archive_url = (
        f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/"
        f"kafka_{KAFKA_SCALA}-{KAFKA_VERSION}.tgz"
    )
    print(f"Downloading Kafka {KAFKA_VERSION} CLI to {KAFKA_HOME}...")
    KAFKA_HOME.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        urllib.request.urlretrieve(archive_url, tmp_path)
        with tarfile.open(tmp_path) as archive:
            archive.extractall(KAFKA_HOME.parent)
        tmp_path.unlink(missing_ok=True)
    os.environ["KAFKA_HOME"] = str(KAFKA_HOME)


def configure_spark_connect_python() -> None:
    if not SPARK_CONNECT_ZIP.is_file():
        print("Spark Connect runtime addon not found at /opt/spark-connect/spark_connect.zip")
        return

    SPARK_CONNECT_DIR.mkdir(parents=True, exist_ok=True)
    marker = SPARK_CONNECT_DIR / ".extracted"
    if not marker.is_file():
        print(f"Extracting Spark Connect client from {SPARK_CONNECT_ZIP}...")
        with zipfile.ZipFile(SPARK_CONNECT_ZIP) as archive:
            archive.extractall(SPARK_CONNECT_DIR)
        marker.write_text("ok", encoding="utf-8")

    user_site = (
        Path.home()
        / ".local"
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    )
    pythonpath_parts: list[str] = []
    if SPARK_CONNECT_NATIVE.is_dir():
        pythonpath_parts.append(str(SPARK_CONNECT_NATIVE))
    pythonpath_parts.append(str(SPARK_CONNECT_DIR))
    if user_site.is_dir():
        pythonpath_parts.append(str(user_site))
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        pythonpath_parts.append(existing)
    os.environ["PYTHONPATH"] = ":".join(pythonpath_parts)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    print(f"PySpark paths: native={SPARK_CONNECT_NATIVE.is_dir()} client={SPARK_CONNECT_DIR}")


def detect_spark_connect_url() -> None:
    if os.getenv("SPARK_REMOTE") or os.getenv("SPARK_CONNECT_URL"):
        return

    host = os.getenv("SPARK_CONNECT_HOST") or os.getenv("CDSW_IP_ADDRESS") or "127.0.0.1"
    engine_id = os.getenv("CDSW_ENGINE_ID", "").upper()
    port = None
    if engine_id:
        port = os.getenv(f"DS_RUNTIME_{engine_id}_SERVICE_PORT_SPARK")
    if not port:
        for key, value in os.environ.items():
            if key.startswith("DS_RUNTIME_") and key.endswith("_SERVICE_PORT_SPARK"):
                port = value
                break
    port = port or os.getenv("SPARK_CONNECT_PORT", "20049")
    url = f"sc://{host}:{port}"
    os.environ["SPARK_CONNECT_URL"] = url
    print(f"Using Spark Connect at {url}")
    print(
        "Spark Connect env: "
        f"CDSW_IP={os.getenv('CDSW_IP_ADDRESS')} "
        f"ENGINE={engine_id or 'missing'} "
        f"PORT={port} "
        f"ZIP={SPARK_CONNECT_ZIP.is_file()} "
        f"NATIVE={SPARK_CONNECT_NATIVE.is_dir()}"
    )


load_env_file(ROOT / "datapulse.env")
os.environ.setdefault("CDSW_APP_POLLING_ENDPOINT", "/")
configure_spark_connect_python()
detect_spark_connect_url()
ensure_kafka_cli()

pip_env = {**os.environ, "PIP_USER": "1", "HOME": str(Path.home())}
for req_file in ("requirements-spark.txt", "requirements.txt"):
    req_path = ROOT / req_file
    if req_path.is_file():
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-q", "--user", "-r", str(req_path)],
            env=pip_env,
        )

subprocess.call(
    [
        sys.executable,
        "-m",
        "uvicorn",
        "app.spark_stream.web:app",
        "--host",
        "127.0.0.1",
        "--port",
        PORT,
    ],
    cwd=str(ROOT),
    env=os.environ.copy(),
)
