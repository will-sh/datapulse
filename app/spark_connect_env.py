from __future__ import annotations

import os
import socket
import subprocess
import sys
import zipfile
from pathlib import Path

SPARK_CONNECT_DIR = Path("/tmp/spark-connect-unpack")


def resolve_spark_connect_paths() -> tuple[Path | None, Path | None, Path | None]:
    """Return (zip_path, native_dir, install_root) for the Spark Connect runtime addon."""
    candidates: list[tuple[Path, Path | None, Path]] = []

    default_root = Path("/opt/spark-connect")
    candidates.append((default_root / "spark_connect.zip", default_root / "native", default_root))

    runtime_addons = Path("/runtime-addons")
    if runtime_addons.is_dir():
        for entry in sorted(runtime_addons.iterdir()):
            if "sparkconnect" not in entry.name.lower():
                continue
            root = entry / "opt/spark-connect"
            candidates.append((root / "spark_connect.zip", root / "native", root))

    for zip_path, native_dir, root in candidates:
        if zip_path.is_file():
            return zip_path, native_dir if native_dir and native_dir.is_dir() else None, root
    return None, None, None


def configure_spark_connect_python() -> None:
    zip_path, native_dir, _ = resolve_spark_connect_paths()
    if zip_path is not None:
        SPARK_CONNECT_DIR.mkdir(parents=True, exist_ok=True)
        marker = SPARK_CONNECT_DIR / ".extracted"
        if not marker.is_file():
            print(f"Extracting Spark Connect client from {zip_path} ...")
            with zipfile.ZipFile(zip_path) as archive:
                archive.extractall(SPARK_CONNECT_DIR)
            marker.write_text("ok", encoding="utf-8")

        pythonpath_parts: list[str] = []
        if native_dir is not None:
            pythonpath_parts.append(str(native_dir))
        pythonpath_parts.append(str(SPARK_CONNECT_DIR))
        existing = os.environ.get("PYTHONPATH", "")
        merged = ":".join([*pythonpath_parts, existing]) if existing else ":".join(pythonpath_parts)
        os.environ["PYTHONPATH"] = merged
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        print(
            f"PySpark configured from runtime zip: native={native_dir is not None} "
            f"client={SPARK_CONNECT_DIR}"
        )
        return

    print("Spark Connect runtime zip not found; installing PySpark client via pip")
    pip_env = {**os.environ, "PIP_USER": "1", "HOME": str(Path.home())}
    req_path = Path.cwd() / "requirements-spark.txt"
    if req_path.is_file():
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-q", "--user", "-r", str(req_path)],
            env=pip_env,
        )
    else:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-q", "--user", "pyspark==3.5.4"],
            env=pip_env,
        )
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def _spark_connect_host() -> str:
    return os.getenv("SPARK_CONNECT_HOST") or "127.0.0.1"


def resolve_spark_connect_port() -> tuple[str, str]:
    engine_id = os.getenv("CDSW_ENGINE_ID", "").upper()
    if engine_id:
        port = os.getenv(f"DS_RUNTIME_{engine_id}_SERVICE_PORT_SPARK")
        if port:
            return port, engine_id

    hostname = socket.gethostname().upper().replace("-", "")
    if hostname:
        port = os.getenv(f"DS_RUNTIME_{hostname}_SERVICE_PORT_SPARK")
        if port:
            return port, hostname

    for key in ("CDSW_JOB_RUN_ID", "CDSW_RUN_ID"):
        run_id = os.getenv(key, "").upper().replace("-", "")
        if not run_id:
            continue
        port = os.getenv(f"DS_RUNTIME_{run_id}_SERVICE_PORT_SPARK")
        if port:
            return port, run_id

    candidates = [
        (key, value)
        for key, value in os.environ.items()
        if key.startswith("DS_RUNTIME_") and key.endswith("_SERVICE_PORT_SPARK")
    ]
    if len(candidates) == 1:
        key, port = candidates[0]
        return port, key.removeprefix("DS_RUNTIME_").removesuffix("_SERVICE_PORT_SPARK")

    host = _spark_connect_host()
    for key, port in candidates:
        host_key = key.removeprefix("DS_RUNTIME_").removesuffix("_SERVICE_PORT_SPARK")
        service_host = os.getenv(f"DS_RUNTIME_{host_key}_SERVICE_HOST", "")
        if service_host and service_host.split(".")[0].upper().replace("-", "") == host.upper().replace("-", ""):
            return port, host_key

    for key, port in candidates:
        if port == "20049":
            return port, key.removeprefix("DS_RUNTIME_").removesuffix("_SERVICE_PORT_SPARK")

    if candidates:
        key, port = candidates[0]
        return port, key.removeprefix("DS_RUNTIME_").removesuffix("_SERVICE_PORT_SPARK")

    return os.getenv("SPARK_CONNECT_PORT", "20049"), engine_id or hostname or "unknown"


def detect_spark_connect_url() -> str:
    existing = os.getenv("SPARK_REMOTE") or os.getenv("SPARK_CONNECT_URL")
    if existing:
        return existing

    host = _spark_connect_host()
    port, engine_id = resolve_spark_connect_port()
    url = f"sc://{host}:{port}"
    os.environ["SPARK_CONNECT_URL"] = url
    print(f"Spark Connect URL: {url} (engine={engine_id})")
    return url


def try_start_spark_connect_server() -> bool:
    if os.getenv("SPARK_CONNECT_AUTOSTART", "true").lower() in {"0", "false", "no"}:
        return False

    _, _, install_root = resolve_spark_connect_paths()
    search_roots = [install_root, Path("/opt/spark-connect")]
    host = _spark_connect_host()
    port, _ = resolve_spark_connect_port()
    env = os.environ.copy()
    env.setdefault("SPARK_CONNECT_URL", f"sc://{host}:{port}")

    for root in search_roots:
        if root is None:
            continue
        for script in (
            root / "bin/start-connect-server.sh",
            root / "start-connect-server.sh",
            root / "sbin/start-connect-server.sh",
        ):
            if not script.is_file():
                continue
            print(f"Attempting Spark Connect server start via {script}")
            subprocess.run(
                [str(script), "--host", host, "--port", port],
                capture_output=True,
                text=True,
                timeout=30,
                env=env,
                check=False,
            )
            return True
    return False


def prepare_spark_connect() -> str:
    configure_spark_connect_python()
    url = detect_spark_connect_url()
    try_start_spark_connect_server()
    return url
