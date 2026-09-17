#!/usr/bin/env python3
"""CAI Job entry for Kafka -> Iceberg (isolated from Applications)."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import traceback
import zipfile
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(os.getcwd())
OUT = ROOT / "config" / "lakehouse" / "last-job-run.json"
SPARK_CONNECT_ZIP = Path("/opt/spark-connect/spark_connect.zip")
SPARK_CONNECT_NATIVE = Path("/opt/spark-connect/native")
SPARK_CONNECT_DIR = Path("/tmp/spark-connect-unpack")
SPARK_MODES = {"bootstrap", "batch", "stream", "verify", "spark-probe", "spark-layout", "spark-pi"}
TRINO_MODES = {"trino-probe", "trino-bootstrap", "trino-verify", "trino-ingest"}


def write_run_log(payload: dict[str, object]) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    payload.setdefault("timestamp", datetime.now(UTC).isoformat())
    OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def configure_spark_connect_python() -> None:
    if SPARK_CONNECT_ZIP.is_file():
        SPARK_CONNECT_DIR.mkdir(parents=True, exist_ok=True)
        marker = SPARK_CONNECT_DIR / ".extracted"
        if not marker.is_file():
            print(f"Extracting Spark Connect client from {SPARK_CONNECT_ZIP} ...")
            with zipfile.ZipFile(SPARK_CONNECT_ZIP) as archive:
                archive.extractall(SPARK_CONNECT_DIR)
            marker.write_text("ok", encoding="utf-8")

        pythonpath_parts: list[str] = []
        if SPARK_CONNECT_NATIVE.is_dir():
            pythonpath_parts.append(str(SPARK_CONNECT_NATIVE))
        pythonpath_parts.append(str(SPARK_CONNECT_DIR))
        existing = os.environ.get("PYTHONPATH", "")
        merged = ":".join([*pythonpath_parts, existing]) if existing else ":".join(pythonpath_parts)
        os.environ["PYTHONPATH"] = merged
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        print(
            f"PySpark configured from runtime zip: native={SPARK_CONNECT_NATIVE.is_dir()} "
            f"client={SPARK_CONNECT_DIR}"
        )
        return

    print("Spark Connect runtime zip not found; installing PySpark client via pip")
    pip_env = {**os.environ, "PIP_USER": "1", "HOME": str(Path.home())}
    req_path = ROOT / "requirements-spark.txt"
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


def _resolve_spark_connect_port() -> tuple[str, str]:
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


def detect_spark_connect_url() -> None:
    if os.getenv("SPARK_REMOTE") or os.getenv("SPARK_CONNECT_URL"):
        return

    host = _spark_connect_host()
    port, engine_id = _resolve_spark_connect_port()
    os.environ["SPARK_CONNECT_URL"] = f"sc://{host}:{port}"
    print(f"Spark Connect URL: sc://{host}:{port} (engine={engine_id})")


def try_start_spark_connect_server() -> bool:
    if os.getenv("SPARK_CONNECT_AUTOSTART", "true").lower() in {"0", "false", "no"}:
        return False

    host = _spark_connect_host()
    port, _ = _resolve_spark_connect_port()
    env = os.environ.copy()
    env.setdefault("SPARK_CONNECT_URL", f"sc://{host}:{port}")
    for script in (
        Path("/opt/spark-connect/bin/start-connect-server.sh"),
        Path("/opt/spark-connect/start-connect-server.sh"),
        Path("/opt/spark-connect/sbin/start-connect-server.sh"),
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


def prepare_spark_connect() -> None:
    configure_spark_connect_python()
    detect_spark_connect_url()
    try_start_spark_connect_server()


def ensure_kafka_config_dir() -> None:
    os.environ.setdefault("KAFKA_CONFIG_DIR", ".")
    config_dir = Path(os.environ["KAFKA_CONFIG_DIR"])
    for name in ("kafka-ca.crt", "oauth-ca.crt"):
        for src in (ROOT / "config" / "kafka" / name, ROOT / name):
            dst = config_dir / name
            if src.is_file() and not dst.is_file():
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes(src.read_bytes())
                print(f"copied {src} -> {dst}")


def run_spark_job_subprocess(mode: str, extra_args: list[str]) -> tuple[int, str, str]:
    argv = [mode, *extra_args]
    script = f"""
import json
import sys
from pathlib import Path

root = Path({str(ROOT)!r})
sys.path[:0] = [str(root), str(root / "jobs")]
from kafka_to_iceberg import main

raise SystemExit(main(json.loads({json.dumps(json.dumps(argv))})))
"""
    timeout = int(os.getenv("SPARK_SESSION_TIMEOUT_SEC", "300")) + 180
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=os.environ.copy(),
        cwd=str(ROOT),
        check=False,
    )
    return result.returncode, result.stdout, result.stderr


if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

mode = os.getenv("LAKEHOUSE_JOB_MODE") or (sys.argv[1] if len(sys.argv) > 1 else "discover")
extra_args: list[str] = []
raw_args = os.getenv("LAKEHOUSE_JOB_EXTRA_ARGS", "").strip()
if raw_args:
    extra_args.extend(raw_args.split())
for arg in sys.argv[2:]:
    if arg.endswith(".json") and "jupyter/runtime/kernel-" in arg:
        continue
    extra_args.append(arg)

OUT.parent.mkdir(parents=True, exist_ok=True)
payload: dict[str, object] = {
    "timestamp": datetime.now(UTC).isoformat(),
    "mode": mode,
    "hostname": socket.gethostname(),
    "steps": [],
}

try:
    if mode in SPARK_MODES:
        if os.getenv("CAI_SPARK_DATA_CONNECTION") or os.getenv("CDSW_DATA_CONNECTION"):
            from app.spark_connect_env import prepare_spark_connect

            prepare_spark_connect()
            payload["steps"].append("data_connection_spark_ready")
        elif not os.getenv("LAKEHOUSE_SPARK_MASTER") and not os.getenv("SPARK_MASTER"):
            prepare_spark_connect()
            payload["steps"].append("spark_connect_ready")
        else:
            configure_spark_connect_python()
            payload["steps"].append("local_spark_ready")
        payload["spark_connect_url"] = os.getenv("SPARK_CONNECT_URL", "")
        payload["spark_master"] = os.getenv("LAKEHOUSE_SPARK_MASTER") or os.getenv("SPARK_MASTER", "")
        payload["cai_spark_data_connection"] = os.getenv("CAI_SPARK_DATA_CONNECTION") or os.getenv(
            "CDSW_DATA_CONNECTION", ""
        )
        write_run_log(payload)
    if mode in TRINO_MODES:
        payload["steps"].append("trino_mode")
        write_run_log(payload)
    ensure_kafka_config_dir()
    payload["steps"].append("preflight_ok")
    write_run_log(payload)

    jobs_dir = ROOT / "jobs"
    if str(jobs_dir) not in sys.path:
        sys.path.insert(0, str(jobs_dir))

    payload["steps"].append("import_ok")
    write_run_log(payload)

    if mode in SPARK_MODES:
        exit_code, stdout, stderr = run_spark_job_subprocess(mode, extra_args)
        if stdout.strip():
            payload["stdout_tail"] = stdout.strip()[-4000:]
        if stderr.strip():
            payload["stderr_tail"] = stderr.strip()[-4000:]
    else:
        from kafka_to_iceberg import main as job_main

        exit_code = job_main([mode, *extra_args])
    payload["status"] = "ok" if exit_code == 0 else "failed"
    payload["exit_code"] = exit_code
    write_run_log(payload)
    if exit_code != 0:
        raise RuntimeError(f"lakehouse job failed with exit code {exit_code}")
except Exception as exc:  # noqa: BLE001
    payload["status"] = "error"
    payload["error"] = str(exc)
    payload["traceback"] = traceback.format_exc()
    write_run_log(payload)
    try:
        from scripts.cai_report_last_job_run import _report_via_trino

        payload["trino_failure_report"] = _report_via_trino(
            {
                "phase": "lakehouse-job-failure",
                "mode": mode,
                "hostname": socket.gethostname(),
                "reported_at": datetime.now(UTC).isoformat(),
                "payload": payload,
            }
        )
    except Exception as report_exc:  # noqa: BLE001
        payload["trino_failure_report"] = {"ok": False, "error": str(report_exc)}
        write_run_log(payload)
    print(json.dumps(payload, indent=2, default=str))
    raise RuntimeError(str(exc)) from exc

print(json.dumps(payload, indent=2, default=str))
