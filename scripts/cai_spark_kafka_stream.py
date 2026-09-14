import glob
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.getcwd())
PORT = os.environ["CDSW_READONLY_PORT"]


def configure_spark_python() -> None:
    spark_home = Path(os.getenv("SPARK_HOME", "/opt/spark"))
    spark_python = spark_home / "python"
    if not spark_python.is_dir():
        print("Spark addon path not found; relying on preconfigured PYTHONPATH")
        return

    py4j_archives = glob.glob(str(spark_python / "lib" / "py4j-*-src.zip"))
    pythonpath_parts = [str(spark_python), *py4j_archives]
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        pythonpath_parts.append(existing)
    os.environ["PYTHONPATH"] = ":".join(pythonpath_parts)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_HOME", str(spark_home))
    print(f"Configured PySpark from {spark_home}")


configure_spark_python()

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "-r", str(ROOT / "requirements.txt")],
    env={**os.environ, "PIP_USER": "1"},
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
