import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.getcwd())
PORT = os.environ["CDSW_READONLY_PORT"]

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
)
