import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV = ROOT / ".venv"
REQUIREMENTS = ROOT / "requirements.txt"

if not (VENV / "bin" / "python").exists():
    subprocess.check_call([sys.executable, "-m", "venv", str(VENV)])
    subprocess.check_call(
        [str(VENV / "bin" / "pip"), "install", "-q", "-r", str(REQUIREMENTS)],
        env={**os.environ, "PIP_USER": "0"},
    )

port = os.environ.get("CDSW_READONLY_PORT", "8080")
host = "127.0.0.1"

os.execv(
    str(VENV / "bin" / "python"),
    [
        str(VENV / "bin" / "python"),
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        host,
        "--port",
        port,
    ],
)
