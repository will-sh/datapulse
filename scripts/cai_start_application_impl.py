import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV = ROOT / ".venv"
REQ = ROOT / "requirements.txt"
PORT = os.environ["CDSW_READONLY_PORT"]
HOST = "127.0.0.1"

if not (VENV / "bin" / "python").exists():
    subprocess.check_call([sys.executable, "-m", "venv", str(VENV)])
    subprocess.check_call(
        [str(VENV / "bin" / "pip"), "install", "-q", "-r", str(REQ)],
        env={**os.environ, "PIP_USER": "0"},
    )

raise SystemExit(
    subprocess.call(
        [
            str(VENV / "bin" / "python"),
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            HOST,
            "--port",
            PORT,
        ]
    )
)
