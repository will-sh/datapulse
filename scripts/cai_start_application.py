import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV = ROOT / ".venv"
REQUIREMENTS = ROOT / "requirements.txt"

port = os.environ["CDSW_READONLY_PORT"]
host = "127.0.0.1"

if not (VENV / "bin" / "python").exists():
    print("Creating virtualenv...", flush=True)
    subprocess.check_call([sys.executable, "-m", "venv", str(VENV)])
    subprocess.check_call(
        [str(VENV / "bin" / "pip"), "install", "-q", "-r", str(REQUIREMENTS)],
        env={**os.environ, "PIP_USER": "0"},
    )
else:
    print("Reusing virtualenv", flush=True)

print(f"Starting uvicorn on {host}:{port}", flush=True)
raise SystemExit(
    subprocess.call(
        [
            str(VENV / "bin" / "python"),
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            host,
            "--port",
            port,
        ]
    )
)
