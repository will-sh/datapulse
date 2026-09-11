import os, subprocess, sys
from pathlib import Path
R = Path(__file__).resolve().parent.parent
V = R / ".venv"
P = os.environ["CDSW_READONLY_PORT"]
H = "127.0.0.1"
if not (V / "bin" / "python").exists():
    subprocess.check_call([sys.executable, "-m", "venv", str(V)])
    subprocess.check_call([str(V / "bin" / "pip"), "install", "-q", "-r", str(R / "requirements.txt")], env={**os.environ, "PIP_USER": "0"})
raise SystemExit(subprocess.call([str(V / "bin" / "python"), "-m", "uvicorn", "app.main:app", "--host", H, "--port", P]))
