#!/usr/bin/env bash
# Idempotent Cloud Agent bootstrap for the DataPulse FastAPI application.
# Safe to run repeatedly: it only refreshes system packages, the virtualenv,
# and Python dependencies pinned in requirements.txt.
set -euo pipefail

cd "$(dirname "$0")/.."

# The default image ships python3.12 but not the ensurepip/venv module.
if ! python3 -c "import ensurepip" >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq python3-venv python3-pip
fi

if [ ! -x ".venv/bin/python" ]; then
  python3 -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

echo "DataPulse environment ready. Producer: uvicorn app.main:app | Consumer: uvicorn app.spark_stream.web:app"
