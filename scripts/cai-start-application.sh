#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${CDSW_READONLY_PORT:-8080}"
HOST="127.0.0.1"
VENV_DIR=".venv"

echo "Installing Python dependencies..."
python3 -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"
pip install -q -r requirements.txt

echo "Starting DataPulse on ${HOST}:${PORT}..."
exec python -m uvicorn app.main:app --host "${HOST}" --port "${PORT}"
