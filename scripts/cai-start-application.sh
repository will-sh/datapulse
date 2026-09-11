#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${CDSW_READONLY_PORT:-8080}"
HOST="127.0.0.1"
VENV_DIR=".venv"

echo "Installing Python dependencies..."
if [ ! -x "${VENV_DIR}/bin/python" ]; then
  python3 -m venv "${VENV_DIR}"
  PIP_USER=0 "${VENV_DIR}/bin/pip" install -q -r requirements.txt
else
  echo "Reusing existing ${VENV_DIR}"
fi
source "${VENV_DIR}/bin/activate"

echo "Starting DataPulse on ${HOST}:${PORT}..."
exec python -m uvicorn app.main:app --host "${HOST}" --port "${PORT}"
