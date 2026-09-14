#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${CDSW_READONLY_PORT:-8080}"
HOST="127.0.0.1"
KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
KAFKA_SCALA="${KAFKA_SCALA:-2.13}"
KAFKA_CACHE_DIR="${KAFKA_CACHE_DIR:-$PWD/.cache/kafka}"
KAFKA_HOME="${KAFKA_HOME:-$KAFKA_CACHE_DIR/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}}"

if [[ "${KAFKA_ENABLED:-false}" == "true" ]]; then
  if [[ ! -x "$KAFKA_HOME/bin/kafka-console-producer.sh" ]]; then
    echo "Downloading Kafka ${KAFKA_VERSION} CLI to ${KAFKA_HOME}..."
    mkdir -p "$KAFKA_CACHE_DIR"
    tmp_archive="$(mktemp)"
    curl -fsSL \
      "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz" \
      -o "$tmp_archive"
    tar xzf "$tmp_archive" -C "$KAFKA_CACHE_DIR"
    rm -f "$tmp_archive"
  fi
  export KAFKA_HOME
  echo "Kafka CLI ready at ${KAFKA_HOME}"
fi

echo "Installing Python dependencies..."
pip install -q -r requirements.txt

echo "Starting DataPulse on ${HOST}:${PORT}..."
exec python -m uvicorn app.main:app --host "${HOST}" --port "${PORT}"
