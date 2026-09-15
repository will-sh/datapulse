#!/usr/bin/env bash
# Start Prometheus, DataPulse exporter, and Grafana for the monitoring CAI Application.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROM_DIR="${SCRIPT_DIR}/prometheus"
PROM_PORT="${PROM_PORT:-9090}"
GRAFANA_DIR="${SCRIPT_DIR}/grafana"
DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/run}"
PROM_DATA_DIR="${DATA_DIR}/prometheus-data"
GRAFANA_DATA_DIR="${DATA_DIR}/grafana-data"
PROM_CONFIG="${PROM_CONFIG:-${SCRIPT_DIR}/prometheus.yml}"
GRAFANA_ADDR="${GRAFANA_ADDR:-127.0.0.1}"
GRAFANA_PORT="${GRAFANA_PORT:-${CDSW_READONLY_PORT:-8100}}"
EXPORTER_PORT="${EXPORTER_PORT:-9191}"
PRODUCER_URL="${PRODUCER_URL:-http://127.0.0.1:8080}"
CONSUMER_URL="${CONSUMER_URL:-http://127.0.0.1:8081}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300}"
SUPERVISOR_INTERVAL="${SUPERVISOR_INTERVAL:-5}"

mkdir -p "${PROM_DATA_DIR}" "${GRAFANA_DATA_DIR}" "${DATA_DIR}/logs" "${DATA_DIR}/pids"

if [[ ! -x "${PROM_DIR}/prometheus" || ! -x "${GRAFANA_DIR}/bin/grafana" ]]; then
  echo "Monitoring binaries missing; running download.sh ..."
  bash "${SCRIPT_DIR}/download.sh"
fi

is_running() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 1
  local pid
  pid="$(cat "${pid_file}" 2>/dev/null || true)"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

start_prometheus() {
  if is_running "${DATA_DIR}/pids/prometheus.pid"; then
    return 0
  fi
  echo "Starting Prometheus on :${PROM_PORT} ..."
  nohup "${PROM_DIR}/prometheus" \
    --config.file="${PROM_CONFIG}" \
    --storage.tsdb.path="${PROM_DATA_DIR}" \
    --web.listen-address="127.0.0.1:${PROM_PORT}" \
    --web.enable-lifecycle \
    >"${DATA_DIR}/logs/prometheus.log" 2>&1 &
  echo $! > "${DATA_DIR}/pids/prometheus.pid"
}

start_exporter() {
  if is_running "${DATA_DIR}/pids/exporter.pid"; then
    return 0
  fi
  echo "Starting DataPulse exporter on 127.0.0.1:${EXPORTER_PORT} ..."
  (
    cd "${ROOT}"
    export PRODUCER_URL CONSUMER_URL EXPORTER_PORT
    export MONITORING_BEARER_TOKEN="${MONITORING_BEARER_TOKEN:-${WORKBENCH_API_KEY:-}}"
    export MONITORING_VERIFY_SSL="${MONITORING_VERIFY_SSL:-false}"
    nohup python3 "${SCRIPT_DIR}/DataPulseExporter.py" \
      >"${DATA_DIR}/logs/exporter.log" 2>&1 &
    echo $!
  ) > "${DATA_DIR}/pids/exporter.pid"
}

start_grafana() {
  if is_running "${DATA_DIR}/pids/grafana.pid"; then
    return 0
  fi
  echo "Starting Grafana on ${GRAFANA_ADDR}:${GRAFANA_PORT} ..."
  export GF_PATHS_HOME="${GRAFANA_DIR}"
  export GF_PATHS_DATA="${GRAFANA_DATA_DIR}"
  export GF_PATHS_LOGS="${DATA_DIR}/logs/grafana"
  export GF_PATHS_PROVISIONING="${GRAFANA_DIR}/conf/provisioning"
  export GF_AUTH_ANONYMOUS_ENABLED=true
  export GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
  export GF_SERVER_HTTP_ADDR="${GRAFANA_ADDR}"
  export GF_SERVER_HTTP_PORT="${GRAFANA_PORT}"
  (
    cd "${GRAFANA_DIR}"
    nohup "${GRAFANA_DIR}/bin/grafana" server \
      --homepath="${GRAFANA_DIR}" \
      --config="${SCRIPT_DIR}/grafana.ini" \
      --packaging=deb \
      >"${DATA_DIR}/logs/grafana.log" 2>&1 &
    echo $!
  ) > "${DATA_DIR}/pids/grafana.pid"
}

wait_for_url() {
  local name="$1"
  local url="$2"
  local elapsed=0
  until curl -sf "${url}" >/dev/null 2>&1; do
    sleep 1
    elapsed=$((elapsed + 1))
    if (( elapsed >= WAIT_TIMEOUT )); then
      echo "Timed out waiting for ${name} at ${url}" >&2
      exit 1
    fi
  done
  echo "${name} ready at ${url}"
}

start_exporter
start_prometheus
start_grafana

wait_for_url "DataPulse exporter" "http://127.0.0.1:${EXPORTER_PORT}/metrics"
wait_for_url "Prometheus" "http://127.0.0.1:${PROM_PORT}/-/ready"
wait_for_url "Grafana" "http://${GRAFANA_ADDR}:${GRAFANA_PORT}/api/health"

echo "DataPulse monitoring stack running."
echo "Grafana UI: http://${GRAFANA_ADDR}:${GRAFANA_PORT}/"
echo "Prometheus: http://127.0.0.1:${PROM_PORT}/"
echo "Exporter:   http://127.0.0.1:${EXPORTER_PORT}/metrics"

while true; do
  is_running "${DATA_DIR}/pids/exporter.pid" || start_exporter
  is_running "${DATA_DIR}/pids/prometheus.pid" || start_prometheus
  is_running "${DATA_DIR}/pids/grafana.pid" || start_grafana
  sleep "${SUPERVISOR_INTERVAL}"
done
