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
    export MONITORING_BEARER_TOKEN="${MONITORING_BEARER_TOKEN:-${CDSW_APIV2_KEY:-${WORKBENCH_API_KEY:-}}}"
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
  # Keep root_url local so CDSW_APP_POLLING_ENDPOINT=/ receives HTTP 200 (not a redirect).
  export GF_SERVER_ROOT_URL="http://${GRAFANA_ADDR}:${GRAFANA_PORT}/"
  export GF_SERVER_DOMAIN="localhost"
  export GF_SERVER_SERVE_FROM_SUB_PATH=false
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

wait_for_url "Grafana" "http://${GRAFANA_ADDR}:${GRAFANA_PORT}/api/health"
wait_for_url "DataPulse exporter" "http://127.0.0.1:${EXPORTER_PORT}/metrics"
wait_for_url "Prometheus" "http://127.0.0.1:${PROM_PORT}/-/ready"

echo "Importing DataPulse Overview dashboard ..."
GRAFANA_URL="http://${GRAFANA_ADDR}:${GRAFANA_PORT}" \
  DASHBOARD_PATH="${SCRIPT_DIR}/dashboards/datapulse.json" \
  python3 "${SCRIPT_DIR}/import_dashboard.py" || echo "Dashboard import script failed; relying on file provisioning."

python3 - <<'PY'
import json
import time
import urllib.request
from pathlib import Path

status = {
    "stack_ready": True,
    "grafana_url": f"http://{__import__('os').environ.get('GRAFANA_ADDR', '127.0.0.1')}:{__import__('os').environ.get('GRAFANA_PORT', '8100')}/",
    "prometheus_url": "http://127.0.0.1:9090/",
    "exporter_url": "http://127.0.0.1:9191/metrics",
    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
for name, url in [
    ("exporter_ready", "http://127.0.0.1:9191/metrics"),
    ("prometheus_ready", "http://127.0.0.1:9090/-/ready"),
    ("grafana_ready", f"http://{__import__('os').environ.get('GRAFANA_ADDR', '127.0.0.1')}:{__import__('os').environ.get('GRAFANA_PORT', '8100')}/api/health"),
    ("prometheus_query_ready", "http://127.0.0.1:9090/api/v1/query?query=up"),
]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            status[name] = resp.status == 200
    except Exception:
        status[name] = False

path = Path("monitoring/status.json")
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(status, indent=2), encoding="utf-8")
print(f"Wrote {path}")
PY

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
