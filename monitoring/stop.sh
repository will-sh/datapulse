#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-${SCRIPT_DIR}/run}"

stop_pid_file() {
  local pid_file="$1"
  local name="$2"
  if [[ ! -f "${pid_file}" ]]; then
    return 0
  fi
  local pid
  pid="$(cat "${pid_file}" 2>/dev/null || true)"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "Stopping ${name} (pid ${pid}) ..."
    kill "${pid}" 2>/dev/null || true
    sleep 1
    kill -9 "${pid}" 2>/dev/null || true
  fi
  rm -f "${pid_file}"
}

stop_pid_file "${DATA_DIR}/pids/grafana.pid" "Grafana"
stop_pid_file "${DATA_DIR}/pids/prometheus.pid" "Prometheus"
stop_pid_file "${DATA_DIR}/pids/exporter.pid" "DataPulse exporter"
echo "Monitoring stack stopped."
