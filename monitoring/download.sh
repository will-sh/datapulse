#!/usr/bin/env bash
# Download Prometheus and Grafana OSS binaries into monitoring/.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROM_VERSION="${PROMETHEUS_VERSION:-2.54.1}"
GRAFANA_VERSION="${GRAFANA_VERSION:-11.3.0}"
ARCH="${MONITORING_ARCH:-linux-amd64}"
CACHE_DIR="${MONITORING_CACHE_DIR:-${SCRIPT_DIR}/../.cache/monitoring}"

mkdir -p "${CACHE_DIR}"

install_prometheus() {
  local dest="${SCRIPT_DIR}/prometheus"
  if [[ -x "${dest}/prometheus" ]]; then
    echo "Prometheus already installed at ${dest}/prometheus"
    return 0
  fi

  local archive="prometheus-${PROM_VERSION}.${ARCH}.tar.gz"
  local url="https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${archive}"
  echo "Downloading ${url} ..."
  curl -fsSL "${url}" -o "${CACHE_DIR}/${archive}"
  rm -rf "${dest}"
  mkdir -p "${dest}"
  tar -xzf "${CACHE_DIR}/${archive}" -C "${dest}" --strip-components=1
  echo "Prometheus installed to ${dest}"
}

install_grafana() {
  local dest="${SCRIPT_DIR}/grafana"
  if [[ -x "${dest}/bin/grafana" ]]; then
    echo "Grafana already installed at ${dest}/bin/grafana"
    return 0
  fi

  local archive="grafana-${GRAFANA_VERSION}.${ARCH}.tar.gz"
  local url="https://dl.grafana.com/oss/release/${archive}"
  echo "Downloading ${url} ..."
  curl -fsSL "${url}" -o "${CACHE_DIR}/${archive}"
  rm -rf "${dest}/bin" "${dest}/public" "${dest}/plugins" "${dest}/LICENSE" "${dest}/VERSION"
  mkdir -p "${dest}"
  tar -xzf "${CACHE_DIR}/${archive}" -C "${dest}" --strip-components=1
  echo "Grafana installed to ${dest}"
}

install_prometheus
install_grafana
echo "Monitoring bundle ready."
