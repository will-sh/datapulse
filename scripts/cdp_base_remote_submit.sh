#!/usr/bin/env bash
# From a dev machine with VPN + SSH to the CDP Base cluster.
# Syncs repo to cluster and runs cdp_base_submit_smoke.sh on ccycloud-1.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-smoke}"
CDP_BASE_HOST="${CDP_BASE_HOST:-ccycloud-1.wxiao-732.root.comops.site}"
CDP_BASE_SSH_USER="${CDP_BASE_SSH_USER:-root}"
CDP_BASE_REMOTE_DIR="${CDP_BASE_REMOTE_DIR:-/tmp/datapulse-cdp-base}"
CDP_BASE_HDFS_USER="${CDP_BASE_HDFS_USER:-systest}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)

echo "Syncing ${ROOT} -> ${CDP_BASE_SSH_USER}@${CDP_BASE_HOST}:${CDP_BASE_REMOTE_DIR}"
ssh "${SSH_OPTS[@]}" "${CDP_BASE_SSH_USER}@${CDP_BASE_HOST}" "mkdir -p '${CDP_BASE_REMOTE_DIR}'"
rsync -az --delete \
  --exclude '.git' --exclude 'monitoring/grafana' --exclude '__pycache__' --exclude '.venv' \
  "${ROOT}/" "${CDP_BASE_SSH_USER}@${CDP_BASE_HOST}:${CDP_BASE_REMOTE_DIR}/"

echo "Submitting mode=${MODE} on cluster..."
ssh "${SSH_OPTS[@]}" "${CDP_BASE_SSH_USER}@${CDP_BASE_HOST}" \
  "chmod +x '${CDP_BASE_REMOTE_DIR}/scripts/cdp_base_submit_smoke.sh' && \
   CDP_BASE_HDFS_USER='${CDP_BASE_HDFS_USER}' \
   '${CDP_BASE_REMOTE_DIR}/scripts/cdp_base_submit_smoke.sh' '${MODE}'"
