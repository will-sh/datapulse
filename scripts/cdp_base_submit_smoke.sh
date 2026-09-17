#!/usr/bin/env bash
# Run ON a CDP Base cluster node (ccycloud-1) as root or systest.
# Submits jobs/cdp_base_iceberg_smoke.py to YARN with the parcel Iceberg runtime jar.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-smoke}"
CDP_BASE_HDFS_USER="${CDP_BASE_HDFS_USER:-systest}"
CDP_BASE_ICEBERG_JAR="${CDP_BASE_ICEBERG_JAR:-/opt/cloudera/parcels/CDH-7.3.2-1.cdh7.3.2.p30000.83076434/lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.7.3.2.30000-26.jar}"
PROPS="${ROOT}/conf/cdp-base/spark-iceberg.conf"
JOB="${ROOT}/jobs/cdp_base_iceberg_smoke.py"
ICEBERG_JAR="${ICEBERG_JAR:-$CDP_BASE_ICEBERG_JAR}"

if [[ ! -f "$JOB" ]]; then
  echo "missing job script: $JOB" >&2
  exit 1
fi
if [[ ! -f "$ICEBERG_JAR" ]]; then
  echo "missing Iceberg jar: $ICEBERG_JAR" >&2
  exit 1
fi

submit() {
  spark3-submit \
    --master yarn \
    --deploy-mode client \
    --name "datapulse-cdp-base-iceberg-${MODE}" \
    --jars "$ICEBERG_JAR" \
    --properties-file "$PROPS" \
    "$JOB" "$MODE"
}

if [[ "$(id -un)" == "$CDP_BASE_HDFS_USER" ]]; then
  export HADOOP_USER_NAME="$CDP_BASE_HDFS_USER"
  submit
else
  sudo -u "$CDP_BASE_HDFS_USER" env HADOOP_USER_NAME="$CDP_BASE_HDFS_USER" bash -lc "$(typeset -f submit); submit"
fi
