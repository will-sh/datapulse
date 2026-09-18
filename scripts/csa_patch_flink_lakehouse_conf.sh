#!/usr/bin/env bash
# Sync Lakehouse HMS Hadoop/Hive conf onto CSA Flink job pods (hive-conf-dir for Iceberg).
# Run on readygo bastion. Requires kubeconfigs for lakehouse + CSA clusters.
set -euo pipefail

CSA_NS="${CSA_NAMESPACE:-csa-bp-csa-ecf1b1}"
LH_NS="${LAKEHOUSE_HMS_NAMESPACE:-lakehouse-bp-ccbe9d}"
LH_CM="${LAKEHOUSE_HMS_CONFIGMAP:-lakehouse-bp-310fe0-cfg}"
CSA_CM="${CSA_LAKEHOUSE_CONF_CM:-flink-lakehouse-hive-conf}"
KAFKA_PVC="${FLINK_KAFKA_CLIENTS_PVC:-flink-kafka-clients-lib-efs}"
KAFKA_JAR="${KAFKA_CLIENTS_JAR:-kafka-clients-3.9.1.7.3.2.0-957.jar}"

CSA_KUBECONFIG="${CSA_KUBECONFIG:-/home/ubuntu/awc_installer_workspace/awc-experience/csa/config/kubeconfig}"
LH_KUBECONFIG="${LAKEHOUSE_KUBECONFIG:-/home/ubuntu/awc_installer_workspace/awc-experience/lakehouse/config/kubeconfig}"

HMS_URI="${HMS_URI:-thrift://hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work:9083}"
OZONE_S3_ENDPOINT="${OZONE_S3_ENDPOINT:-https://lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work}"
RANGER_REST_URL="${RANGER_REST_URL:-https://ranger.lakehouse-bp-6b4b81.cldr-csk-lakehouse.a70735.test.cldr.work}"
HADOOP_USER_NAME="${HADOOP_USER_NAME:-admin}"
OP_NS="${FLINK_OPERATOR_NAMESPACE:-flink-kubernetes-operator}"
MOUNT_PATH="/opt/flink/lakehouse-conf"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "=== Export ${LH_CM} from Lakehouse (${LH_NS}) ==="
python3 <<PY
import json, subprocess
keys = [
    "core-site.xml",
    "hive-site.xml",
    "metastore-site.xml",
    "ranger-hive-security.xml",
    "ranger-hive-audit.xml",
]
raw = subprocess.check_output([
    "kubectl", "--kubeconfig=${LH_KUBECONFIG}",
    "get", "cm", "${LH_CM}", "-n", "${LH_NS}", "-o", "json",
], text=True)
data = json.loads(raw)["data"]
work = "${WORKDIR}"
for key in keys:
    content = data.get(key, "")
    if not content:
        raise SystemExit(f"missing or empty {key} in ${LH_CM}")
    path = f"{work}/{key}"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    print(f"exported {key} ({len(content)} bytes)")
PY

echo "=== Patch conf for cross-cluster CSA Flink clients ==="
python3 <<PY
import re
from pathlib import Path

work = Path("${WORKDIR}")
hms_uri = "${HMS_URI}"
ozone = "${OZONE_S3_ENDPOINT}".rstrip("/")
ranger = "${RANGER_REST_URL}".rstrip("/")

hive = work / "hive-site.xml"
text = hive.read_text(encoding="utf-8")
text = re.sub(
    r"(<name>hive\\.metastore\\.uris</name>\\s*<value>)[^<]*(</value>)",
    rf"\\1{hms_uri}\\2",
    text,
    count=1,
)
hive.write_text(text, encoding="utf-8")

core = work / "core-site.xml"
text = core.read_text(encoding="utf-8")
text = re.sub(
    r"(<name>fs\\.s3a\\.endpoint</name>\\s*<value>)[^<]*(</value>)",
    rf"\\1{ozone}\\2",
    text,
    count=1,
)
if "fs.s3a.connection.ssl.enabled" in text:
    text = re.sub(
        r"(<name>fs\\.s3a\\.connection\\.ssl\\.enabled</name>\\s*<value>)[^<]*(</value>)",
        r"\\1true\\2",
        text,
        count=1,
    )
else:
    text = text.replace(
        "</configuration>",
        "  <property>\\n    <name>fs.s3a.connection.ssl.enabled</name>\\n    <value>true</value>\\n  </property>\\n</configuration>",
        1,
    )
core.write_text(text, encoding="utf-8")

ranger_xml = work / "ranger-hive-security.xml"
rtext = ranger_xml.read_text(encoding="utf-8")
rtext = re.sub(
    r"(<name>ranger\\.plugin\\.hive\\.policy\\.rest\\.url</name>\\s*<value>)[^<]*(</value>)",
    rf"\\1{ranger}\\2",
    rtext,
    count=1,
)
ranger_xml.write_text(rtext, encoding="utf-8")

# Flink HadoopUtils expects hdfs-site.xml in hadoop-conf-dir (S3-only lakehouse has none).
hdfs = work / "hdfs-site.xml"
if not hdfs.exists() or hdfs.stat().st_size == 0:
    hdfs.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n<configuration>\n</configuration>\n',
        encoding="utf-8",
    )
print("patched hive-site.xml, core-site.xml, ranger-hive-security.xml; ensured hdfs-site.xml")
PY

echo "=== Publish ${CSA_CM} on CSA (${CSA_NS}) ==="
kubectl --kubeconfig="$CSA_KUBECONFIG" create configmap "$CSA_CM" -n "$CSA_NS" \
  --from-file="${WORKDIR}/core-site.xml" \
  --from-file="${WORKDIR}/hive-site.xml" \
  --from-file="${WORKDIR}/hdfs-site.xml" \
  --from-file="${WORKDIR}/metastore-site.xml" \
  --from-file="${WORKDIR}/ranger-hive-security.xml" \
  --from-file="${WORKDIR}/ranger-hive-audit.xml" \
  --dry-run=client -o yaml | kubectl --kubeconfig="$CSA_KUBECONFIG" apply -f -

echo "=== Patch SSB pod volumes (Kafka EFS + Lakehouse conf) ==="
POD_VOLUMES=$(cat <<YAML
- name: kafka-clients-lib
  persistentVolumeClaim:
    claimName: ${KAFKA_PVC}
- name: lakehouse-hive-conf
  configMap:
    name: ${CSA_CM}
YAML
)
POD_MOUNTS=$(cat <<YAML
- name: kafka-clients-lib
  mountPath: /opt/flink/lib/${KAFKA_JAR}
  subPath: ${KAFKA_JAR}
- name: kafka-clients-lib
  mountPath: /opt/flink/certs/oauth-ca.crt
  subPath: oauth-ca.crt
- name: kafka-clients-lib
  mountPath: /opt/flink/certs/kafka-ca.crt
  subPath: kafka-ca.crt
- name: lakehouse-hive-conf
  mountPath: /opt/flink/lakehouse-conf
  readOnly: true
YAML
)
kubectl --kubeconfig="$CSA_KUBECONFIG" patch configmap ssb-config-pod-volumes -n "$CSA_NS" --type merge \
  -p "$(python3 -c "import json,sys; print(json.dumps({'data': {'pod-volumes.yaml': sys.stdin.read()}}))" <<< "$POD_VOLUMES")"
kubectl --kubeconfig="$CSA_KUBECONFIG" patch configmap ssb-config-pod-volume-mounts -n "$CSA_NS" --type merge \
  -p "$(python3 -c "import json,sys; print(json.dumps({'data': {'pod-volume-mounts.yaml': sys.stdin.read()}}))" <<< "$POD_MOUNTS")"

echo "=== Optional: set HADOOP_USER_NAME on Flink job pods (operator podTemplate) ==="
python3 <<PY
import json, subprocess, textwrap
user = "${HADOOP_USER_NAME}"
op_ns = "${OP_NS}"
kube = ["kubectl", "--kubeconfig=${CSA_KUBECONFIG}"]
snippet = textwrap.dedent(f"""\
  apiVersion: v1
  kind: Pod
  spec:
    containers:
      - name: flink-main-container
        env:
          - name: HADOOP_USER_NAME
            value: {user}
""").strip()
key = "kubernetes.operator.default-configuration.flink-version.v1_19+.podTemplate"
try:
    raw = subprocess.check_output(
        kube + ["get", "cm", "flink-operator-config", "-n", op_ns, "-o", "jsonpath={.data.config\\.yaml}"],
        text=True,
    )
except subprocess.CalledProcessError:
    print("skip flink-operator podTemplate patch (configmap missing)")
else:
    if f"value: {user}" in raw and "HADOOP_USER_NAME" in raw:
        print(f"operator podTemplate already sets HADOOP_USER_NAME={user}")
    else:
        block = key + ": |\\n" + "\\n".join("  " + line for line in snippet.splitlines())
        if key + ":" in raw:
            lines, out, in_block = raw.splitlines(), [], False
            for line in lines:
                if line.startswith(key + ":"):
                    out.append(block)
                    in_block = True
                    continue
                if in_block:
                    if line and not line.startswith(" "):
                        in_block = False
                        out.append(line)
                    continue
                out.append(line)
            merged = "\\n".join(out) + "\\n"
        else:
            merged = raw.rstrip() + "\\n\\n" + block + "\\n"
        patch = {"data": {"config.yaml": merged, "flink-conf.yaml": merged}}
        subprocess.check_call(kube + ["patch", "cm", "flink-operator-config", "-n", op_ns, "--type", "merge", "-p", json.dumps(patch)])
        subprocess.check_call(kube + ["rollout", "restart", "deployment", "-n", op_ns, "-l", "app.kubernetes.io/name=flink-kubernetes-operator"])
        print(f"patched flink-operator podTemplate HADOOP_USER_NAME={user}")
PY

kubectl --kubeconfig="$CSA_KUBECONFIG" rollout restart deployment -n "$CSA_NS" -l app.kubernetes.io/name=ssb
kubectl --kubeconfig="$CSA_KUBECONFIG" rollout status deployment -n "$CSA_NS" -l app.kubernetes.io/name=ssb --timeout=180s

echo "=== Verify ConfigMap + pod mount hooks ==="
python3 <<PY
import json, subprocess, sys
kube = ["kubectl", "--kubeconfig=${CSA_KUBECONFIG}"]
ns = "${CSA_NS}"
cm = "${CSA_CM}"
mount = "${MOUNT_PATH}"
raw = subprocess.check_output(kube + ["get", "cm", cm, "-n", ns, "-o", "json"], text=True)
data = json.loads(raw).get("data") or {}
required = ["core-site.xml", "hive-site.xml", "hdfs-site.xml", "metastore-site.xml", "ranger-hive-security.xml"]
missing = [k for k in required if not (data.get(k) or "").strip()]
if missing:
    raise SystemExit(f"CSA ConfigMap {cm} missing/non-empty keys: {missing}")
for key in required:
    print(f"{cm}/{key}: {len(data[key])} bytes")
mounts = subprocess.check_output(
    kube + ["get", "cm", "ssb-config-pod-volume-mounts", "-n", ns, "-o", "jsonpath={.data.pod-volume-mounts\\.yaml}"],
    text=True,
)
if mount not in mounts:
    raise SystemExit(f"ssb-config-pod-volume-mounts does not mount {mount}")
print(f"pod-volume-mounts includes {mount}")
PY

echo "Done. Lakehouse conf mounted at ${MOUNT_PATH}"
echo "Verify conf mount: python3 scripts/csa_flink_kafka_iceberg.py probe-hms-conf"
echo "Verify sink write:  python3 scripts/csa_flink_kafka_iceberg.py probe-hms"
