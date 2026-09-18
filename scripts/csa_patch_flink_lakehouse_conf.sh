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
SERVLET_JAR="${HMS_SERVLET_JAR:-javax.servlet-api-4.0.1.jar}"
SERVLET_URL="${HMS_SERVLET_URL:-https://repo1.maven.org/maven2/javax/servlet/javax.servlet-api/4.0.1/javax.servlet-api-4.0.1.jar}"
# flink-sql-connector-hive bundles a HiveMetaStoreClient without HTTP Thrift; hive-exec has THttpClient.
HIVE_EXEC_JAR="${HIVE_EXEC_JAR:-hive-exec-3.1.3000.7.3.2.0-957.jar}"
HIVE_EXEC_FLINK_LIB="${HIVE_EXEC_FLINK_LIB:-000-${HIVE_EXEC_JAR}}"
HADOOP_AWS_JAR="${HADOOP_AWS_JAR:-hadoop-aws-3.4.2.7.3.2.0-957.jar}"
HADOOP_AWS_FLINK_LIB="${HADOOP_AWS_FLINK_LIB:-001-${HADOOP_AWS_JAR}}"
HADOOP_AWS_SRC="${HADOOP_AWS_SRC:-/usr/lib/hadoop/${HADOOP_AWS_JAR}}"
S3A_AWS_SDK_JAR="${S3A_AWS_SDK_JAR:-impala-minimal-s3a-aws-sdk-4.5.0.7.3.2.0-957.jar}"
S3A_AWS_SDK_FLINK_LIB="${S3A_AWS_SDK_FLINK_LIB:-002-${S3A_AWS_SDK_JAR}}"
S3A_AWS_SDK_SRC="${S3A_AWS_SDK_SRC:-/usr/lib/hive/lib/${S3A_AWS_SDK_JAR}}"
LAKEHOUSE_HMS_IMAGE="${LAKEHOUSE_HMS_IMAGE:-container.repository.cloudera.com/cloudera/hive:3.1.3000.7320957-26}"
FLINK_IMAGE="${FLINK_IMAGE:-container.repository.cloudera.com/cloudera/flink-extended-hadoop:1.20.5-csads1.0.0-b76}"

CSA_KUBECONFIG="${CSA_KUBECONFIG:-/home/ubuntu/awc_installer_workspace/awc-experience/csa/config/kubeconfig}"
LH_KUBECONFIG="${LAKEHOUSE_KUBECONFIG:-/home/ubuntu/awc_installer_workspace/awc-experience/lakehouse/config/kubeconfig}"

HMS_URI="${HMS_URI:-thrift://hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work:9083}"
OZONE_S3_ENDPOINT="${OZONE_S3_ENDPOINT:-https://lakehouse-bp-ozone-s3.cldr-csk-lakehouse.a70735.test.cldr.work}"
OZONE_S3_HOST="${OZONE_S3_HOST:-${OZONE_S3_ENDPOINT#https://}}"
OZONE_S3_HOST="${OZONE_S3_HOST#http://}"
OZONE_S3_CA_FILE="${OZONE_S3_CA_FILE:-ozone-s3-ca.crt}"
OZONE_S3_TRUSTSTORE="${OZONE_S3_TRUSTSTORE:-ozone-s3-truststore.jks}"
OZONE_S3_TRUSTSTORE_PASS="${OZONE_S3_TRUSTSTORE_PASS:-changeit}"
AWC_CA_SECRET="${AWC_CA_SECRET:-default-awc-ca}"
AWC_CA_SECRET_NS="${AWC_CA_SECRET_NS:-cert-manager}"
OZONE_S3_CA_MOUNT="/opt/flink/certs/${OZONE_S3_CA_FILE}"
OZONE_S3_TRUSTSTORE_MOUNT="/opt/flink/certs/${OZONE_S3_TRUSTSTORE}"
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
# Lakehouse HMS (in-cluster + public TCPRoute :9083) uses HTTP Thrift — keep client mode http.
if "hive.metastore.client.thrift.transport.mode" in text:
    text = re.sub(
        r"(<name>hive\\.metastore\\.client\\.thrift\\.transport\\.mode</name>\\s*<value>)[^<]*(</value>)",
        r"\\1http\\2",
        text,
        count=1,
    )
else:
    text = text.replace(
        "</configuration>",
        "  <property>\\n    <name>hive.metastore.client.thrift.transport.mode</name>\\n    <value>http</value>\\n  </property>\\n</configuration>",
        1,
    )
if "metastore.client.transport.mode" not in text:
    text = text.replace(
        "</configuration>",
        "  <property>\\n    <name>metastore.client.transport.mode</name>\\n    <value>http</value>\\n  </property>\\n</configuration>",
        1,
    )
# HTTP Thrift sends x-actor-username from this property (else UGI=default on Flink pods).
user = "${HADOOP_USER_NAME}"
for plain_key in ("metastore.client.plain.username", "hive.metastore.client.plain.username"):
    if plain_key in text:
        text = re.sub(
            rf"(<name>{re.escape(plain_key)}</name>\\s*<value>)[^<]*(</value>)",
            rf"\\1{user}\\2",
            text,
            count=1,
        )
    else:
        text = text.replace(
            "</configuration>",
            f"  <property>\\n    <name>{plain_key}</name>\\n    <value>{user}</value>\\n  </property>\\n</configuration>",
            1,
        )
hive.write_text(text, encoding="utf-8")

# Remote Flink HMS clients must not load Ranger authorizer classes (not on classpath).
auth_disable = {
    "hive.security.authorization.enabled": "false",
}
auth_strip = (
    "hive.security.authorization.manager",
    "hive.metastore.pre.event.listeners",
    "hive.metastore.filter.hook",
)
text = hive.read_text(encoding="utf-8")
for key, val in auth_disable.items():
    if key in text:
        text = re.sub(
            rf"(<name>{re.escape(key)}</name>\s*<value>)[^<]*(</value>)",
            rf"\1{val}\2",
            text,
            count=1,
        )
for key in auth_strip:
    text = re.sub(
        rf"\s*<property>\s*<name>{re.escape(key)}</name>\s*<value>[^<]*</value>\s*</property>\s*",
        "\n",
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
if "fs.s3a.path.style.access" in text:
    text = re.sub(
        r"(<name>fs\\.s3a\\.path\\.style\\.access</name>\\s*<value>)[^<]*(</value>)",
        r"\\1true\\2",
        text,
        count=1,
    )
else:
    text = text.replace(
        "</configuration>",
        "  <property>\\n    <name>fs.s3a.path.style.access</name>\\n    <value>true</value>\\n  </property>\\n</configuration>",
        1,
    )
# S3A uses JVM/Hadoop SSL truststores (not fs.s3a.ssl.truststore.*).
truststore = "${OZONE_S3_TRUSTSTORE_MOUNT}"
trustpass = "${OZONE_S3_TRUSTSTORE_PASS}"
ssl_props = {
    "ssl.client.truststore.location": truststore,
    "ssl.client.truststore.password": trustpass,
    "ssl.client.truststore.type": "JKS",
}
for key, val in ssl_props.items():
    if key in text:
        text = re.sub(
            rf"(<name>{re.escape(key)}</name>\s*<value>)[^<]*(</value>)",
            rf"\1{val}\2",
            text,
            count=1,
        )
    else:
        text = text.replace(
            "</configuration>",
            f"  <property>\\n    <name>{key}</name>\\n    <value>{val}</value>\\n  </property>\\n</configuration>",
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
print("patched hive-site.xml (uris + client thrift transport=http), core-site.xml, ranger-hive-security.xml; ensured hdfs-site.xml")
PY

echo "=== Publish javax.servlet-api on EFS (hive-exec HMSHandler needs javax.servlet on SSB) ==="
kubectl --kubeconfig="$CSA_KUBECONFIG" delete job -n "$CSA_NS" flink-hms-servlet-populate --ignore-not-found --wait=true
kubectl --kubeconfig="$CSA_KUBECONFIG" apply -n "$CSA_NS" -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-hms-servlet-populate
spec:
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: populate
        image: curlimages/curl:8.5.0
        command:
        - /bin/sh
        - -c
        - |
          set -e
          curl -fsSL -o /data/${SERVLET_JAR} ${SERVLET_URL}
          chmod 644 /data/${SERVLET_JAR}
        volumeMounts:
        - name: lib
          mountPath: /data
      volumes:
      - name: lib
        persistentVolumeClaim:
          claimName: ${KAFKA_PVC}
YAML
kubectl --kubeconfig="$CSA_KUBECONFIG" wait -n "$CSA_NS" --for=condition=complete job/flink-hms-servlet-populate --timeout=120s

echo "=== Publish hive-exec on EFS (HTTP Thrift HiveMetaStoreClient for Flink JM) ==="
kubectl --kubeconfig="$CSA_KUBECONFIG" delete job -n "$CSA_NS" flink-hive-exec-populate --ignore-not-found --wait=true
kubectl --kubeconfig="$CSA_KUBECONFIG" apply -n "$CSA_NS" -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-hive-exec-populate
spec:
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: populate
        image: ${FLINK_IMAGE}
        command:
        - /bin/sh
        - -c
        - |
          set -e
          src="/opt/hadoop/lib/${HIVE_EXEC_JAR}"
          dst="/data/${HIVE_EXEC_FLINK_LIB}"
          test -f "\$src"
          cp "\$src" "\$dst"
          chmod 644 "\$dst"
          ls -la "\$dst"
        volumeMounts:
        - name: lib
          mountPath: /data
      volumes:
      - name: lib
        persistentVolumeClaim:
          claimName: ${KAFKA_PVC}
YAML
kubectl --kubeconfig="$CSA_KUBECONFIG" wait -n "$CSA_NS" --for=condition=complete job/flink-hive-exec-populate --timeout=300s

echo "=== Publish hadoop-aws on EFS (S3AFileSystem for Iceberg warehouse) ==="
kubectl --kubeconfig="$CSA_KUBECONFIG" delete job -n "$CSA_NS" flink-hadoop-aws-populate --ignore-not-found --wait=true
kubectl --kubeconfig="$CSA_KUBECONFIG" apply -n "$CSA_NS" -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-hadoop-aws-populate
spec:
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: populate
        image: ${LAKEHOUSE_HMS_IMAGE}
        command:
        - /bin/sh
        - -c
        - |
          set -e
          src="${HADOOP_AWS_SRC}"
          dst="/data/${HADOOP_AWS_FLINK_LIB}"
          test -f "\$src"
          cp "\$src" "\$dst"
          chmod 644 "\$dst"
          ls -la "\$dst"
        volumeMounts:
        - name: lib
          mountPath: /data
      volumes:
      - name: lib
        persistentVolumeClaim:
          claimName: ${KAFKA_PVC}
YAML
kubectl --kubeconfig="$CSA_KUBECONFIG" wait -n "$CSA_NS" --for=condition=complete job/flink-hadoop-aws-populate --timeout=300s

echo "=== Publish S3A AWS SDK on EFS (hadoop-aws runtime deps) ==="
kubectl --kubeconfig="$CSA_KUBECONFIG" delete job -n "$CSA_NS" flink-s3a-aws-sdk-populate --ignore-not-found --wait=true
kubectl --kubeconfig="$CSA_KUBECONFIG" apply -n "$CSA_NS" -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-s3a-aws-sdk-populate
spec:
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: populate
        image: ${LAKEHOUSE_HMS_IMAGE}
        command:
        - /bin/sh
        - -c
        - |
          set -e
          src="${S3A_AWS_SDK_SRC}"
          dst="/data/${S3A_AWS_SDK_FLINK_LIB}"
          test -f "\$src"
          cp "\$src" "\$dst"
          chmod 644 "\$dst"
          ls -la "\$dst"
        volumeMounts:
        - name: lib
          mountPath: /data
      volumes:
      - name: lib
        persistentVolumeClaim:
          claimName: ${KAFKA_PVC}
YAML
kubectl --kubeconfig="$CSA_KUBECONFIG" wait -n "$CSA_NS" --for=condition=complete job/flink-s3a-aws-sdk-populate --timeout=300s

echo "=== Export AWC Internal CA for Ozone S3 TLS (cert-manager/${AWC_CA_SECRET}) ==="
OZONE_CA_PATH="${WORKDIR}/${OZONE_S3_CA_FILE}"
kubectl --kubeconfig="$LH_KUBECONFIG" get secret "$AWC_CA_SECRET" -n "$AWC_CA_SECRET_NS" \
  -o "jsonpath={.data.ca\.crt}" | base64 -d > "$OZONE_CA_PATH"
python3 <<PY
import subprocess
from pathlib import Path

ca = Path("${OZONE_CA_PATH}")
host = "${OZONE_S3_HOST}"
if ca.stat().st_size < 100:
    raise SystemExit(f"missing/short CA at {ca}")
leaf = subprocess.check_output(
    [
        "openssl", "s_client",
        "-connect", f"{host}:443",
        "-servername", host,
    ],
    input=b"",
    stderr=subprocess.DEVNULL,
)
leaf_path = Path("${WORKDIR}/ozone-leaf.pem")
leaf_path.write_bytes(
    subprocess.check_output(["openssl", "x509"], input=leaf)
)
verify = subprocess.run(
    ["openssl", "verify", "-CAfile", str(ca), str(leaf_path)],
    capture_output=True,
    text=True,
)
if verify.returncode != 0:
    raise SystemExit(f"CA does not verify Ozone S3 leaf: {verify.stdout} {verify.stderr}")
print(f"verified {ca.name} against https://{host} ({ca.stat().st_size} bytes)")
PY

kubectl --kubeconfig="$CSA_KUBECONFIG" create configmap flink-ozone-s3-ca-src -n "$CSA_NS" \
  --from-file=ca.crt="$OZONE_CA_PATH" \
  --dry-run=client -o yaml | kubectl --kubeconfig="$CSA_KUBECONFIG" apply -f -

echo "=== Publish Ozone S3 CA + JKS truststore on EFS ==="
kubectl --kubeconfig="$CSA_KUBECONFIG" delete job -n "$CSA_NS" flink-ozone-s3-ca-populate --ignore-not-found --wait=true
kubectl --kubeconfig="$CSA_KUBECONFIG" apply -n "$CSA_NS" -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-ozone-s3-ca-populate
spec:
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: populate
        image: ${FLINK_IMAGE}
        command:
        - /bin/sh
        - -c
        - |
          set -e
          cp /ca/ca.crt /data/${OZONE_S3_CA_FILE}
          rm -f /data/${OZONE_S3_TRUSTSTORE}
          keytool -importcert -noprompt -alias awc-internal-ca \\
            -file /ca/ca.crt \\
            -keystore /data/${OZONE_S3_TRUSTSTORE} \\
            -storepass ${OZONE_S3_TRUSTSTORE_PASS}
          chmod 644 /data/${OZONE_S3_CA_FILE} /data/${OZONE_S3_TRUSTSTORE}
          ls -la /data/${OZONE_S3_CA_FILE} /data/${OZONE_S3_TRUSTSTORE}
        volumeMounts:
        - name: ca
          mountPath: /ca
          readOnly: true
        - name: lib
          mountPath: /data
      volumes:
      - name: ca
        configMap:
          name: flink-ozone-s3-ca-src
      - name: lib
        persistentVolumeClaim:
          claimName: ${KAFKA_PVC}
YAML
kubectl --kubeconfig="$CSA_KUBECONFIG" wait -n "$CSA_NS" --for=condition=complete job/flink-ozone-s3-ca-populate --timeout=300s

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
- name: kafka-clients-lib
  mountPath: /opt/hadoop/lib/${SERVLET_JAR}
  subPath: ${SERVLET_JAR}
- name: kafka-clients-lib
  mountPath: /opt/flink/lib/${HIVE_EXEC_FLINK_LIB}
  subPath: ${HIVE_EXEC_FLINK_LIB}
  readOnly: true
- name: kafka-clients-lib
  mountPath: /opt/flink/lib/${HADOOP_AWS_FLINK_LIB}
  subPath: ${HADOOP_AWS_FLINK_LIB}
  readOnly: true
- name: kafka-clients-lib
  mountPath: /opt/flink/lib/${S3A_AWS_SDK_FLINK_LIB}
  subPath: ${S3A_AWS_SDK_FLINK_LIB}
  readOnly: true
- name: kafka-clients-lib
  mountPath: ${OZONE_S3_CA_MOUNT}
  subPath: ${OZONE_S3_CA_FILE}
  readOnly: true
- name: kafka-clients-lib
  mountPath: ${OZONE_S3_TRUSTSTORE_MOUNT}
  subPath: ${OZONE_S3_TRUSTSTORE}
  readOnly: true
- name: lakehouse-hive-conf
  mountPath: /opt/flink/lakehouse-conf
  readOnly: true
- name: lakehouse-hive-conf
  mountPath: /etc/hadoop/conf
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

echo "=== Patch ssb-flink-config (Flink JM/TM env + Ozone S3 JVM truststore) ==="
python3 <<PY
import json, subprocess
kube = ["kubectl", "--kubeconfig=${CSA_KUBECONFIG}"]
ns = "${CSA_NS}"
user = "${HADOOP_USER_NAME}"
cm = "ssb-flink-config"
truststore = "${OZONE_S3_TRUSTSTORE_MOUNT}"
trustpass = "${OZONE_S3_TRUSTSTORE_PASS}"
java_opts = (
    f"-Djavax.net.ssl.trustStore={truststore} "
    f"-Djavax.net.ssl.trustStorePassword={trustpass} "
    "-Djavax.net.ssl.trustStoreType=JKS"
)
raw = subprocess.check_output(kube + ["get", "cm", cm, "-n", ns, "-o", "json"], text=True)
data = json.loads(raw).get("data") or {}
conf = data.get("flink-conf.yaml", "")
mount = "${MOUNT_PATH}"
hadoop_conf = "/etc/hadoop/conf"
extra = [
    "# Lakehouse HMS: image defaults HADOOP_CONF_DIR=/etc/hadoop/conf (no HTTP Thrift)",
    f"containerized.master.env.HADOOP_CONF_DIR: {mount}",
    f"containerized.taskmanager.env.HADOOP_CONF_DIR: {mount}",
    f"containerized.master.env.HIVE_CONF_DIR: {hadoop_conf}",
    f"containerized.taskmanager.env.HIVE_CONF_DIR: {hadoop_conf}",
    "# Lakehouse HMS client UGI (overrides flink-extended-hadoop image default flink)",
    f"containerized.master.env.HADOOP_USER_NAME: {user}",
    f"containerized.taskmanager.env.HADOOP_USER_NAME: {user}",
    "# Ozone S3 public HTTPS: trust AWC Internal CA (S3A uses JVM truststore)",
    f"env.java.opts.all: {java_opts}",
]
changed = False
if f"containerized.master.env.HADOOP_CONF_DIR: {mount}" not in conf:
    conf = conf.rstrip() + "\\n" + "\\n".join(extra) + "\\n"
    changed = True
elif f"env.java.opts.all: {java_opts}" not in conf:
    lines = [ln for ln in conf.splitlines() if not ln.startswith("env.java.opts.all:")]
    conf = "\\n".join(lines).rstrip() + f"\\nenv.java.opts.all: {java_opts}\\n"
    changed = True
if changed:
    patch = {"data": {"flink-conf.yaml": conf}}
    subprocess.check_call(kube + ["patch", "cm", cm, "-n", ns, "--type", "merge", "-p", json.dumps(patch)])
    print(f"patched {cm} HADOOP_USER_NAME={user} + Ozone S3 JVM truststore")
else:
    print(f"{cm} already sets HADOOP_USER_NAME={user} and env.java.opts.all")
PY

echo "=== Patch SSB deployment pod (SQL validation reads hive-conf-dir on SSB, not only Flink pods) ==="
python3 <<PY
import json, subprocess
kube = ["kubectl", "--kubeconfig=${CSA_KUBECONFIG}"]
ns = "${CSA_NS}"
dep = "ssb-sse"
cm = "${CSA_CM}"
mount = "${MOUNT_PATH}"
user = "${HADOOP_USER_NAME}"
raw = subprocess.check_output(kube + ["get", "deployment", dep, "-n", ns, "-o", "json"], text=True)
obj = json.loads(raw)
tpl = obj["spec"]["template"]["spec"]
container = tpl["containers"][0]
vols = tpl.setdefault("volumes", [])
mounts = container.setdefault("volumeMounts", [])
env = container.setdefault("env", [])
servlet_jar = "${SERVLET_JAR}"
pvc = "${KAFKA_PVC}"
servlet_mount = f"/opt/cloudera/ssb-sse/lib/{servlet_jar}"
if not any(v.get("name") == "lakehouse-hive-conf" for v in vols):
    vols.append({"name": "lakehouse-hive-conf", "configMap": {"name": cm}})
if not any(v.get("name") == "kafka-clients-lib" for v in vols):
    vols.append({"name": "kafka-clients-lib", "persistentVolumeClaim": {"claimName": pvc}})
if not any(m.get("mountPath") == mount for m in mounts):
    mounts.append({"name": "lakehouse-hive-conf", "mountPath": mount, "readOnly": True})
if not any(m.get("mountPath") == servlet_mount for m in mounts):
    mounts.append({"name": "kafka-clients-lib", "mountPath": servlet_mount, "subPath": servlet_jar, "readOnly": True})
env = [e for e in env if e.get("name") != "HADOOP_USER_NAME"]
env.append({"name": "HADOOP_USER_NAME", "value": user})
patch = {
    "spec": {
        "template": {
            "spec": {
                "volumes": vols,
                "containers": [{"name": container["name"], "volumeMounts": mounts, "env": env}],
            }
        }
    }
}
subprocess.check_call(kube + ["patch", "deployment", dep, "-n", ns, "--type", "strategic", "-p", json.dumps(patch)])
print(f"patched {dep}: {mount} + HADOOP_USER_NAME={user}")
PY

echo "=== Extend SSB FlinkDeployment wait (slow cross-cluster HMS metadata ~10+ min) ==="
python3 <<PY
import json, subprocess
kube = ["kubectl", "--kubeconfig=${CSA_KUBECONFIG}"]
ns = "${CSA_NS}"
raw = subprocess.check_output(kube + ["get", "cm", "ssb-config", "-n", ns, "-o", "json"], text=True)
props = json.loads(raw)["data"]["application.properties"]
additions = [
    "# Iceberg/HMS probe: slow cross-cluster HMS metadata (SHOW DATABASES ~10+ min)",
    "kubernetes.resource.timeout.ms=1800000",
    "kubernetes.request.timeout=1800000",
    "kubernetes.deployment.timeout.ms=1800000",
]
if "kubernetes.resource.timeout.ms=1800000" not in props:
    props = props.rstrip() + "\\n" + "\\n".join(additions) + "\\n"
    patch = {"data": {"application.properties": props}}
    subprocess.check_call(kube + ["patch", "cm", "ssb-config", "-n", ns, "--type", "merge", "-p", json.dumps(patch)])
    print("patched ssb-config kubernetes timeouts (30m)")
else:
    print("ssb-config kubernetes timeouts already set")
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
