#!/usr/bin/env bash
# Patch CSA Flink JM/TM pods for Kafka OAuth (certs + allowed token URL JVM opt).
# Run on readygo bastion with KUBECONFIG pointing at cldr-csk-csa-1.
set -euo pipefail

NS="${CSA_NAMESPACE:-csa-bp-csa-ecf1b1}"
TOKEN_URL="${KAFKA_OAUTH_TOKEN_URL:-https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token}"
JAR_NAME="${KAFKA_CLIENTS_JAR:-kafka-clients-3.9.1.7.3.2.0-957.jar}"
SSB_IMAGE="${SSB_IMAGE:-container.repository.cloudera.com/cloudera/ssb-sse-hadoop:1.20.5-csads1.0.0-b76}"
PVC="${FLINK_KAFKA_CLIENTS_PVC:-flink-kafka-clients-lib-efs}"
OP_NS="${FLINK_OPERATOR_NAMESPACE:-flink-kubernetes-operator}"
CERT_SRC="${KAFKA_CONFIG_DIR:-config/kafka}"

echo "=== Ensure EFS PVC ${PVC} ==="
kubectl apply -n "$NS" -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${PVC}
  labels:
    app: flink-kafka-oauth-fix
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: shared-fs
  resources:
    requests:
      storage: 128Mi
YAML

kubectl wait -n "$NS" --for=jsonpath='{.status.phase}'=Bound "pvc/${PVC}" --timeout=120s

echo "=== Publish Kafka TLS certs ConfigMap ==="
if [[ ! -f "${CERT_SRC}/oauth-ca.crt" || ! -f "${CERT_SRC}/kafka-ca.crt" ]]; then
  echo "Missing ${CERT_SRC}/oauth-ca.crt or kafka-ca.crt" >&2
  exit 1
fi
kubectl create configmap flink-kafka-oauth-certs -n "$NS" \
  --from-file=oauth-ca.crt="${CERT_SRC}/oauth-ca.crt" \
  --from-file=kafka-ca.crt="${CERT_SRC}/kafka-ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "=== Populate kafka-clients jar + TLS certs on PVC ==="
kubectl delete job -n "$NS" flink-kafka-clients-populate-efs --ignore-not-found --wait=true
kubectl apply -n "$NS" -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: flink-kafka-clients-populate-efs
spec:
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsUser: 0
      imagePullSecrets:
        - name: awc-console-registry-creds
      containers:
        - name: populate
          image: ${SSB_IMAGE}
          command:
            - /bin/sh
            - -c
            - |
              set -e
              cp /opt/cloudera/ssb-sse/lib/${JAR_NAME} /data/${JAR_NAME}
              cp /certs/oauth-ca.crt /data/oauth-ca.crt
              cp /certs/kafka-ca.crt /data/kafka-ca.crt
              chmod 644 /data/${JAR_NAME} /data/oauth-ca.crt /data/kafka-ca.crt
          securityContext:
            runAsUser: 0
          volumeMounts:
            - name: lib
              mountPath: /data
            - name: certs
              mountPath: /certs
              readOnly: true
      volumes:
        - name: lib
          persistentVolumeClaim:
            claimName: ${PVC}
        - name: certs
          configMap:
            name: flink-kafka-oauth-certs
YAML
kubectl wait -n "$NS" --for=condition=complete job/flink-kafka-clients-populate-efs --timeout=180s

echo "=== Patch SSB pod volume hooks (CustomVolumesResourceDecorator) ==="
POD_VOLUMES=$(cat <<YAML
- name: kafka-clients-lib
  persistentVolumeClaim:
    claimName: ${PVC}
YAML
)
POD_MOUNTS=$(cat <<YAML
- name: kafka-clients-lib
  mountPath: /opt/flink/lib/${JAR_NAME}
  subPath: ${JAR_NAME}
- name: kafka-clients-lib
  mountPath: /opt/flink/certs/oauth-ca.crt
  subPath: oauth-ca.crt
- name: kafka-clients-lib
  mountPath: /opt/flink/certs/kafka-ca.crt
  subPath: kafka-ca.crt
YAML
)
kubectl patch configmap ssb-config-pod-volumes -n "$NS" --type merge -p "$(python3 -c "import json,sys; print(json.dumps({'data': {'pod-volumes.yaml': sys.stdin.read()}}))" <<< "$POD_VOLUMES")"
kubectl patch configmap ssb-config-pod-volume-mounts -n "$NS" --type merge -p "$(python3 -c "import json,sys; print(json.dumps({'data': {'pod-volume-mounts.yaml': sys.stdin.read()}}))" <<< "$POD_MOUNTS")"

echo "=== Append OAuth allowed URL to Flink operator Java 17 defaults ==="
python3 <<PY
import json, subprocess
KAFKA_PROP = "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=${TOKEN_URL}"
key = "kubernetes.operator.default-configuration.flink-version.v1_19+.env.java.default-opts.all"
raw = subprocess.check_output([
    "kubectl", "get", "cm", "flink-operator-config", "-n", "${OP_NS}",
    "-o", "jsonpath={.data.config\\.yaml}",
], text=True)
if KAFKA_PROP in raw:
    print("operator config already contains OAuth allowed.urls")
else:
    out = []
    for line in raw.splitlines():
        out.append(line + (" " + KAFKA_PROP if line.startswith(key + ":") else ""))
    patch = {"data": {"config.yaml": "\n".join(out) + "\n", "flink-conf.yaml": "\n".join(out) + "\n"}}
    subprocess.check_call([
        "kubectl", "patch", "cm", "flink-operator-config", "-n", "${OP_NS}",
        "--type", "merge", "-p", json.dumps(patch),
    ])
    print("patched flink-operator-config")
PY

kubectl rollout restart deployment -n "$NS" -l app.kubernetes.io/name=ssb
kubectl rollout restart deployment -n "$OP_NS" -l app.kubernetes.io/name=flink-kubernetes-operator
kubectl rollout status deployment -n "$NS" -l app.kubernetes.io/name=ssb --timeout=180s
kubectl rollout status deployment -n "$OP_NS" -l app.kubernetes.io/name=flink-kubernetes-operator --timeout=180s

echo "Done. Verify with: python3 scripts/csa_flink_kafka_iceberg.py probe-kafka"
