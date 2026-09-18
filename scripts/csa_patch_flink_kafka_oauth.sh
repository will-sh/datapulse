#!/usr/bin/env bash
# Patch CSA Flink JM/TM pods for Kafka OAuth (unshaded kafka-clients + allowed token URL).
# Run on readygo bastion with KUBECONFIG pointing at cldr-csk-csa-1.
set -euo pipefail

NS="${CSA_NAMESPACE:-csa-bp-csa-ecf1b1}"
TOKEN_URL="${KAFKA_OAUTH_TOKEN_URL:-https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token}"
JAR_NAME="${KAFKA_CLIENTS_JAR:-kafka-clients-3.9.1.7.3.2.0-957.jar}"
SSB_IMAGE="${SSB_IMAGE:-container.repository.cloudera.com/cloudera/ssb-sse-hadoop:1.20.5-csads1.0.0-b76}"
PVC="${FLINK_KAFKA_CLIENTS_PVC:-flink-kafka-clients-lib-efs}"
OP_NS="${FLINK_OPERATOR_NAMESPACE:-flink-kubernetes-operator}"

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

echo "=== Populate kafka-clients jar on PVC ==="
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
          command: ["/bin/sh", "-c", "cp /opt/cloudera/ssb-sse/lib/${JAR_NAME} /data/${JAR_NAME} && chmod 644 /data/${JAR_NAME}"]
          securityContext:
            runAsUser: 0
          volumeMounts:
            - name: lib
              mountPath: /data
      volumes:
        - name: lib
          persistentVolumeClaim:
            claimName: ${PVC}
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

echo "Done. Verify with: KAFKA_CALLBACK_HANDLER=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler python3 scripts/csa_flink_kafka_iceberg.py probe-kafka"
