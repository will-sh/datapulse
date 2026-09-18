#!/usr/bin/env bash
# Expose Lakehouse HMS Thrift (HTTP mode on :9083) on the public internet.
# Run on readygo bastion with Lakehouse kubeconfig (+ AWS CLI for Route53/SG).
#
# Istio waypoint breaks NodePort -> HMS. Use hostNetwork socat proxies that
# listen on node :9083 and forward to the in-cluster HMS ClusterIP.
set -euo pipefail

LH_K="${KUBECONFIG:-/home/ubuntu/awc_installer_workspace/awc-experience/lakehouse/config/kubeconfig}"
export KUBECONFIG="$LH_K"

LH_NS="${LAKEHOUSE_HMS_NAMESPACE:-lakehouse-bp-ccbe9d}"
HMS_SVC="${LAKEHOUSE_HMS_SERVICE:-lakehouse-bp-hms-hms-service}"
HMS_PORT="${LAKEHOUSE_HMS_PORT:-9083}"
PROXY_NAME="${HMS_PROXY_NAME:-hivemetastore-public-proxy}"
PUBLIC_SVC="${HMS_PUBLIC_SERVICE:-hivemetastore-public-lb}"
PUBLIC_SUBNET="${HMS_PUBLIC_SUBNET:-subnet-0722d44fcee4166ef}"
HMS_HOST="${HMS_PUBLIC_HOST:-hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work}"
ROUTE53_ZONE="${ROUTE53_ZONE_ID:-Z04021161DHC0MRSG6S6C}"
WORKER_SG="${LAKEHOUSE_WORKER_SG:-sg-0c501f15c681ddd3f}"
AWS_REGION="${AWS_REGION:-us-west-1}"
SOCAT_IMAGE="${HMS_SOCAT_IMAGE:-alpine/socat:1.0.5}"

kubectl get ns "$LH_NS" >/dev/null

# Remove experimental routes/policies from prior attempts.
kubectl delete httproute hivemetastore-public -n "$LH_NS" --ignore-not-found
kubectl delete authorizationpolicy hivemetastore-public-allow -n "$LH_NS" --ignore-not-found
kubectl delete tcproute hivemetastore-public -n "$LH_NS" --ignore-not-found

cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: ${PROXY_NAME}
  namespace: ${LH_NS}
  labels:
    app: ${PROXY_NAME}
    datapulse.cloudera.com/managed-by: lakehouse_patch_public_hms.sh
spec:
  selector:
    matchLabels:
      app: ${PROXY_NAME}
  template:
    metadata:
      labels:
        app: ${PROXY_NAME}
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      tolerations:
      - operator: Exists
      containers:
      - name: socat
        image: ${SOCAT_IMAGE}
        args:
        - TCP-LISTEN:${HMS_PORT},fork,reuseaddr
        - TCP:${HMS_SVC}.${LH_NS}.svc.cluster.local:${HMS_PORT}
        ports:
        - containerPort: ${HMS_PORT}
          hostPort: ${HMS_PORT}
          name: hms-thrift
          protocol: TCP
        readinessProbe:
          tcpSocket:
            port: ${HMS_PORT}
          initialDelaySeconds: 3
          periodSeconds: 10
        resources:
          requests:
            cpu: 50m
            memory: 64Mi
          limits:
            cpu: 200m
            memory: 128Mi
---
apiVersion: v1
kind: Service
metadata:
  name: ${PUBLIC_SVC}
  namespace: ${LH_NS}
  labels:
    app: ${PROXY_NAME}
    datapulse.cloudera.com/managed-by: lakehouse_patch_public_hms.sh
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
    service.beta.kubernetes.io/aws-load-balancer-subnets: ${PUBLIC_SUBNET}
    service.beta.kubernetes.io/aws-load-balancer-type: nlb
spec:
  type: LoadBalancer
  loadBalancerClass: service.k8s.aws/nlb
  externalTrafficPolicy: Local
  selector:
    app: ${PROXY_NAME}
  ports:
  - name: hms-thrift
    port: ${HMS_PORT}
    targetPort: ${HMS_PORT}
    protocol: TCP
EOF

# Drop old Deployment if present (DaemonSet replaces it).
kubectl delete deployment "${PROXY_NAME}" -n "$LH_NS" --ignore-not-found

echo "Waiting for proxy DaemonSet ..."
kubectl rollout status daemonset/"$PROXY_NAME" -n "$LH_NS" --timeout=180s

echo "Waiting for LoadBalancer hostname ..."
LB_HOST=""
for _ in $(seq 1 36); do
  LB_HOST="$(kubectl get svc "$PUBLIC_SVC" -n "$LH_NS" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
  if [[ -n "$LB_HOST" ]]; then
    break
  fi
  sleep 5
done
if [[ -z "$LB_HOST" ]]; then
  echo "ERROR: LoadBalancer hostname not assigned" >&2
  kubectl describe svc "$PUBLIC_SVC" -n "$LH_NS" | tail -20
  exit 1
fi
echo "NLB hostname: $LB_HOST"

if command -v aws >/dev/null 2>&1 && aws sts get-caller-identity >/dev/null 2>&1; then
  LB_ARN=""
  for _ in $(seq 1 24); do
    LB_ARN="$(aws elbv2 describe-load-balancers --region "$AWS_REGION" \
      --query "LoadBalancers[?DNSName=='${LB_HOST}'].LoadBalancerArn | [0]" --output text 2>/dev/null || true)"
    if [[ -n "$LB_ARN" && "$LB_ARN" != "None" ]]; then
      break
    fi
    sleep 5
  done

  if [[ -n "$LB_ARN" && "$LB_ARN" != "None" ]]; then
    LB_NAME="$(aws elbv2 describe-load-balancers --load-balancer-arns "$LB_ARN" --region "$AWS_REGION" \
      --query 'LoadBalancers[0].LoadBalancerName' --output text)"
    echo "NLB name: $LB_NAME"
    for ENI in $(aws ec2 describe-network-interfaces --region "$AWS_REGION" \
      --filters "Name=description,Values=*${LB_NAME}*" \
      --query 'NetworkInterfaces[*].Groups[*].GroupId' --output text 2>/dev/null | tr '\t' '\n' | sort -u); do
      echo "Opening :${HMS_PORT} on SG $ENI ..."
      if aws ec2 describe-security-groups --group-ids "$ENI" --region "$AWS_REGION" \
        --query "SecurityGroups[0].IpPermissions[?FromPort<=\`${HMS_PORT}\` && ToPort>=\`${HMS_PORT}\`]" \
        --output text 2>/dev/null | grep -q .; then
        echo "  already open"
      else
        aws ec2 authorize-security-group-ingress \
          --group-id "$ENI" \
          --ip-permissions "IpProtocol=tcp,FromPort=${HMS_PORT},ToPort=${HMS_PORT},IpRanges=[{CidrIp=0.0.0.0/0,Description=Lakehouse HMS public Thrift}]" \
          --region "$AWS_REGION" || true
      fi
    done
    LB_ZONE="$(aws elbv2 describe-load-balancers --load-balancer-arns "$LB_ARN" --region "$AWS_REGION" \
      --query 'LoadBalancers[0].CanonicalHostedZoneId' --output text)"
    # K8s NLB maps instance targets to NodePort (31053), not hostNetwork :9083 — retarget.
    OLD_TG="$(aws elbv2 describe-target-groups --region "$AWS_REGION" \
      --query "TargetGroups[?contains(LoadBalancerArns, '${LB_ARN}')].TargetGroupArn | [0]" --output text 2>/dev/null || true)"
    VPC_ID="$(aws elbv2 describe-load-balancers --load-balancer-arns "$LB_ARN" --region "$AWS_REGION" \
      --query 'LoadBalancers[0].VpcId' --output text)"
    HOST_TG="$(aws elbv2 describe-target-groups --region "$AWS_REGION" \
      --names hms-public-9083-host --query 'TargetGroups[0].TargetGroupArn' --output text 2>/dev/null || true)"
    if [[ -z "$HOST_TG" || "$HOST_TG" == "None" ]]; then
      HOST_TG="$(aws elbv2 create-target-group --name hms-public-9083-host --protocol TCP --port "${HMS_PORT}" \
        --vpc-id "$VPC_ID" --target-type instance --health-check-protocol TCP --health-check-port "${HMS_PORT}" \
        --region "$AWS_REGION" --query 'TargetGroups[0].TargetGroupArn' --output text)"
      echo "Created hostNetwork target group: $HOST_TG"
    fi
    if [[ -n "$OLD_TG" && "$OLD_TG" != "None" ]]; then
      for inst in $(aws elbv2 describe-target-health --target-group-arn "$OLD_TG" --region "$AWS_REGION" \
        --query 'TargetHealthDescriptions[*].Target.Id' --output text 2>/dev/null); do
        aws elbv2 register-targets --target-group-arn "$HOST_TG" --targets "Id=${inst},Port=${HMS_PORT}" \
          --region "$AWS_REGION" >/dev/null 2>&1 || true
      done
    fi
    LISTENER_ARN="$(aws elbv2 describe-listeners --load-balancer-arn "$LB_ARN" --region "$AWS_REGION" \
      --query "Listeners[?Port==\`${HMS_PORT}\`].ListenerArn | [0]" --output text)"
    if [[ -n "$LISTENER_ARN" && "$LISTENER_ARN" != "None" ]]; then
      aws elbv2 modify-listener --listener-arn "$LISTENER_ARN" \
        --default-actions "Type=forward,TargetGroupArn=${HOST_TG}" --region "$AWS_REGION" >/dev/null
      echo "Listener :${HMS_PORT} -> hostNetwork target group"
    fi
    echo "Opening worker SG ${WORKER_SG} :${HMS_PORT} from 0.0.0.0/0 (NLB preserves client IP) ..."
    aws ec2 authorize-security-group-ingress \
      --group-id "$WORKER_SG" \
      --ip-permissions "IpProtocol=tcp,FromPort=${HMS_PORT},ToPort=${HMS_PORT},IpRanges=[{CidrIp=0.0.0.0/0,Description=Public Lakehouse HMS hostNetwork}]" \
      --region "$AWS_REGION" 2>/dev/null || echo "  worker SG rule may already exist"

    echo "Upserting Route53 alias ${HMS_HOST} -> ${LB_HOST} ..."
    aws route53 change-resource-record-sets --hosted-zone-id "$ROUTE53_ZONE" --change-batch "{
      \"Changes\": [{
        \"Action\": \"UPSERT\",
        \"ResourceRecordSet\": {
          \"Name\": \"${HMS_HOST}.\",
          \"Type\": \"A\",
          \"AliasTarget\": {
            \"HostedZoneId\": \"${LB_ZONE}\",
            \"DNSName\": \"${LB_HOST}.\",
            \"EvaluateTargetHealth\": true
          }
        }
      }]
    }" --region "$AWS_REGION" >/dev/null
  else
    echo "WARN: could not resolve NLB ARN; set Route53 alias manually"
  fi
else
  echo "WARN: AWS CLI/credentials unavailable; open NLB SG :${HMS_PORT} and Route53 alias manually"
fi

echo
echo "Waiting for NLB targets healthy ..."
if command -v aws >/dev/null 2>&1 && aws sts get-caller-identity >/dev/null 2>&1; then
  for _ in $(seq 1 30); do
    TG_ARN="$(aws elbv2 describe-target-groups --region "$AWS_REGION" \
      --query "TargetGroups[?TargetGroupName=='hms-public-9083-host'].TargetGroupArn | [0]" --output text 2>/dev/null || true)"
    if [[ -z "$TG_ARN" || "$TG_ARN" == "None" ]]; then
      TG_ARN="$(aws elbv2 describe-target-groups --region "$AWS_REGION" \
        --query "TargetGroups[?contains(TargetGroupName, 'hivemeta')].TargetGroupArn | [0]" --output text 2>/dev/null || true)"
    fi
    if [[ -n "$TG_ARN" && "$TG_ARN" != "None" ]]; then
      states="$(aws elbv2 describe-target-health --target-group-arn "$TG_ARN" --region "$AWS_REGION" \
        --query 'TargetHealthDescriptions[*].TargetHealth.State' --output text 2>/dev/null || true)"
      count="$(echo "$states" | wc -w | tr -d ' ')"
      if echo "$states" | grep -q healthy && ! echo "$states" | grep -qE 'initial|unhealthy|draining'; then
        echo "Target health (${count} targets): $states"
        break
      fi
      echo "Target health (${count} targets): $states (waiting)"
    fi
    sleep 10
  done
fi

echo
echo "=== Probe ==="
python3 - <<PY
import socket, time
host = "${HMS_HOST}"
port = ${HMS_PORT}
for attempt in range(8):
    s = socket.socket()
    s.settimeout(10)
    try:
        s.connect((host, port))
        s.sendall(b"GET /metastore/ HTTP/1.0\r\nHost: " + host.encode() + b"\r\n\r\n")
        s.settimeout(5)
        data = s.recv(256)
        print("tcp_ok", repr(data[:160]))
        break
    except Exception as exc:
        print(f"attempt {attempt+1} fail:", exc)
        time.sleep(10)
    finally:
        s.close()
PY

echo
echo "Public HMS URI: thrift://${HMS_HOST}:${HMS_PORT}"
echo "Client: hive.metastore.client.thrift.transport.mode=http"
