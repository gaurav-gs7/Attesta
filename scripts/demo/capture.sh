#!/usr/bin/env bash
# Captures the real terminal output shown in the README demo
# (docs/assets/attesta-demo.gif). Runs on a disposable kind cluster with the
# controller and API images loaded (see .github/workflows/demo.yml), and
# writes every command and its output to $CAPTURE (default capture.json).
# scripts/demo/render.py draws the video from that file; nothing on screen
# is typed by hand.
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
NS=payments
CAPTURE="${CAPTURE:-$ROOT_DIR/capture.json}"
AGNHOST="${AGNHOST_IMAGE:-registry.k8s.io/e2e-test-images/agnhost:2.52}"
STEPS="$(mktemp)"
WORK="$(mktemp -d)"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }
trap 'log "error near line $LINENO"; kubectl -n attesta-system logs -l app.kubernetes.io/name=attesta-controller --tail=80 >&2 || true' ERR

# step <id> <command>: runs the command as shown and records it with its
# output and exit code.
step() {
  local id="$1" cmd="$2" out rc
  set +e
  out="$(cd "$WORK" && PATH="$ROOT_DIR/bin:$PATH" bash -c "$cmd" 2>&1)"
  rc=$?
  set -e
  python3 - "$STEPS" "$id" "$cmd" "$out" "$rc" <<'PY'
import json, sys
path, sid, cmd, out, rc = sys.argv[1:]
with open(path, "a") as f:
    f.write(json.dumps({"id": sid, "command": cmd, "output": out, "rc": int(rc)}) + "\n")
PY
  printf '$ %s\n%s\n(rc=%s)\n\n' "$cmd" "$out" "$rc" >&2
}

wait_for() {
  local what="$1" timeout="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail "timed out waiting for: $what"
    sleep 2
  done
  log "ok: $what"
}

log "building the CLI"
go build -o bin/attesta ./cmd/attesta

log "installing (not shown): keys, CRDs, controller, API, Gateway API CRDs, a Prometheus stub"
openssl genpkey -algorithm ed25519 -out "$WORK/signing.pem"
openssl pkey -in "$WORK/signing.pem" -pubout -out "$WORK/public.pem"
API_TOKEN="$(openssl rand -hex 24)"
printf %s "$API_TOKEN" >"$WORK/token"
kubectl create namespace attesta-system
kubectl -n attesta-system create secret generic attesta-decision-signing-key --from-file=key.pem="$WORK/signing.pem"
kubectl -n attesta-system create secret generic attesta-archive-token --from-file=token="$WORK/token"
kubectl -n attesta-system create secret generic attesta-api --from-file=token="$WORK/token" --from-file=public.pem="$WORK/public.pem"
kubectl apply -k deploy/crds >/dev/null
kubectl wait --for=condition=Established crd/rolloutguards.attesta.io --timeout=60s
kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.1/standard-install.yaml >/dev/null
kubectl wait --for=condition=Established crd/httproutes.gateway.networking.k8s.io --timeout=60s
# The demo namespace replaces the e2e one in the controller's Prometheus URL.
kubectl kustomize test/e2e | sed 's/prom-stub.attesta-e2e.svc/prom-stub.payments.svc/' | kubectl apply -f - >/dev/null
kubectl create namespace "$NS"
kubectl -n "$NS" create configmap prom-stub --from-file=stub.py=test/e2e/prometheus_stub.py
# loadgen sends real requests through a Service and counts which pods answer.
cat >"$WORK/share.py" <<'EOF'
import urllib.request
hits = [urllib.request.urlopen("http://checkout.payments.svc", timeout=5).read().decode() for _ in range(300)]
canary = sum(1 for h in hits if "-primary-" not in h)
print(f"canary pods served {canary} of {len(hits)} requests ({round(100 * canary / len(hits))}%)")
EOF
kubectl -n "$NS" create configmap loadgen --from-file=share.py="$WORK/share.py"
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: prom-stub, namespace: $NS}
spec:
  replicas: 1
  selector: {matchLabels: {app: prom-stub}}
  template:
    metadata: {labels: {app: prom-stub}}
    spec:
      containers:
        - name: stub
          image: python:3.12-alpine
          command: ["python", "/stub/stub.py"]
          volumeMounts: [{name: stub, mountPath: /stub}]
      volumes: [{name: stub, configMap: {name: prom-stub}}]
---
apiVersion: v1
kind: Service
metadata: {name: prom-stub, namespace: $NS}
spec:
  selector: {app: prom-stub}
  ports: [{port: 9090, targetPort: 9090}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: loadgen, namespace: $NS}
spec:
  replicas: 1
  selector: {matchLabels: {app: loadgen}}
  template:
    metadata: {labels: {app: loadgen}}
    spec:
      containers:
        - name: loadgen
          image: python:3.12-alpine
          command: ["sleep", "infinity"]
          volumeMounts: [{name: share, mountPath: /share}]
      volumes: [{name: share, configMap: {name: loadgen}}]
EOF
kubectl -n attesta-system rollout status deployment/attesta-controller --timeout=240s
kubectl -n attesta-system rollout status deployment/attesta-api --timeout=240s
kubectl -n "$NS" rollout status deployment/prom-stub --timeout=240s
kubectl -n "$NS" rollout status deployment/loadgen --timeout=240s
stub() { kubectl -n "$NS" exec deployment/prom-stub -- python -c "import urllib.request; urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:9090$1', method='POST'))"; }
# Every revision's checks fail unless its hash is the "healthy" one: none is.
stub "/admin/healthy?hash=none"

step install "kubectl get pods -n attesta-system"

log "scene: a canary gets a share of real traffic"
cat >"$WORK/checkout.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: checkout
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: checkout-rollout
spec:
  replicas: 2
  selector: {matchLabels: {app: checkout}}
  template:
    metadata: {labels: {app: checkout}}
    spec:
      containers:
        - name: app
          image: $AGNHOST
          args: ["serve-hostname"]
          ports: [{containerPort: 9376}]
---
apiVersion: v1
kind: Service
metadata: {name: checkout, namespace: $NS}
spec:
  selector: {app: checkout}
  ports: [{port: 80, targetPort: 9376}]
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: checkout-rollout
  namespace: $NS
spec:
  deploymentRef: checkout
  canary:
    steps:
      - weight: 33
        pause: 90s
EOF
step canary-apply "kubectl apply -f checkout.yaml"
wait_for "checkout at rest" 240 bash -c "[[ \"\$(kubectl -n $NS get rolloutguard checkout-rollout -o 'jsonpath={.status.canary.phase}')\" == Idle && \"\$(kubectl -n $NS get deployment checkout -o 'jsonpath={.spec.replicas}')\" == 0 ]]"
kubectl -n "$NS" rollout status deployment/checkout-primary --timeout=120s
step canary-rest "kubectl get deploy -n $NS checkout checkout-primary"
step canary-new "kubectl set env -n $NS deployment/checkout VERSION=v1.8.0"
wait_for "checkout canary has its pod" 180 bash -c "[[ \"\$(kubectl -n $NS get rolloutguard checkout-rollout -o 'jsonpath={.status.canary.phase}')\" == Progressing && \"\$(kubectl -n $NS get deployment checkout -o 'jsonpath={.status.availableReplicas}')\" == 1 ]]"
sleep 5
step canary-status "kubectl get rolloutguard -n $NS checkout-rollout -o custom-columns=PHASE:.status.canary.phase,WEIGHT:.status.canary.weight,REVISION:.status.canary.canaryRevision"
step canary-share "kubectl exec -n $NS deploy/loadgen -- python /share/share.py"
wait_for "checkout promoted" 240 bash -c "[[ \"\$(kubectl -n $NS get rolloutguard checkout-rollout -o 'jsonpath={.status.canary.phase}')\" == Idle && \"\$(kubectl -n $NS get deployment checkout -o 'jsonpath={.spec.replicas}')\" == 0 ]]"
step canary-promoted "kubectl get rolloutdecisions -n $NS -l attesta.io/guard=checkout-rollout --sort-by=.spec.sequence -o custom-columns=SEQ:.spec.sequence,ACTION:.spec.action,TRIGGER:.spec.trigger,REVISION:.spec.deployment.revision"

log "scene: a failing canary is taken out of traffic (Gateway API)"
cat >"$WORK/payments-api.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payments-api
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: payments-api-rollout
spec:
  replicas: 2
  selector: {matchLabels: {app: payments-api}}
  template:
    metadata: {labels: {app: payments-api}}
    spec:
      containers:
        - name: app
          image: $AGNHOST
          args: ["serve-hostname"]
          ports: [{containerPort: 9376}]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: {name: payments-api, namespace: $NS}
spec:
  parentRefs: [{name: public-gateway}]
  rules:
    - backendRefs:
        - {name: payments-api-primary, port: 80}
        - {name: payments-api-canary, port: 80}
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: payments-api-rollout
  namespace: $NS
spec:
  deploymentRef: payments-api
  canary:
    steps:
      - {weight: 10, pause: 1m}
      - {weight: 50, pause: 1m}
    trafficRouting:
      gatewayAPI: {httpRoute: payments-api}
  checks: {errorRateThreshold: 1, window: 1m, interval: 5s, minRequests: 10, successfulChecks: 1}
  rollback: {consecutiveFailures: 2}
EOF
kubectl apply -f "$WORK/payments-api.yaml" >/dev/null
weights='{.spec.rules[0].backendRefs[*].weight}'
wait_for "payments-api at rest" 240 bash -c "[[ \"\$(kubectl -n $NS get httproute payments-api -o 'jsonpath=$weights')\" == '100 0' ]]"
step gw-rest "kubectl get httproute -n $NS payments-api -o jsonpath='$weights'"
step gw-new "kubectl set env -n $NS deployment/payments-api VERSION=v2.4.1"
wait_for "payments-api canary at 10%" 120 bash -c "[[ \"\$(kubectl -n $NS get httproute payments-api -o 'jsonpath=$weights')\" == '90 10' ]]"
step gw-ten "kubectl get httproute -n $NS payments-api -o jsonpath='$weights'"
wait_for "payments-api canary aborted" 180 bash -c "[[ \"\$(kubectl -n $NS get rolloutguard payments-api-rollout -o 'jsonpath={.status.canary.phase}')\" == Aborted ]]"
step gw-after "kubectl get httproute -n $NS payments-api -o jsonpath='$weights'"
step gw-record "kubectl get rolloutdecisions -n $NS -l attesta.io/guard=payments-api-rollout -o json | jq -r '.items[] | select(.spec.action==\"CanaryAborted\") | .spec | \"\(.action) \(.trigger)\", (.evidence[] | select(.name==\"error-rate\") | \"\(.name) \(.value) (threshold \(.threshold))\")'"

log "scene: the SLO gate holds a rollout while the error budget is spent"
cat >"$WORK/ledger.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ledger
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: ledger-rollout
spec:
  replicas: 4
  minReadySeconds: 20
  strategy: {rollingUpdate: {maxSurge: 1, maxUnavailable: 0}}
  selector: {matchLabels: {app: ledger}}
  template:
    metadata: {labels: {app: ledger}}
    spec:
      containers:
        - name: app
          image: $AGNHOST
          args: ["serve-hostname"]
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: ledger-rollout
  namespace: $NS
spec:
  deploymentRef: ledger
  sloGate:
    availabilityTarget: 99.9
    recheckInterval: 10s
EOF
stub "/admin/slo?service=ledger&errorRatio=0"
kubectl apply -f "$WORK/ledger.yaml" >/dev/null
kubectl -n "$NS" rollout status deployment/ledger --timeout=300s
wait_for "ledger recorded" 120 bash -c "[[ -n \"\$(kubectl -n $NS get rolloutguard ledger-rollout -o 'jsonpath={.status.healthyTemplateHash}')\" ]]"
stub "/admin/slo?service=ledger&errorRatio=0.005"
step gate-new "kubectl set env -n $NS deployment/ledger VERSION=v3.1.0"
wait_for "ledger paused" 90 bash -c "[[ \"\$(kubectl -n $NS get deployment ledger -o 'jsonpath={.spec.paused}')\" == true ]]"
step gate-paused "kubectl get deploy -n $NS ledger -o jsonpath='{.spec.paused}{\"  \"}{.metadata.annotations.attesta\.io/paused-reason}'"
stub "/admin/slo?service=ledger&errorRatio=0"
wait_for "ledger resumed" 90 bash -c "[[ \"\$(kubectl -n $NS get rolloutguard ledger-rollout -o 'jsonpath={.status.sloGate.resumed}')\" == true ]]"
step gate-record "kubectl get rolloutdecisions -n $NS -l attesta.io/guard=ledger-rollout --sort-by=.spec.sequence -o custom-columns=SEQ:.spec.sequence,ACTION:.spec.action,TRIGGER:.spec.trigger"

log "scene: the records"
archived() {
  for g in checkout-rollout payments-api-rollout ledger-rollout; do
    [[ "$(kubectl -n "$NS" get rolloutguard "$g" -o 'jsonpath={.status.archivedSequence}')" == "$(kubectl -n "$NS" get rolloutguard "$g" -o 'jsonpath={.status.lastDecision.sequence}')" ]] || return 1
  done
}
wait_for "every decision archived" 90 archived
step records-verify "kubectl get rolloutguards,rolloutdecisions -n $NS -o json > records.json && attesta verify -f records.json --public-key public.pem | grep -E '^(OK|BROKEN)'"
victim="$(kubectl -n "$NS" get rolloutdecisions -l attesta.io/guard=payments-api-rollout -o json | jq -r '.items[] | select(.spec.action=="CanaryAborted") | .metadata.name')"
step delete-refused "kubectl delete rolloutdecision -n $NS $victim"
step edit-refused "kubectl patch rolloutdecision -n $NS $victim --type=merge -p '{\"spec\":{\"trigger\":\"Manual\"}}'"
step tamper-export "jq '(.items[] | select(.spec.action? == \"CanaryAborted\") | .spec.evidence[] | select(.name == \"error-rate\") | .value) = \"0.4%\"' records.json > edited.json && attesta verify -f edited.json --public-key public.pem | grep -E '^(OK|BROKEN)'"
kubectl -n attesta-system port-forward svc/attesta-api 18080:8080 >/dev/null 2>&1 &
pf=$!
wait_for "API reachable" 30 curl -fsS http://127.0.0.1:18080/livez
step api-verify "curl -s -H \"Authorization: Bearer \$(cat token)\" localhost:18080/api/v1/guards/$NS/payments-api-rollout | jq -c '{chain, archive}'"
kill "$pf" 2>/dev/null || true

python3 - "$STEPS" "$CAPTURE" "$(git rev-parse --short HEAD)" "$(kubectl version -o json | jq -r .serverVersion.gitVersion)" <<'PY'
import json, sys
steps_path, out_path, commit, k8s = sys.argv[1:]
steps = {}
for line in open(steps_path):
    s = json.loads(line)
    steps[s["id"]] = s
json.dump({"commit": commit, "kubernetes": k8s, "steps": steps}, open(out_path, "w"), indent=1)
print(f"wrote {len(steps)} steps to {out_path}")
PY
