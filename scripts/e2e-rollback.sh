#!/usr/bin/env bash
# End-to-end test of the RolloutGuard controller on a live cluster (kind in CI).
#
# Asserts that:
#   1. the controller records the healthy template of a rolled-out Deployment
#      that consents to rollback through attesta.io/rollback-guards;
#   2. a rollout that exceeds its progress deadline is rolled back to that
#      template, with audit annotations on the Deployment;
#   3. the rollback happens exactly once and the controller then leaves the
#      Deployment alone;
#   4. a paused Deployment is reported as Paused and never modified;
#   5. the leader's /metrics counts the rollback and the standby replica did
#      nothing;
#   6. after the leader pod is deleted the standby takes the Lease, and the
#      new leader does not repeat the rollback it inherits;
#   7. when the bad template is re-applied after the rollback, as GitOps
#      self-heal would, the guard reports RollbackReverted and the controller
#      does not roll back again;
#   8. with metric analysis, a rollout Kubernetes considers healthy is rolled
#      back after consecutive failed checks of the new revision's metrics;
#   9. the SLO gate pauses a revision while the service's error budget is
#      spent, holds it, and resumes it when the SLOs recover;
#  10. a canary without a router gets its share of real traffic as a share
#      of pods, steps through and is promoted into <deployment>-primary;
#  11. a canary routed by Gateway API gets its HTTPRoute weight, fails its
#      metric checks and is taken out of traffic;
#  12. every decision record hashes correctly (checked with jq and sha256sum),
#      is signed, cannot be edited or deleted even by cluster-admin, reaches
#      the archive outside the cluster, and the API verifies all of it.
#
# Prerequisites: kubectl pointing at a disposable cluster, and the image
# attesta-controller:e2e loaded into it (see test/e2e/kustomization.yaml).
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NS="${E2E_NAMESPACE:-attesta-e2e}"
GOOD_IMAGE="${GOOD_IMAGE:-registry.k8s.io/pause:3.10}"
BAD_IMAGE="${BAD_IMAGE:-registry.k8s.io/pause:attesta-e2e-does-not-exist}"
OTHER_GOOD_IMAGE="${OTHER_GOOD_IMAGE:-registry.k8s.io/pause:3.9}"
STUB_IMAGE="${STUB_IMAGE:-python:3.12-alpine}"
SETTLE_SECONDS="${SETTLE_SECONDS:-45}"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() { log "FAIL: $*"; exit 1; }

dump() {
  log "collecting diagnostics"
  set +e
  kubectl -n attesta-system get pods -o wide
  kubectl -n attesta-system logs -l app.kubernetes.io/name=attesta-controller --tail=200 --prefix
  kubectl -n attesta-system logs deployment/attesta-api --tail=100
  kubectl -n "$NS" get rolloutguard app-rollout -o yaml
  kubectl -n "$NS" describe deployment app
  kubectl -n "$NS" get replicasets -o wide --show-labels
  kubectl -n "$NS" get rolloutguard app2-rollout -o yaml
  kubectl -n "$NS" describe deployment app2
  kubectl -n "$NS" get rolloutguard app3-rollout -o yaml
  kubectl -n "$NS" describe deployment app3
  kubectl -n "$NS" get rolloutguard app4-rollout app5-rollout -o yaml
  kubectl -n "$NS" get deployments,services,httproutes -o wide
  kubectl -n "$NS" logs deployment/prom-stub --tail=50
  kubectl -n "$NS" get events --sort-by=.lastTimestamp
}
trap 'log "error near line $LINENO"; dump' ERR

# wait_until <description> <timeout-seconds> <command...>
wait_until() {
  local what="$1" timeout="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      dump
      fail "timed out after ${timeout}s waiting for: $what"
    fi
    sleep 2
  done
  log "ok: $what"
}

# Helpers act on the Deployment named in $APP and its guard $APP-rollout.
APP=app
guard() { kubectl -n "$NS" get rolloutguard "$APP-rollout" -o "jsonpath={.status.$1}"; }
image() { kubectl -n "$NS" get deployment "$APP" -o 'jsonpath={.spec.template.spec.containers[0].image}'; }
annotation() { kubectl -n "$NS" get deployment "$APP" -o "jsonpath={.metadata.annotations.attesta\\.io/$1}"; }
has_healthy_template() { [[ -n "$(guard healthyTemplateHash)" ]]; }
image_is() { [[ "$(image)" == "$1" ]]; }
phase_is() { [[ "$(guard phase)" == "$1" ]]; }

leader() { kubectl -n attesta-system get lease attesta-controller -o 'jsonpath={.spec.holderIdentity}'; }
# controller-runtime's leader identity is "<pod name>_<uuid>".
leader_pod() { local id; id="$(leader)"; echo "${id%%_*}"; }

pod_metric() {
  kubectl get --raw "/api/v1/namespaces/attesta-system/pods/$1:8080/proxy/metrics" | grep -F "$2" | awk '{print $2}'
}

leader_metric() { pod_metric "$(leader_pod)" "$1"; }

# all_replicas_metric <metric> sums a counter over every controller pod; a
# replica that never acted does not export the series and counts as zero.
all_replicas_metric() {
  local total=0 pod value
  for pod in $(kubectl -n attesta-system get pods -l app.kubernetes.io/name=attesta-controller -o 'jsonpath={.items[*].metadata.name}'); do
    value="$(pod_metric "$pod" "$1" || true)"
    total=$((total + ${value:-0}))
  done
  echo "$total"
}

holder_changed_from() { local now; now="$(leader)"; [[ -n "$now" && "$now" != "$1" ]]; }

log "creating the decision signing key and the API token"
KEY_DIR="$(mktemp -d)"
openssl genpkey -algorithm ed25519 -out "$KEY_DIR/key.pem"
openssl pkey -in "$KEY_DIR/key.pem" -pubout -out "$KEY_DIR/public.pem"
API_TOKEN="$(openssl rand -hex 24)"
printf %s "$API_TOKEN" >"$KEY_DIR/token"
export DECISION_PUBLIC_KEY="$KEY_DIR/public.pem"
kubectl create namespace attesta-system --dry-run=client -o yaml | kubectl apply -f -
kubectl -n attesta-system create secret generic attesta-decision-signing-key --from-file=key.pem="$KEY_DIR/key.pem"
kubectl -n attesta-system create secret generic attesta-archive-token --from-file=token="$KEY_DIR/token"
kubectl -n attesta-system create secret generic attesta-api --from-file=token="$KEY_DIR/token" --from-file=public.pem="$KEY_DIR/public.pem"

log "installing CRDs, the controller and the API"
kubectl apply -k deploy/crds
kubectl wait --for=condition=Established crd/rolloutguards.attesta.io --timeout=60s
kubectl apply -k test/e2e
kubectl -n attesta-system rollout status deployment/attesta-controller --timeout=180s
kubectl -n attesta-system rollout status deployment/attesta-api --timeout=180s
wait_until "a controller replica holds the leader lease" 60 \
  bash -c "[[ -n \"\$(kubectl -n attesta-system get lease attesta-controller -o 'jsonpath={.spec.holderIdentity}')\" ]]"

log "creating a guarded Deployment on $GOOD_IMAGE"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: app-rollout
spec:
  replicas: 2
  progressDeadlineSeconds: 20
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: e2e-app
  template:
    metadata:
      labels:
        app: e2e-app
    spec:
      containers:
        - name: app
          image: $GOOD_IMAGE
          resources:
            requests:
              cpu: 5m
              memory: 8Mi
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: app-rollout
  namespace: $NS
spec:
  deploymentRef: app
  strategy: rolling
  rollback:
    enabled: true
EOF
kubectl -n "$NS" rollout status deployment/app --timeout=120s
wait_until "guard records the healthy template" 120 has_healthy_template
healthy_hash="$(guard healthyTemplateHash)"
log "healthy template hash: $healthy_hash (revision $(guard healthyRevision))"
authorized="$(kubectl -n "$NS" get rolloutguard app-rollout -o 'jsonpath={.status.conditions[?(@.type=="RollbackAuthorized")].status}')"
[[ "$authorized" == "True" ]] || fail "RollbackAuthorized=$authorized for a Deployment that names the guard"

log "pausing the Deployment: the controller must report Paused and change nothing"
kubectl -n "$NS" rollout pause deployment/app
wait_until "guard reports Paused" 60 phase_is Paused
generation="$(kubectl -n "$NS" get deployment app -o 'jsonpath={.metadata.generation}')"
sleep 15
[[ "$(kubectl -n "$NS" get deployment app -o 'jsonpath={.metadata.generation}')" == "$generation" ]] ||
  fail "controller modified a paused Deployment"
[[ -z "$(annotation rollback-at)" ]] || fail "controller rolled back a paused Deployment"
kubectl -n "$NS" rollout resume deployment/app
wait_until "guard reports Healthy after resume" 60 phase_is Healthy

log "rolling out $BAD_IMAGE, which cannot be pulled"
kubectl -n "$NS" set image deployment/app app="$BAD_IMAGE"
wait_until "controller restores $GOOD_IMAGE" 180 image_is "$GOOD_IMAGE"

[[ "$(annotation rollback-to-revision)" == "1" ]] || fail "rollback-to-revision is '$(annotation rollback-to-revision)', want 1"
[[ "$(annotation rollback-from-revision)" == "2" ]] || fail "rollback-from-revision is '$(annotation rollback-from-revision)', want 2"
[[ "$(annotation rollback-by-guard)" == "app-rollout" ]] || fail "rollback-by-guard is '$(annotation rollback-by-guard)'"
[[ "$(guard rollbackTemplateHash)" == "$healthy_hash" ]] || fail "guard rolled back to '$(guard rollbackTemplateHash)', want $healthy_hash"
kubectl -n "$NS" rollout status deployment/app --timeout=120s
log "ok: Deployment is healthy again on $GOOD_IMAGE"

log "waiting ${SETTLE_SECONDS}s to confirm there is no second rollback"
rolled_back_at="$(annotation rollback-at)"
revision="$(kubectl -n "$NS" get deployment app -o 'jsonpath={.metadata.annotations.deployment\.kubernetes\.io/revision}')"
sleep "$SETTLE_SECONDS"
[[ "$(annotation rollback-at)" == "$rolled_back_at" ]] || fail "controller rolled back again"
[[ "$(kubectl -n "$NS" get deployment app -o 'jsonpath={.metadata.annotations.deployment\.kubernetes\.io/revision}')" == "$revision" ]] ||
  fail "Deployment revision moved after the rollback"
image_is "$GOOD_IMAGE" || fail "image changed after the rollback: $(image)"

triggered="$(leader_metric 'attesta_rolloutguard_rollback_total{outcome="triggered"}')"
[[ "$triggered" == "1" ]] || fail "leader reports rollback_total{outcome=\"triggered\"}=${triggered:-absent}, want 1"
total="$(all_replicas_metric 'attesta_rolloutguard_rollback_total{outcome="triggered"}')"
[[ "$total" == "1" ]] || fail "replicas together report $total rollbacks, want 1: the standby acted"
log "ok: leader metrics count exactly one rollback and the standby did nothing"

events="$(kubectl -n "$NS" get events --field-selector involvedObject.name=app-rollout,reason=RollbackTriggered -o name | wc -l)"
((events >= 1)) || fail "no RollbackTriggered event on the guard"

old_leader="$(leader)"
log "failover: deleting the leader pod $(leader_pod)"
kubectl -n attesta-system delete pod "$(leader_pod)" --wait=false
wait_until "the standby to take the leader lease" 60 holder_changed_from "$old_leader"
log "ok: $(leader_pod) now holds the lease"
wait_until "the new leader to sync and report ready" 60 \
  kubectl -n attesta-system wait --for=condition=Ready "pod/$(leader_pod)" --timeout=5s
sleep 10
[[ "$(annotation rollback-at)" == "$rolled_back_at" ]] || fail "the new leader repeated the inherited rollback"
log "ok: the new leader left the already rolled-back Deployment alone"

log "gitops revert: re-applying $BAD_IMAGE, as a self-healing GitOps tool would"
kubectl -n "$NS" set image deployment/app app="$BAD_IMAGE"
wait_until "the guard to report the reverted rollback" 120 \
  bash -c "[[ \"\$(kubectl -n $NS get rolloutguard app-rollout -o 'jsonpath={.status.reason}')\" == RollbackReverted ]]"
sleep 10
image_is "$BAD_IMAGE" || fail "controller rolled back the re-applied template: image is $(image)"
[[ "$(annotation rollback-at)" == "$rolled_back_at" ]] || fail "controller rolled back a second time after the revert"
reverted="$(kubectl -n "$NS" get events --field-selector involvedObject.name=app-rollout,reason=RollbackReverted -o name | wc -l)"
((reverted >= 1)) || fail "no RollbackReverted event on the guard"
log "ok: re-applied template reported as RollbackReverted and left alone"

log "metric analysis: deploying the in-cluster Prometheus stub"
kubectl -n "$NS" create configmap prom-stub --from-file=stub.py=test/e2e/prometheus_stub.py --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prom-stub
  namespace: $NS
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prom-stub
  template:
    metadata:
      labels:
        app: prom-stub
    spec:
      containers:
        - name: stub
          image: $STUB_IMAGE
          command: ["python", "/stub/stub.py"]
          ports:
            - containerPort: 9090
          volumeMounts:
            - name: stub
              mountPath: /stub
      volumes:
        - name: stub
          configMap:
            name: prom-stub
---
apiVersion: v1
kind: Service
metadata:
  name: prom-stub
  namespace: $NS
spec:
  selector:
    app: prom-stub
  ports:
    - port: 9090
      targetPort: 9090
EOF
kubectl -n "$NS" rollout status deployment/prom-stub --timeout=180s

log "metric analysis: creating a guarded Deployment with spec.checks"
APP=app2
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app2
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: app2-rollout
spec:
  replicas: 2
  selector:
    matchLabels:
      app: e2e-app2
  template:
    metadata:
      labels:
        app: e2e-app2
    spec:
      containers:
        - name: app
          image: $GOOD_IMAGE
          resources:
            requests:
              cpu: 5m
              memory: 8Mi
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: app2-rollout
  namespace: $NS
spec:
  deploymentRef: app2
  strategy: rolling
  checks:
    errorRateThreshold: 1
    latencyP95ThresholdMs: 300
    window: 1m
    interval: 5s
    minRequests: 10
    successfulChecks: 2
  rollback:
    enabled: true
    consecutiveFailures: 2
EOF
kubectl -n "$NS" rollout status deployment/app2 --timeout=120s
wait_until "guard records app2's healthy template after passing checks" 120 has_healthy_template
healthy_hash="$(guard healthyTemplateHash)"
log "app2 healthy template hash: $healthy_hash"

log "the stub now reports a 20% error rate for any revision other than $healthy_hash"
kubectl -n "$NS" exec deployment/prom-stub -- python -c \
  "import urllib.request; urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:9090/admin/healthy?hash=$healthy_hash', method='POST'))"

log "rolling app2 out to $OTHER_GOOD_IMAGE, which pulls and runs fine"
kubectl -n "$NS" set image deployment/app2 app="$OTHER_GOOD_IMAGE"
wait_until "controller rolls app2 back to $GOOD_IMAGE on failed metric checks" 120 image_is "$GOOD_IMAGE"

[[ "$(annotation rollback-reason)" == "AnalysisFailed" ]] || fail "app2 rollback-reason is '$(annotation rollback-reason)', want AnalysisFailed"
[[ "$(guard rollbackTemplateHash)" == "$healthy_hash" ]] || fail "app2 rolled back to '$(guard rollbackTemplateHash)', want $healthy_hash"
if kubectl -n "$NS" get deployment app2 -o 'jsonpath={.status.conditions[*].reason}' | grep -q ProgressDeadlineExceeded; then
  fail "app2 hit ProgressDeadlineExceeded; the rollback must come from metric analysis"
fi
message="$(kubectl -n "$NS" get rolloutguard app2-rollout -o 'jsonpath={.status.conditions[?(@.type=="RolledBack")].message}')"
[[ "$message" == *"AnalysisFailed"* && "$message" == *"error rate 20.00% > 1.00%"* ]] ||
  fail "RolledBack condition does not carry the measured cause: $message"
kubectl -n "$NS" rollout status deployment/app2 --timeout=120s

failed="$(leader_metric 'attesta_rolloutguard_analysis_total{result="Failed"}')"
((${failed:-0} >= 2)) || fail "leader reports analysis_total{result=\"Failed\"}=${failed:-absent}, want at least 2"
triggered="$(leader_metric 'attesta_rolloutguard_rollback_total{outcome="triggered"}')"
# The leader changed after the first rollback, so its counter holds only this one.
[[ "$triggered" == "1" ]] || fail "new leader reports rollback_total{outcome=\"triggered\"}=${triggered:-absent}, want 1"
log "ok: app2 rolled back by metric analysis: $message"

log "SLO gate: a guarded service whose error budget is spent"
APP=app3
stub_slo() {
  kubectl -n "$NS" exec deployment/prom-stub -- python -c \
    "import urllib.request; urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:9090/admin/slo?service=app3&errorRatio=$1', method='POST'))"
}
stub_slo 0
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app3
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: app3-rollout
spec:
  replicas: 4
  # Slow enough that the gate acts while the revision is still rolling out.
  minReadySeconds: 20
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: e2e-app3
  template:
    metadata:
      labels:
        app: e2e-app3
    spec:
      containers:
        - name: app
          image: $GOOD_IMAGE
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: app3-rollout
  namespace: $NS
spec:
  deploymentRef: app3
  strategy: rolling
  sloGate:
    availabilityTarget: 99.9
    recheckInterval: 10s
EOF
kubectl -n "$NS" rollout status deployment/app3 --timeout=240s
wait_until "guard records app3's first revision" 120 has_healthy_template

log "SLO gate: 0.5% of requests fail against a 0.1% budget; rolling app3 out to $OTHER_GOOD_IMAGE"
stub_slo 0.005
kubectl -n "$NS" set image deployment/app3 app="$OTHER_GOOD_IMAGE"
paused() { [[ "$(kubectl -n "$NS" get deployment app3 -o 'jsonpath={.spec.paused}')" == "true" ]]; }
wait_until "the SLO gate pauses app3's new revision" 60 paused
[[ "$(annotation paused-by-guard)" == "app3-rollout" ]] || fail "app3 paused without the gate's annotation"
[[ "$(guard reason)" == "SLOGatePaused" ]] || fail "app3 guard reason is $(guard reason), want SLOGatePaused"
sleep 15
paused || fail "the gate resumed app3 while its budget was still spent"

log "SLO gate: the errors stop; the gate resumes the rollout on its own"
stub_slo 0
resumed() { ! paused && [[ "$(kubectl -n "$NS" get rolloutguard app3-rollout -o 'jsonpath={.status.sloGate.resumed}')" == "true" ]]; }
wait_until "the SLO gate resumes app3" 60 resumed
kubectl -n "$NS" rollout status deployment/app3 --timeout=300s
image_is "$OTHER_GOOD_IMAGE" || fail "app3 is on $(image) after the resumed rollout"
kubectl -n "$NS" get rolloutdecisions -o json >/tmp/app3-decisions.json
actions="$(jq -r '[.items[] | select(.spec.guard=="app3-rollout")] | sort_by(.spec.sequence) | map(.spec.action) | join(",")' /tmp/app3-decisions.json)"
[[ "$actions" == Promoted,Paused,Resumed* ]] || fail "app3 decisions are $actions, want Promoted,Paused,Resumed..."
budget="$(jq -r '.items[] | select(.spec.guard=="app3-rollout" and .spec.action=="Paused") | .spec.evidence[] | select(.name=="error-budget-remaining") | "\(.value) \(.threshold) \(.passed)"' /tmp/app3-decisions.json)"
[[ "$budget" == "-400% >= 10% false" ]] || fail "app3's pause carries budget evidence '$budget', want '-400% >= 10% false'"
log "ok: the gate paused app3 on a spent budget ($budget), held it, and resumed it when the SLOs recovered: $actions"

log "canary: replica-based traffic split, checked with real requests"
AGNHOST_IMAGE="${AGNHOST_IMAGE:-registry.k8s.io/e2e-test-images/agnhost:2.52}"
APP=app4
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app4
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: app4-rollout
spec:
  replicas: 2
  selector:
    matchLabels:
      app: e2e-app4
  template:
    metadata:
      labels:
        app: e2e-app4
    spec:
      containers:
        - name: app
          image: $AGNHOST_IMAGE
          args: ["serve-hostname"]
          ports:
            - containerPort: 9376
---
apiVersion: v1
kind: Service
metadata:
  name: app4
  namespace: $NS
spec:
  selector:
    app: e2e-app4
  ports:
    - port: 80
      targetPort: 9376
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: app4-rollout
  namespace: $NS
spec:
  deploymentRef: app4
  canary:
    steps:
      - weight: 33
        pause: 60s
EOF
canary_phase() { kubectl -n "$NS" get rolloutguard "$APP-rollout" -o 'jsonpath={.status.canary.phase}'; }
canary_phase_is() { [[ "$(canary_phase)" == "$1" ]]; }
replicas_of() { kubectl -n "$NS" get deployment "$1" -o 'jsonpath={.spec.replicas}'; }
available_of() { kubectl -n "$NS" get deployment "$1" -o 'jsonpath={.status.availableReplicas}'; }
wait_until "app4's primary is created and serves, and app4 is at rest" 180 \
  bash -c "[[ \"\$(kubectl -n $NS get rolloutguard app4-rollout -o 'jsonpath={.status.canary.phase}')\" == Idle && \"\$(kubectl -n $NS get deployment app4 -o 'jsonpath={.spec.replicas}')\" == 0 ]]"
# share_from_canary <service>: the fraction of 300 requests served by pods
# that are not the primary's, in percent.
share_from_canary() {
  kubectl -n "$NS" exec deployment/prom-stub -- python -c "
import urllib.request
hits = [urllib.request.urlopen('http://$1.$NS.svc', timeout=5).read().decode() for _ in range(300)]
print(round(100 * sum(1 for h in hits if '-primary-' not in h) / len(hits)))"
}
at_rest="$(share_from_canary app4)"
[[ "$at_rest" == "0" ]] || fail "at rest, $at_rest% of app4's traffic reached non-primary pods"

kubectl -n "$NS" set env deployment/app4 REVISION=2
wait_until "app4's new revision gets one pod beside the primary's two" 120 \
  bash -c "[[ \"\$(kubectl -n $NS get rolloutguard app4-rollout -o 'jsonpath={.status.canary.phase}')\" == Progressing && \"\$(kubectl -n $NS get deployment app4 -o 'jsonpath={.status.availableReplicas}')\" == 1 ]]"
sleep 5
share="$(share_from_canary app4)"
((share >= 15 && share <= 55)) || fail "the canary served $share% of 300 requests at a 33% step"
log "ok: the canary served $share% of 300 real requests at its 33% step"
wait_until "app4's canary is promoted into the primary" 240 canary_phase_is Idle
[[ "$(replicas_of app4)" == "0" ]] || fail "app4 is not back at zero after promotion"
kubectl -n "$NS" rollout status deployment/app4-primary --timeout=120s
kubectl -n "$NS" get deployment app4-primary -o jsonpath='{.spec.template.spec.containers[0].env}' | grep -q REVISION ||
  fail "the promoted template did not reach app4-primary"
[[ "$(share_from_canary app4)" == "0" ]] || fail "after promotion, traffic reached non-primary pods"
app4_actions="$(kubectl -n "$NS" get rolloutdecisions -o json |
  jq -r '[.items[] | select(.spec.guard=="app4-rollout")] | sort_by(.spec.sequence) | map(.spec.action) | join(",")')"
[[ "$app4_actions" == "Promoted,CanaryStarted,Promoted" ]] || fail "app4 decisions are $app4_actions"
log "ok: app4 stepped through its canary and was promoted: $app4_actions"

log "canary: Gateway API weights, and an abort on failed metric checks"
kubectl apply --server-side -f "${GATEWAY_API_CRDS:-https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.1/standard-install.yaml}"
kubectl wait --for=condition=Established crd/httproutes.gateway.networking.k8s.io --timeout=60s
APP=app5
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app5
  namespace: $NS
  annotations:
    attesta.io/rollback-guards: app5-rollout
spec:
  replicas: 2
  selector:
    matchLabels:
      app: e2e-app5
  template:
    metadata:
      labels:
        app: e2e-app5
    spec:
      containers:
        - name: app
          image: $AGNHOST_IMAGE
          args: ["serve-hostname"]
          ports:
            - containerPort: 9376
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: app5
  namespace: $NS
spec:
  parentRefs:
    - name: e2e-gateway
  rules:
    - backendRefs:
        - name: app5-primary
          port: 80
        - name: app5-canary
          port: 80
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: app5-rollout
  namespace: $NS
spec:
  deploymentRef: app5
  canary:
    steps:
      - weight: 10
        pause: 30s
      - weight: 50
        pause: 30s
    trafficRouting:
      gatewayAPI:
        httpRoute: app5
  checks:
    errorRateThreshold: 1
    window: 1m
    interval: 5s
    minRequests: 10
    successfulChecks: 1
  rollback:
    consecutiveFailures: 2
EOF
weights() { kubectl -n "$NS" get httproute app5 -o 'jsonpath={.spec.rules[0].backendRefs[*].weight}'; }
wait_until "app5 is at rest with all traffic routed to its primary" 180 \
  bash -c "[[ \"\$(kubectl -n $NS get httproute app5 -o 'jsonpath={.spec.rules[0].backendRefs[*].weight}')\" == '100 0' ]]"
[[ "$(kubectl -n "$NS" get service app5-primary -o 'jsonpath={.spec.selector.attesta\.io/track}')" == "primary" ]] ||
  fail "app5-primary does not select the primary's pods"
# The stub reports 20% errors for every revision but app2's healthy one.
kubectl -n "$NS" set env deployment/app5 REVISION=2
wait_until "the route sends app5's canary 10%" 60 bash -c "[[ \"\$(kubectl -n $NS get httproute app5 -o 'jsonpath={.spec.rules[0].backendRefs[*].weight}')\" == '90 10' ]]"
wait_until "app5's canary is aborted on its failed checks" 120 canary_phase_is Aborted
[[ "$(weights)" == "100 0" ]] || fail "after the abort the route sends $(weights)"
[[ "$(replicas_of app5)" == "0" ]] || fail "app5's aborted canary still has $(replicas_of app5) pods"
aborted="$(kubectl -n "$NS" get rolloutdecisions -o json |
  jq -r '.items[] | select(.spec.guard=="app5-rollout" and .spec.action=="CanaryAborted") | "\(.spec.trigger) \(.spec.evidence[] | select(.name=="error-rate") | .value)"')"
[[ "$aborted" == "AnalysisFailed 20%" ]] || fail "app5's abort record reads '$aborted', want 'AnalysisFailed 20%'"
log "ok: app5 got 10% through the HTTPRoute, failed its checks at 20% errors, and was taken out of traffic"

log "each decision is on record with its evidence, and every record hashes to what it claims"
kubectl -n "$NS" get rolloutdecisions -o json >/tmp/decisions.json
rollbacks="$(jq '[.items[] | select(.spec.guard=="app2-rollout" and .spec.action=="RolledBack")] | length' /tmp/decisions.json)"
[[ "$rollbacks" == "1" ]] || fail "app2 has $rollbacks RolledBack decision records, want 1"
measured="$(jq -r '.items[] | select(.spec.guard=="app2-rollout" and .spec.action=="RolledBack")
  | .spec.evidence[] | select(.name=="error-rate") | "\(.value) \(.threshold) \(.passed)"' /tmp/decisions.json)"
[[ "$measured" == "20% <= 1% false" ]] || fail "app2's rollback record carries error-rate evidence '$measured', want '20% <= 1% false'"
# The documented check, with no Attesta code involved.
count="$(jq '.items | length' /tmp/decisions.json)"
for ((i = 0; i < count; i++)); do
  want="$(jq -r ".items[$i].spec.hash" /tmp/decisions.json)"
  got="$(jq -cjS ".items[$i].spec | del(.hash, .signature)" /tmp/decisions.json | sha256sum | cut -d' ' -f1)"
  [[ "$got" == "$want" ]] || fail "decision $(jq -r ".items[$i].metadata.name" /tmp/decisions.json) hashes to $got, claims $want"
done
if kubectl -n "$NS" patch rolloutdecision "$(jq -r '.items[0].metadata.name' /tmp/decisions.json)" --type=merge \
  -p '{"spec":{"reason":"edited"}}' 2>/tmp/patch.err; then
  fail "the API server accepted an edit to a decision record"
fi
grep -q immutable /tmp/patch.err || fail "editing a decision failed for the wrong reason: $(cat /tmp/patch.err)"
log "ok: $count decision records verified by sha256 over jq's canonical form; edits are refused"

log "records are signed, cannot be deleted even by cluster-admin, and reach the archive outside the cluster"
jq -e '[.items[] | select(.spec.signature == null)] | length == 0' /tmp/decisions.json >/dev/null ||
  fail "unsigned decision records"
victim="$(jq -r '.items[0].metadata.name' /tmp/decisions.json)"
if kubectl -n "$NS" delete rolloutdecision "$victim" 2>/tmp/delete.err; then
  fail "cluster-admin deleted decision record $victim"
fi
grep -q "not deleted" /tmp/delete.err || fail "deleting a decision failed for the wrong reason: $(cat /tmp/delete.err)"
archived() {
  [[ "$(kubectl -n "$NS" get rolloutguard app2-rollout -o 'jsonpath={.status.archivedSequence}')" == \
    "$(kubectl -n "$NS" get rolloutguard app2-rollout -o 'jsonpath={.status.lastDecision.sequence}')" ]]
}
wait_until "every app2 decision is in the archive" 60 archived
kubectl -n attesta-system port-forward svc/attesta-api 18080:8080 >/tmp/api-port-forward.log 2>&1 &
pf=$!
wait_until "the API answers through the port-forward" 30 curl -fsS http://127.0.0.1:18080/livez
api_guard="$(curl -fsS -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:18080/api/v1/guards/$NS/app2-rollout")"
kill "$pf" 2>/dev/null || true
jq -e '.chain.verified and .chain.signaturesChecked and .archive.matches' <<<"$api_guard" >/dev/null ||
  fail "the API does not verify app2's chain against signatures and the archive: $(jq -c '{chain, archive}' <<<"$api_guard")"
log "ok: signed, undeletable, archived, and the API verifies all three: $(jq -c '{chain, archive}' <<<"$api_guard")"

log "PASS: deadline and metric-analysis rollbacks both restored the healthy template once and are on record; paused rollouts were left alone"
