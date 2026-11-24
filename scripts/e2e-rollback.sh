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
#      back after consecutive failed checks of the new revision's metrics.
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
  kubectl -n "$NS" get rolloutguard app-rollout -o yaml
  kubectl -n "$NS" describe deployment app
  kubectl -n "$NS" get replicasets -o wide --show-labels
  kubectl -n "$NS" get rolloutguard app2-rollout -o yaml
  kubectl -n "$NS" describe deployment app2
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

log "installing CRDs and the controller"
kubectl apply -k deploy/crds
kubectl wait --for=condition=Established crd/rolloutguards.attesta.io --timeout=60s
kubectl apply -k test/e2e
kubectl -n attesta-system rollout status deployment/attesta-controller --timeout=180s
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
  got="$(jq -cjS ".items[$i].spec | del(.hash)" /tmp/decisions.json | sha256sum | cut -d' ' -f1)"
  [[ "$got" == "$want" ]] || fail "decision $(jq -r ".items[$i].metadata.name" /tmp/decisions.json) hashes to $got, claims $want"
done
if kubectl -n "$NS" patch rolloutdecision "$(jq -r '.items[0].metadata.name' /tmp/decisions.json)" --type=merge \
  -p '{"spec":{"reason":"edited"}}' 2>/tmp/patch.err; then
  fail "the API server accepted an edit to a decision record"
fi
grep -q immutable /tmp/patch.err || fail "editing a decision failed for the wrong reason: $(cat /tmp/patch.err)"
log "ok: $count decision records verified by sha256 over jq's canonical form; edits are refused"

log "PASS: deadline and metric-analysis rollbacks both restored the healthy template once and are on record; paused rollouts were left alone"
