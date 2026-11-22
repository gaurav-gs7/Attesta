#!/usr/bin/env bash
# End-to-end test of the RolloutGuard controller on a live cluster (kind in CI).
#
# Asserts that:
#   1. the controller records the healthy template of a rolled-out Deployment;
#   2. a rollout that exceeds its progress deadline is rolled back to that
#      template, with audit annotations on the Deployment;
#   3. the rollback happens exactly once and the controller then leaves the
#      Deployment alone;
#   4. a paused Deployment is reported as Paused and never modified;
#   5. the leader's /metrics counts the rollback.
#
# Prerequisites: kubectl pointing at a disposable cluster, and the image
# attesta-controller:e2e loaded into it (see test/e2e/kustomization.yaml).
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NS="${E2E_NAMESPACE:-attesta-e2e}"
GOOD_IMAGE="${GOOD_IMAGE:-registry.k8s.io/pause:3.10}"
BAD_IMAGE="${BAD_IMAGE:-registry.k8s.io/pause:attesta-e2e-does-not-exist}"
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

guard() { kubectl -n "$NS" get rolloutguard app-rollout -o "jsonpath={.status.$1}"; }
image() { kubectl -n "$NS" get deployment app -o 'jsonpath={.spec.template.spec.containers[0].image}'; }
annotation() { kubectl -n "$NS" get deployment app -o "jsonpath={.metadata.annotations.attesta\\.io/$1}"; }
has_healthy_template() { [[ -n "$(guard healthyTemplateHash)" ]]; }
image_is() { [[ "$(image)" == "$1" ]]; }
phase_is() { [[ "$(guard phase)" == "$1" ]]; }

leader_metric() {
  local leader
  leader="$(kubectl -n attesta-system get lease attesta-controller -o 'jsonpath={.spec.holderIdentity}')"
  kubectl get --raw "/api/v1/namespaces/attesta-system/pods/${leader}:8080/proxy/metrics" | grep -F "$1" | awk '{print $2}'
}

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
log "ok: leader metrics count exactly one rollback"

events="$(kubectl -n "$NS" get events --field-selector involvedObject.name=app-rollout,reason=RollbackTriggered -o name | wc -l)"
((events >= 1)) || fail "no RollbackTriggered event on the guard"

log "PASS: rollback restored the healthy template once, paused rollouts were left alone"
