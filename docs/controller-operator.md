# Attesta Controller Operator

`attesta-controller` watches `RolloutGuard` resources, compares each guard with its Deployment's live rollout status, writes status conditions, emits Kubernetes Events on state transitions, and, when explicitly enabled, restores the last healthy pod template of a rollout that Kubernetes has declared stuck.

## Control Loop

1. Watch `attesta.io/v1` `RolloutGuard` resources and the Deployments they reference.
2. Resolve `spec.deploymentRef`, falling back to `spec.serviceRef`, then to the guard name without its `-rollout` suffix.
3. Ignore Deployment status until the Deployment controller has observed the current generation. Older conditions describe the previous rollout.
4. Classify the rollout:
   - `Paused`: `spec.paused` is true. Reported, never acted on.
   - `Blocked`: the `Progressing` condition is `False` with reason `ProgressDeadlineExceeded` at the observed generation.
   - `Healthy`: every replica runs the new template, old pods are gone and all replicas are available (the Deployment controller's own definition of complete).
   - `Progressing`: anything else, including the transient states every healthy rollout passes through.
5. On `Healthy`, record the current ReplicaSet's revision and `pod-template-hash` as the last healthy template.
6. On `Blocked`, if `spec.rollback.enabled` is true, restore the last healthy template. See Rollback Rules.
7. Patch `status` only when it changed, and emit an Event only when the phase or reason changed.

## Rollback Rules

Rollback rewrites a Deployment's pod template, so every rule errs towards doing nothing:

- **Opt-in.** A guard without `spec.rollback.enabled: true` only reports.
- **Healthy target only.** The controller restores the template it last saw fully rolled out. If it has never seen this Deployment healthy, it reports `NoHealthyRevision` and does nothing.
- **Never twice.** If the Deployment is already on the last healthy template and still blocked, it reports `HealthyRevisionFailing` and does nothing. This prevents rolling "back" onto a bad revision after the controller's own rollback produced a newer revision number.
- **Reuses the healthy ReplicaSet.** The template is restored without its `pod-template-hash` label and without extra annotations, so the Deployment controller adopts the existing healthy ReplicaSet, as `kubectl rollout undo` does. Audit annotations (`attesta.io/rollback-from-revision`, `rollback-to-revision`, `rollback-at`, `rollback-by-guard`) are written on the Deployment itself.
- **Retried on failure.** API errors during rollback set `RollbackFailed` and return an error, so the workqueue retries with backoff. If the healthy ReplicaSet has been garbage-collected, the controller reports `HealthyRevisionGone` and stops.

## Not Yet Implemented

`spec.steps`, `spec.checks` and `spec.rollback.consecutiveFailures` are accepted by the CRD but not evaluated. The only rollback trigger today is Kubernetes' own `ProgressDeadlineExceeded`, which is already debounced by the Deployment's `progressDeadlineSeconds` (600 s by default). Metric-based analysis against Prometheus is planned.

If an Argo CD Application with `selfHeal: true` manages the same Deployment, Argo CD will revert the controller's rollback to the version in Git. Disable self-heal for guarded applications, or leave rollback disabled and act on the guard's status instead.

## Run Locally

```bash
go run ./controller/cmd/attesta-controller --kubeconfig "$HOME/.kube/config" --resync=30s
```

## Deploy To A Cluster

```bash
kubectl apply -k deploy/crds
kubectl apply -k deploy/controller
```

The deployment expects an image named `ghcr.io/example/attesta-controller:latest`. Build and publish your own image with:

```bash
docker build -f controller/Dockerfile -t ghcr.io/example/attesta-controller:latest .
```

## Status Contract

`status.phase` is one of `Progressing`, `Healthy`, `Paused`, `Blocked`, `RolledBack` or `RollbackFailed`.

Conditions:

- `DeploymentFound`
- `Healthy`
- `Blocked`
- `RolledBack`, once a rollback has been attempted. It stays `True` until a different revision rolls out healthy.

`lastTransitionTime` changes only when a condition's status changes. Other fields: `healthyRevision`, `healthyTemplateHash`, `healthyGeneration`, `rollbackRevision`, `rollbackTemplateHash`, replica counts, and the observed generations of the guard and the Deployment.

```bash
kubectl get rolloutguards -A
kubectl describe rolloutguard payments-api-rollout -n platform
```
