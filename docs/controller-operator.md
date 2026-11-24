# Attesta Controller Operator

`attesta-controller` watches `RolloutGuard` resources, compares each guard with its Deployment's live rollout status, optionally checks the new revision's metrics in Prometheus, writes status conditions, emits Kubernetes Events on state transitions, and, when explicitly enabled, restores the last healthy pod template of a rollout that Kubernetes declared stuck or that failed its metric checks.

## Control Loop

1. Watch `attesta.io/v1` `RolloutGuard` resources and the Deployments they reference.
2. Resolve `spec.deploymentRef`, falling back to `spec.serviceRef`, then to the guard name without its `-rollout` suffix.
3. Ignore Deployment status until the Deployment controller has observed the current generation. Older conditions describe the previous rollout.
4. Classify the rollout:
   - `Paused`: `spec.paused` is true. Reported, never acted on.
   - `Blocked`: the `Progressing` condition is `False` with reason `ProgressDeadlineExceeded` at the observed generation.
   - `Healthy`: every replica runs the new template, old pods are gone and all replicas are available (the Deployment controller's own definition of complete).
   - `Progressing`: anything else, including the transient states every healthy rollout passes through.
5. If the guard sets `spec.checks` and the controller has `--prometheus-url`, analyse the revision rolling out. See Metric Analysis.
6. On `Healthy` (and, with analysis, after enough passing checks), record the current ReplicaSet's revision and `pod-template-hash` as the last healthy template.
7. On `Blocked` (a progress deadline, or too many consecutive failed checks), if `spec.rollback.enabled` is true, restore the last healthy template. See Rollback Rules.
8. Write `status` only when it changed, and emit an Event (events.k8s.io/v1) only when the phase or reason changed.

### Built On controller-runtime

The controller is a controller-runtime manager: typed `attesta.io/v1` objects from `api/v1`, the manager's shared cache, leader election, health probes and metrics. It watches RolloutGuards, Deployments and ReplicaSets once, at startup, and serves every read from the cache after that. Event mapping and reconciles make no direct GET or LIST calls, so API load does not grow with the number of Deployments or with how often they change; the only steady-state requests are status patches when something changed and the Deployment update of a rollback.

- A Deployment or ReplicaSet event finds its guards through an index on each guard's target Deployment. ReplicaSet updates enqueue only when the revision, template hash or owner changes, not on every replica count change.
- A reconcile finds a Deployment's ReplicaSets through an index on the owning Deployment's UID.
- A cache can trail the API server. Writes carry the cached `resourceVersion`: the status update and the rollback update fail with a conflict if the object changed after the read, and the guard is requeued a second later on a fresher view. If a Deployment reports a revision that the ReplicaSet cache has not seen yet, the reconcile is requeued too, so the controller never picks a candidate or rollback target from an outdated ReplicaSet list. Neither case is reported as a reconcile error.
- Guard updates reconcile only when the spec's generation changes or on the periodic resync (`--resync`), not on the controller's own status writes.
- Memory grows with the number of Deployments and ReplicaSets in the watched namespaces. Use `--namespace` to bound it.

## Rollback Rules

Rollback rewrites a Deployment's pod template, so every rule errs towards doing nothing:

- **Opt-in.** A guard without `spec.rollback.enabled: true` only reports.
- **The Deployment must consent.** The target Deployment has to name the guard in its `attesta.io/rollback-guards` annotation (comma-separated for several guards). Otherwise the controller reports `RollbackNotAuthorized` and does nothing. The guard's `RollbackAuthorized` condition shows the answer before any incident. This closes a confused-deputy hole: the controller can update Deployments, so without consent anyone allowed to create a RolloutGuard in a namespace could have it rewrite a Deployment there that they have no rights on. Only someone with update rights on the Deployment can set the annotation, and the controller checks it at the moment it acts, not just when the guard is created. Name a guard only once it exists, or limit who can create RolloutGuards, so nobody can create a listed guard first.
- **Healthy target only.** The controller restores the template it last saw fully rolled out. If it has never seen this Deployment healthy, it reports `NoHealthyRevision` and does nothing.
- **Never twice.** If the Deployment is already on the last healthy template and still blocked, it reports `HealthyRevisionFailing` and does nothing. This prevents rolling "back" onto a bad revision after the controller's own rollback produced a newer revision number.
- **Reuses the healthy ReplicaSet.** The template is restored without its `pod-template-hash` label and without extra annotations, so the Deployment controller adopts the existing healthy ReplicaSet, as `kubectl rollout undo` does. Audit annotations (`attesta.io/rollback-from-revision`, `rollback-to-revision`, `rollback-at`, `rollback-by-guard`) are written on the Deployment itself.
- **Never against whoever re-applied it.** The guard remembers the template it rolled away from. If that template comes back, from a GitOps tool with self-heal or a manual `kubectl apply`, the controller reports `RollbackReverted` with a Warning Event, names the Argo CD application or Flux object if the Deployment carries their markers, and does not roll back again. Rolling back would start a loop that flips the Deployment every progress deadline. The protection clears once a different revision rolls out healthy. See [Who owns the pod template](architecture.md#who-owns-the-pod-template).
- **Retried on failure.** API errors during rollback set `RollbackFailed` and return an error, so the workqueue retries with backoff. If the healthy ReplicaSet has been garbage-collected, the controller reports `HealthyRevisionGone` and stops.

## Metric Analysis

With `--prometheus-url`, a guard that sets any threshold in `spec.checks` has each new revision judged on its own metrics:

```yaml
spec:
  checks:
    errorRateThreshold: 1        # percent of requests with a 5xx status
    latencyP95ThresholdMs: 300
    minAvailability: 99.5        # optional, percent
    window: 5m                   # look-back window for each check
    interval: 30s                # time between checks (at least 5s)
    minRequests: 20              # fewer requests than this is inconclusive
    successfulChecks: 3          # passes needed before the revision counts as healthy
    revisionLabel: pod_template_hash
  rollback:
    enabled: true
    consecutiveFailures: 2       # failed checks in a row before the rollout is blocked
```

- **What is measured.** `http_requests_total` and `http_request_duration_seconds` for the guard's `service` label (`spec.serviceRef`, or the Deployment name) and the candidate's `pod_template_hash`, so old pods serving during the rollout do not dilute the result. The generated ServiceMonitor adds that label with `podTargetLabels`. Setting `revisionLabel: ""` measures the whole service instead.
- **When.** While the Deployment's current template differs from the recorded healthy template: at most once per `interval`, on a timer that does not depend on cluster events.
- **Failures must be consecutive.** A passing check resets the failure count. When the count reaches `consecutiveFailures`, the rollout is `Blocked` with reason `AnalysisFailed`, and the measured values go into the status message and the `attesta.io/rollback-reason` annotation.
- **Missing data never decides.** Too few requests, no series, a `NaN` result or an unreachable Prometheus give an `Inconclusive` check. It counts neither as a pass nor a failure, so it can neither roll back nor promote.
- **Promotion waits for evidence.** A rollout Kubernetes considers complete stays `Verifying` until `successfulChecks` checks have passed; only then is it recorded as the healthy rollback target.
- **Invalid `spec.checks`** are reported in the `Analysis` condition with reason `InvalidChecks`, and no queries are sent. Without `--prometheus-url` the condition says so, and the guard relies on the progress deadline alone.

`ProgressDeadlineExceeded` still blocks a rollout with or without analysis. It is debounced by the Deployment's `progressDeadlineSeconds` (600 s by default).

**Not a canary.** Attesta does not shift traffic. `spec.strategy` (`rolling`, `canary`, `blue-green`) is a label the controller records and reports; it behaves the same for each. The Deployment does a standard rolling update, analysis judges the new revision's pods as they take their share of traffic, and a failing revision is rolled back. That share is whatever the rolling update has reached, not a chosen percentage, so with `maxSurge: 1` on ten replicas the first checks see about a tenth of traffic, and a fast rollout can finish before `successfulChecks` passes: the revision then stays `Verifying` until it does, and is rolled back if it fails. For weighted steps and traffic splitting, use Argo Rollouts or Flagger; `spec.steps` is ignored and deprecated.

If an Argo CD Application with `selfHeal: true` manages the same Deployment, Argo CD reverts the controller's rollback to the version in Git; the controller then reports `RollbackReverted` and stands down. The Argo CD Application that `attesta service init` generates turns self-heal off when rollback is enabled. See [Who owns the pod template](architecture.md#who-owns-the-pod-template).

## SLO Gate

Analysis judges a new revision on its own pods. The SLO gate asks a different question first: should anything new roll out while the service is already out of its SLOs?

```yaml
spec:
  sloGate:
    availabilityTarget: 99.9     # percent of requests that succeed over 30 days
    latencyP95TargetMs: 300      # optional, default 300
    errorRateThreshold: 1        # optional, percent over 5m, default 1
    onUnknown: Proceed           # or Pause; default Proceed
    minErrorBudgetPct: 10        # optional: block while less of the 30-day budget remains
    fastBurnRate: 14.4           # optional: block while the 1h and 5m burn rates both reach it
    slowBurnRate: 6              # optional: block while the 6h and 30m burn rates both reach it
    resume: Automatic            # or Manual; default Automatic
    recheckInterval: 1m          # optional: how often a paused revision is measured again (at least 10s)
```

- **One evaluator.** The gate measures with the same Prometheus client and metric names as the API's SLO status (`internal/prometheus`) and judges with the same function (`slo.Evaluate`). It blocks when less than `minErrorBudgetPct` (default 10%) of the 30-day error budget remains, when the service-wide 5-minute error rate or p95 exceeds its target, or when both windows of a burn-rate pair are over (1h and 5m at `fastBurnRate`, default 14.4x; 6h and 30m at `slowBurnRate`, default 6x). The API's SLO status uses the defaults, or `ATTESTA_SLO_MIN_ERROR_BUDGET_PCT`, `ATTESTA_SLO_FAST_BURN_RATE` and `ATTESTA_SLO_SLOW_BURN_RATE`. Analysis builds its queries from the same metric names, so `--prometheus-requests-metric` and the other `--prometheus-*` name flags change both.
- **When.** Once per revision: when a template other than the healthy one starts rolling out, the Deployment is not paused, and analysis has not already blocked or promoted it. The first rollout a guard sees is never gated; there is no healthy revision to keep serving.
- **What it does.** On `blocked` it sets `spec.paused` on the Deployment, adds `attesta.io/paused-by-guard`, `paused-at` and `paused-reason`, moves the guard to `Paused` with reason `SLOGatePaused`, and records a `Paused` decision whose evidence is every measured value with its PromQL. Pods of the new revision that already started keep running; no more are created.
- **Resuming.** With `resume: Automatic` the controller measures a revision it paused again every `recheckInterval` and, once the gate allows it, clears `spec.paused`, adds `attesta.io/resumed-at`, and records a `Resumed` decision with the values that allowed it. While it stays blocked, the `SLOGate` condition says `StillBlocked` with the latest reason, and nothing new is recorded. With `resume: Manual` the pause holds until a person runs `kubectl rollout resume deployment/<name>`. Either way, a person resuming is the override: the gate never pauses that revision again, and a later pause of the Deployment is theirs, which the gate never resumes.
- **Unknown data.** With too little traffic or Prometheus unreachable the verdict is `unknown`. `onUnknown: Proceed` lets the revision through; `Pause` treats it as blocked.
- **Consent.** Pausing needs the same consent as rollback: the Deployment's `attesta.io/rollback-guards` annotation must name the guard. Without it the guard records `PauseSkipped` and does not touch the Deployment.
- **Status.** The `SLOGate` condition and `status.sloGate` (template hash, verdict, reason, time, whether it paused) show the latest verdict. `attesta_rolloutguard_slo_gate_total{gate}` counts verdicts.

## Decision Records

Every decision the controller makes is written as a `RolloutDecision` (short name `rdec`) in the guard's namespace, before the guard's status is updated:

| Action | Recorded when |
| --- | --- |
| `Promoted` | A new template becomes the guard's healthy template, the one a rollback restores. |
| `RolledBack` | The controller restored the healthy template. |
| `RollbackFailed` | The rollback could not be done (the Deployment update failed, or the healthy ReplicaSet is gone). Recorded when the reason first appears, not on every retry. |
| `RollbackSkipped` | A rollback was due and the controller declined: the Deployment did not consent, no healthy template was recorded, the healthy template is itself failing, or the bad template was re-applied by GitOps. Recorded once per reason. |
| `Paused` | The SLO gate paused a new revision. |
| `PauseSkipped` | The SLO gate would have paused a revision, but the Deployment did not consent. |

A record holds the trigger (`ProgressDeadlineExceeded`, `AnalysisFailed`, `AnalysisPassed`, `RolloutComplete`, `SLOGateBlocked`, `SLOGateUnknown`), the Deployment's UID, generation, revision, template hash and images, the revision restored, the controller version and pod that decided, and the evidence: every PromQL query sent with its value, threshold and verdict, and the Deployment's `Progressing` condition and replica counts. A failed metric check's evidence reads like `error-rate 20% <= 1% false`.

**The chain.** Each record carries `sequence`, `previousHash` (the hash of the record before it) and `hash`: SHA-256 over the record's `spec` without `hash`, as compact JSON with sorted keys and no HTML escaping, which is exactly what `jq -cjS '.spec | del(.hash, .signature)'` prints. The guard's `status.lastDecision` points at the head. An edited record no longer matches its hash; an inserted or deleted one breaks the sequence or the `previousHash` link; a record deleted from the end no longer matches the guard's head. `attesta verify` checks all of this for every guard in kubectl's JSON output and prints the trail:

```bash
kubectl get rolloutguards,rolloutdecisions -n platform -o json | attesta verify
```

**Immutability and deletion.** The CRD refuses any change to a record's `spec` (`self == oldSelf`), for every user, the controller included. `deploy/controller/decision-protection.yaml`, a ValidatingAdmissionPolicy (Kubernetes 1.30+), refuses deleting a record or changing its labels to everyone outside the group `attesta:decision-admins`, cluster-admins included: admission policies are not bypassed by `system:masters`. Deleting a guard leaves its records in place. Deleting a namespace still deletes its records (the namespace controller is let through, or the namespace would never terminate); the archive below keeps them. The controller's RBAC on `rolloutdecisions` is `get` and `create`, nothing else. Grant `create` to no one else: a record someone else writes at a guard's next position is never adopted; the guard stops recording, and its reconciles fail with an error naming the record, until it is removed.

**Signatures.** With `--signing-key-file` (an Ed25519 key, `openssl genpkey -algorithm ed25519`; `deploy/controller/components/signing` mounts it from a Secret), every record carries `spec.signature`: the key ID and an Ed25519 signature over `attesta.io/v1 RolloutDecision <hash>`. The hash covers everything else, so a valid signature vouches for the whole record. Someone with write access to the cluster can create a record with a correct hash, but not one that verifies against the controller's public key. `attesta verify --public-key decision-signing.pub.pem` and the API (`ATTESTA_DECISION_PUBLIC_KEYS_FILE`) check them; a key file may hold several public keys, to verify across a rotation.

**Archive outside the cluster.** With `--archive-url` (`deploy/controller/components/archive`), every record is sent, in order, to an archive the cluster cannot reach into: attesta-api's `POST /api/v1/decision-archive`, or any endpoint that answers 2xx. `status.archivedSequence` and the `Archived` condition show how far it got; an archive that is down never stops the guard, and the backlog is sent when it is back. attesta-api keeps the records in PostgreSQL in a table that refuses `UPDATE` and `DELETE`, accepts a record only if it hashes correctly, is signed (when keys are configured) and links to the record before it, and refuses a different record at a position it already holds (a fork) with 409 (`attesta_decision_archive_refused_total` on the API, `attesta_rolloutguard_archive_refused_total` on the controller: alert on either). `GET /api/v1/guards/{ns}/{name}` compares the cluster's records with the archive and lists sequences missing from the cluster or changed there; `GET /api/v1/decision-archive` lists every archived chain, including those of guards no longer in the cluster.

**Crash safety.** A decision that changes a Deployment (a rollback, a pause) is sealed before the change and travels on the same update, in the `attesta.io/decision` annotation, so the action and its record are written together. The record is then created; if the controller stops first, or the create fails, the next pass finds the sealed decision on the Deployment, checks that it hashes to what it claims and belongs to this guard, and records it (`attesta_rolloutguard_decisions_recovered_total`). An annotation edited by hand no longer hashes correctly and is never recorded. The status is written even when the record could not be, so a rollback is never decided twice. Decisions that change nothing (a promotion, a skip) are recorded before the status that points at them; a lost status write leaves a record the next pass adopts after checking its link and hash.

**What this does not cover.** Someone who holds both the cluster and the archive's database can still rewrite history in both; keep the archive in a different trust domain (another account, another team's database) for it to mean anything. Without `--archive-url`, a deleted namespace takes its records with it. Without `--signing-key-file`, records are tamper-evident only: a forged record with a correct hash fails nothing but the archive comparison. Records are never garbage-collected; a guard deleted and recreated under the same name starts a new chain (the name and `spec.guardUID` include the guard's UID).

## Run Locally

```bash
go run ./cmd/attesta-controller --kubeconfig "$HOME/.kube/config" --leader-elect=false
```

## Deploy To A Cluster

```bash
kubectl apply -k deploy/crds
kubectl apply -k deploy/controller
```

RBAC is split. `rbac.yaml` grants read access, status writes on RolloutGuards and Events. `rollback-rbac.yaml` holds the one other write, `update` on Deployments, in its own ClusterRole, `attesta-controller-rollback`. The default binds it cluster-wide; the file shows how to replace that with a RoleBinding in each namespace where rollback is allowed.

`deploy/controller` runs two replicas with leader election (a Lease named `attesta-controller` in `attesta-system`), so one replica reconciles and the other takes over if it goes away. Handover never overlaps: on shutdown, controller-runtime stops the reconcilers, waits for in-flight reconciles to finish (up to `--shutdown-timeout`), and only then releases the Lease, so the standby cannot start while the old leader is mid-rollback. A leader that loses the Lease without shutting down exits and restarts with fresh caches. Rollback state lives in the guard's status, not in memory, so a new leader does not repeat a rollback it inherits. Pods run as UID 65532 from a distroless image with a read-only root filesystem.

Images are published to `ghcr.io/gaurav-gs7/attesta-controller` by `.github/workflows/release.yml`: `main` on every push to main, and a version tag for every `v*` git tag. The kustomization defaults to `main`; pin a version or digest for anything that matters:

```bash
cd deploy/controller
kustomize edit set image attesta-controller=ghcr.io/gaurav-gs7/attesta-controller:v0.2.0
```

To build your own image:

```bash
docker build -f cmd/attesta-controller/Dockerfile --build-arg VERSION=$(git describe --tags --always) -t attesta-controller:dev .
```

## Operating The Controller

| Endpoint | Port | Purpose |
| --- | --- | --- |
| `/healthz` | 8081 | Liveness. |
| `/readyz` | 8081 | Readiness, once the caches have synced. Standby replicas sync their caches too, so they report ready and a rolling update of the controller can proceed. |
| `/metrics` | 8080 | Prometheus metrics, exposed by the `attesta-controller-metrics` Service. |

Metrics:

- `attesta_rolloutguard_rollback_total{outcome}`: `triggered`, `failed`, or the reason a rollback was skipped
- `attesta_rolloutguard_phase_transitions_total{phase}`
- `attesta_rolloutguard_analysis_total{result}`: `Passed`, `Failed` or `Inconclusive` metric checks
- `attesta_rolloutguard_decisions_recorded_total{action}`: `RolloutDecision` records written
- `attesta_rolloutguard_slo_gate_total{gate}`: SLO gate verdicts, `allowed`, `blocked` or `unknown`
- controller-runtime's own: `controller_runtime_reconcile_total{controller="rolloutguard",result}`, `controller_runtime_reconcile_time_seconds`, `controller_runtime_reconcile_errors_total`, and the `workqueue_*` metrics
- Go runtime and process metrics

Useful flags: `--prometheus-url` to enable metric analysis (with `--prometheus-bearer-token-file`, `--prometheus-ca-file` and `--prometheus-query-timeout` for a secured Prometheus), `--namespace` to watch one namespace, `--workers`, `--resync`, `--leader-elect=false` for a single local process, `--shutdown-timeout` (how long in-flight reconciles get before the Lease is released), and `--version`.

## Status Contract

`status.phase` is one of `Progressing`, `Verifying` (rolled out, waiting for passing metric checks), `Healthy`, `Paused`, `Blocked`, `RolledBack` or `RollbackFailed`.

Conditions:

- `DeploymentFound`
- `Healthy`
- `Blocked`
- `RolledBack`, once a rollback has been attempted. It stays `True` until a different revision rolls out healthy.
- `Analysis`, when the guard sets `spec.checks`.
- `RollbackAuthorized`, when rollback is enabled: whether the Deployment names the guard in `attesta.io/rollback-guards`.
- `SLOGate`, when the guard sets `spec.sloGate`: the verdict on the latest revision.

`lastTransitionTime` changes only when a condition's status changes. Other fields: `healthyRevision`, `healthyTemplateHash`, `healthyGeneration`, `rollbackRevision`, `rollbackTemplateHash`, `rollbackFromTemplateHash`, replica counts, the observed generations of the guard and the Deployment, `lastDecision` (the head of the decision chain) and, while a revision is analysed, `analysis.lastEvidence` (the queries and values of the last check).

```bash
kubectl get rolloutguards -A
kubectl describe rolloutguard payments-api-rollout -n platform
```
