# Attesta Architecture

Attesta has two processes that do not depend on each other:

- **`attesta-controller`** reconciles `RolloutGuard` resources against Deployments in the cluster. It is the only part of Attesta that changes cluster state, and only when a guard opts in to rollback.
- **`attesta-api`** is an HTTP service with a catalog of registered services, an SLO gate backed by Prometheus, incident records, a metadata readiness score, a scaffold generator and an in-process workflow runner. It records decisions and requests; it does not change the cluster.

## Controller

The controller watches `RolloutGuard` resources and the Deployments they reference. For each guard it:

1. Waits until the Deployment controller has observed the current generation.
2. Classifies the rollout as `Progressing`, `Healthy`, `Paused` or `Blocked` (`ProgressDeadlineExceeded`).
3. If the guard sets `spec.checks` and the controller has `--prometheus-url`, checks the new revision's error rate, p95 latency and availability in Prometheus every interval. Enough consecutive failures block the rollout; inconclusive data never decides.
4. On `Healthy`, and with analysis only after enough passing checks, records the current ReplicaSet's revision and template hash in the guard's status.
5. On `Blocked`, with `spec.rollback.enabled: true`, restores the recorded healthy template, at most once per bad template.
6. Writes status only when it changed and emits an Event only on a phase or reason transition.

It runs two replicas behind a leader-election Lease and exposes `/healthz`, `/readyz` and Prometheus metrics. The rules and limits are in [controller-operator.md](controller-operator.md).

### Who owns the pod template

A Deployment's pod template can have one owner. With GitOps that owner is Git: Argo CD or Flux makes the cluster match it, and with self-heal it undoes any change made in the cluster, including a rollback. An in-cluster rollback controller and a self-healing GitOps tool on the same Deployment will fight, flipping the template every time either reconciles.

Attesta does not write to Git, so it treats a rollback as an emergency override of Git, not a new source of truth:

- **Rollback on means self-heal off for that Deployment.** The generated Argo CD Application sets `selfHeal: false` when `rollbackOnFailure` is true, and keeps `selfHeal: true` otherwise. Automated sync stays on, so new commits still deploy. After a rollback the application shows OutOfSync. That is the correct signal: Git still holds the bad version, and someone has to revert the commit. The next sync of that application re-applies whatever Git holds.
- **The controller never fights back.** If the template it rolled away from returns, from self-heal, a sync of an unrelated change in the same application, or a manual apply, the guard goes to `Blocked` with reason `RollbackReverted` and the controller does nothing more for that template. It names the Argo CD application or Flux object when the Deployment carries their tracking annotation or labels.
- **If Git must stay authoritative at all times**, leave `spec.rollback.enabled` off. The guard still reports `Blocked` and `AnalysisFailed`, and the rollback belongs in the GitOps tool, for example by reverting the commit or using Argo Rollouts.

Writing the revert to Git (opening a revert commit from the controller) would remove the conflict entirely. It is not implemented.

### Who may make the controller act

The controller holds `update` on Deployments, so a RolloutGuard must not let its author borrow that right. The target Deployment has to name the guard in its `attesta.io/rollback-guards` annotation, which only someone with update rights on the Deployment can set. The controller checks it every time it is about to roll back, so revoking consent takes effect immediately.

An admission webhook that runs a SubjectAccessReview for the guard's creator was the alternative. It was not chosen: it adds a TLS serving certificate and a component that must be up for guards to be created, it checks only once, at creation, and it is bypassed whenever its failure policy lets requests through. The update right can also be bound per namespace (`deploy/controller/rollback-rbac.yaml`), so a cluster can limit where rollback is possible at all.

## API

### SLO gate

`GET /slos` queries Prometheus for the service's error rate and p95 latency (5m), burn rate (1h), and availability and remaining error budget (30d). The gate is `allowed`, `blocked` or `unknown`. Unknown, which covers no Prometheus, failed queries, no traffic, no series and `NaN`, blocks `POST /rollouts`. Values that could not be measured are omitted, never reported as zero.

### Incidents

Incidents open from Alertmanager's webhook or a JSON call. Opening an incident evaluates the SLO gate, loads deployment records from the previous 2 hours, and builds a timeline from those records, the alert and the evaluation. A rollout recorded in that window is named as a rollback candidate and labelled as a correlation. The postmortem draft marks the cause and follow-ups as TODO.

### Health gate and rollback requests

The health gate applies fixed thresholds to metrics in the request body; it measures nothing itself. Rollback endpoints record a `rollback-requested` entry. Acting on that request is left to deployment tooling or to the controller.

### Readiness score

The score counts which metadata fields were registered for a service. It does not inspect the cluster or the repository.

### Workflow runner

API operations run as ordered steps in-process. Each run records per-step state, attempts and events, and is persisted to PostgreSQL when `DATABASE_URL` is set. Service onboarding uses a deterministic idempotency key derived from the normalised spec.

The runner is not crash-resumable: if the process exits mid-run, the last persisted state stays `running` and the run is not resumed. Retries are immediate, without backoff. A failed onboarding run is currently replayed for the same spec instead of being retried; this is a known issue. Persistence failures fail the request rather than being reported as success.

## CRDs

| CRD | Reconciled | Purpose |
| --- | --- | --- |
| `RolloutGuard` | Yes, by `attesta-controller` | Guards one Deployment's rollouts |
| `AttestaService` | No | Declarative record of service metadata, generated by the scaffold |
| `SLOPolicy` | No | Declarative record of SLO targets and queries, generated by the scaffold |

## Data

With PostgreSQL, Attesta stores the service catalog, deployment records, workflow runs with their steps and events, and incident records. Without it, everything is kept in memory and lost on restart.
