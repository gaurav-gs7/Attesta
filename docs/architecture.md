# Attesta Architecture

Attesta has two processes. The controller acts; the API reports what it did. They share the SLO evaluator and the Prometheus query builder, and the cluster is where they meet: the controller writes RolloutDecisions, the API reads them.

- **`attesta-controller`** reconciles `RolloutGuard` resources against Deployments in the cluster. It is the only part of Attesta that changes cluster state: it rolls a Deployment back or pauses it, only when a guard opts in and the Deployment consents, and records each decision as a hash-chained `RolloutDecision`.
- **`attesta-api`** is an HTTP service with a catalog of registered services, SLO status measured from Prometheus, a read-only view of guards and their verified decisions, incident records whose timelines cite those decisions, a metadata readiness score, a scaffold generator and an in-process workflow runner. It never writes to the cluster.

```
            Prometheus
            │        │
  slo.Evaluate     analysis queries
            │        │
  attesta-api ◀──read── RolloutGuard / RolloutDecision ◀──write── attesta-controller ──update──▶ Deployment
  (SLO status,                                                    (rollback, pause,
   guards, incidents)                                              decision records)
```

## Controller

The controller watches `RolloutGuard` resources and the Deployments they reference. For each guard it:

1. Waits until the Deployment controller has observed the current generation.
2. Classifies the rollout as `Progressing`, `Healthy`, `Paused` or `Blocked` (`ProgressDeadlineExceeded`).
3. If the guard sets `spec.checks` and the controller has `--prometheus-url`, checks the new revision's error rate, p95 latency and availability in Prometheus every interval. Enough consecutive failures block the rollout; inconclusive data never decides.
4. On `Healthy`, and with analysis only after enough passing checks, records the current ReplicaSet's revision and template hash in the guard's status.
5. On `Blocked`, with `spec.rollback.enabled: true`, restores the recorded healthy template, at most once per bad template.
   With `spec.canary` instead, it runs the last promoted revision in `<deployment>-primary`, steps the new one through its traffic weights (an HTTPRoute's, or a share of pods), checks it at each step, and promotes it into the primary or takes it out of traffic.
6. With `spec.sloGate`, when a new revision starts while the service is out of its error budget or burning it fast, pauses the Deployment.
7. Records each promotion, rollback, pause, and skipped or failed action as a `RolloutDecision`, before the status that points at it.
8. Writes status only when it changed and emits an Event only on a phase or reason transition.

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

`GET /slos` queries Prometheus for the service's error rate and p95 latency (5m), burn rates over 5m, 30m, 1h and 6h, and availability and remaining error budget (30d, from `increase()`). Burn rates gate in multi-window pairs: blocked when 1h and 5m are both at or above 14.4x, or 6h and 30m both at or above 6x. The gate is `allowed`, `blocked` or `unknown`. Unknown covers no Prometheus, failed queries, no series, `NaN`, and fewer than 20 requests in a window. The controller's `spec.sloGate` applies the same function (`slo.Evaluate`) to the same queries when a new revision starts, and pauses it on `blocked` (and on `unknown` with `onUnknown: Pause`). Values that could not be measured are omitted, never reported as zero.

### Incidents

Incidents open from Alertmanager's webhook or a JSON call. Opening an incident evaluates the SLOs, reads the controller's decisions for the service's guards from the previous 2 hours, and builds a timeline from those records, the alert and the evaluation. The latest revision the controller promoted in that window is named as a rollback candidate and labelled as a correlation. Without cluster access the timeline says the decisions were not read. The postmortem draft marks the cause and follow-ups as TODO.

### No caller-supplied health

No endpoint accepts health numbers from a caller, and none starts, pauses or rolls back anything. Revision health is measured by the controller from Prometheus; the API reads the decisions it recorded.

### Readiness score

The score counts which metadata fields were registered for a service. It does not inspect the cluster or the repository.

### Workflow runner

API operations run as ordered steps in-process. Each run records per-step state, attempts and events, and is persisted to PostgreSQL when `DATABASE_URL` is set. Service onboarding uses a deterministic idempotency key derived from the normalised spec.

Idempotency keys follow one rule: at most one run per key is not failed.

- **Succeeded** runs are replayed: an identical request gets the earlier run and nothing executes.
- **Failed** runs are history and do not hold the key, so the next identical request runs the workflow again. That includes a run that failed only because the store was briefly unavailable. Steps are written to be re-run: onboarding accepts a service row that an earlier attempt created with the same spec, and records the initial deployment only if that attempt did not.
- **Running** runs hold the key while they heartbeat. An identical request waits up to five seconds for the run to finish and then gets its result, or a `409` with `Retry-After` if it is still running. A running run whose heartbeat is more than a minute old, because its process crashed, is marked failed (`abandoned`) and the key is released. Its owner, if it is merely stalled, finds its run abandoned on the next save or heartbeat and stops. It cannot write the run back.
- A run that failed to persist is never treated as recorded. The engine keeps no in-memory copy, so it cannot replay one.
- With PostgreSQL the rule is enforced by the database: a unique partial index on the key over runs that are not failed. A claim is a single `INSERT ... ON CONFLICT DO NOTHING` against it, so concurrent identical requests, on one replica or several, produce one run. The others wait for it and replay its result. Migrating a database written by an earlier version keeps one active run per key (a succeeded one first, then the newest) and marks the rest failed as superseded. Migrations take an advisory lock, so replicas that start together do not race.

### Schema migrations

The schema is a sequence of versioned files, `internal/catalog/migrations/NNNN_name.sql`, embedded in the binary. At start-up the API takes a PostgreSQL session advisory lock, reads the highest version in `schema_migrations`, and applies each newer file in its own transaction together with its version row, so a failure leaves the schema at the last version that applied completely and the next start retries from there. A binary refuses to start against a schema newer than it knows. Version 1 is the schema as it stood before versioning; it is idempotent, so a database created by an earlier release is adopted as version 1. Shipped migrations are never edited: a change is the next file. The migrator is about 130 lines in `migrate.go` rather than goose or golang-migrate; it needs nothing those add, and avoids their test-only dependency trees.

A run executes on its own context, not the request's. A client that disconnects or times out does not cancel a run between a side effect and the write that records it; the run finishes and is recorded, and the response is simply not delivered. Each run is bounded by a timeout (two minutes) instead. A step that panics fails its run with the panic as the error and is logged with its stack; the process keeps serving. Every minute a reaper marks failed any running run whose heartbeat is stale, keyed or not, so a crash leaves nothing "running".

Each save of a run is one SQL statement: the run row is upserted, steps are updated in place and only new events are inserted, through data-modifying CTEs over arrays, with step and event IDs derived from the run. A run is saved when it starts, when each step attempt starts (carrying the previous step's result) and when it ends. A one-step run costs 3 statements and onboarding 6, down from 34 and 101 when every save deleted and re-inserted all steps and events in a transaction.

The runner does not resume a crashed run from its last step; the next request re-runs it from the start. A step is retried, up to its attempt limit, only when its error is transient: marked with `workflows.Transient`, a network timeout, or a refused, reset or cut connection. Anything else, such as a validation error or a conflict, fails the step at once and records the attempts left unused. Retries wait with exponential backoff and full jitter (100 ms doubling to at most 5 s), and stop when the run's context ends. Persistence failures fail the request rather than being reported as success.

## CRDs

| CRD | Reconciled | Purpose |
| --- | --- | --- |
| `RolloutGuard` | Yes, by `attesta-controller` | Guards one Deployment's rollouts |
| `RolloutDecision` | Written by `attesta-controller`, never changed | One decision, hash-chained per guard; read by the API |
| `AttestaService` | No | Declarative record of service metadata, generated by the scaffold |
| `SLOPolicy` | No | Declarative record of SLO targets and queries, generated by the scaffold |

The Go types in `api/v1` are the source of truth. `make generate` (controller-gen) writes their deepcopy functions and the CRDs in `deploy/crds`, with full OpenAPI schemas: enums for strategy, tier and language, ranges for thresholds, patterns for durations and label names. The API server therefore rejects a malformed guard at `kubectl apply` instead of the controller reporting it later, and the controller decodes and writes status with the same structs, so it cannot write a field the server prunes. CI regenerates both and fails on any difference, and the kind job submits the generated scaffold's three custom resources to a real API server with `--dry-run=server`.

## Data

With PostgreSQL, Attesta stores the service catalog, workflow runs with their steps and events, and incident records. Without it, everything is kept in memory and lost on restart; the in-memory workflow store keeps the newest 1,000 runs and drops the oldest finished ones beyond that. Every API replica prunes hourly: finished workflow runs (with their steps and events) 30 days after they completed, and resolved incidents (with their timelines) a year after they were resolved. Running runs and active incidents are never pruned. Deletes go in batches of 1,000 with `SKIP LOCKED`, so replicas pruning at once do not block each other. `ATTESTA_WORKFLOW_RETENTION` and `ATTESTA_INCIDENT_RETENTION` take a duration of at least 1h or a number of days (`90d`); `0` keeps records forever. A pruned succeeded run no longer holds its idempotency key: an identical request after that runs again, which onboarding answers as `200 idempotent`.

An incident is a row in `incidents` with its timeline in `incident_events`, one row per event, appended and never rewritten. The SLO evaluation taken when it opened is kept as one JSONB snapshot, since it is never queried by field or changed; the service is not copied but joined from `services`, so an incident shows the service's current owner and pager. Every update carries the `version` it read and applies only if the row is still at that version: two concurrent resolutions cannot both write, and the second request answers `alreadyResolved` with the stored record. A partial unique index allows one active incident per service and Alertmanager fingerprint, so two notifications for one firing alert open one incident and the second reports `duplicate`.

Only operations that change something create workflow runs: onboarding and incidents. Reads (readiness, score, SLOs and error budget, status, guards) compute their answer and write nothing.

Rollouts are not stored in PostgreSQL. Earlier versions recorded rollout and rollback *requests* made to the API, which changed nothing in the cluster; migration 6 drops those tables. What happened is in the cluster: the controller's RolloutDecisions, which the API reads (`ATTESTA_CLUSTER_READ`) and verifies, and which incident timelines cite.
