# Changelog

All notable changes to Attesta. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/). Before 1.0.0, a minor version may change the API, the CRDs or the database schema; each such change is listed here with what to do.

## [Unreleased]

### Added

- `RolloutDecision`: an immutable, hash-chained record of every promotion, rollback and skipped or failed rollback, with the PromQL queries and values and the Deployment conditions it was decided on. Apply the new CRD (`kubectl apply -k deploy/crds`) and the updated controller RBAC (`get` and `create` on `rolloutdecisions`) before upgrading the controller. `status.lastDecision` points at a guard's chain; `attesta verify` checks chains from `kubectl ... -o json`. See [Decision Records](docs/controller-operator.md#decision-records).
- `status.analysis.lastEvidence` on RolloutGuards, and the `attesta_rolloutguard_decisions_recorded_total{action}` metric.

### Fixed

- `attesta-controller` built from the controller-runtime commit panicked at start-up: it defined `--kubeconfig`, which controller-runtime already registers. The flag still works; it is controller-runtime's.

### Changed

- The controller runs on controller-runtime (v0.25): typed `api/v1` objects, the manager's cache, field indexes, leader election that releases the Lease only after reconciles stop, health probes and metrics. Its reconcile metrics are now controller-runtime's (`controller_runtime_reconcile_*`, `workqueue_*`); `attesta_rolloutguard_reconcile_total` and `_duration_seconds` are gone. Events are written through events.k8s.io/v1; add that API group to custom RBAC.
- The controller has an envtest suite: a real API server and etcd with the generated CRDs, run in CI.

### Removed

- `POST /api/v1/services/{name}/health-gate` and `attesta health-gate`. No endpoint acts on health numbers sent by a caller; the controller measures canaries from Prometheus.

## [0.1.0] - 2026-11-24

The first tagged release. It is the result of a production-readiness audit of the earlier prototype; each change is a separate commit on `main`, most named by their audit finding.

### Upgrading from an untagged build

- **Layout.** The binaries are built from `cmd/attesta`, `cmd/attesta-api` and `cmd/attesta-controller`; the Dockerfiles moved beside them.
- **CRDs.** `deploy/crds` now carries full OpenAPI schemas generated from `api/v1`. Re-apply them with `kubectl apply -k deploy/crds`. A `RolloutGuard` with an unknown strategy, an out-of-range threshold or a malformed duration is now rejected at apply time.
- **Rollback is opt-in twice.** A guard rolls a Deployment back only with `spec.rollback.enabled: true` and the Deployment's consent: its `attesta.io/rollback-guards` annotation must name the guard. The `update` permission on Deployments is in `deploy/controller/rollback-rbac.yaml`.
- **API authentication is required.** Configure `ATTESTA_API_TOKENS_FILE`, `ATTESTA_AUTH_KUBERNETES` or `ATTESTA_API_TOKEN`; `ATTESTA_INSECURE_NO_AUTH=true` is for local development only.
- **Database.** Migrations are versioned and applied at start-up (`schema_migrations`). An existing database is adopted as version 1 and migrated forward; incidents move from `incident_records` to `incidents` and `incident_events`.
- **Scaffold.** The API no longer writes scaffolds to disk; the CLI downloads them (`attesta service scaffold`). `ATTESTA_OUTPUT_DIR` is gone; set `ATTESTA_IMAGE_REGISTRY`.

### Controller

- Rolls back on `ProgressDeadlineExceeded` and on failed metric analysis of the new revision only (its `pod_template_hash`), restoring the last template it saw healthy.
- Never fights GitOps: a reverted rollback is reported as `RollbackReverted`, not repeated; the generated Argo CD Application turns self-heal off when rollback is on.
- Reads from informer caches, writes status with a resourceVersion precondition, and hands over leadership only after reconciles stop.
- Rate-limits events per reason and exposes reconcile, rollback and work-queue metrics.
- Status is written with the `api/v1` types, so it cannot contain a field the API server prunes.

### API

- Authentication by hashed tokens (hot-reloaded), Kubernetes TokenReview, or a shared token; viewer, operator and admin roles per route; the caller recorded on every run, rollout transition and incident.
- Per-client rate limits before and after authentication, a 25s request deadline, header and body caps.
- Internal errors are logged under a request ID and returned as `internal error` with that ID; store and Prometheus detail never reaches a client.
- Workflow runs: one non-failed run per idempotency key, enforced by PostgreSQL; heartbeats and a reaper; retries of transient errors with jittered backoff; typed error codes; saved in one statement.
- Rollouts are entities with a state machine and one active rollout per service.
- Lists paginate with keyset cursors.

### SLO gate and health gate

- Only measured data counts: missing series, `NaN` or fewer than 20 requests make the gate `unknown`, which blocks.
- Multi-window burn rates: 14.4x over 1h and 5m, 6x over 6h and 30m; the error budget from counts over 30 days.
- Canaries are judged against the service's own SLO, p95 to p95, sized by requests, with an early fail.
- The Prometheus client supports bearer and basic auth, a CA file, per-query timeouts and configurable metric and label names.

### Data

- Versioned migrations with an advisory lock; a binary refuses a newer schema.
- Unused tables dropped; dependencies stored as `text[]`.
- pgxpool with connection limits and lifetimes; every session has statement, lock and idle-in-transaction timeouts.
- Incidents are normalised, every update is versioned, and one active incident is allowed per alert fingerprint.
- Finished workflow runs are pruned after 30 days and resolved incidents after a year.

### Generated service scaffold

- Services are instrumented with `client_golang` and `prometheus_client`, shut down gracefully, and have separate liveness and readiness probes and a preStop delay.
- Alerts and the runbook take the service's SLO; burn-rate rules use the same windows as the gate.
- Images come from a configured registry, are tagged with the commit and promoted by digest; nothing runs `latest`.
- The autoscaler owns replicas per environment; the PodDisruptionBudget is `maxUnavailable: 1`.
- Argo CD is the namespace's only owner.
- The generated CI pins every action to a commit and grants `packages: write` only to the push job on `main`.
- User values are validated and quoted, so they cannot inject YAML.

### Build and release

- CI: formatting, tidy, vet, race tests against PostgreSQL with a coverage floor, golangci-lint, ShellCheck, govulncheck, generated-code drift, kubeconform, actionlint, image builds with Trivy scans, and an end-to-end rollback test on kind.
- Every action pinned by SHA; Dependabot for modules, actions, images and the scaffold's Python requirements.
- Tagged releases publish multi-arch images and a GitHub release with these notes and checksummed binaries.

[Unreleased]: https://github.com/gaurav-gs7/Attesta/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/gaurav-gs7/Attesta/releases/tag/v0.1.0
