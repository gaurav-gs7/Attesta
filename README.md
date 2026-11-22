# Attesta

[![CI](https://github.com/gaurav-gs7/Attesta/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/gaurav-gs7/Attesta/actions/workflows/ci.yml)

Attesta is a Kubernetes rollout guard and a small reliability API. The `attesta-controller` watches Deployments that have a `RolloutGuard`, records their last healthy pod template, and can restore it when Kubernetes declares a rollout stuck. The API gates rollouts on SLOs measured from Prometheus, opens incidents from Alertmanager, and generates a service scaffold with manifests, alerts and a runbook.

Every value Attesta reports is either measured, recorded, or explicitly marked as unknown or simulated. Where it has no data it says `unknown` and fails closed.

## Status

| Area | What is real | What is not (yet) |
| --- | --- | --- |
| Rollout guard controller | Watches Deployments, writes status and Events, and restores the last healthy template when Kubernetes reports `ProgressDeadlineExceeded` or when the new revision fails `consecutiveFailures` metric checks in a row (error rate, p95 latency, availability from Prometheus, scoped to that revision). Inconclusive data never decides. Rolls back only Deployments that name the guard in `attesta.io/rollback-guards`, and never fights a GitOps tool that re-applies the bad version. Leader election, metrics, probes. | No traffic shifting, so no true canary: `spec.steps` is not used. |
| SLO gate | Error rate, p95, burn rate and 30-day error budget from Prometheus. Unknown data blocks `POST /rollouts`. | Single burn-rate window (1h); no multi-window alerting logic in the gate. |
| Health gate | Applies thresholds to the metrics in the request. | It does not measure anything: inputs are caller-supplied, and thresholds are fixed at 300ms p99 and 1%. |
| Rollback endpoints | Record a rollback request. | They do not change the cluster. Your deployment tooling, or the controller, must act. |
| Incidents | Alertmanager webhook (v4) with fingerprint dedupe and resolve; timeline from recorded events only. | Root cause is never inferred; the postmortem draft leaves it as TODO. |
| Readiness score | Checks which metadata fields were registered. | It does not inspect the cluster or the repository. |
| Workflow runner | Ordered in-process steps with per-step state and events, persisted to PostgreSQL. | Not crash-resumable; retries are immediate. A failed onboarding run is currently replayed for the same spec (known issue). |
| CRDs | `RolloutGuard` is reconciled. | `AttestaService` and `SLOPolicy` are declarative records only; nothing reconciles them. |

## Components

- `controller/`: the `RolloutGuard` controller. See [docs/controller-operator.md](docs/controller-operator.md).
- `api/`: the HTTP API: service catalog, SLO gate, incidents, readiness, scaffold generator, workflow runner.
- `cli/`: a thin client for the API.
- `templates/`: the generated service scaffold.
- `deploy/`: CRDs and the controller's manifests.

## Quick Start

Start the API:

```bash
make run-api
```

In another terminal:

```bash
export ATTESTA_API_TOKEN=local-dev-token

go run ./cli/cmd/attesta service init payments-api \
  --owner payments-platform \
  --team platform \
  --tier critical \
  --language go \
  --slo-availability 99.9 \
  --slo-latency-p95 300ms \
  --deployment canary \
  --pager payments-oncall
```

The scaffold is written to `generated/services/payments-api`. Without `ATTESTA_PROMETHEUS_URL` the SLO state is `unknown`, so `attesta slo status payments-api` reports why, and rollouts are refused.

## Authentication

The API refuses to start unless `ATTESTA_API_TOKEN` is set. Clients send the token in the `X-Attesta-Token` header; `/livez`, `/readyz` and `/metrics` stay open for probes and scrapers. For throwaway local runs only, `ATTESTA_INSECURE_NO_AUTH=true` starts the API without authentication and logs a warning.


## Kubernetes Controller

```bash
kubectl apply -k deploy/crds
kubectl apply -k deploy/controller
```

Run it against your current kube context:

```bash
go run ./controller/cmd/attesta-controller --kubeconfig "$HOME/.kube/config" --leader-elect=false
```

The controller classifies each guarded rollout as `Progressing`, `Verifying`, `Healthy`, `Paused` or `Blocked`. Blocked means Kubernetes reported `ProgressDeadlineExceeded` for the current generation or, with `--prometheus-url` and `spec.checks`, that the new revision failed `consecutiveFailures` metric checks in a row. With `spec.rollback.enabled: true` it restores the last template that passed, never acts on a paused Deployment, and never rolls back twice onto the same template. See [docs/controller-operator.md](docs/controller-operator.md) for the rules, metrics and limits.

## SLO Evaluation

Set `ATTESTA_PROMETHEUS_URL` to evaluate SLOs from Prometheus:

```bash
ATTESTA_PROMETHEUS_URL=http://prometheus.monitoring.svc:9090 make run-api
```

Attesta reads `http_requests_total` and `http_request_duration_seconds` for the service's `service` label:

| Signal | Window |
| --- | --- |
| Error rate (5xx share) and p95 latency | 5m |
| Burn rate | 1h |
| Availability and remaining error budget | 30d |

The deployment gate is `allowed`, `blocked` or `unknown`. It is `unknown`, and `POST /rollouts` returns 409, whenever a signal cannot be measured: no Prometheus configured, a failed query, no traffic, no series for the service, or a `NaN` result. Measured values are omitted from the response rather than reported as zero. Prometheus must retain at least 30 days for the budget to be computed.

## Incidents

`POST /api/v1/alerts/alertmanager` accepts Alertmanager's webhook payload (version 4). A firing alert with a `service` label opens an incident unless one is already active for its fingerprint; the matching resolved alert closes it at its `endsAt`. Alertmanager can authenticate with `http_config.authorization` (a Bearer token); `X-Attesta-Token` also works.

```yaml
receivers:
  - name: attesta
    webhook_configs:
      - url: http://attesta-api.attesta-system.svc:8080/api/v1/alerts/alertmanager
        http_config:
          authorization:
            credentials_file: /etc/alertmanager/attesta-token
```

`POST /api/v1/incidents` opens an incident from plain JSON. Error rate and latency in that body are stored as values the sender reported, separate from Attesta's own SLO evaluation.

Timelines contain only recorded events: deployment records from the 2 hours before the alert, the alert, the incident opening and the SLO evaluation. When a recorded rollout started in that window, a correlation event names it as a rollback candidate and says it is not a confirmed cause. The postmortem draft marks what Attesta cannot know as TODO.


## Health Gate And Rollback Requests

`POST /api/v1/services/{name}/health-gate` evaluates the `p99LatencyMs`, `errorRate` and `successCount` in the request body against fixed thresholds (300ms, 1%, at least 50 successes). The response marks its input as `caller-supplied`. When the decision is `rollback` and the service has `rollbackOnFailure: true`, Attesta records a `rollback-requested` deployment entry; with `rollbackOnFailure: false` the decision is `hold` and nothing is recorded.

`POST /api/v1/services/{name}/rollback` records a rollback request. Neither endpoint changes the cluster.

## Readiness Score

`GET /api/v1/services/{name}/readiness` scores which fields were registered for the service: ownership, pager, tier, runbook, dashboard, SLOs, strategy, rollback policy, namespace, repository and, for critical services, dependencies. The API does not fill these in for you, so an unregistered field fails its check. The report's `scope` field states that the cluster and repository are not inspected.

## Generated Service Scaffold

`attesta service init` writes a Go or Python service with Dockerfile, Kustomize base and overlays, GitHub Actions CI, a `RolloutGuard` and `SLOPolicy`, PrometheusRule alerts, a ServiceMonitor, a Grafana dashboard, OpenTelemetry and Jaeger manifests, Terraform, an Argo CD Application and a runbook.

- The service exposes real `http_requests_total` and `http_request_duration_seconds` metrics, the series the SLO gate reads.
- Alert thresholds and the runbook use the service's registered SLO.
- Fields you did not register stay visible as gaps: the runbook marks a missing pager as TODO, and the Argo CD Application uses a `CHANGE-ME` repository placeholder.

## Demos

`make demo-full` builds the scaffold, deploys a stable and then a candidate image to kind, and walks through the API. Parts of it are simulated, and the script's log says so:

- The candidate image replaces every pod. There is no traffic splitting.
- The "bad canary" is a fixed set of metrics posted to the health gate.
- The rollback is `kubectl rollout undo`, run by the script.
- The incident is a hand-written alert payload.

`make demo-failures` runs failure scenarios against the API and writes evidence under `artifacts/failure-modes/`: invalid metadata rejected, duplicate onboarding, low-traffic hold, rollback request recorded, Prometheus unreachable (gate `unknown`, rollout refused), and PostgreSQL unreachable at startup. See [docs/failure-modes.md](docs/failure-modes.md).

## Development

```bash
make test   # go test -race ./...
make lint   # golangci-lint and shellcheck
make e2e    # controller end-to-end test on a local kind cluster
```

CI runs on every push: formatting, `go mod tidy` drift, vet, race-enabled tests, golangci-lint, ShellCheck, govulncheck, manifest validation with kubeconform, actionlint, both container images (checked to run as non-root), and `scripts/e2e-rollback.sh` on kind. The end-to-end test deploys the controller and runs two scenarios. First, an image that cannot be pulled is rolled back once on its progress deadline. Second, against an in-cluster Prometheus stub, an image that Kubernetes considers healthy but whose metrics fail is rolled back by metric analysis alone. It also checks that a paused Deployment is left alone, that only the leader acted, and that after the leader pod is deleted the standby takes over without repeating the rollback.

## PostgreSQL Mode

```bash
docker compose up --build
```

Without `DATABASE_URL`, Attesta keeps everything in memory. With it, the service catalog, deployment records, workflow runs and incidents are stored in PostgreSQL.

## API Surface

| Method and path | Purpose |
| --- | --- |
| `GET /livez`, `GET /readyz`, `GET /metrics` | Probes and metrics (no token) |
| `POST /api/v1/services`, `GET /api/v1/services` | Register and list services |
| `GET /api/v1/services/{name}` | Service record |
| `GET /api/v1/services/{name}/status` | Measured SLO state and the latest deployment record |
| `GET /api/v1/services/{name}/readiness`, `POST .../check`, `GET .../score` | Metadata readiness score |
| `GET /api/v1/services/{name}/slos`, `GET .../error-budget` | SLO evaluation |
| `GET /api/v1/services/{name}/deployments` | Deployment records |
| `POST /api/v1/services/{name}/rollouts` | Record a rollout start if the SLO gate allows it |
| `GET /api/v1/services/{name}/rollouts/{id}`, `POST .../pause`, `POST .../rollback` | Rollout records and requests |
| `POST /api/v1/services/{name}/health-gate` | Evaluate caller-supplied canary metrics |
| `POST /api/v1/services/{name}/rollback` | Record a rollback request |
| `GET /api/v1/services/{name}/runbook` | Runbook rendered from the service record |
| `POST /api/v1/incidents`, `GET /api/v1/incidents` | Open and list incidents |
| `POST /api/v1/alerts/alertmanager` | Alertmanager webhook |
| `GET /api/v1/incidents/{id}`, `.../timeline`, `.../postmortem`, `POST .../resolve` | Incident record, timeline, draft, resolve |
| `GET /api/v1/workflows`, `GET /api/v1/workflows/{id}` | Workflow runs |

## Repository Layout

```text
api/          HTTP API: catalog, SLO gate, incidents, readiness, scaffold generator, workflow runner
cli/          CLI client for the API
controller/   RolloutGuard controller binary and image
internal/     Controller reconcile logic
templates/    Generated service scaffold
deploy/crds/  AttestaService, SLOPolicy and RolloutGuard CRDs
deploy/       Controller manifests
infra/        Terraform, Argo CD and monitoring values for local clusters
scripts/      Local demo and failure-scenario runners
docs/         Operator and design documentation
```

## License

Attesta is licensed under the Apache License 2.0. You may use, modify, distribute, and sublicense the project under the license terms, with attribution and preservation of copyright/license notices.

The license also includes an express patent grant from contributors and is provided on an "AS IS" basis without warranties. See [LICENSE](LICENSE) for the full text.
