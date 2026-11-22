# Attesta

Attesta is an evidence-driven Kubernetes reliability control plane for SLOs, progressive delivery, production readiness, and incident automation. It turns observed service health, rollout state, and remediation actions into durable operational evidence while a Helios-style workflow engine provides ordered execution, retries, and idempotency.

It keeps the useful Harbor-style golden path for service onboarding, but the product focus is stronger: developers can ship services quickly, while platform/SRE teams get reliability controls before and during production rollout.

## What Attesta Does

- Registers services with owner, tier, pager, repository, runbook, dashboard, SLO, deployment strategy, and rollback policy metadata.
- Generates production-ready service scaffolds with Kubernetes manifests, GitHub Actions, Kustomize overlays, Terraform, Argo CD, Grafana, Alertmanager, OpenTelemetry, Jaeger, SLO policy, rollout guard, and runbook files.
- Scores services for production readiness.
- Evaluates SLOs from Prometheus: error rate and p95 latency over 5m, burn rate over 1h, availability and remaining error budget over 30d. Without Prometheus, or without data, the SLO state is `unknown` and rollouts are refused.
- Gates canary rollouts with latency and error-rate health checks.
- Reconciles `RolloutGuard` resources with a Kubernetes controller that updates status, emits Events, and, when rollback is opted in, restores the last healthy pod template of a rollout that exceeded its progress deadline.
- Records automated rollback decisions through workflow-backed rollout steps.
- Opens incidents from Alertmanager's webhook or a plain JSON call, records the SLO evaluation at that moment, and builds the timeline and postmortem draft only from recorded deployments, the alert and that evaluation.
- Exposes workflow runs, steps, retry state, and audit-style events for onboarding, readiness, SLO checks, rollouts, rollback, and incidents.
- Runs locally on a MacBook with Go, Docker, kind, kubectl, and optional Trivy.

## Helios Integration

Attesta owns the Kubernetes reliability domain. The Helios-style workflow engine owns orchestration semantics: ordered steps, retries, run state, idempotency keys, and execution events. This keeps Attesta distinct from Helios while reusing the strongest control-plane idea from Helios as the execution substrate.

Workflow-backed operations include:

- Service onboarding and scaffold generation.
- Production-readiness and SLO/error-budget evaluation.
- Rollout start, pause, rollback, and health-gate rollback.
- Alertmanager-style incident enrichment, RCA timeline generation, and incident resolution.

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

Generated service assets appear under:

```text
generated/services/payments-api
```

## Authentication

The API refuses to start unless `ATTESTA_API_TOKEN` is set. Clients send the token in the `X-Attesta-Token` header; `/livez`, `/readyz` and `/metrics` stay open for probes and scrapers. For throwaway local runs only, `ATTESTA_INSECURE_NO_AUTH=true` starts the API without authentication and logs a warning.

## CLI Demo

```bash
go run ./cli/cmd/attesta check payments-api
go run ./cli/cmd/attesta score payments-api
go run ./cli/cmd/attesta slo status payments-api
go run ./cli/cmd/attesta rollout status payments-api
go run ./cli/cmd/attesta health-gate --name payments-api --p99-latency-ms 450 --error-rate 0.2 --success-count 100
go run ./cli/cmd/attesta incident create --service payments-api --alert HighErrorRate --error-rate 8.2 --latency-p95-ms 850
go run ./cli/cmd/attesta incident list
go run ./cli/cmd/attesta workflows list
```

## Full Local CI/CD Demo

```bash
make demo-full
```

This runs a production-style local pipeline:

1. Starts or reuses the Attesta API.
2. Onboards `payments-api`.
3. Validates generated production-readiness guardrails.
4. Runs service tests.
5. Renders Kustomize manifests.
6. Builds stable and canary Docker images.
7. Runs Trivy when installed.
8. Creates or reuses a kind cluster.
9. Deploys the stable revision.
10. Deploys the canary revision.
11. Verifies `/readyz` and `/metrics`.
12. Sends a bad canary signal to Attesta.
13. Records a rollback decision.
14. Runs `kubectl rollout undo`.
15. Creates an incident.
16. Generates timeline and postmortem output.
17. Lists Helios-style workflow runs for audit evidence.
18. Writes evidence to `artifacts/local-ci-cd/`.

## Failure-Mode Demo

```bash
make demo-failures
```

This runs production failure scenarios and writes evidence under `artifacts/failure-modes/`: bad canary rollback, low-traffic sample hold, Prometheus fail-closed behavior, PostgreSQL startup failure, duplicate onboarding idempotency, invalid metadata rejection, and rollback evidence recording. See `docs/failure-modes.md`.

## Kubernetes Controller

Attesta includes a controller/operator for `RolloutGuard` resources:

```bash
go run ./controller/cmd/attesta-controller --kubeconfig "$HOME/.kube/config" --leader-elect=false
```

In-cluster manifests live under:

```bash
kubectl apply -k deploy/crds
kubectl apply -k deploy/controller
```

The controller watches `RolloutGuard`, evaluates the referenced Deployment, patches `status.conditions`, creates Kubernetes Events, and, when `spec.rollback.enabled` is true, restores the last pod template it saw fully rolled out once Kubernetes reports `ProgressDeadlineExceeded`. Paused Deployments are never touched, and the controller never rolls back twice onto the same healthy template. See `docs/controller-operator.md` for the rules and current limits.

## Generated Service Files

`attesta service init` generates:

```text
generated/services/payments-api/
  attesta-service.yaml
  slo.yaml
  rollout.yaml
  Dockerfile
  Makefile
  .github/workflows/ci.yml
  k8s/base/deployment.yaml
  k8s/base/service.yaml
  k8s/base/hpa.yaml
  k8s/base/pdb.yaml
  observability/dashboard.json
  observability/alerts.yaml
  observability/prometheus-rules.yaml
  observability/servicemonitor.yaml
  observability/otel-collector.yaml
  observability/jaeger.yaml
  runbooks/incident-runbook.md
  infra/argocd/application.yaml
  infra/terraform/main.tf
```

## API Surface

- `POST /api/v1/services`
- `GET /api/v1/services`
- `GET /api/v1/services/{name}`
- `GET /api/v1/services/{name}/readiness`
- `GET /api/v1/services/{name}/score`
- `GET /api/v1/services/{name}/slos`
- `GET /api/v1/services/{name}/error-budget`
- `POST /api/v1/services/{name}/health-gate`
- `POST /api/v1/services/{name}/rollback`
- `POST /api/v1/services/{name}/rollouts`
- `GET /api/v1/services/{name}/rollouts/{id}`
- `POST /api/v1/services/{name}/rollouts/{id}/pause`
- `POST /api/v1/services/{name}/rollouts/{id}/rollback`
- `POST /api/v1/incidents`
- `POST /api/v1/alerts/alertmanager`
- `POST /api/v1/incidents/{id}/resolve`
- `GET /api/v1/incidents`
- `GET /api/v1/incidents/{id}`
- `GET /api/v1/incidents/{id}/timeline`
- `GET /api/v1/incidents/{id}/postmortem`
- `GET /api/v1/workflows`
- `GET /api/v1/workflows/{id}`

## PostgreSQL Mode

```bash
docker compose up --build
```

Without `DATABASE_URL`, Attesta uses an in-memory store for fast laptop-local development. With `DATABASE_URL`, Attesta persists the service catalog, deployments, reliability metadata, and workflow run tables in PostgreSQL.

PostgreSQL mode also persists incident records and workflow idempotency lookup state, so repeated onboarding operations can be recognized across API restarts.

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

## Repository Layout

```text
api/          Go API, catalog, workflow engine, readiness, SLO, rollout, incident logic
cli/          Go CLI for service onboarding and reliability workflows
controller/   Kubernetes RolloutGuard controller/operator
templates/    Service, CI/CD, GitOps, observability, SLO, rollout, and runbook templates
deploy/crds/  AttestaService, SLOPolicy, RolloutGuard, and Incident CRDs
infra/        Terraform, Argo CD, and monitoring defaults
scripts/      Local demo and CI/CD runner
docs/         Architecture and operator documentation
```

## License

Attesta is licensed under the Apache License 2.0. You may use, modify, distribute, and sublicense the project under the license terms, with attribution and preservation of copyright/license notices.

The license also includes an express patent grant from contributors and is provided on an "AS IS" basis without warranties. See [LICENSE](LICENSE) for the full text.
