# Attesta

[![CI](https://github.com/gaurav-gs7/Attesta/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/gaurav-gs7/Attesta/actions/workflows/ci.yml)

Attesta is a Kubernetes rollout guard and a small reliability API that reports what the guard did. The `attesta-controller` watches Deployments that have a `RolloutGuard`. It either guards a rolling update, restoring the last healthy pod template when the new one fails, or runs a canary: the new revision gets traffic in steps (Gateway API weights or a share of pods), is checked on its own metrics at each, and is promoted or taken out of traffic. The controller can also pause a new revision while the service's error budget is spent, and records every decision as a hash-chained `RolloutDecision`. The API reports SLOs measured from Prometheus and the guards' verified decisions, opens incidents from Alertmanager with those decisions on the timeline, and generates a service scaffold with manifests, alerts and a runbook.

Every value Attesta reports is either measured, recorded, or explicitly marked as unknown or simulated. Where it has no data it says `unknown` and fails closed.

## Status

| Area | What is real | What is not (yet) |
| --- | --- | --- |
| Rollout guard controller | Watches Deployments, writes status and Events. Rolling mode restores the last healthy template when Kubernetes reports `ProgressDeadlineExceeded` or the new revision fails `consecutiveFailures` metric checks in a row (error rate, p95 latency, availability from Prometheus, scoped to that revision). Canary mode (`spec.canary`) runs the last promoted revision in a managed `<deployment>-primary`, gives the new one a share of traffic in steps (Gateway API HTTPRoute weights, or a share of pods), checks it at each, and promotes it or takes it out of traffic. Inconclusive data never decides. Acts only on Deployments that name the guard in `attesta.io/rollback-guards`, and never fights a GitOps tool that re-applies the bad version. Leader election, metrics, probes. | Canary routing by header or cookie, traffic mirroring, and service meshes other than through Gateway API are not supported. One canary per Deployment. |
| SLO gate | Error rate, p95, multi-window burn rates (1h+5m, 6h+30m) and the 30-day error budget from Prometheus, judged by one evaluator (`internal/slo`) that the API's SLO status and the controller share. A guard with `spec.sloGate` pauses each new revision while the service is out of budget or burning it fast, resumes it when the service recovers, and records both. Unknown data blocks `POST /rollouts`. | Burn-rate windows are fixed at the SRE workbook's pairs (5m/1h, 30m/6h); their thresholds and the budget floor are per guard (and per API process). |
| Decision records | Every promotion, rollback, pause, and skipped or failed action is written by the controller as an immutable `RolloutDecision`: what it measured (each PromQL query and value, the Deployment's conditions), the revision and images, what it restored, and when. Records form a SHA-256 hash chain per guard, are signed with Ed25519 (`--signing-key-file`), cannot be deleted or relabelled even by cluster-admins (a ValidatingAdmissionPolicy), and are copied in order to an append-only archive outside the cluster (`--archive-url`, attesta-api), which refuses forks and flags records missing or changed in the cluster. A rollback and its sealed record travel on the same Deployment update, so a crash cannot separate them. | Whoever controls both the cluster and the archive's database can rewrite both; keep them in different hands. |
| API cluster view | Reads RolloutGuards and the controller's RolloutDecisions (`ATTESTA_CLUSTER_READ`), verifies each chain on read, and cites the decisions in incident timelines. Read-only RBAC; the API never writes to the cluster. | No cache: each request reads the API server. |
| Incidents | Alertmanager webhook (v4) with fingerprint dedupe and resolve; timeline from recorded events only. | Root cause is never inferred; the postmortem draft leaves it as TODO. |
| Readiness score | Checks which metadata fields were registered. | It does not inspect the cluster or the repository. |
| Workflow runner | Ordered in-process steps with per-step state and events, persisted to PostgreSQL. | A crashed run is re-run from the start, not resumed. Only succeeded runs are replayed; failed and abandoned ones are re-run. |
| CRDs | `RolloutGuard` is reconciled; `RolloutDecision` is written by the controller only. | `AttestaService` and `SLOPolicy` are declarative records only; nothing reconciles them. |

## Components

- `cmd/attesta-controller`: the `RolloutGuard` controller. See [docs/controller-operator.md](docs/controller-operator.md).
- `cmd/attesta-api`: the HTTP API: service catalog, SLO status, a read-only view of guards and their decisions, incidents, readiness, scaffold generator, workflow runner.
- `cmd/attesta`: a thin client for the API, sending the API's own request types.
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

go run ./cmd/attesta service init payments-api \
  --owner payments-platform \
  --team platform \
  --tier critical \
  --language go \
  --slo-availability 99.9 \
  --slo-latency-p95 300ms \
  --deployment canary \
  --pager payments-oncall
```

The API renders the scaffold in memory and never writes it to its own disk; the CLI downloads it from `GET /api/v1/services/payments-api/scaffold` (a `.tar.gz` with every file under `payments-api/`) and writes it to `generated/services/payments-api` (`--output` changes the parent, `--output ""` skips it; `attesta service scaffold payments-api` fetches it again). The CLI accepts only regular files under that one directory, within size limits, and writes them to a temporary directory that is renamed into place, so a failed download leaves an earlier scaffold as it was. Without `ATTESTA_PROMETHEUS_URL` the SLO state is `unknown`, so `attesta slo status payments-api` reports why, and rollouts are refused.

A generated service's image is `<ATTESTA_IMAGE_REGISTRY>/<name>` (a lowercase `host[:port][/path]`, such as `ghcr.io/acme`; unset, it is the placeholder `ghcr.io/change-me` and the API warns at start-up). The base Deployment names the image without a tag, and each overlay pins what that environment runs with Kustomize's `images:`, starting at the tag `unset`, which no one pushes, so nothing starts until a build is promoted. The generated CI tags each push with the commit SHA, never `latest`, and writes the pushed digest to the job summary with the command that promotes it: `kustomize edit set image <image>@sha256:<digest>` in the overlay. CI logs in with `GITHUB_TOKEN` for `ghcr.io`, and with the `REGISTRY_USERNAME` and `REGISTRY_PASSWORD` secrets elsewhere.

## Authentication

Every `/api/` request is made by an identity with a role; `/livez`, `/readyz` and `/metrics` stay open for probes and scrapers. Roles are ordered, each including the one before:

| Role | May |
| --- | --- |
| `viewer` | read everything |
| `operator` | open and resolve incidents, post Alertmanager alerts |
| `admin` | register services |

Callers send `Authorization: Bearer <token>` (the older `X-Attesta-Token` header still works). Identities come from one or more of:

- **A tokens file**, `ATTESTA_API_TOKENS_FILE`: JSON listing each token's name, role and SHA-256 (`{"tokens":[{"name":"ci-deployer","role":"operator","sha256":"..."}]}`; hash a token with `printf %s "$TOKEN" | sha256sum`). Only hashes are stored. The file is re-read when it changes, so a token is rotated by adding the new hash, moving the caller over and removing the old one, without a restart.
- **Kubernetes TokenReview**, `ATTESTA_AUTH_KUBERNETES=true`: service account and user tokens are checked with the cluster's TokenReview API (results cached for a minute). `ATTESTA_AUTH_ROLE_BINDINGS_FILE` maps users and groups to roles (`{"bindings":[{"role":"operator","users":["system:serviceaccount:ci:deployer"]},{"role":"viewer","groups":["system:authenticated"]}]}`); an authenticated caller with no binding is refused. `ATTESTA_AUTH_AUDIENCES` (for example `attesta`) requires tokens issued for this API.
- **`ATTESTA_API_TOKEN`**, one shared token acting as admin under the name `shared-api-token`. It is kept for compatibility and logs a warning, because every caller is the same identity.

The caller's name is recorded as the actor of every workflow run, every rollout transition, the opener and resolver of every incident, and in the access log. Without any of these the API refuses to start; `ATTESTA_INSECURE_NO_AUTH=true` runs it for throwaway local use, every caller an anonymous admin. `ATTESTA_TLS_CERT_FILE` and `ATTESTA_TLS_KEY_FILE` make it serve HTTPS (TLS 1.2 or later); without them it warns that TLS must be terminated in front of it. OIDC is not implemented.


## Kubernetes Controller

```bash
kubectl apply -k deploy/crds
kubectl apply -k deploy/controller
```

Run it against your current kube context:

```bash
go run ./cmd/attesta-controller --kubeconfig "$HOME/.kube/config" --leader-elect=false
```

The controller classifies each guarded rollout as `Progressing`, `Verifying`, `Healthy`, `Paused` or `Blocked`. Blocked means Kubernetes reported `ProgressDeadlineExceeded` for the current generation or, with `--prometheus-url` and `spec.checks`, that the new revision failed `consecutiveFailures` metric checks in a row. With `spec.rollback.enabled: true` it restores the last template that passed, never acts on a paused Deployment, and never rolls back twice onto the same template. With `spec.sloGate`, it pauses a new revision while the service's error budget is below 10% or burning fast, using the same SLO evaluator as the API. See [docs/controller-operator.md](docs/controller-operator.md) for the rules, metrics and limits.

Each decision is recorded as a `RolloutDecision` in the guard's namespace and chained to the one before:

```bash
kubectl get rolloutdecisions -n platform
# NAME                                    GUARD                  SEQ   ACTION       TRIGGER          REVISION
# payments-api-rollout-0f3c9a2e-000001    payments-api-rollout   1     Promoted     AnalysisPassed   3
# payments-api-rollout-0f3c9a2e-000002    payments-api-rollout   2     RolledBack   AnalysisFailed   4

kubectl get rolloutguards,rolloutdecisions -n platform -o json | attesta verify
```

Without Attesta's code, a record checks out when `jq -cjS '.spec | del(.hash, .signature)' | sha256sum` equals its `spec.hash`, and its `spec.previousHash` equals the hash of the record before it.

## SLO Evaluation

Set `ATTESTA_PROMETHEUS_URL` to evaluate SLOs from Prometheus:

```bash
ATTESTA_PROMETHEUS_URL=http://prometheus.monitoring.svc:9090 make run-api
```

Attesta reads `http_requests_total` and `http_request_duration_seconds` for the service's `service` label:

| Signal | Window |
| --- | --- |
| Error rate (5xx share) and p95 latency | 5m |
| Burn rate | 5m, 30m, 1h and 6h |
| Availability and remaining error budget | 30d, from request counts |

The gate blocks when the error budget remaining is under 10%, when p95 or the error rate is over the service's objective, or when a burn-rate pair is over its threshold on both windows: 14.4x over 1h and 5m (2% of the budget in an hour), or 6x over 6h and 30m (5% in six hours). Requiring both windows means a burn that has already stopped does not block a fix. The generated `PrometheusRule` records the error ratio over the same four windows and alerts on the same pairs.

The API keeps one Prometheus client for its lifetime. Its queries for one evaluation run concurrently (at most six at a time), each with its own timeout. It is configured through the environment:

| Variable | Default | Purpose |
| --- | --- | --- |
| `ATTESTA_PROMETHEUS_URL` | unset (gate `unknown`) | Prometheus base URL |
| `ATTESTA_PROMETHEUS_BEARER_TOKEN` or `..._BEARER_TOKEN_FILE` | none | Bearer token; prefer the file, from a mounted Secret |
| `ATTESTA_PROMETHEUS_USERNAME`, `ATTESTA_PROMETHEUS_PASSWORD` or `..._PASSWORD_FILE` | none | Basic auth, instead of a token |
| `ATTESTA_PROMETHEUS_CA_FILE` | system roots | PEM bundle to trust for Prometheus's TLS certificate |
| `ATTESTA_PROMETHEUS_QUERY_TIMEOUT` | `3s` | Timeout for each query |
| `ATTESTA_PROMETHEUS_REQUESTS_METRIC`, `..._DURATION_METRIC`, `..._SERVICE_LABEL`, `..._STATUS_LABEL`, `..._ERROR_STATUS` | `http_requests_total`, `http_request_duration_seconds`, `service`, `status`, `5..` | Series and labels the SLIs are computed from, for services not instrumented like the generated ones |
| `ATTESTA_SLO_MIN_ERROR_BUDGET_PCT`, `ATTESTA_SLO_FAST_BURN_RATE`, `ATTESTA_SLO_SLOW_BURN_RATE` | `10`, `14.4`, `6` | The gate's thresholds in the API's SLO status; a guard's `spec.sloGate` sets its own |

Names are validated as Prometheus metric and label names, and a bad CA file or conflicting auth stops the API at startup. Responses over 1 MiB are rejected. The controller takes `--prometheus-bearer-token-file`, `--prometheus-ca-file` and `--prometheus-query-timeout`. Per-service query templates from `SLOPolicy` are not implemented: nothing reconciles `SLOPolicy` yet.

The gate is `allowed`, `blocked` or `unknown`; a RolloutGuard with `spec.sloGate` acts on the same verdict, computed by the same code. It is `unknown` whenever a signal cannot be measured: no Prometheus configured, a failed query, no series for the service, a `NaN` result, or fewer than 20 requests in a signal's window (5m for error rate and p95, each burn-rate window, 30d for the budget), where one request would decide the answer. Measured values are omitted from the response rather than reported as zero. Prometheus must retain at least 30 days for the budget to be computed.

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

Timelines contain only recorded events: the controller's decisions for the service's guards in the 2 hours before the alert (each citing its RolloutDecision and hash), the alert, the incident opening and the SLO evaluation. When the controller promoted a revision in that window, a correlation event names the latest one as a rollback candidate and says it is not a confirmed cause. The postmortem draft marks what Attesta cannot know as TODO.


## Guards And Decisions

The API never writes to the cluster. It reads what the controller does: the RolloutGuards and the RolloutDecisions they recorded, with each guard's hash chain verified on every read. There is no endpoint that starts, pauses or rolls back a rollout, and none that takes a latency or error rate from a caller; the controller measures from Prometheus and acts, and the API reports it.

```bash
ATTESTA_CLUSTER_READ=true make run-api          # uses $KUBECONFIG, or the in-cluster service account
attesta guards payments-api                     # the service's guards, chains verified
attesta guard payments/payments-api-rollout     # one guard and every decision it recorded
```

A guard belongs to a service by its `spec.serviceRef`, or else its Deployment's name, in any namespace. `deploy/api/cluster-read-rbac.yaml` grants the API `get` and `list` on `rolloutguards` and `rolloutdecisions`, nothing else. Without `ATTESTA_CLUSTER_READ`, the guard endpoints answer 503 and say so, `GET .../status` carries `guardsUnavailable`, and incident timelines state that the decisions were not read rather than implying nothing happened.

Each guard comes back with `chain`: `verified`, the number of decisions, and, when it fails, `brokenAt` and `problem`, the first sequence where a record was edited, inserted or deleted. The API reads the API server on each request, without a cache.

## Readiness Score

`GET /api/v1/services/{name}/readiness` scores which fields were registered for the service: ownership, pager, tier, runbook, dashboard, SLOs, strategy, rollback policy, namespace, repository and, for critical services, dependencies. The API does not fill these in for you, so an unregistered field fails its check. The report's `scope` field states that the cluster and repository are not inspected.

## Generated Service Scaffold

`attesta service init` writes a Go or Python service with Dockerfile, Kustomize base and overlays, GitHub Actions CI, a `RolloutGuard` and `SLOPolicy`, PrometheusRule alerts, a ServiceMonitor, a Grafana dashboard, OpenTelemetry and Jaeger manifests, an Argo CD Application (which also creates the namespace) and a runbook.

- The service exposes `http_requests_total` and the `http_request_duration_seconds` histogram, the series the SLO gate reads, from the standard clients (`client_golang`, `prometheus_client`) through middleware that counts every request except probes and scrapes, plus their runtime and process metrics. The Go client and its dependencies are pinned in `go.mod`/`go.sum`, the Python one by version and hash.
- Alert thresholds and the runbook use the service's registered SLO.
- Fields you did not register stay visible as gaps: the runbook marks a missing pager as TODO, and the Argo CD Application uses a `CHANGE-ME` repository placeholder.

## Demos

`make demo-full` builds the scaffold, deploys a stable and then a candidate image to kind, and walks through the API. Parts of it are simulated, and the script's log says so:

- The candidate image replaces every pod. There is no traffic splitting.
- The rollback is `kubectl rollout undo`, run by the script; this demo does not run the controller. `make e2e` does, and there the controller measures the bad revision and rolls it back itself.
- The incident is a hand-written alert payload.

`make demo-failures` runs failure scenarios against the API and writes evidence under `artifacts/failure-modes/`: invalid metadata rejected, duplicate onboarding, low-traffic hold, rollback request recorded, Prometheus unreachable (gate `unknown`, rollout refused), and PostgreSQL unreachable at startup. See [docs/failure-modes.md](docs/failure-modes.md).

## Development

```bash
make test   # go test -race ./...
make lint   # golangci-lint and shellcheck
make e2e    # controller end-to-end test on a local kind cluster
make generate  # deepcopy and CRDs from api/v1 (controller-gen v0.23.0)
```

CI runs on every push: formatting, `go mod tidy` drift, vet, race-enabled tests against PostgreSQL with a statement-coverage floor (68%; raise it, never lower it), golangci-lint, ShellCheck, govulncheck, generated-code drift, manifest validation with kubeconform (the controller's and the generated scaffold's overlays), actionlint, both container images (checked to run as non-root and scanned with Trivy, failing on fixable HIGH or CRITICAL findings), and `scripts/e2e-rollback.sh` on kind, followed by a server-side dry run of the scaffold's custom resources against the generated CRDs. Every action is pinned to a commit SHA. Dependabot opens weekly, grouped updates for Go modules, the pinned actions, the base images and the generated Python service's hashed requirements. The end-to-end test deploys the controller and runs two scenarios. First, an image that cannot be pulled is rolled back once on its progress deadline. Second, against an in-cluster Prometheus stub, an image that Kubernetes considers healthy but whose metrics fail is rolled back by metric analysis alone. It also checks that a paused Deployment is left alone, that only the leader acted, and that after the leader pod is deleted the standby takes over without repeating the rollback.

## PostgreSQL Mode

```bash
docker compose up --build
```

Without `DATABASE_URL`, Attesta keeps everything in memory. With it, the service catalog, workflow runs and incidents are stored in PostgreSQL. What happened in the cluster is not copied there: it is read from the RolloutDecisions.

## API Surface

| Method and path | Purpose |
| --- | --- |
| `GET /livez`, `GET /readyz`, `GET /metrics` | Probes and metrics (no token) |
| `POST /api/v1/services`, `GET /api/v1/services` | Register and list services |
| `GET /api/v1/services/{name}` | Service record |
| `GET /api/v1/services/{name}/status` | Measured SLO state and the service's guards |
| `GET /api/v1/services/{name}/readiness`, `POST .../check`, `GET .../score` | Metadata readiness score |
| `GET /api/v1/services/{name}/slos`, `GET .../error-budget` | SLO evaluation |
| `GET /api/v1/services/{name}/guards` | The service's RolloutGuards, each chain verified |
| `GET /api/v1/guards[?namespace=]` | RolloutGuards |
| `GET /api/v1/guards/{namespace}/{name}` | A guard, every decision it recorded, the chain's verification, and how it compares with the archive |
| `POST /api/v1/decision-archive` | Archive one decision (the controller's `--archive-url`); append-only, refuses forks and gaps |
| `GET /api/v1/decision-archive`, `GET .../{guardUID}` | Archived chains, including deleted guards', verified |
| `GET /api/v1/services/{name}/runbook` | Runbook rendered from the service record |
| `GET /api/v1/services/{name}/scaffold` | The service's scaffold as a `.tar.gz`, rendered on demand |
| `POST /api/v1/incidents`, `GET /api/v1/incidents` | Open and list incidents |
| `POST /api/v1/alerts/alertmanager` | Alertmanager webhook |
| `GET /api/v1/incidents/{id}`, `.../timeline`, `.../postmortem`, `POST .../resolve` | Incident record, timeline, draft, resolve |
| `GET /api/v1/workflows`, `GET /api/v1/workflows/{id}` | Workflow runs |

The incident and workflow lists are paginated newest first: pass `?limit=` (1 to 200, default 50) and, for the next page, the `nextCursor` value from the previous response as `?cursor=`. `nextCursor` is absent on the last page. A store error is reported as `500`, never answered from a cache.

With `DATABASE_URL` set, the store uses a pgx connection pool: at most 10 connections, each closed after 30 minutes (plus up to 5 of jitter) or 5 idle minutes, health-checked every 30s. Every session runs with `statement_timeout=10s`, `lock_timeout=5s`, `idle_in_transaction_session_timeout=30s` and `application_name=attesta-api`. Any of these can be set in the URL instead (`pool_max_conns`, `pool_max_conn_lifetime`, `pool_max_conn_idle_time`, `pool_health_check_period`, or the session parameter by name). Migrations lift the two timeouts for their own session only. No read holds more than one connection, so the pool size is the number of requests the store serves at once.

Each client is rate-limited on `/api/` with a token bucket: by address before authentication (50 requests/s, burst 100), so a flood of bad credentials never reaches a token lookup or TokenReview, and by caller identity after it (20/s, burst 40), so one caller cannot starve others from many addresses. Over the limit the API answers 429 with `Retry-After`. The limiters keep at most 10,000 clients; past that, idle entries are dropped and new clients share one bucket. Every `/api/` request's context is cancelled after 25s (below the 30s write timeout), so store and Prometheus calls stop when nobody is waiting; headers are capped at 64 KiB and bodies at 1 MiB, and every request field has a length cap (a service name is a DNS label, at most 63 characters).

| Variable | Default | Purpose |
| --- | --- | --- |
| `ATTESTA_RATE_LIMIT_IP_RPS`, `..._IP_BURST` | `50`, `100` | Per-address limit; an RPS of `0` disables it |
| `ATTESTA_RATE_LIMIT_PRINCIPAL_RPS`, `..._PRINCIPAL_BURST` | `20`, `40` | Per-caller limit; `0` disables it |
| `ATTESTA_TRUST_FORWARDED_FOR` | `false` | Key the per-address limit on the last `X-Forwarded-For` hop. Only behind a proxy that sets it, or clients pick their own key |
| `ATTESTA_REQUEST_TIMEOUT` | `25s` | Context deadline for each `/api/` request; must be under 30s |

Every response carries an `X-Request-ID` header: the caller's own, if it sends a well-formed one, or a generated ID. Access logs, panic logs (which include the panic value and stack) and logs from workflow runs started by the request all carry it as `request_id`, and every 500 returns it in the body as `requestId`. A 500 never carries the underlying error: store and Prometheus errors name hosts, ports, database users and SQL, so the detail is logged under the request ID (and, for workflow runs, the run ID) and the client gets `internal error` plus the ID to quote. Only errors the caller can act on, such as conflicts, closed gates and validation failures, keep their wording. `/readyz` answers 503 when the store does not respond within 2 seconds; `/livez` does not depend on the store.

## Repository Layout

```text
api/v1/                  attesta.io/v1 API types; deploy/crds is generated from them
cmd/attesta/             CLI
cmd/attesta-api/         HTTP API binary and image
cmd/attesta-controller/  RolloutGuard controller binary and image
internal/                Everything the binaries share: catalog, SLO gate, workflows, controller, scaffold, ...
templates/               Generated service scaffold
deploy/crds/             AttestaService, SLOPolicy and RolloutGuard CRDs (generated: make generate)
deploy/controller/       Controller manifests
infra/                   Terraform, Argo CD and monitoring values for local clusters
scripts/                 Local demo and failure-scenario runners
test/e2e/                End-to-end manifests for kind
docs/                    Operator and design documentation
```

## Contributing and releases

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and what a change needs, [CHANGELOG.md](CHANGELOG.md) for releases and upgrade notes, and [SECURITY.md](SECURITY.md) to report a vulnerability.

## License

Attesta is licensed under the Apache License 2.0. You may use, modify, distribute, and sublicense the project under the license terms, with attribution and preservation of copyright/license notices.

The license also includes an express patent grant from contributors and is provided on an "AS IS" basis without warranties. See [LICENSE](LICENSE) for the full text.
