# Attesta

[![CI](https://github.com/gaurav-gs7/Attesta/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/gaurav-gs7/Attesta/actions/workflows/ci.yml)

Attesta is a Kubernetes rollout controller that proves what it did. It runs canaries on real traffic, pauses rollouts while a service's error budget is spent, and rolls back revisions that fail their checks. Every one of those decisions is written as a `RolloutDecision`: the evidence it acted on, hash-chained to the decision before it, signed with Ed25519, refused deletion by an admission policy, and copied to an append-only archive outside the cluster. Anyone with the public key can verify the history with `attesta verify`, or with `jq` and `sha256sum`.

```bash
kubectl apply -k deploy/crds
kubectl apply -k deploy/controller
```

![Two-minute Attesta explainer: why rollout decisions need a verifiable record, then Attesta on a kind cluster. A canary gets a third of real traffic and is promoted; a canary behind a Gateway API HTTPRoute answers 20% errors at its 10% step and is taken out of traffic; the SLO gate pauses a rollout while the error budget is spent and resumes it when the service recovers; every decision verifies as a signed hash chain; deleting or editing a record is refused; and an edited export fails verification while the API confirms the chain matches the off-cluster archive.](https://raw.githubusercontent.com/gaurav-gs7/Attesta/main/docs/assets/attesta-demo.gif)

Every terminal line in this two-minute walkthrough comes from a real run on a kind cluster, recorded by [`scripts/demo/capture.sh`](scripts/demo/capture.sh) in the [Demo capture](.github/workflows/demo.yml) workflow. The [full video](docs/assets/attesta-explainer.mp4) is in `docs/assets`.

## At a Glance

| What was tested on a real cluster | Result |
| --- | --- |
| Canary without a router, at its 33% step (one pod beside the primary's two) | **91 of 300** real requests (30%) reached the canary; promoted when the step passed |
| Gateway API canary answering 20% errors at its 10% step | HTTPRoute weights `90 10` → `100 0` after two failed checks; `CanaryAborted` recorded with `error-rate 20% (threshold <= 1%)` |
| SLO gate with the 30-day budget at -400% | New revision paused (`SLOGateBlocked`), resumed on its own when errors stopped (`SLOGateAllowed`) |
| Image that never becomes ready | Rolled back once on its progress deadline; a GitOps revert is not fought |
| Revision Kubernetes calls healthy whose metrics fail | Rolled back by metric analysis alone |
| Leader pod deleted mid-incident | Standby takes over without repeating the rollback |
| `kubectl delete` on a record, as cluster-admin | Refused by the `attesta-decision-records` ValidatingAdmissionPolicy |
| One number changed in an exported record | `BROKEN ... breaks at sequence 3: record ... was altered` |
| Every chain in the run | Verifies with signatures; the archive holds the same records |

The first three rows and the last three are in the video. All of them run on every push in CI: [`scripts/e2e-rollback.sh`](scripts/e2e-rollback.sh) on kind, followed by `attesta verify` over every record the run produced.

## Why Attesta

Argo Rollouts and Flagger are mature progressive-delivery controllers, with routing by header and cookie, mirroring and service-mesh integrations Attesta does not try to match. What they decided, though, lives in Events (gone after an hour), status fields, annotations and analysis objects: all editable, all deletable, none signed. After an incident, "why did the controller roll back at 03:12, and on what numbers?" is answered from logs, if they were kept.

Attesta's answer is a record you can hand to an auditor:

```bash
kubectl get rolloutdecisions -n payments -l attesta.io/guard=payments-api-rollout
# NAME                                   GUARD                  SEQ   ACTION          TRIGGER             REVISION
# payments-api-rollout-2fe37ad7-000001   payments-api-rollout   1     Promoted        CanaryInitialized   1
# payments-api-rollout-2fe37ad7-000002   payments-api-rollout   2     CanaryStarted   NewRevision         2
# payments-api-rollout-2fe37ad7-000003   payments-api-rollout   3     CanaryAborted   AnalysisFailed      2
```

Each record holds every PromQL query the controller ran with its value and threshold, the Deployment's conditions, the revision and images, what was restored, who acted (the controller instance) and when.

## What It Does

- **Canaries on real traffic.** The last promoted revision runs in a controller-managed `<deployment>-primary`. A new revision gets traffic in steps, as Gateway API HTTPRoute weights or as a share of pods, is checked on its own metrics at each step, and is promoted into the primary or taken out of traffic.
- **Analysis scoped to the new revision.** Error rate, p95 latency and availability from Prometheus, filtered to the revision's `pod_template_hash`. Inconclusive data (too few requests, a failed query) never decides.
- **An SLO gate.** Multi-window burn rates (1h+5m, 6h+30m) and the 30-day error budget, judged by the same evaluator the API uses. A rollout is paused while the service is out of budget or burning it fast, and resumed when it recovers. Thresholds are set per guard.
- **Rollbacks that never fight you.** Rolling guards restore the last healthy template on a progress deadline or failed checks. Only Deployments that name the guard are touched, a paused Deployment is left alone, and a revert by Argo CD, Flux or `kubectl apply` is reported instead of looped on.
- **Decision records that hold up.** SHA-256 chain per guard, Ed25519 signatures (`--signing-key-file`), an immutable spec, a ValidatingAdmissionPolicy that refuses deletes even from cluster-admins, and an off-cluster archive (`--archive-url`) that refuses forks and flags records missing or changed in the cluster. A rollback and its sealed record travel on the same Deployment update, so a crash between them loses neither.
- **A read-only API.** `attesta-api` reads guards and their decisions, verifies every chain on read, reports SLOs from Prometheus, and puts the controller's decisions on incident timelines from Alertmanager. It never writes to the cluster, and never takes a health number from a caller.

```text
Deployment ──▶ attesta-controller ──▶ HTTPRoute weights or pod counts · pause · promote · roll back
                 │    ▲
                 │    └── Prometheus: the new revision's error rate and latency, the service's burn rates and budget
                 ▼
          RolloutDecision n ── previousHash = hash(n-1) ── Ed25519 signature
                 │
                 ├──▶ ValidatingAdmissionPolicy: no deletes, no relabels; CRD: spec immutable
                 └──▶ attesta-api archive (PostgreSQL, append-only) ──▶ attesta verify / GET /api/v1/guards/...
```

## Quick Start

Install the CRDs and the controller (Kubernetes 1.30 or later, for the admission policy):

```bash
kubectl apply -k deploy/crds
kubectl apply -k deploy/controller
```

Guard a Deployment with a canary. The Deployment names the guard in `attesta.io/rollback-guards`: without that consent the controller never changes it.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payments-api
  namespace: payments
  annotations:
    attesta.io/rollback-guards: payments-api-rollout
# spec: ...
---
apiVersion: attesta.io/v1
kind: RolloutGuard
metadata:
  name: payments-api-rollout
  namespace: payments
spec:
  deploymentRef: payments-api
  canary:
    steps:
      - {weight: 10, pause: 1m}
      - {weight: 50, pause: 1m}
    trafficRouting:
      gatewayAPI: {httpRoute: payments-api}   # omit to split by pod count
  checks: {errorRateThreshold: 1, window: 1m, interval: 5s, minRequests: 10, successfulChecks: 1}
  rollback: {consecutiveFailures: 2}
  sloGate: {availabilityTarget: 99.9}     # hold new revisions while the error budget is spent
```

Give the controller `--prometheus-url` for the checks and the SLO gate. To sign records, create a key and add the signing component; to archive them, add the archive component and point it at `attesta-api`:

```bash
openssl genpkey -algorithm ed25519 -out decision-signing.pem
openssl pkey -in decision-signing.pem -pubout -out decision-signing.pub.pem
kubectl -n attesta-system create secret generic attesta-decision-signing-key --from-file=key.pem=decision-signing.pem
# kustomization.yaml: components: [deploy/controller/components/signing, deploy/controller/components/archive]
```

Pin a released image (`ghcr.io/gaurav-gs7/attesta-controller`) before running it anywhere that matters. [docs/controller-operator.md](docs/controller-operator.md) has every rule, flag, metric and condition.

## Verifying Records

```bash
kubectl get rolloutguards,rolloutdecisions -n payments -o json > records.json
attesta verify -f records.json --public-key decision-signing.pub.pem
# OK      payments/checkout-rollout: 3 decisions
# OK      payments/ledger-rollout: 3 decisions
# OK      payments/payments-api-rollout: 3 decisions
```

`attesta verify` checks each record's hash, its link to the record before it, the guard's head (so records deleted from the end show), and the signatures. Without Attesta's code, a record checks out when `jq -cjS '.spec | del(.hash, .signature)' | sha256sum` equals its `spec.hash`, and its `spec.previousHash` equals the hash of the record before it. The API does the same on every read and compares the cluster with the archive:

```bash
curl -s -H "Authorization: Bearer $TOKEN" https://attesta-api/api/v1/guards/payments/payments-api-rollout | jq -c '{chain, archive}'
# {"chain":{"verified":true,"signaturesChecked":true,"decisions":3},"archive":{"archived":3,"matches":true}}
```

## The API

`attesta-api` is the read side. With `ATTESTA_CLUSTER_READ=true` it reads RolloutGuards and RolloutDecisions (`deploy/api/cluster-read-rbac.yaml` grants `get` and `list` on those two, nothing else) and verifies each chain on read; `ATTESTA_DECISION_PUBLIC_KEYS_FILE` adds signature checks. With `ATTESTA_PROMETHEUS_URL` it reports SLOs through the same evaluator as the gate: `allowed`, `blocked`, or `unknown` when a signal cannot be measured, never a guess. Alertmanager webhooks open incidents whose timelines cite the controller's decisions by hash.

```bash
ATTESTA_CLUSTER_READ=true make run-api
attesta guards payments-api                     # the service's guards, chains verified
attesta guard payments/payments-api-rollout     # one guard and every decision it recorded
attesta archive                                 # archived chains, including deleted guards'
```

| Method and path | Purpose |
| --- | --- |
| `GET /api/v1/guards[?namespace=]`, `GET /api/v1/guards/{namespace}/{name}` | Guards, every decision, the chain's verification and the archive comparison |
| `POST /api/v1/decision-archive`, `GET /api/v1/decision-archive[/{guardUID}]` | The append-only archive the controller writes to |
| `GET /api/v1/services/{name}/status`, `.../slos`, `.../error-budget`, `.../guards` | Measured SLO state and the service's guards |
| `POST /api/v1/alerts/alertmanager`, `GET /api/v1/incidents/{id}/timeline`, `.../postmortem` | Incidents from Alertmanager, with recorded events only |
| `POST /api/v1/services`, `GET /api/v1/services[/{name}]` | Service catalog |
| `GET /livez`, `/readyz`, `/metrics` | Probes and metrics (no token) |

Callers authenticate with a hashed-token file, Kubernetes TokenReview, or both, as `viewer`, `operator` or `admin`; every request is rate-limited per address and per caller, carries a request ID, and has bounded bodies and timeouts. With `DATABASE_URL`, the catalog, incidents and the archive are stored in PostgreSQL. Configuration, limits and every endpoint are in [docs/api.md](docs/api.md).

The API also renders a service scaffold (`attesta service init`: Go or Python service, Kustomize overlays, CI, a RolloutGuard, PrometheusRule alerts, a dashboard and a runbook), scores catalog metadata, and runs onboarding workflows. See [docs/api.md](docs/api.md#generated-service-scaffold).

## Development

```bash
make test       # go test -race ./...
make lint       # golangci-lint and shellcheck
make e2e        # the controller end to end on a local kind cluster
make generate   # deepcopy and CRDs from api/v1
python3 scripts/demo/render.py   # redraw docs/assets from scripts/demo/capture.json
```

CI runs on every push: formatting, vet, race-enabled tests against PostgreSQL with a coverage floor, envtest against a real API server, golangci-lint, ShellCheck, govulncheck, generated-code drift, kubeconform, actionlint, both images (non-root, scanned with Trivy), and the kind end-to-end run. Every action is pinned to a commit SHA.

```text
api/v1/                  attesta.io/v1 types; deploy/crds is generated from them
cmd/attesta-controller/  the RolloutGuard controller
cmd/attesta-api/         the HTTP API
cmd/attesta/             the CLI, including attesta verify
internal/                controller, SLO evaluator, archive, cluster reader, catalog, scaffold, ...
deploy/                  CRDs, controller (with signing and archive components), API RBAC
scripts/                 kind end-to-end test, demo capture and render
docs/                    operator, architecture and security documentation
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for what a change needs, [CHANGELOG.md](CHANGELOG.md) for releases, and [SECURITY.md](SECURITY.md) to report a vulnerability.

## License

Apache License 2.0. See [LICENSE](LICENSE).
