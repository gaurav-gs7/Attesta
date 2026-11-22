# Failure-Mode Demo

`scripts/failure-mode-demo.sh` proves Attesta handles production failure cases intentionally, not just happy paths.

Run it with:

```bash
make demo-failures
```

Evidence is written to:

```text
artifacts/failure-modes/<run-id>/
```

## Scenarios

- Bad canary signal is recorded: a health-gate request with high p99 latency (caller-supplied) returns a `rollback` decision and moves the rollout in flight, if any, to `rollback-requested`. Nothing in the cluster changes.
- Low traffic waits for more samples: a high-latency request with too few samples returns `continue` instead of rolling back prematurely.
- Prometheus unavailable fails closed: when `ATTESTA_PROMETHEUS_URL` is configured but a query fails, the SLO state is `unknown`, no values are reported, and `POST /rollouts` returns 409. The same applies when Prometheus answers with no data or `NaN`.
- PostgreSQL unavailable fails predictably: API startup with an unreachable `DATABASE_URL` exits and records the catalog connection error.
- Duplicate onboarding is idempotent: submitting the same service spec twice returns `idempotent: true` on the second request.
- Invalid service metadata is rejected: unsupported language, invalid Kubernetes name, and invalid environment return policy findings.
- Rollback evidence is recorded: rollback response and deployment history are saved as JSON artifacts.

## Why This Matters

These scenarios demonstrate production engineering judgment:

- fail closed when telemetry is unavailable
- avoid rollback on low-confidence signals
- make duplicate operations safe
- preserve workflow idempotency across API restarts when backed by durable storage
- reject unsafe metadata before it reaches Kubernetes
- record rollback requests so they can be audited
