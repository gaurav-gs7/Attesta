# Local CI/CD Demo

`scripts/local-ci-cd.sh` (`make demo-full`) runs the generated service through a laptop-local delivery flow on kind. Several steps are simulated; the script's log prefixes those with `SIMULATED` or says what it does instead.

## Flow

1. Preflight checks for Go, Docker, kind, kubectl, curl and perl.
2. Start the Attesta API if it is not running.
3. Onboard `payments-api` with `attesta service init`.
4. Validate the generated scaffold with `scripts/validate-service-scaffold.sh`.
5. Run the generated service's tests.
6. Render the Kustomize manifests.
7. Build a stable and a candidate image tag.
8. Scan the candidate image with Trivy if it is installed; otherwise record that the scan was skipped.
9. Create or reuse the `attesta-local` kind cluster and load both images.
10. Deploy the stable revision.
11. Deploy the candidate revision. This replaces every pod; there is no traffic splitting.
12. Check `/readyz` and `/metrics` through a port-forward.
13. **Simulated:** post fixed bad metrics (p99 450ms) to the health gate, which records a rollback request.
14. Run `kubectl rollout undo` from the script, standing in for the tooling that would act on that request, and check the stable image is back.
15. Show the readiness score and SLO status. Without Prometheus the SLO state is `unknown`.
16. **Simulated:** open an incident from a hand-written alert, then print its timeline and postmortem draft.
17. List the workflow runs behind these calls.

The RolloutGuard controller is not part of this demo.

## Evidence

Each run writes to `artifacts/local-ci-cd/<run-id>/`:

- `pipeline.log`
- `summary.json`
- `rendered-stable.yaml`
- `rendered-canary.yaml` (the candidate revision)
- `rollback.json`
- `incident.json`
- `trivy-skipped.txt` when Trivy is not installed
