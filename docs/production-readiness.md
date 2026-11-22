# Production Readiness

Attesta's readiness score checks that a service has registered the metadata needed to operate it: ownership, paging, SLOs, rollout policy and links. It scores registered fields only; it does not inspect the cluster or the repository. The API does not fill these fields in, so a field you did not register fails its check.

## Attesta Service Checks

The API and CLI validate:

- service name and namespace are Kubernetes DNS labels
- owner and team are set
- tier is one of `critical`, `high`, `medium`, or `low`
- pager metadata exists
- repository is linked
- runbook and dashboard are linked
- availability SLO is configured
- p95 latency objective is configured
- error-rate threshold is configured
- rollback-on-failure is enabled for progressive delivery

## Generated Kubernetes Guardrails

Generated services include:

- readiness probe
- liveness probe
- CPU and memory requests
- CPU and memory limits
- non-root runtime
- read-only root filesystem
- privilege escalation disabled
- Linux capabilities dropped
- service account token automount disabled
- HPA
- PDB
- Prometheus scrape annotations

## Demo Evidence

`make demo-full` exercises these controls by generating a service, validating the scaffold, rendering Kustomize, building images, deploying to kind and checking readiness and metrics. Its canary signal, rollback and incident steps are simulated; see [local-ci-cd.md](local-ci-cd.md).
