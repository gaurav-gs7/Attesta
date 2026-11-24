# Service Onboarding Flow

1. Developer runs `attesta service init payments-api`.
2. CLI sends metadata to `POST /api/v1/services`.
3. API validates ownership, tier, pager, naming, SLO, and deployment strategy.
4. An onboarding workflow run starts in the API's in-process runner.
5. Workflow steps store the service, record the initial generated deployment, and render the scaffold in memory to check it renders. The API writes nothing to its own disk.
6. The response lists the scaffold's files and links `GET /api/v1/services/{name}/scaffold`. The CLI downloads that archive (source, Dockerfile, CI, Kustomize overlays, Attesta CR-style resources, observability, Argo CD, and runbooks) and writes it to `generated/services/<name>` atomically.
7. Developer commits generated assets to the service repository or GitOps repository.
8. GitHub Actions builds, tests, scans, and publishes the image.
9. Argo CD syncs the Kubernetes desired state.
10. Attesta's SLO gate, incident ingestion and, if a RolloutGuard is applied with rollback enabled, the controller then cover the running service.
