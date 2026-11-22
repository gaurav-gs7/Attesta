# Incident Flow

1. A Prometheus alerting rule fires. Generated rules carry a `service` label.
2. Alertmanager posts the alert to `POST /api/v1/alerts/alertmanager`. A JSON call to `POST /api/v1/incidents` works too.
3. Attesta opens an incident unless one is already active for the alert's fingerprint. It records:
   - the SLO evaluation at that moment, or `unknown` with the reason;
   - deployment records from the 2 hours before the alert;
   - values the sender reported, kept separate from Attesta's own measurements.
4. The timeline orders those records. If a rollout was recorded in the window, a correlation event names it as a rollback candidate and says it is not a confirmed cause.
5. On-call works from the generated runbook and the RolloutGuard's status. Attesta does not roll back on an incident. The controller rolls back only for guards with rollback enabled, and only when Kubernetes reports `ProgressDeadlineExceeded` or the new revision fails its metric checks.
6. When Alertmanager sends the resolved notification, Attesta resolves the incident at the alert's `endsAt`. `POST /api/v1/incidents/{id}/resolve` resolves it manually, with an optional note.
7. `GET /api/v1/incidents/{id}/postmortem` returns a draft. The cause and follow-ups are left as TODO for the review.
