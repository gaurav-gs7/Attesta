# SRE Runbooks

Attesta runbooks are generated per service and include:

- Ownership and escalation
- Availability and latency SLO
- First checks for pods, events, rollout, and resource saturation
- Rollback commands: `kubectl rollout undo`, and the API call that records the rollback request
- Alert links and dashboard expectations

The goal is to make the first 10 minutes of incident response boring and repeatable.
