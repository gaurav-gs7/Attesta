-- Audit D-2: created by the baseline and never read or written. Incident
-- timelines live in incident_records; readiness is computed on request;
-- SLO state is read from Prometheus; rollout transitions are in
-- rollouts.events. incident_events references incidents, so it goes first.
DROP TABLE IF EXISTS incident_events;
DROP TABLE IF EXISTS incidents;
DROP TABLE IF EXISTS slo_snapshots;
DROP TABLE IF EXISTS rollout_events;
DROP TABLE IF EXISTS readiness_checks;
