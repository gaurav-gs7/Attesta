-- The schema as it stood before versioned migrations. Every statement is
-- idempotent, so this applies cleanly both to an empty database and to one
-- created by the old start-up DDL, which it then records as version 1.
CREATE TABLE IF NOT EXISTS services (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    team TEXT NOT NULL,
    owner TEXT NOT NULL,
    language TEXT NOT NULL,
    repo_url TEXT NOT NULL DEFAULT '',
    namespace TEXT NOT NULL,
    environment TEXT NOT NULL,
    slo_target NUMERIC NOT NULL,
    deployment_strategy TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE services ADD COLUMN IF NOT EXISTS tier TEXT NOT NULL DEFAULT 'medium';
ALTER TABLE services ADD COLUMN IF NOT EXISTS repository TEXT NOT NULL DEFAULT '';
ALTER TABLE services ADD COLUMN IF NOT EXISTS pager TEXT NOT NULL DEFAULT '';
ALTER TABLE services ADD COLUMN IF NOT EXISTS runbook_url TEXT NOT NULL DEFAULT '';
ALTER TABLE services ADD COLUMN IF NOT EXISTS dashboard_url TEXT NOT NULL DEFAULT '';
ALTER TABLE services ADD COLUMN IF NOT EXISTS dependencies TEXT NOT NULL DEFAULT '';
ALTER TABLE services ADD COLUMN IF NOT EXISTS slo_latency_p95_ms INTEGER NOT NULL DEFAULT 300;
ALTER TABLE services ADD COLUMN IF NOT EXISTS slo_error_rate NUMERIC NOT NULL DEFAULT 1;
ALTER TABLE services ADD COLUMN IF NOT EXISTS rollback_on_failure BOOLEAN NOT NULL DEFAULT true;

CREATE TABLE IF NOT EXISTS deployments (
    id UUID PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    version TEXT NOT NULL,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    strategy TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

-- Deployment history: one row per rollout (and the initial scaffold). The
-- rollout carries the state machine; its row carries the current status.
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS rollout_id UUID;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS image TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS rollouts (
    id UUID PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    deployment_id UUID NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    version TEXT NOT NULL,
    image TEXT NOT NULL,
    git_sha TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    events JSONB NOT NULL DEFAULT '[]'
);
-- At most one active (started or paused) rollout per service.
CREATE UNIQUE INDEX IF NOT EXISTS uq_rollouts_active_per_service
    ON rollouts(service_id) WHERE state IN ('started', 'paused');
CREATE INDEX IF NOT EXISTS idx_rollouts_service_created
    ON rollouts(service_id, created_at DESC);

CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    root_cause TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS slo_snapshots (
    id UUID PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    current_value NUMERIC NOT NULL,
    error_budget_remaining NUMERIC NOT NULL,
    burn_rate NUMERIC NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rollout_events (
    id UUID PRIMARY KEY,
    deployment_id UUID NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    step TEXT NOT NULL,
    weight INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS incident_events (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    description TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS readiness_checks (
    id UUID PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    service TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL DEFAULT '',
    attempt INTEGER NOT NULL DEFAULT 1,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    error TEXT NOT NULL DEFAULT ''
);

-- Refreshed on every save and periodically while a run executes. A running
-- run with a stale heartbeat no longer holds its idempotency key.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS error_code TEXT NOT NULL DEFAULT '';
-- Who started the run; runs from before identities were recorded are blank.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS workflow_steps (
    id UUID PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT NOT NULL DEFAULT ''
);

ALTER TABLE workflow_steps ADD COLUMN IF NOT EXISTS position INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS workflow_events (
    id UUID PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON workflow_steps(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow ON workflow_events(workflow_id);

-- Position of the event in its run. Rows written before it existed keep NULL
-- and sort by timestamp.
ALTER TABLE workflow_events ADD COLUMN IF NOT EXISTS seq INTEGER;

CREATE INDEX IF NOT EXISTS idx_deployments_service_started
    ON deployments(service_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_service_started
    ON workflow_runs(service, started_at DESC);
-- Earlier versions allowed several active runs per idempotency key. Keep one
-- per key (a succeeded run first, then the newest) and retire the rest, so
-- the unique index below can be built.
UPDATE workflow_runs r
SET state = 'failed',
    error = CASE WHEN r.error = '' THEN 'superseded: duplicate run for the same idempotency key' ELSE r.error END,
    completed_at = COALESCE(r.completed_at, now())
WHERE r.idempotency_key <> ''
  AND r.state <> 'failed'
  AND r.id <> (
    SELECT keep.id FROM workflow_runs keep
    WHERE keep.idempotency_key = r.idempotency_key AND keep.state <> 'failed'
    ORDER BY (keep.state = 'succeeded') DESC, keep.started_at DESC, keep.id
    LIMIT 1
  );
DROP INDEX IF EXISTS idx_workflow_runs_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_runs_active_idempotency_key
    ON workflow_runs(idempotency_key)
    WHERE idempotency_key <> '' AND state <> 'failed';

CREATE TABLE IF NOT EXISTS incident_records (
    id TEXT PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    service_name TEXT NOT NULL,
    incident JSONB NOT NULL,
    service JSONB NOT NULL,
    signals JSONB NOT NULL,
    events JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_incident_records_service_created
    ON incident_records(service_name, created_at DESC);
