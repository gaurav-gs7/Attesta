-- Audit D-5: an incident was four JSONB documents in incident_records,
-- one of them a full copy of the service, and an update overwrote the row
-- with no concurrency check. Incidents become columns, their timeline a
-- table of events, and every update must name the version it read.
--
-- signals stays a document: it is the SLO evaluation taken when the
-- incident opened, a snapshot that is never queried by field or updated.
-- The service is no longer copied; reads join services.
CREATE TABLE incidents (
    id TEXT PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    alert_name TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    fingerprint TEXT NOT NULL DEFAULT '',
    alert_starts_at TIMESTAMPTZ,
    reported_error_rate_pct DOUBLE PRECISION,
    reported_latency_p95_ms DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    resolution_note TEXT NOT NULL DEFAULT '',
    opened_by TEXT NOT NULL DEFAULT '',
    resolved_by TEXT NOT NULL DEFAULT '',
    signals JSONB NOT NULL DEFAULT '{}',
    -- Incremented by every update; an update names the version it read.
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE incident_events (
    incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (incident_id, seq)
);

INSERT INTO incidents (
    id, service_id, severity, title, status, alert_name, source, fingerprint,
    alert_starts_at, reported_error_rate_pct, reported_latency_p95_ms,
    created_at, resolved_at, resolution_note, opened_by, resolved_by,
    signals, version, updated_at
)
SELECT r.id, r.service_id,
       COALESCE(r.incident->>'severity', ''),
       COALESCE(r.incident->>'title', ''),
       COALESCE(r.incident->>'status', 'active'),
       COALESCE(r.incident->>'alertName', ''),
       COALESCE(r.incident->>'source', ''),
       COALESCE(r.incident->>'fingerprint', ''),
       (r.incident->>'alertStartsAt')::timestamptz,
       (r.incident->'alertReported'->>'errorRatePct')::double precision,
       (r.incident->'alertReported'->>'latencyP95Ms')::double precision,
       COALESCE((r.incident->>'createdAt')::timestamptz, r.created_at),
       (r.incident->>'resolvedAt')::timestamptz,
       COALESCE(r.incident->>'resolutionNote', ''),
       COALESCE(r.incident->>'openedBy', ''),
       COALESCE(r.incident->>'resolvedBy', ''),
       r.signals, 1, r.updated_at
FROM incident_records r;

INSERT INTO incident_events (incident_id, seq, at, event_type, description)
SELECT r.id, e.n, (e.value->>'timestamp')::timestamptz,
       COALESCE(e.value->>'eventType', ''), COALESCE(e.value->>'description', '')
FROM incident_records r
CROSS JOIN LATERAL jsonb_array_elements(r.events) WITH ORDINALITY AS e(value, n);

-- Earlier versions checked for an active incident and then inserted, so two
-- concurrent notifications could open two. Keep the newest and resolve the
-- rest, so the unique index below can be built.
UPDATE incidents i
SET status = 'resolved',
    resolved_at = COALESCE(i.resolved_at, now()),
    resolution_note = 'Superseded during migration: a newer active incident has the same alert fingerprint.'
WHERE i.status = 'active'
  AND i.fingerprint <> ''
  AND i.id <> (
    SELECT k.id FROM incidents k
    WHERE k.service_id = i.service_id AND k.fingerprint = i.fingerprint AND k.status = 'active'
    ORDER BY k.created_at DESC, k.id DESC
    LIMIT 1
  );

-- One active incident per service and alert fingerprint, enforced here
-- rather than by a check-then-insert in the API.
CREATE UNIQUE INDEX uq_incidents_active_fingerprint
    ON incidents (service_id, fingerprint)
    WHERE status = 'active' AND fingerprint <> '';
CREATE INDEX idx_incidents_created ON incidents (created_at DESC, id DESC);

DROP TABLE incident_records;
