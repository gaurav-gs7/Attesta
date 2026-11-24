-- An append-only copy of every RolloutDecision, outside the cluster it was
-- made in. A row is never changed or deleted: the trigger refuses both, so
-- removing history takes dropping the trigger, which is itself visible.
CREATE TABLE IF NOT EXISTS decision_archive (
    guard_uid TEXT NOT NULL,
    namespace TEXT NOT NULL,
    guard TEXT NOT NULL,
    name TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence >= 1),
    hash TEXT NOT NULL CHECK (hash ~ '^[0-9a-f]{64}$'),
    previous_hash TEXT NOT NULL DEFAULT '',
    key_id TEXT NOT NULL DEFAULT '',
    record JSONB NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guard_uid, sequence)
);

CREATE OR REPLACE FUNCTION decision_archive_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'decision_archive is append-only: % refused', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS decision_archive_append_only ON decision_archive;
CREATE TRIGGER decision_archive_append_only
    BEFORE UPDATE OR DELETE ON decision_archive
    FOR EACH ROW EXECUTE FUNCTION decision_archive_append_only();
