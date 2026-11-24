-- Audit D-5: nothing was ever pruned. The retention job deletes finished
-- workflow runs and resolved incidents older than their retention; these
-- indexes let it find them without scanning the tables.
--
-- Runs failed by earlier versions of the reaper may lack completed_at.
UPDATE workflow_runs SET completed_at = heartbeat_at
WHERE state <> 'running' AND completed_at IS NULL;

CREATE INDEX idx_workflow_runs_finished ON workflow_runs (completed_at) WHERE state <> 'running';
CREATE INDEX idx_incidents_resolved ON incidents (resolved_at) WHERE status = 'resolved';
