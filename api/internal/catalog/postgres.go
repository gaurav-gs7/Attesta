package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	return newPostgresStore(ctx, databaseURL, nil)
}

// newPostgresStore opens the store, optionally tracing every statement. Tests
// use the tracer to count round trips.
func newPostgresStore(ctx context.Context, databaseURL string, tracer pgx.QueryTracer) (*PostgresStore, error) {
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.Tracer = tracer
	db := stdlib.OpenDB(*cfg)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &PostgresStore{db: db}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schemaSQL)
	return err
}

func (s *PostgresStore) CreateService(ctx context.Context, service models.Service) (models.Service, error) {
	if service.ID == "" {
		service.ID = NewID()
	}
	now := nowUTC()
	service.CreatedAt = now
	service.UpdatedAt = now

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO services (
			id, name, team, owner, tier, language, repo_url, repository, pager,
			runbook_url, dashboard_url, dependencies, namespace, environment,
			slo_target, slo_latency_p95_ms, slo_error_rate, deployment_strategy,
			rollback_on_failure, created_at, updated_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
	`, service.ID, service.Name, service.Team, service.Owner, service.Tier, service.Language,
		service.RepoURL, service.Repository, service.Pager, service.RunbookURL, service.DashboardURL,
		encodeList(service.Dependencies), service.Namespace, service.Environment, service.SLOTarget,
		service.SLOLatencyP95Ms, service.SLOErrorRate, service.DeploymentStrategy,
		service.RollbackOnFailure, service.CreatedAt, service.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return models.Service{}, ErrConflict
		}
		return models.Service{}, err
	}
	return service, nil
}

func (s *PostgresStore) ListServices(ctx context.Context) ([]models.Service, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, team, owner, tier, language, repo_url, repository, pager,
		       runbook_url, dashboard_url, dependencies, namespace, environment,
		       slo_target, slo_latency_p95_ms, slo_error_rate, deployment_strategy,
		       rollback_on_failure, created_at, updated_at
		FROM services
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var services []models.Service
	for rows.Next() {
		var service models.Service
		var dependencies string
		if err := rows.Scan(&service.ID, &service.Name, &service.Team, &service.Owner,
			&service.Tier, &service.Language, &service.RepoURL, &service.Repository,
			&service.Pager, &service.RunbookURL, &service.DashboardURL, &dependencies,
			&service.Namespace, &service.Environment, &service.SLOTarget, &service.SLOLatencyP95Ms,
			&service.SLOErrorRate, &service.DeploymentStrategy, &service.RollbackOnFailure,
			&service.CreatedAt, &service.UpdatedAt); err != nil {
			return nil, err
		}
		service.Dependencies = decodeList(dependencies)
		services = append(services, service)
	}
	return services, rows.Err()
}

func (s *PostgresStore) GetServiceByName(ctx context.Context, name string) (models.Service, error) {
	var service models.Service
	var dependencies string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, team, owner, tier, language, repo_url, repository, pager,
		       runbook_url, dashboard_url, dependencies, namespace, environment,
		       slo_target, slo_latency_p95_ms, slo_error_rate, deployment_strategy,
		       rollback_on_failure, created_at, updated_at
		FROM services
		WHERE name = $1
	`, name).Scan(&service.ID, &service.Name, &service.Team, &service.Owner,
		&service.Tier, &service.Language, &service.RepoURL, &service.Repository,
		&service.Pager, &service.RunbookURL, &service.DashboardURL, &dependencies,
		&service.Namespace, &service.Environment, &service.SLOTarget, &service.SLOLatencyP95Ms,
		&service.SLOErrorRate, &service.DeploymentStrategy, &service.RollbackOnFailure,
		&service.CreatedAt, &service.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return models.Service{}, ErrNotFound
	}
	service.Dependencies = decodeList(dependencies)
	return service, err
}

func (s *PostgresStore) RecordDeployment(ctx context.Context, deployment models.Deployment) (models.Deployment, error) {
	if deployment.ID == "" {
		deployment.ID = NewID()
	}
	if deployment.StartedAt.IsZero() {
		deployment.StartedAt = nowUTC()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO deployments (
			id, service_id, version, environment, status, strategy, started_at, completed_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, deployment.ID, deployment.ServiceID, deployment.Version, deployment.Environment,
		deployment.Status, deployment.Strategy, deployment.StartedAt, nullableTime(deployment.CompletedAt))
	if err != nil {
		return models.Deployment{}, err
	}
	return deployment, nil
}

func (s *PostgresStore) LatestDeployment(ctx context.Context, serviceID string) (models.Deployment, error) {
	var deployment models.Deployment
	var completed sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT id, service_id, version, environment, status, strategy, started_at, completed_at
		FROM deployments
		WHERE service_id = $1
		ORDER BY started_at DESC
		LIMIT 1
	`, serviceID).Scan(&deployment.ID, &deployment.ServiceID, &deployment.Version,
		&deployment.Environment, &deployment.Status, &deployment.Strategy,
		&deployment.StartedAt, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return models.Deployment{}, ErrNotFound
	}
	if completed.Valid {
		deployment.CompletedAt = &completed.Time
	}
	return deployment, err
}

func (s *PostgresStore) ListDeployments(ctx context.Context, serviceID string) ([]models.Deployment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, service_id, version, environment, status, strategy, started_at, completed_at
		FROM deployments
		WHERE service_id = $1
		ORDER BY started_at DESC
		LIMIT 50
	`, serviceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var deployments []models.Deployment
	for rows.Next() {
		var deployment models.Deployment
		var completed sql.NullTime
		if err := rows.Scan(&deployment.ID, &deployment.ServiceID, &deployment.Version,
			&deployment.Environment, &deployment.Status, &deployment.Strategy,
			&deployment.StartedAt, &completed); err != nil {
			return nil, err
		}
		if completed.Valid {
			deployment.CompletedAt = &completed.Time
		}
		deployments = append(deployments, deployment)
	}
	return deployments, rows.Err()
}

// Workflow runs are written with one statement per save. The run row is
// upserted, and its steps and events are upserted from arrays in
// data-modifying CTEs that depend on the run row, so the whole save is atomic
// without an explicit transaction. Step and event IDs are derived from the
// run ID and their position, so a re-save updates steps in place and inserts
// only events that are not stored yet, instead of deleting and re-inserting
// everything.

const workflowRunColumns = `id, name, kind, state, service, idempotency_key, attempt, max_attempts,
			started_at, completed_at, error, heartbeat_at`

// workflowChildrenCTEs upserts the steps and events of the run returned by
// the "run" CTE. Parameters $12-$25 carry them as parallel arrays.
const workflowChildrenCTEs = `
	steps AS (
		INSERT INTO workflow_steps (id, workflow_id, position, name, state, attempt, max_attempts, started_at, completed_at, error)
		SELECT s.id, run.id, s.position, s.name, s.state, s.attempt, s.max_attempts, s.started_at, s.completed_at, s.error
		FROM run, unnest($12::uuid[], $13::int[], $14::text[], $15::text[], $16::int[], $17::int[],
		                 $18::timestamptz[], $19::timestamptz[], $20::text[])
		     AS s(id, position, name, state, attempt, max_attempts, started_at, completed_at, error)
		ON CONFLICT (id) DO UPDATE SET
			state = EXCLUDED.state,
			attempt = EXCLUDED.attempt,
			max_attempts = EXCLUDED.max_attempts,
			started_at = EXCLUDED.started_at,
			completed_at = EXCLUDED.completed_at,
			error = EXCLUDED.error
	),
	events AS (
		INSERT INTO workflow_events (id, workflow_id, seq, event_type, message, timestamp)
		SELECT e.id, run.id, e.seq, e.event_type, e.message, e.ts
		FROM run, unnest($21::uuid[], $22::int[], $23::text[], $24::text[], $25::timestamptz[])
		     AS e(id, seq, event_type, message, ts)
		ON CONFLICT (id) DO NOTHING
	)
	SELECT count(*) FROM run`

var saveWorkflowRunSQL = `
	WITH run AS (
		INSERT INTO workflow_runs (` + workflowRunColumns + `)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
		ON CONFLICT (id) DO UPDATE SET
			state = EXCLUDED.state,
			attempt = EXCLUDED.attempt,
			max_attempts = EXCLUDED.max_attempts,
			completed_at = EXCLUDED.completed_at,
			error = EXCLUDED.error,
			heartbeat_at = now()
		-- A failed run stays failed: an owner whose run was abandoned must not
		-- revive it after another run took the idempotency key.
		WHERE workflow_runs.state <> 'failed' OR EXCLUDED.state = 'failed'
		RETURNING id
	),` + workflowChildrenCTEs

var claimWorkflowRunSQL = `
	WITH run AS (
		INSERT INTO workflow_runs (` + workflowRunColumns + `)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
		ON CONFLICT (idempotency_key) WHERE idempotency_key <> '' AND state <> 'failed' DO NOTHING
		RETURNING id
	),` + workflowChildrenCTEs

// workflowRunArgs returns the parameters of saveWorkflowRunSQL and
// claimWorkflowRunSQL.
func workflowRunArgs(run models.WorkflowRun) []any {
	n := len(run.Steps)
	stepIDs, positions, names, states := make([]string, n), make([]int32, n), make([]string, n), make([]string, n)
	attempts, maxAttempts, errs := make([]int32, n), make([]int32, n), make([]string, n)
	started, completed := make([]*time.Time, n), make([]*time.Time, n)
	for i, step := range run.Steps {
		stepIDs[i] = childID(run.ID, "step", i)
		positions[i] = int32(i)
		names[i], states[i], errs[i] = step.Name, step.State, step.Error
		attempts[i], maxAttempts[i] = int32(step.Attempt), int32(step.MaxAttempts)
		started[i], completed[i] = step.StartedAt, step.CompletedAt
	}
	m := len(run.Events)
	eventIDs, seqs, types, messages, stamps := make([]string, m), make([]int32, m), make([]string, m), make([]string, m), make([]time.Time, m)
	for i, event := range run.Events {
		eventIDs[i] = childID(run.ID, "event", i)
		seqs[i] = int32(i)
		types[i], messages[i], stamps[i] = event.Type, event.Message, event.Timestamp
	}
	return []any{
		run.ID, run.Name, run.Kind, run.State, run.Service, run.IdempotencyKey, run.Attempt,
		run.MaxAttempts, run.StartedAt, nullableTime(run.CompletedAt), run.Error,
		stepIDs, positions, names, states, attempts, maxAttempts, started, completed, errs,
		eventIDs, seqs, types, messages, stamps,
	}
}

// childID derives a stable UUID for a run's step or event at index i.
func childID(runID, kind string, i int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d", runID, kind, i)))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// SaveWorkflowRun writes the run, its steps and events, and refreshes its
// heartbeat, in one statement. It returns workflows.ErrAbandoned if the
// stored run was marked failed after its heartbeat went stale, or if another
// run holds its idempotency key, so a stalled owner cannot bring it back.
func (s *PostgresStore) SaveWorkflowRun(ctx context.Context, run models.WorkflowRun) error {
	var saved int
	err := s.db.QueryRowContext(ctx, saveWorkflowRunSQL, workflowRunArgs(run)...).Scan(&saved)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == activeKeyIndex {
			return fmt.Errorf("idempotency key %q is held by another run: %w", run.IdempotencyKey, workflows.ErrAbandoned)
		}
		return err
	}
	if saved == 0 {
		return workflows.ErrAbandoned
	}
	return nil
}

// ClaimWorkflowRun implements the engine's idempotency contract: it releases
// the key from a running run whose heartbeat is older than staleAfter, then
// inserts run unless another run for the key is running or succeeded, which it
// returns instead.
//
// The insert and the check are one statement, arbitrated by the unique
// partial index on active keys. A concurrent claim for the same key waits on
// the index entry until the first one commits, then inserts nothing.
func (s *PostgresStore) ClaimWorkflowRun(ctx context.Context, run models.WorkflowRun, staleAfter time.Duration) (models.WorkflowRun, bool, error) {
	if _, err := s.abandonStaleRuns(ctx, &run.IdempotencyKey, staleAfter); err != nil {
		return models.WorkflowRun{}, false, err
	}
	var inserted int
	if err := s.db.QueryRowContext(ctx, claimWorkflowRunSQL, workflowRunArgs(run)...).Scan(&inserted); err != nil {
		return models.WorkflowRun{}, false, err
	}
	if inserted == 1 {
		return run, true, nil
	}
	// The key is held. This new statement sees the committed winner.
	var holderID string
	err := s.db.QueryRowContext(ctx, `
		SELECT id FROM workflow_runs
		WHERE idempotency_key = $1 AND state <> 'failed'
	`, run.IdempotencyKey).Scan(&holderID)
	if errors.Is(err, sql.ErrNoRows) {
		// The holder failed after the insert gave way. Report it as failed so
		// the engine claims again.
		return models.WorkflowRun{IdempotencyKey: run.IdempotencyKey, State: workflows.StateFailed}, false, nil
	}
	if err != nil {
		return models.WorkflowRun{}, false, err
	}
	holder, err := s.loadWorkflowRun(ctx, holderID)
	if err != nil {
		return models.WorkflowRun{}, false, err
	}
	return holder, false, nil
}

// AbandonStaleWorkflowRuns marks failed every running run whose heartbeat is
// older than staleAfter.
func (s *PostgresStore) AbandonStaleWorkflowRuns(ctx context.Context, staleAfter time.Duration) (int, error) {
	return s.abandonStaleRuns(ctx, nil, staleAfter)
}

// abandonStaleRuns marks stale running runs failed, those for key or all of
// them when key is nil, and appends a workflow-abandoned event to each, in
// one statement.
func (s *PostgresStore) abandonStaleRuns(ctx context.Context, key *string, staleAfter time.Duration) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		WITH abandoned AS (
			UPDATE workflow_runs
			SET state = 'failed', error = $3, completed_at = now()
			WHERE ($1::text IS NULL OR idempotency_key = $1) AND state = 'running'
			  AND heartbeat_at < now() - make_interval(secs => $2)
			RETURNING id
		), events AS (
			INSERT INTO workflow_events (id, workflow_id, seq, event_type, message, timestamp)
			SELECT gen_random_uuid(), a.id,
			       (SELECT coalesce(max(seq), -1) + 1 FROM workflow_events e WHERE e.workflow_id = a.id),
			       'workflow-abandoned', $3, now()
			FROM abandoned a
		)
		SELECT count(*) FROM abandoned
	`, key, staleAfter.Seconds(), workflows.AbandonedMessage(staleAfter)).Scan(&n)
	return n, err
}

// HeartbeatWorkflowRun refreshes a running run's heartbeat. It returns
// workflows.ErrAbandoned if the run is no longer running.
func (s *PostgresStore) HeartbeatWorkflowRun(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE workflow_runs SET heartbeat_at = now() WHERE id = $1 AND state = 'running'`, id)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return workflows.ErrAbandoned
	}
	return nil
}

func (s *PostgresStore) GetWorkflowRun(ctx context.Context, id string) (models.WorkflowRun, error) {
	run, err := s.loadWorkflowRun(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return models.WorkflowRun{}, ErrNotFound
	}
	return run, err
}

func (s *PostgresStore) GetWorkflowRunByIdempotencyKey(ctx context.Context, key string) (models.WorkflowRun, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `
		SELECT id
		FROM workflow_runs
		WHERE idempotency_key = $1
		ORDER BY (state <> 'failed') DESC, started_at DESC
		LIMIT 1
	`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return models.WorkflowRun{}, ErrNotFound
	}
	if err != nil {
		return models.WorkflowRun{}, err
	}
	return s.loadWorkflowRun(ctx, id)
}

func (s *PostgresStore) ListWorkflowRuns(ctx context.Context) ([]models.WorkflowRun, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id
		FROM workflow_runs
		ORDER BY started_at DESC
		LIMIT 100
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var runs []models.WorkflowRun
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		run, err := s.loadWorkflowRun(ctx, id)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *PostgresStore) loadWorkflowRun(ctx context.Context, id string) (models.WorkflowRun, error) {
	var run models.WorkflowRun
	var completed sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, kind, state, service, idempotency_key, attempt, max_attempts,
		       started_at, completed_at, error
		FROM workflow_runs
		WHERE id = $1
	`, id).Scan(&run.ID, &run.Name, &run.Kind, &run.State, &run.Service, &run.IdempotencyKey,
		&run.Attempt, &run.MaxAttempts, &run.StartedAt, &completed, &run.Error)
	if err != nil {
		return models.WorkflowRun{}, err
	}
	if completed.Valid {
		run.CompletedAt = &completed.Time
	}

	steps, err := s.loadWorkflowSteps(ctx, id)
	if err != nil {
		return models.WorkflowRun{}, err
	}
	events, err := s.loadWorkflowEvents(ctx, id)
	if err != nil {
		return models.WorkflowRun{}, err
	}
	run.Steps = steps
	run.Events = events
	return run, nil
}

func (s *PostgresStore) loadWorkflowSteps(ctx context.Context, workflowID string) ([]models.WorkflowStep, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, state, attempt, max_attempts, started_at, completed_at, error
		FROM workflow_steps
		WHERE workflow_id = $1
		ORDER BY position ASC
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var steps []models.WorkflowStep
	for rows.Next() {
		var step models.WorkflowStep
		var started, completed sql.NullTime
		if err := rows.Scan(&step.Name, &step.State, &step.Attempt, &step.MaxAttempts,
			&started, &completed, &step.Error); err != nil {
			return nil, err
		}
		if started.Valid {
			step.StartedAt = timePtr(started.Time)
		}
		if completed.Valid {
			step.CompletedAt = timePtr(completed.Time)
		}
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func (s *PostgresStore) loadWorkflowEvents(ctx context.Context, workflowID string) ([]models.WorkflowEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT timestamp, event_type, message
		FROM workflow_events
		WHERE workflow_id = $1
		ORDER BY seq ASC NULLS FIRST, timestamp ASC
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var events []models.WorkflowEvent
	for rows.Next() {
		var event models.WorkflowEvent
		if err := rows.Scan(&event.Timestamp, &event.Type, &event.Message); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *PostgresStore) SaveIncidentRecord(ctx context.Context, record models.IncidentRecord) error {
	incidentJSON, err := json.Marshal(record.Incident)
	if err != nil {
		return err
	}
	serviceJSON, err := json.Marshal(record.Service)
	if err != nil {
		return err
	}
	signalsJSON, err := json.Marshal(record.Signals)
	if err != nil {
		return err
	}
	eventsJSON, err := json.Marshal(record.Events)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO incident_records (
			id, service_id, service_name, incident, service, signals, events, created_at, updated_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())
		ON CONFLICT (id) DO UPDATE SET
			service_id = EXCLUDED.service_id,
			service_name = EXCLUDED.service_name,
			incident = EXCLUDED.incident,
			service = EXCLUDED.service,
			signals = EXCLUDED.signals,
			events = EXCLUDED.events,
			updated_at = now()
	`, record.Incident.ID, record.Service.ID, record.Service.Name, incidentJSON, serviceJSON, signalsJSON,
		eventsJSON, record.Incident.CreatedAt)
	return err
}

func (s *PostgresStore) GetIncidentRecord(ctx context.Context, id string) (models.IncidentRecord, error) {
	record, err := s.loadIncidentRecord(ctx, `
		SELECT incident, service, signals, events
		FROM incident_records
		WHERE id = $1
	`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return models.IncidentRecord{}, ErrNotFound
	}
	return record, err
}

func (s *PostgresStore) ListIncidentRecords(ctx context.Context) ([]models.IncidentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT incident, service, signals, events
		FROM incident_records
		ORDER BY created_at DESC
		LIMIT 100
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var records []models.IncidentRecord
	for rows.Next() {
		record, err := scanIncidentRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

type incidentScanner interface {
	Scan(dest ...any) error
}

func (s *PostgresStore) loadIncidentRecord(ctx context.Context, query string, args ...any) (models.IncidentRecord, error) {
	return scanIncidentRecord(s.db.QueryRowContext(ctx, query, args...))
}

func scanIncidentRecord(scanner incidentScanner) (models.IncidentRecord, error) {
	var record models.IncidentRecord
	var incidentJSON, serviceJSON, signalsJSON, eventsJSON []byte
	if err := scanner.Scan(&incidentJSON, &serviceJSON, &signalsJSON, &eventsJSON); err != nil {
		return models.IncidentRecord{}, err
	}
	if err := json.Unmarshal(incidentJSON, &record.Incident); err != nil {
		return models.IncidentRecord{}, err
	}
	if err := json.Unmarshal(serviceJSON, &record.Service); err != nil {
		return models.IncidentRecord{}, err
	}
	if err := json.Unmarshal(signalsJSON, &record.Signals); err != nil {
		return models.IncidentRecord{}, err
	}
	if err := json.Unmarshal(eventsJSON, &record.Events); err != nil {
		return models.IncidentRecord{}, err
	}
	return record, nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}

// activeKeyIndex enforces that at most one run per idempotency key is not
// failed.
const activeKeyIndex = "uq_workflow_runs_active_idempotency_key"

// schemaSQL runs in one implicit transaction. The advisory lock keeps API
// replicas that start together from migrating at the same time.
const schemaSQL = `
SELECT pg_advisory_xact_lock(7314162024);
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
`

func encodeList(values []string) string {
	return strings.Join(values, ",")
}

func decodeList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
