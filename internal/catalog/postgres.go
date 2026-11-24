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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/page"
	"github.com/gaurav-gs7/attesta/internal/workflows"
)

type PostgresStore struct {
	db   *sql.DB
	pool *pgxpool.Pool
}

// Connection defaults, each overridable in DATABASE_URL: pool_max_conns,
// pool_max_conn_lifetime, pool_max_conn_idle_time and
// pool_health_check_period for the pool, and statement_timeout,
// lock_timeout, idle_in_transaction_session_timeout and application_name
// as session parameters.
const (
	defaultMaxConns         = 10
	defaultConnLifetime     = 30 * time.Minute
	defaultLifetimeJitter   = 5 * time.Minute
	defaultConnIdleTime     = 5 * time.Minute
	defaultHealthCheck      = 30 * time.Second
	defaultStatementTimeout = "10s"
)

var defaultSessionParams = map[string]string{
	// No single statement runs longer than this. Requests have a 25s
	// deadline; one slow statement must not use all of it.
	"statement_timeout": defaultStatementTimeout,
	// A statement waiting on a row or table lock gives up rather than
	// queueing behind a long transaction.
	"lock_timeout": "5s",
	// A transaction left open by a stuck caller is ended by the server and
	// its locks released.
	"idle_in_transaction_session_timeout": "30s",
	"application_name":                    "attesta-api",
}

func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	return newPostgresStore(ctx, databaseURL, nil)
}

// poolConfig parses databaseURL and fills in the defaults it does not set.
func poolConfig(databaseURL string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	set := func(key string) bool { return strings.Contains(databaseURL, key+"=") }
	if !set("pool_max_conns") {
		cfg.MaxConns = defaultMaxConns
	}
	if !set("pool_max_conn_lifetime") {
		cfg.MaxConnLifetime = defaultConnLifetime
	}
	if !set("pool_max_conn_lifetime_jitter") {
		cfg.MaxConnLifetimeJitter = defaultLifetimeJitter
	}
	if !set("pool_max_conn_idle_time") {
		cfg.MaxConnIdleTime = defaultConnIdleTime
	}
	if !set("pool_health_check_period") {
		cfg.HealthCheckPeriod = defaultHealthCheck
	}
	for key, value := range defaultSessionParams {
		if _, ok := cfg.ConnConfig.RuntimeParams[key]; !ok {
			cfg.ConnConfig.RuntimeParams[key] = value
		}
	}
	return cfg, nil
}

// newPostgresStore opens the store, optionally tracing every statement. Tests
// use the tracer to count round trips.
func newPostgresStore(ctx context.Context, databaseURL string, tracer pgx.QueryTracer) (*PostgresStore, error) {
	cfg, err := poolConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// database/sql on top of pgxpool: pgxpool owns the connections, their
	// limits and lifetimes, and database/sql keeps none idle of its own.
	db := stdlib.OpenDBFromPool(pool)
	store := &PostgresStore{db: db, pool: pool}
	if err := db.PingContext(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := migrate(ctx, db); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// Ping checks that PostgreSQL answers.
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the store. Closing the *sql.DB does not close the pool
// under it.
func (s *PostgresStore) Close() error {
	err := s.db.Close()
	if s.pool != nil {
		s.pool.Close()
	}
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
		textArray(service.Dependencies), service.Namespace, service.Environment, service.SLOTarget,
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
		var dependencies []string
		if err := rows.Scan(&service.ID, &service.Name, &service.Team, &service.Owner,
			&service.Tier, &service.Language, &service.RepoURL, &service.Repository,
			&service.Pager, &service.RunbookURL, &service.DashboardURL, arrayScanner(&dependencies),
			&service.Namespace, &service.Environment, &service.SLOTarget, &service.SLOLatencyP95Ms,
			&service.SLOErrorRate, &service.DeploymentStrategy, &service.RollbackOnFailure,
			&service.CreatedAt, &service.UpdatedAt); err != nil {
			return nil, err
		}
		service.Dependencies = nonEmpty(dependencies)
		services = append(services, service)
	}
	return services, rows.Err()
}

func (s *PostgresStore) GetServiceByName(ctx context.Context, name string) (models.Service, error) {
	var service models.Service
	var dependencies []string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, team, owner, tier, language, repo_url, repository, pager,
		       runbook_url, dashboard_url, dependencies, namespace, environment,
		       slo_target, slo_latency_p95_ms, slo_error_rate, deployment_strategy,
		       rollback_on_failure, created_at, updated_at
		FROM services
		WHERE name = $1
	`, name).Scan(&service.ID, &service.Name, &service.Team, &service.Owner,
		&service.Tier, &service.Language, &service.RepoURL, &service.Repository,
		&service.Pager, &service.RunbookURL, &service.DashboardURL, arrayScanner(&dependencies),
		&service.Namespace, &service.Environment, &service.SLOTarget, &service.SLOLatencyP95Ms,
		&service.SLOErrorRate, &service.DeploymentStrategy, &service.RollbackOnFailure,
		&service.CreatedAt, &service.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return models.Service{}, ErrNotFound
	}
	service.Dependencies = nonEmpty(dependencies)
	return service, err
}

// keysetArgs returns the cursor and Since bounds of req as nullable query
// parameters.
func keysetArgs(req page.Request) (after sql.NullTime, afterID string, since sql.NullTime) {
	if !req.After.IsZero() {
		after = sql.NullTime{Time: req.After.Time, Valid: true}
		afterID = req.After.ID
	}
	if !req.Since.IsZero() {
		since = sql.NullTime{Time: req.Since, Valid: true}
	}
	return after, afterID, since
}

// trimPage drops the extra row fetched to detect a next page and returns the
// next cursor.
func trimPage[T any](items []T, req page.Request, key func(T) (time.Time, string)) ([]T, string) {
	if len(items) <= req.Size() {
		return items, ""
	}
	items = items[:req.Size()]
	t, id := key(items[len(items)-1])
	return items, page.Cursor{Time: t, ID: id}.Encode()
}

// Workflow runs are written with one statement per save. The run row is
// upserted, and its steps and events are upserted from arrays in
// data-modifying CTEs that depend on the run row, so the whole save is atomic
// without an explicit transaction. Step and event IDs are derived from the
// run ID and their position, so a re-save updates steps in place and inserts
// only events that are not stored yet, instead of deleting and re-inserting
// everything.

const workflowRunColumns = `id, name, kind, state, service, idempotency_key, attempt, max_attempts,
			started_at, completed_at, error, heartbeat_at, error_code, actor`

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
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),$26,$27)
		ON CONFLICT (id) DO UPDATE SET
			state = EXCLUDED.state,
			attempt = EXCLUDED.attempt,
			max_attempts = EXCLUDED.max_attempts,
			completed_at = EXCLUDED.completed_at,
			error = EXCLUDED.error,
			error_code = EXCLUDED.error_code,
			heartbeat_at = now()
		-- A failed run stays failed: an owner whose run was abandoned must not
		-- revive it after another run took the idempotency key.
		WHERE workflow_runs.state <> 'failed' OR EXCLUDED.state = 'failed'
		RETURNING id
	),` + workflowChildrenCTEs

var claimWorkflowRunSQL = `
	WITH run AS (
		INSERT INTO workflow_runs (` + workflowRunColumns + `)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),$26,$27)
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
		run.ErrorCode, run.Actor,
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
			SET state = 'failed', error = $3, error_code = 'persistence', completed_at = now()
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

// ListWorkflowRuns returns one page of runs with their steps and events in
// three queries, whatever the page size.
func (s *PostgresStore) ListWorkflowRuns(ctx context.Context, req page.Request) ([]models.WorkflowRun, string, error) {
	after, afterID, since := keysetArgs(req)
	runs, err := s.loadWorkflowRuns(ctx, `
		SELECT id, name, kind, state, service, idempotency_key, attempt, max_attempts,
		       started_at, completed_at, error, error_code, actor
		FROM workflow_runs
		WHERE ($1::timestamptz IS NULL OR (started_at, id) < ($1, $2))
		  AND ($3::timestamptz IS NULL OR started_at >= $3)
		ORDER BY started_at DESC, id DESC
		LIMIT $4
	`, after, afterID, since, req.Size()+1)
	if err != nil {
		return nil, "", err
	}
	runs, next := trimPage(runs, req, func(r models.WorkflowRun) (time.Time, string) { return r.StartedAt, r.ID })
	return runs, next, nil
}

func (s *PostgresStore) loadWorkflowRun(ctx context.Context, id string) (models.WorkflowRun, error) {
	runs, err := s.loadWorkflowRuns(ctx, `
		SELECT id, name, kind, state, service, idempotency_key, attempt, max_attempts,
		       started_at, completed_at, error, error_code, actor
		FROM workflow_runs
		WHERE id = $1
	`, id)
	if err != nil {
		return models.WorkflowRun{}, err
	}
	if len(runs) == 0 {
		return models.WorkflowRun{}, sql.ErrNoRows
	}
	return runs[0], nil
}

// loadWorkflowRuns runs a query selecting run rows, then loads the steps and
// events of all of them with one query each.
func (s *PostgresStore) loadWorkflowRuns(ctx context.Context, query string, args ...any) ([]models.WorkflowRun, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var runs []models.WorkflowRun
	index := map[string]int{}
	for rows.Next() {
		var run models.WorkflowRun
		var completed sql.NullTime
		if err := rows.Scan(&run.ID, &run.Name, &run.Kind, &run.State, &run.Service, &run.IdempotencyKey,
			&run.Attempt, &run.MaxAttempts, &run.StartedAt, &completed, &run.Error, &run.ErrorCode, &run.Actor); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if completed.Valid {
			run.CompletedAt = &completed.Time
		}
		index[run.ID] = len(runs)
		runs = append(runs, run)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return runs, nil
	}
	ids := make([]string, len(runs))
	for i, run := range runs {
		ids[i] = run.ID
	}

	stepRows, err := s.db.QueryContext(ctx, `
		SELECT workflow_id, name, state, attempt, max_attempts, started_at, completed_at, error
		FROM workflow_steps
		WHERE workflow_id = ANY($1)
		ORDER BY workflow_id, position ASC
	`, ids)
	if err != nil {
		return nil, err
	}
	for stepRows.Next() {
		var workflowID string
		var step models.WorkflowStep
		var started, completed sql.NullTime
		if err := stepRows.Scan(&workflowID, &step.Name, &step.State, &step.Attempt, &step.MaxAttempts,
			&started, &completed, &step.Error); err != nil {
			_ = stepRows.Close()
			return nil, err
		}
		if started.Valid {
			step.StartedAt = timePtr(started.Time)
		}
		if completed.Valid {
			step.CompletedAt = timePtr(completed.Time)
		}
		i := index[workflowID]
		runs[i].Steps = append(runs[i].Steps, step)
	}
	if err := stepRows.Close(); err != nil {
		return nil, err
	}
	if err := stepRows.Err(); err != nil {
		return nil, err
	}

	eventRows, err := s.db.QueryContext(ctx, `
		SELECT workflow_id, timestamp, event_type, message
		FROM workflow_events
		WHERE workflow_id = ANY($1)
		ORDER BY workflow_id, seq ASC NULLS FIRST, timestamp ASC
	`, ids)
	if err != nil {
		return nil, err
	}
	for eventRows.Next() {
		var workflowID string
		var event models.WorkflowEvent
		if err := eventRows.Scan(&workflowID, &event.Timestamp, &event.Type, &event.Message); err != nil {
			_ = eventRows.Close()
			return nil, err
		}
		i := index[workflowID]
		runs[i].Events = append(runs[i].Events, event)
	}
	if err := eventRows.Close(); err != nil {
		return nil, err
	}
	return runs, eventRows.Err()
}

// incidentColumns selects an incident joined with its service, in the order
// scanIncident reads them.
const incidentColumns = `
	i.id, i.service_id, s.name, i.severity, i.title, i.status, i.alert_name, i.source,
	i.fingerprint, i.alert_starts_at, i.reported_error_rate_pct, i.reported_latency_p95_ms,
	i.created_at, i.resolved_at, i.resolution_note, i.opened_by, i.resolved_by, i.signals, i.version,
	s.id, s.name, s.team, s.owner, s.tier, s.language, s.repo_url, s.repository, s.pager,
	s.runbook_url, s.dashboard_url, s.dependencies, s.namespace, s.environment,
	s.slo_target, s.slo_latency_p95_ms, s.slo_error_rate, s.deployment_strategy,
	s.rollback_on_failure, s.created_at, s.updated_at`

func scanIncident(scanner interface{ Scan(...any) error }) (models.IncidentRecord, error) {
	var (
		r                   models.IncidentRecord
		in                  = &r.Incident
		svc                 = &r.Service
		alertStarts, solved sql.NullTime
		errRate, p95        sql.NullFloat64
		signals             []byte
		deps                []string
	)
	if err := scanner.Scan(&in.ID, &in.ServiceID, &in.Service, &in.Severity, &in.Title, &in.Status,
		&in.AlertName, &in.Source, &in.Fingerprint, &alertStarts, &errRate, &p95,
		&in.CreatedAt, &solved, &in.ResolutionNote, &in.OpenedBy, &in.ResolvedBy, &signals, &in.Version,
		&svc.ID, &svc.Name, &svc.Team, &svc.Owner, &svc.Tier, &svc.Language, &svc.RepoURL, &svc.Repository,
		&svc.Pager, &svc.RunbookURL, &svc.DashboardURL, arrayScanner(&deps), &svc.Namespace, &svc.Environment,
		&svc.SLOTarget, &svc.SLOLatencyP95Ms, &svc.SLOErrorRate, &svc.DeploymentStrategy,
		&svc.RollbackOnFailure, &svc.CreatedAt, &svc.UpdatedAt); err != nil {
		return models.IncidentRecord{}, err
	}
	svc.Dependencies = nonEmpty(deps)
	if alertStarts.Valid {
		in.AlertStartsAt = &alertStarts.Time
	}
	if solved.Valid {
		in.ResolvedAt = &solved.Time
	}
	if errRate.Valid || p95.Valid {
		in.AlertReported = &models.AlertSignals{}
		if errRate.Valid {
			in.AlertReported.ErrorRatePct = &errRate.Float64
		}
		if p95.Valid {
			in.AlertReported.LatencyP95Ms = &p95.Float64
		}
	}
	if err := json.Unmarshal(signals, &r.Signals); err != nil {
		return models.IncidentRecord{}, fmt.Errorf("incident %s signals: %w", in.ID, err)
	}
	return r, nil
}

// incidentArgs are $1..$17 of the incident insert; the update omits
// created_at ($12).
func incidentArgs(r models.IncidentRecord) ([]any, error) {
	in := r.Incident
	signals, err := json.Marshal(r.Signals)
	if err != nil {
		return nil, err
	}
	var errRate, p95 *float64
	if in.AlertReported != nil {
		errRate, p95 = in.AlertReported.ErrorRatePct, in.AlertReported.LatencyP95Ms
	}
	return []any{in.ID, in.ServiceID, in.Severity, in.Title, in.Status, in.AlertName, in.Source,
		in.Fingerprint, in.AlertStartsAt, errRate, p95, in.CreatedAt, in.ResolvedAt,
		in.ResolutionNote, in.OpenedBy, in.ResolvedBy, signals}, nil
}

// eventArrays returns events from index from on as parallel arrays for
// unnest, and the seq of the first one.
func eventArrays(events []models.IncidentEvent, from int) (at []time.Time, kinds, descriptions []string) {
	at, kinds, descriptions = []time.Time{}, []string{}, []string{}
	for _, e := range events[min(from, len(events)):] {
		at = append(at, e.Timestamp)
		kinds = append(kinds, e.EventType)
		descriptions = append(descriptions, e.Description)
	}
	return at, kinds, descriptions
}

// CreateIncident stores a new incident and its timeline at version 1, in
// one statement. A second active incident for the same service and alert
// fingerprint is ErrConflict.
func (s *PostgresStore) CreateIncident(ctx context.Context, record models.IncidentRecord) (models.IncidentRecord, error) {
	roundIncidentTimes(&record)
	args, err := incidentArgs(record)
	if err != nil {
		return models.IncidentRecord{}, err
	}
	at, kinds, descriptions := eventArrays(record.Events, 0)
	_, err = s.db.ExecContext(ctx, `
		WITH inserted AS (
			INSERT INTO incidents (
				id, service_id, severity, title, status, alert_name, source, fingerprint,
				alert_starts_at, reported_error_rate_pct, reported_latency_p95_ms,
				created_at, resolved_at, resolution_note, opened_by, resolved_by, signals
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
			RETURNING id
		)
		INSERT INTO incident_events (incident_id, seq, at, event_type, description)
		SELECT inserted.id, e.n, e.at, e.kind, e.description
		FROM inserted, unnest($18::timestamptz[], $19::text[], $20::text[]) WITH ORDINALITY AS e(at, kind, description, n)
	`, append(args, at, kinds, descriptions)...)
	if isUniqueViolation(err) {
		return models.IncidentRecord{}, fmt.Errorf("%w: %s already has an active incident for this alert", ErrConflict, record.Service.Name)
	}
	if err != nil {
		return models.IncidentRecord{}, err
	}
	record.Incident.Version = 1
	return record, nil
}

// UpdateIncident stores record over the incident if it is still at
// record.Incident.Version, appends the events beyond those already stored,
// and returns the record at its new version. Stored events are never
// rewritten. A changed or missing incident is ErrConflict or ErrNotFound.
func (s *PostgresStore) UpdateIncident(ctx context.Context, record models.IncidentRecord) (models.IncidentRecord, error) {
	roundIncidentTimes(&record)
	args, err := incidentArgs(record)
	if err != nil {
		return models.IncidentRecord{}, err
	}
	at, kinds, descriptions := eventArrays(record.Events, 0)
	var updated bool
	err = s.db.QueryRowContext(ctx, `
		WITH updated AS (
			UPDATE incidents SET
				severity = $3, title = $4, status = $5, alert_name = $6, source = $7, fingerprint = $8,
				alert_starts_at = $9, reported_error_rate_pct = $10, reported_latency_p95_ms = $11,
				resolved_at = $12, resolution_note = $13, opened_by = $14, resolved_by = $15, signals = $16,
				version = version + 1, updated_at = now()
			WHERE id = $1 AND service_id = $2 AND version = $17
			RETURNING id
		), appended AS (
			INSERT INTO incident_events (incident_id, seq, at, event_type, description)
			SELECT updated.id, e.n, e.at, e.kind, e.description
			FROM updated, unnest($18::timestamptz[], $19::text[], $20::text[]) WITH ORDINALITY AS e(at, kind, description, n)
			ON CONFLICT (incident_id, seq) DO NOTHING
		)
		SELECT EXISTS (SELECT 1 FROM updated)
	`, append(append(args[:11:11], args[12:]...), record.Incident.Version, at, kinds, descriptions)...).Scan(&updated)
	if isUniqueViolation(err) {
		return models.IncidentRecord{}, fmt.Errorf("%w: %s already has an active incident for this alert", ErrConflict, record.Service.Name)
	}
	if err != nil {
		return models.IncidentRecord{}, err
	}
	if !updated {
		var version int
		err := s.db.QueryRowContext(ctx, `SELECT version FROM incidents WHERE id = $1`, record.Incident.ID).Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			return models.IncidentRecord{}, ErrNotFound
		}
		if err != nil {
			return models.IncidentRecord{}, err
		}
		return models.IncidentRecord{}, fmt.Errorf("%w: incident %s is at version %d, not %d", ErrConflict, record.Incident.ID, version, record.Incident.Version)
	}
	record.Incident.Version++
	return record, nil
}

// roundIncidentTimes rounds the record's times to the microsecond PostgreSQL
// stores, so the record a write returns equals the one a read returns.
func roundIncidentTimes(r *models.IncidentRecord) {
	round := func(t *time.Time) {
		if t != nil {
			*t = t.Truncate(time.Microsecond)
		}
	}
	round(&r.Incident.CreatedAt)
	round(r.Incident.AlertStartsAt)
	round(r.Incident.ResolvedAt)
	for i := range r.Events {
		round(&r.Events[i].Timestamp)
	}
}

// PruneWorkflowRuns deletes finished runs completed before cutoff, with
// their steps and events, in batches.
func (s *PostgresStore) PruneWorkflowRuns(ctx context.Context, cutoff time.Time) (int, error) {
	return s.pruneBatches(ctx, `
		DELETE FROM workflow_runs WHERE id IN (
			SELECT id FROM workflow_runs
			WHERE state <> 'running' AND completed_at < $1
			ORDER BY completed_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, cutoff)
}

// PruneResolvedIncidents deletes incidents resolved before cutoff, with
// their events, in batches.
func (s *PostgresStore) PruneResolvedIncidents(ctx context.Context, cutoff time.Time) (int, error) {
	return s.pruneBatches(ctx, `
		DELETE FROM incidents WHERE id IN (
			SELECT id FROM incidents
			WHERE status = 'resolved' AND resolved_at < $1
			ORDER BY resolved_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, cutoff)
}

func (s *PostgresStore) pruneBatches(ctx context.Context, query string, cutoff time.Time) (int, error) {
	total := 0
	for {
		result, err := s.db.ExecContext(ctx, query, cutoff, pruneBatch)
		if err != nil {
			return total, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return total, err
		}
		total += int(n)
		if n < pruneBatch {
			return total, nil
		}
	}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *PostgresStore) GetIncidentRecord(ctx context.Context, id string) (models.IncidentRecord, error) {
	records, err := s.loadIncidents(ctx, `SELECT `+incidentColumns+`
		FROM incidents i JOIN services s ON s.id = i.service_id
		WHERE i.id = $1`, id)
	if err != nil {
		return models.IncidentRecord{}, err
	}
	if len(records) == 0 {
		return models.IncidentRecord{}, ErrNotFound
	}
	return records[0], nil
}

func (s *PostgresStore) ListIncidentRecords(ctx context.Context, req page.Request) ([]models.IncidentRecord, string, error) {
	after, afterID, since := keysetArgs(req)
	records, err := s.loadIncidents(ctx, `SELECT `+incidentColumns+`
		FROM incidents i JOIN services s ON s.id = i.service_id
		WHERE ($1::timestamptz IS NULL OR (i.created_at, i.id) < ($1, $2))
		  AND ($3::timestamptz IS NULL OR i.created_at >= $3)
		ORDER BY i.created_at DESC, i.id DESC
		LIMIT $4`, after, afterID, since, req.Size()+1)
	if err != nil {
		return nil, "", err
	}
	records, next := trimPage(records, req, func(r models.IncidentRecord) (time.Time, string) {
		return r.Incident.CreatedAt, r.Incident.ID
	})
	return records, next, nil
}

func (s *PostgresStore) FindActiveIncident(ctx context.Context, service, fingerprint string) (models.IncidentRecord, error) {
	records, err := s.loadIncidents(ctx, `SELECT `+incidentColumns+`
		FROM incidents i JOIN services s ON s.id = i.service_id
		WHERE s.name = $1 AND i.fingerprint = $2 AND i.status = 'active'
		ORDER BY i.created_at DESC
		LIMIT 1`, service, fingerprint)
	if err != nil {
		return models.IncidentRecord{}, err
	}
	if len(records) == 0 {
		return models.IncidentRecord{}, ErrNotFound
	}
	return records[0], nil
}

// loadIncidents runs query, then loads the timelines of the incidents it
// returned in one more query. The first result set is closed before the
// second query, so a read holds one connection.
func (s *PostgresStore) loadIncidents(ctx context.Context, query string, args ...any) ([]models.IncidentRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var records []models.IncidentRecord
	index := map[string]int{}
	for rows.Next() {
		r, err := scanIncident(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		index[r.Incident.ID] = len(records)
		records = append(records, r)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return records, nil
	}
	ids := make([]string, len(records))
	for i, r := range records {
		ids[i] = r.Incident.ID
	}
	events, err := s.db.QueryContext(ctx, `
		SELECT incident_id, at, event_type, description
		FROM incident_events
		WHERE incident_id = ANY($1)
		ORDER BY incident_id, seq`, ids)
	if err != nil {
		return nil, err
	}
	defer func() { _ = events.Close() }()
	for events.Next() {
		var id string
		var e models.IncidentEvent
		if err := events.Scan(&id, &e.Timestamp, &e.EventType, &e.Description); err != nil {
			return nil, err
		}
		r := &records[index[id]]
		r.Events = append(r.Events, e)
	}
	return records, events.Err()
}

func timePtr(t time.Time) *time.Time {
	return &t
}

// activeKeyIndex enforces that at most one run per idempotency key is not
// failed.
const activeKeyIndex = "uq_workflow_runs_active_idempotency_key"

// textArray is a []string for a NOT NULL text[] column: nil becomes '{}'.
func textArray(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

// arrayScanner scans a text[] column; database/sql cannot scan arrays
// itself. A pgtype.Map caches scan plans and is not safe for concurrent
// use, so each scan gets its own.
func arrayScanner(dst *[]string) any { return pgtype.NewMap().SQLScanner(dst) }

func nonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}
