package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/page"
	"github.com/gaurav-gs7/attesta/api/internal/rollouts"
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
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// Ping checks that PostgreSQL answers.
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
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

func (s *PostgresStore) RecordDeployment(ctx context.Context, deployment models.Deployment) (models.Deployment, error) {
	if deployment.ID == "" {
		deployment.ID = NewID()
	}
	if deployment.StartedAt.IsZero() {
		deployment.StartedAt = nowUTC()
	}
	if _, err := insertDeployment(ctx, s.db, deployment); err != nil {
		return models.Deployment{}, err
	}
	return deployment, nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertDeployment(ctx context.Context, db execer, d models.Deployment) (sql.Result, error) {
	return db.ExecContext(ctx, `
		INSERT INTO deployments (
			id, service_id, rollout_id, version, image, environment, status, strategy, started_at, completed_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, d.ID, d.ServiceID, nullableString(d.RolloutID), d.Version, d.Image, d.Environment,
		d.Status, d.Strategy, d.StartedAt, nullableTime(d.CompletedAt))
}

func nullableString(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

// deploymentColumns and scanDeployment read a deployments row.
const deploymentColumns = `id, service_id, coalesce(rollout_id::text, ''), version, image, environment, status, strategy, started_at, completed_at`

// historyOnly excludes rows written by versions that used the deployments
// table as an event log for pauses and rollback requests. They are kept, but
// they are not deployments.
const historyOnly = `NOT (rollout_id IS NULL AND status IN ('rollout-paused', 'rollback-requested'))`

func scanDeployment(scanner interface{ Scan(...any) error }) (models.Deployment, error) {
	var d models.Deployment
	var completed sql.NullTime
	err := scanner.Scan(&d.ID, &d.ServiceID, &d.RolloutID, &d.Version, &d.Image, &d.Environment,
		&d.Status, &d.Strategy, &d.StartedAt, &completed)
	if completed.Valid {
		d.CompletedAt = &completed.Time
	}
	return d, err
}

func (s *PostgresStore) LatestDeployment(ctx context.Context, serviceID string) (models.Deployment, error) {
	deployment, err := scanDeployment(s.db.QueryRowContext(ctx, `
		SELECT `+deploymentColumns+`
		FROM deployments
		WHERE service_id = $1 AND `+historyOnly+`
		ORDER BY started_at DESC, id::text DESC
		LIMIT 1
	`, serviceID))
	if errors.Is(err, sql.ErrNoRows) {
		return models.Deployment{}, ErrNotFound
	}
	return deployment, err
}

func (s *PostgresStore) ListDeployments(ctx context.Context, serviceID string, req page.Request) ([]models.Deployment, string, error) {
	after, afterID, since := keysetArgs(req)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+deploymentColumns+`
		FROM deployments
		WHERE service_id = $1 AND `+historyOnly+`
		  AND ($2::timestamptz IS NULL OR (started_at, id::text) < ($2, $3))
		  AND ($4::timestamptz IS NULL OR started_at >= $4)
		ORDER BY started_at DESC, id::text DESC
		LIMIT $5
	`, serviceID, after, afterID, since, req.Size()+1)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()

	var deployments []models.Deployment
	for rows.Next() {
		deployment, err := scanDeployment(rows)
		if err != nil {
			return nil, "", err
		}
		deployments = append(deployments, deployment)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	deployments, next := trimPage(deployments, req, func(d models.Deployment) (time.Time, string) { return d.StartedAt, d.ID })
	return deployments, next, nil
}

// CreateRollout inserts the rollout and its deployment record in one
// transaction. The unique index on active rollouts per service turns a
// second concurrent start into ErrConflict.
func (s *PostgresStore) CreateRollout(ctx context.Context, rollout models.Rollout, deployment models.Deployment) (_ models.Rollout, err error) {
	if rollout.ID == "" {
		rollout.ID = NewID()
	}
	if deployment.ID == "" {
		deployment.ID = NewID()
	}
	rollout.DeploymentID, deployment.RolloutID = deployment.ID, rollout.ID
	events, err := json.Marshal(rollout.Events)
	if err != nil {
		return models.Rollout{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return models.Rollout{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = insertDeployment(ctx, tx, deployment); err != nil {
		return models.Rollout{}, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO rollouts (id, service_id, deployment_id, version, image, git_sha, state, created_at, updated_at, events)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, rollout.ID, rollout.ServiceID, rollout.DeploymentID, rollout.Version, rollout.Image, rollout.GitSHA,
		rollout.State, rollout.CreatedAt, rollout.UpdatedAt, events)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uq_rollouts_active_per_service" {
			err = fmt.Errorf("%w: service already has an active rollout", ErrConflict)
		}
		return models.Rollout{}, err
	}
	if err = tx.Commit(); err != nil {
		return models.Rollout{}, err
	}
	return rollout, nil
}

const rolloutColumns = `r.id, r.service_id, s.name, r.deployment_id, r.version, r.image, r.git_sha, r.state, r.created_at, r.updated_at, r.events`

func scanRollout(scanner interface{ Scan(...any) error }) (models.Rollout, error) {
	var r models.Rollout
	var events []byte
	if err := scanner.Scan(&r.ID, &r.ServiceID, &r.Service, &r.DeploymentID, &r.Version, &r.Image, &r.GitSHA,
		&r.State, &r.CreatedAt, &r.UpdatedAt, &events); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Rollout{}, ErrNotFound
		}
		return models.Rollout{}, err
	}
	if err := json.Unmarshal(events, &r.Events); err != nil {
		return models.Rollout{}, err
	}
	return r, nil
}

func (s *PostgresStore) GetRollout(ctx context.Context, serviceID, id string) (models.Rollout, error) {
	return scanRollout(s.db.QueryRowContext(ctx, `
		SELECT `+rolloutColumns+`
		FROM rollouts r JOIN services s ON s.id = r.service_id
		WHERE r.service_id = $1 AND r.id::text = $2
	`, serviceID, id))
}

func (s *PostgresStore) LatestRollout(ctx context.Context, serviceID string) (models.Rollout, error) {
	return scanRollout(s.db.QueryRowContext(ctx, `
		SELECT `+rolloutColumns+`
		FROM rollouts r JOIN services s ON s.id = r.service_id
		WHERE r.service_id = $1
		ORDER BY r.created_at DESC, r.id::text DESC
		LIMIT 1
	`, serviceID))
}

// UpdateRollout writes the transition only if the stored state is still
// fromState, and sets the deployment record's status in the same statement.
func (s *PostgresStore) UpdateRollout(ctx context.Context, rollout models.Rollout, fromState string) error {
	events, err := json.Marshal(rollout.Events)
	if err != nil {
		return err
	}
	var updated int
	err = s.db.QueryRowContext(ctx, `
		WITH r AS (
			UPDATE rollouts SET state = $3, updated_at = $4, events = $5
			WHERE id = $1 AND state = $2
			RETURNING deployment_id
		), d AS (
			UPDATE deployments SET status = $6,
				completed_at = CASE WHEN $7 THEN coalesce(completed_at, $4) ELSE completed_at END
			WHERE id = (SELECT deployment_id FROM r)
		)
		SELECT count(*) FROM r
	`, rollout.ID, fromState, rollout.State, rollout.UpdatedAt, events,
		rollouts.DeploymentStatus(rollout.State), !rollouts.Active(rollout.State)).Scan(&updated)
	if err != nil {
		return err
	}
	if updated == 0 {
		return fmt.Errorf("%w: rollout %s is no longer %s", ErrConflict, rollout.ID, fromState)
	}
	return nil
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

func (s *PostgresStore) ListIncidentRecords(ctx context.Context, req page.Request) ([]models.IncidentRecord, string, error) {
	after, afterID, since := keysetArgs(req)
	rows, err := s.db.QueryContext(ctx, `
		SELECT incident, service, signals, events, created_at
		FROM incident_records
		WHERE ($1::timestamptz IS NULL OR (created_at, id) < ($1, $2))
		  AND ($3::timestamptz IS NULL OR created_at >= $3)
		ORDER BY created_at DESC, id DESC
		LIMIT $4
	`, after, afterID, since, req.Size()+1)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()

	// The cursor uses the stored created_at, which PostgreSQL rounds to
	// microseconds, not the nanosecond time inside the incident JSON.
	type row struct {
		record  models.IncidentRecord
		created time.Time
	}
	var page []row
	for rows.Next() {
		var r row
		var incidentJSON, serviceJSON, signalsJSON, eventsJSON []byte
		if err := rows.Scan(&incidentJSON, &serviceJSON, &signalsJSON, &eventsJSON, &r.created); err != nil {
			return nil, "", err
		}
		record, err := decodeIncidentRecord(incidentJSON, serviceJSON, signalsJSON, eventsJSON)
		if err != nil {
			return nil, "", err
		}
		r.record = record
		page = append(page, r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	page, next := trimPage(page, req, func(r row) (time.Time, string) { return r.created, r.record.Incident.ID })
	records := make([]models.IncidentRecord, len(page))
	for i, r := range page {
		records[i] = r.record
	}
	return records, next, nil
}

func (s *PostgresStore) FindActiveIncident(ctx context.Context, service, fingerprint string) (models.IncidentRecord, error) {
	record, err := s.loadIncidentRecord(ctx, `
		SELECT incident, service, signals, events
		FROM incident_records
		WHERE service_name = $1
		  AND incident->>'fingerprint' = $2
		  AND incident->>'status' = 'active'
		ORDER BY created_at DESC
		LIMIT 1
	`, service, fingerprint)
	if errors.Is(err, sql.ErrNoRows) {
		return models.IncidentRecord{}, ErrNotFound
	}
	return record, err
}

type incidentScanner interface {
	Scan(dest ...any) error
}

func (s *PostgresStore) loadIncidentRecord(ctx context.Context, query string, args ...any) (models.IncidentRecord, error) {
	return scanIncidentRecord(s.db.QueryRowContext(ctx, query, args...))
}

func scanIncidentRecord(scanner incidentScanner) (models.IncidentRecord, error) {
	var incidentJSON, serviceJSON, signalsJSON, eventsJSON []byte
	if err := scanner.Scan(&incidentJSON, &serviceJSON, &signalsJSON, &eventsJSON); err != nil {
		return models.IncidentRecord{}, err
	}
	return decodeIncidentRecord(incidentJSON, serviceJSON, signalsJSON, eventsJSON)
}

func decodeIncidentRecord(incidentJSON, serviceJSON, signalsJSON, eventsJSON []byte) (models.IncidentRecord, error) {
	var record models.IncidentRecord
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
