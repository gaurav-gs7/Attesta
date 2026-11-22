package workflows

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

const (
	StatePending   = "pending"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
)

// ErrAbandoned is returned by a Store when a run it is asked to save or
// heartbeat has been marked failed because its heartbeat went stale. The run's
// owner must stop: another run may already hold the idempotency key.
var ErrAbandoned = errors.New("workflow run was abandoned")

type StepFunc func(context.Context) error

type Step struct {
	Name        string
	MaxAttempts int
	Run         StepFunc
}

type Spec struct {
	Name           string
	Kind           string
	Service        string
	IdempotencyKey string
	MaxAttempts    int
	Steps          []Step
}

// Store persists workflow runs. ClaimWorkflowRun and HeartbeatWorkflowRun
// carry the idempotency contract:
//
//   - At most one run per idempotency key is not failed. A succeeded run is
//     replayed; a running run is in flight; failed runs are history and do not
//     hold the key.
//   - ClaimWorkflowRun first marks failed any running run for the key whose
//     heartbeat is older than staleAfter, then inserts run if no run for the
//     key is running or succeeded. It returns the run that holds the key and
//     whether it is the one just inserted.
//   - SaveWorkflowRun and HeartbeatWorkflowRun refresh the heartbeat and
//     return ErrAbandoned for a run that was marked failed underneath its
//     owner.
type Store interface {
	ClaimWorkflowRun(ctx context.Context, run models.WorkflowRun, staleAfter time.Duration) (holder models.WorkflowRun, claimed bool, err error)
	HeartbeatWorkflowRun(ctx context.Context, id string) error
	SaveWorkflowRun(context.Context, models.WorkflowRun) error
	GetWorkflowRun(context.Context, string) (models.WorkflowRun, error)
	GetWorkflowRunByIdempotencyKey(context.Context, string) (models.WorkflowRun, error)
	ListWorkflowRuns(context.Context) ([]models.WorkflowRun, error)
}

// Outcome says what a keyed Run did.
type Outcome int

const (
	// Executed: this call ran the workflow. The run may have succeeded or
	// failed, or failed to persist.
	Executed Outcome = iota
	// Replayed: an earlier run with the same key had succeeded; it is
	// returned and nothing ran.
	Replayed
	// InProgress: another run with the same key is still running. It is
	// returned and nothing ran.
	InProgress
)

type Engine struct {
	store        Store
	staleAfter   time.Duration
	inFlightWait time.Duration
	pollInterval time.Duration
}

type Option func(*Engine)

// WithStaleAfter sets how long a running run may go without a heartbeat
// before another request may take its idempotency key over. Running runs
// heartbeat every third of this.
func WithStaleAfter(d time.Duration) Option { return func(e *Engine) { e.staleAfter = d } }

// WithInFlightWait sets how long a request waits for an identical in-flight
// run to finish before it returns InProgress.
func WithInFlightWait(d time.Duration) Option { return func(e *Engine) { e.inFlightWait = d } }

// NewEngine returns an engine backed by store, or by an in-process store when
// store is nil.
func NewEngine(store Store, opts ...Option) *Engine {
	if store == nil {
		store = NewMemoryStore()
	}
	e := &Engine{
		store:        store,
		staleAfter:   time.Minute,
		inFlightWait: 5 * time.Second,
		pollInterval: 100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Run runs spec and returns the run. For a spec with an idempotency key it
// may instead return an earlier succeeded or in-flight run; use RunIdempotent
// to tell which.
func (e *Engine) Run(ctx context.Context, spec Spec) models.WorkflowRun {
	run, _ := e.RunIdempotent(ctx, spec)
	return run
}

// RunIdempotent runs spec at most once per idempotency key among runs that
// have not failed. A failed run, or a running run whose owner stopped
// heartbeating, does not hold the key, so a retry runs the workflow again.
func (e *Engine) RunIdempotent(ctx context.Context, spec Spec) (models.WorkflowRun, Outcome) {
	if spec.MaxAttempts <= 0 {
		spec.MaxAttempts = 1
	}
	run := newRun(spec)
	if spec.IdempotencyKey == "" {
		if err := e.store.SaveWorkflowRun(ctx, run); err != nil {
			return persistFailed(run, "workflow start", err), Executed
		}
		return e.execute(ctx, spec, run), Executed
	}

	deadline := time.Now().Add(e.inFlightWait)
	for {
		holder, claimed, err := e.store.ClaimWorkflowRun(ctx, run, e.staleAfter)
		if err != nil {
			// Nothing was stored, so the key is free for the retry.
			return persistFailed(run, "workflow start", err), Executed
		}
		if claimed {
			return e.execute(ctx, spec, run), Executed
		}
		if holder.State == StateSucceeded {
			return holder, Replayed
		}
		if holder.State == StateRunning && !time.Now().Before(deadline) {
			return holder, InProgress
		}
		// Running elsewhere, or it failed between our insert and our read:
		// wait and claim again.
		wait := min(e.pollInterval, time.Until(deadline))
		if wait <= 0 {
			wait = e.pollInterval
		}
		select {
		case <-ctx.Done():
			return holder, InProgress
		case <-time.After(wait):
		}
	}
}

func newRun(spec Spec) models.WorkflowRun {
	now := time.Now().UTC()
	run := models.WorkflowRun{
		ID:             newID(),
		Name:           spec.Name,
		Kind:           spec.Kind,
		State:          StateRunning,
		Service:        spec.Service,
		IdempotencyKey: spec.IdempotencyKey,
		Attempt:        1,
		MaxAttempts:    spec.MaxAttempts,
		StartedAt:      now,
		Events: []models.WorkflowEvent{{
			Timestamp: now,
			Type:      "workflow-started",
			Message:   fmt.Sprintf("%s workflow started", spec.Name),
		}},
	}
	for _, step := range spec.Steps {
		maxAttempts := step.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		run.Steps = append(run.Steps, models.WorkflowStep{
			Name:        step.Name,
			State:       StatePending,
			MaxAttempts: maxAttempts,
		})
	}
	return run
}

// execute runs the steps of a run that has been stored as running.
func (e *Engine) execute(ctx context.Context, spec Spec, run models.WorkflowRun) models.WorkflowRun {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopHeartbeat := e.heartbeat(run.ID, cancel)
	defer stopHeartbeat()

	for i, step := range spec.Steps {
		maxAttempts := run.Steps[i].MaxAttempts
		var err error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			now := time.Now().UTC()
			run.Steps[i].State = StateRunning
			run.Steps[i].Attempt = attempt
			run.Steps[i].StartedAt = &now
			run.Events = append(run.Events, models.WorkflowEvent{Timestamp: now, Type: "step-started", Message: step.Name})
			if !e.persist(ctx, &run, "step start") {
				return run
			}

			if step.Run != nil {
				err = step.Run(ctx)
			}
			if cause := context.Cause(ctx); errors.Is(cause, ErrAbandoned) {
				return persistFailed(run, "heartbeat", cause)
			}
			done := time.Now().UTC()
			run.Steps[i].CompletedAt = &done
			if err == nil {
				run.Steps[i].State = StateSucceeded
				run.Events = append(run.Events, models.WorkflowEvent{Timestamp: done, Type: "step-succeeded", Message: step.Name})
				if !e.persist(ctx, &run, "step success") {
					return run
				}
				break
			}
			run.Steps[i].Error = err.Error()
			run.Events = append(run.Events, models.WorkflowEvent{Timestamp: done, Type: "step-failed", Message: fmt.Sprintf("%s: %v", step.Name, err)})
			if !e.persist(ctx, &run, "step failure") {
				return run
			}
		}
		if err != nil {
			done := time.Now().UTC()
			run.State = StateFailed
			run.Error = err.Error()
			run.CompletedAt = &done
			run.Events = append(run.Events, models.WorkflowEvent{Timestamp: done, Type: "workflow-failed", Message: err.Error()})
			e.persist(ctx, &run, "workflow failure")
			return run
		}
	}

	done := time.Now().UTC()
	run.State = StateSucceeded
	run.CompletedAt = &done
	run.Events = append(run.Events, models.WorkflowEvent{Timestamp: done, Type: "workflow-succeeded", Message: fmt.Sprintf("%s workflow completed", spec.Name)})
	e.persist(ctx, &run, "workflow success")
	return run
}

// heartbeat keeps the run's lease fresh while it executes. If the store says
// the run was abandoned, it cancels the run so its steps stop.
func (e *Engine) heartbeat(id string, cancel context.CancelCauseFunc) (stop func()) {
	interval := e.staleAfter / 3
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				ctx, cancelCall := context.WithTimeout(context.Background(), interval)
				err := e.store.HeartbeatWorkflowRun(ctx, id)
				cancelCall()
				if errors.Is(err, ErrAbandoned) {
					cancel(err)
					return
				}
			}
		}
	}()
	return func() { close(done); <-finished }
}

// persist saves the run. If that fails, the run is marked failed in the
// returned copy and one more save of the failed state is attempted, so a
// store that has recovered frees the idempotency key at once. If that also
// fails, the stored run stays running until its heartbeat goes stale, and
// the key is free again after staleAfter. A run that failed to persist is
// never kept anywhere as if it had been recorded.
func (e *Engine) persist(ctx context.Context, run *models.WorkflowRun, phase string) bool {
	err := e.store.SaveWorkflowRun(ctx, *run)
	if err == nil {
		return true
	}
	*run = persistFailed(*run, phase, err)
	if !errors.Is(err, ErrAbandoned) {
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = e.store.SaveWorkflowRun(saveCtx, *run)
	}
	return false
}

func persistFailed(run models.WorkflowRun, phase string, err error) models.WorkflowRun {
	now := time.Now().UTC()
	run.State = StateFailed
	run.Error = fmt.Sprintf("persist %s: %v", phase, err)
	run.CompletedAt = &now
	run.Events = append(run.Events, models.WorkflowEvent{
		Timestamp: now,
		Type:      "workflow-persistence-failed",
		Message:   run.Error,
	})
	return run
}

func (e *Engine) Get(id string) (models.WorkflowRun, bool) {
	run, err := e.store.GetWorkflowRun(context.Background(), id)
	return run, err == nil
}

// GetByIdempotencyKey returns the run that holds key (running or
// succeeded), or else the latest failed run for it.
func (e *Engine) GetByIdempotencyKey(key string) (models.WorkflowRun, bool) {
	run, err := e.store.GetWorkflowRunByIdempotencyKey(context.Background(), key)
	return run, err == nil
}

func (e *Engine) List(ctx context.Context) ([]models.WorkflowRun, error) {
	runs, err := e.store.ListWorkflowRuns(ctx)
	if runs == nil && err == nil {
		runs = []models.WorkflowRun{}
	}
	return runs, err
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("wf-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(b[:])
	return "wf-" + encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
