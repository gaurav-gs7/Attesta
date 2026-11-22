package workflows

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

// ErrNotFound is returned by MemoryStore for unknown runs.
var ErrNotFound = errors.New("workflow run not found")

// MemoryStore is an in-process Store with the same idempotency contract as
// the PostgreSQL store. It does not survive a restart.
type MemoryStore struct {
	mu        sync.Mutex
	runs      map[string]models.WorkflowRun
	heartbeat map[string]time.Time
	now       func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		runs:      make(map[string]models.WorkflowRun),
		heartbeat: make(map[string]time.Time),
		now:       time.Now,
	}
}

func (s *MemoryStore) ClaimWorkflowRun(_ context.Context, run models.WorkflowRun, staleAfter time.Duration) (models.WorkflowRun, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for id, existing := range s.runs {
		if existing.IdempotencyKey != run.IdempotencyKey || existing.State != StateRunning {
			continue
		}
		if now.Sub(s.heartbeat[id]) > staleAfter {
			s.runs[id] = abandon(existing, now, staleAfter)
		}
	}
	if holder, ok := s.holderLocked(run.IdempotencyKey); ok {
		return clone(holder), false, nil
	}
	s.runs[run.ID] = clone(run)
	s.heartbeat[run.ID] = now
	return run, true, nil
}

func (s *MemoryStore) AbandonStaleWorkflowRuns(_ context.Context, staleAfter time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	n := 0
	for id, run := range s.runs {
		if run.State == StateRunning && now.Sub(s.heartbeat[id]) > staleAfter {
			s.runs[id] = abandon(run, now, staleAfter)
			n++
		}
	}
	return n, nil
}

func (s *MemoryStore) HeartbeatWorkflowRun(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok || run.State != StateRunning {
		return ErrAbandoned
	}
	s.heartbeat[id] = s.now()
	return nil
}

func (s *MemoryStore) SaveWorkflowRun(_ context.Context, run models.WorkflowRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.runs[run.ID]; ok && existing.State == StateFailed && run.State != StateFailed {
		return ErrAbandoned
	}
	if run.IdempotencyKey != "" && run.State != StateFailed {
		if holder, ok := s.holderLocked(run.IdempotencyKey); ok && holder.ID != run.ID {
			return fmt.Errorf("idempotency key %q is held by run %s: %w", run.IdempotencyKey, holder.ID, ErrAbandoned)
		}
	}
	s.runs[run.ID] = clone(run)
	s.heartbeat[run.ID] = s.now()
	return nil
}

func (s *MemoryStore) GetWorkflowRun(_ context.Context, id string) (models.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return models.WorkflowRun{}, ErrNotFound
	}
	return clone(run), nil
}

func (s *MemoryStore) GetWorkflowRunByIdempotencyKey(_ context.Context, key string) (models.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if holder, ok := s.holderLocked(key); ok {
		return clone(holder), nil
	}
	var latest *models.WorkflowRun
	for _, run := range s.runs {
		if run.IdempotencyKey == key && (latest == nil || run.StartedAt.After(latest.StartedAt)) {
			r := run
			latest = &r
		}
	}
	if latest == nil {
		return models.WorkflowRun{}, ErrNotFound
	}
	return clone(*latest), nil
}

func (s *MemoryStore) ListWorkflowRuns(_ context.Context) ([]models.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runs := make([]models.WorkflowRun, 0, len(s.runs))
	for _, run := range s.runs {
		runs = append(runs, clone(run))
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.After(runs[j].StartedAt) })
	return runs, nil
}

// holderLocked returns the run that holds key: the one run for it that is
// running or succeeded.
func (s *MemoryStore) holderLocked(key string) (models.WorkflowRun, bool) {
	if key == "" {
		return models.WorkflowRun{}, false
	}
	for _, run := range s.runs {
		if run.IdempotencyKey == key && run.State != StateFailed {
			return run, true
		}
	}
	return models.WorkflowRun{}, false
}

func abandon(run models.WorkflowRun, now time.Time, staleAfter time.Duration) models.WorkflowRun {
	t := now.UTC()
	run.State = StateFailed
	run.Error = AbandonedMessage(staleAfter)
	run.CompletedAt = &t
	run.Events = append(run.Events, models.WorkflowEvent{Timestamp: t, Type: "workflow-abandoned", Message: run.Error})
	return run
}

// AbandonedMessage is the error recorded on a run taken over after its
// heartbeat went stale.
func AbandonedMessage(staleAfter time.Duration) string {
	return fmt.Sprintf("abandoned: no heartbeat for %s; its idempotency key was released for a retry", staleAfter)
}

// clone copies a run's slices so the store never shares memory with a run
// its owner is still mutating.
func clone(run models.WorkflowRun) models.WorkflowRun {
	run.Steps = append([]models.WorkflowStep(nil), run.Steps...)
	run.Events = append([]models.WorkflowEvent(nil), run.Events...)
	return run
}
