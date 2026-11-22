package workflows

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

func okStep(name string) Step {
	return Step{Name: name, Run: func(context.Context) error { return nil }}
}

func TestEngineRunsStepsInOrder(t *testing.T) {
	engine := NewEngine(nil)
	var order []string
	run := engine.Run(context.Background(), Spec{
		Name:           "test-workflow",
		Kind:           "test",
		IdempotencyKey: "test-key",
		Steps: []Step{
			{Name: "first", Run: func(context.Context) error { order = append(order, "first"); return nil }},
			{Name: "second", Run: func(context.Context) error { order = append(order, "second"); return nil }},
		},
	})
	if run.State != StateSucceeded {
		t.Fatalf("expected succeeded workflow, got %+v", run)
	}
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("steps ran out of order: %v", order)
	}
	stored, ok := engine.GetByIdempotencyKey("test-key")
	if !ok || stored.ID != run.ID {
		t.Fatalf("expected idempotent run lookup")
	}
}

func TestEngineRetriesAndFails(t *testing.T) {
	engine := NewEngine(nil)
	attempts := 0
	run := engine.Run(context.Background(), Spec{
		Name: "failure-workflow",
		Kind: "test",
		Steps: []Step{{
			Name:        "retryable",
			MaxAttempts: 2,
			Run: func(context.Context) error {
				attempts++
				return errors.New("nope")
			},
		}},
	})
	if run.State != StateFailed {
		t.Fatalf("expected failed workflow, got %+v", run)
	}
	if attempts != 2 || run.Steps[0].Attempt != 2 {
		t.Fatalf("expected 2 attempts, got %d (step %+v)", attempts, run.Steps[0])
	}
}

func TestEngineReplaysSucceededRunAcrossEngines(t *testing.T) {
	store := NewMemoryStore()
	spec := Spec{Name: "durable-idempotency", Kind: "test", IdempotencyKey: "same-operation", Steps: []Step{okStep("once")}}
	first, outcome := NewEngine(store).RunIdempotent(context.Background(), spec)
	if outcome != Executed || first.State != StateSucceeded {
		t.Fatalf("first run: outcome %v state %s", outcome, first.State)
	}
	second, outcome := NewEngine(store).RunIdempotent(context.Background(), spec)
	if outcome != Replayed || second.ID != first.ID {
		t.Fatalf("restarted engine: outcome %v id %s, want Replayed %s", outcome, second.ID, first.ID)
	}
}

func TestEngineFailsClosedWhenInitialPersistenceFails(t *testing.T) {
	store := &flakyStore{MemoryStore: NewMemoryStore()}
	store.failSaves.Store(1)
	stepRan := false
	run := NewEngine(store).Run(context.Background(), Spec{
		Name: "persistence-failure", Kind: "test",
		Steps: []Step{{Name: "must-not-run", Run: func(context.Context) error { stepRan = true; return nil }}},
	})
	if stepRan {
		t.Fatal("step ran even though the workflow start could not be persisted")
	}
	if run.State != StateFailed || !strings.Contains(run.Error, "persist workflow start: database unavailable") {
		t.Fatalf("expected persistence failure, got state %s error %q", run.State, run.Error)
	}
	if run.Events[len(run.Events)-1].Type != "workflow-persistence-failed" {
		t.Fatalf("expected persistence failure event, got %+v", run.Events)
	}
}

// Audit W-1: a failed run must not hold the key. The audit saw every retry of
// a key whose first run failed return the same failure.
func TestFailedRunIsReDrivenNotReplayed(t *testing.T) {
	engine := NewEngine(nil)
	fail := true
	spec := Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{{
		Name: "flaky",
		Run: func(context.Context) error {
			if fail {
				return errors.New("downstream unavailable")
			}
			return nil
		},
	}}}
	first, _ := engine.RunIdempotent(context.Background(), spec)
	if first.State != StateFailed {
		t.Fatalf("first run state = %s, want failed", first.State)
	}
	fail = false
	second, outcome := engine.RunIdempotent(context.Background(), spec)
	if outcome != Executed || second.State != StateSucceeded || second.ID == first.ID {
		t.Fatalf("retry after a failed run: outcome %v state %s id %s (first %s); want a new succeeded run", outcome, second.State, second.ID, first.ID)
	}
	third, outcome := engine.RunIdempotent(context.Background(), spec)
	if outcome != Replayed || third.ID != second.ID {
		t.Fatalf("after success: outcome %v id %s, want Replayed %s", outcome, third.ID, second.ID)
	}
}

// Audit W-1, the reproduced case: one failed save gave 500 on the request and
// on every retry. The key must be free once the store is healthy again.
func TestRunThatFailedToPersistIsNotReplayed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fail  func(*flakyStore)
		phase string
	}{
		{"claim fails", func(s *flakyStore) { s.failClaims.Store(1) }, "persist workflow start"},
		{"save mid-run fails", func(s *flakyStore) { s.failSaves.Store(1) }, "persist step start"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &flakyStore{MemoryStore: NewMemoryStore()}
			tc.fail(store)
			engine := NewEngine(store)
			spec := Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{okStep("only")}}

			first, _ := engine.RunIdempotent(context.Background(), spec)
			if first.State != StateFailed || !strings.Contains(first.Error, tc.phase) {
				t.Fatalf("first run: state %s error %q, want a %q failure", first.State, first.Error, tc.phase)
			}
			second, outcome := engine.RunIdempotent(context.Background(), spec)
			if outcome != Executed || second.State != StateSucceeded {
				t.Fatalf("retry with a healthy store: outcome %v state %s error %q", outcome, second.State, second.Error)
			}
		})
	}
}

// Audit W-1: after a crash the run stays "running" in the store. Once its
// heartbeat is stale the key is released and the retry runs.
func TestStaleRunningRunIsTakenOver(t *testing.T) {
	store := NewMemoryStore()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	store.now = clock.Now
	crashed := newRun(Spec{Name: "onboard", IdempotencyKey: "k", MaxAttempts: 1})
	if _, claimed, err := store.ClaimWorkflowRun(context.Background(), crashed, time.Minute); err != nil || !claimed {
		t.Fatalf("seed claim: claimed=%v err=%v", claimed, err)
	}
	engine := NewEngine(store, WithStaleAfter(time.Minute), WithInFlightWait(0))
	spec := Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{okStep("only")}}

	clock.Advance(30 * time.Second)
	holder, outcome := engine.RunIdempotent(context.Background(), spec)
	if outcome != InProgress || holder.ID != crashed.ID {
		t.Fatalf("before the lease expires: outcome %v holder %s, want InProgress %s", outcome, holder.ID, crashed.ID)
	}

	clock.Advance(2 * time.Minute)
	run, outcome := engine.RunIdempotent(context.Background(), spec)
	if outcome != Executed || run.State != StateSucceeded {
		t.Fatalf("after the lease expired: outcome %v state %s", outcome, run.State)
	}
	old, _ := store.GetWorkflowRun(context.Background(), crashed.ID)
	if old.State != StateFailed || !strings.Contains(old.Error, "abandoned") {
		t.Fatalf("crashed run: state %s error %q, want failed/abandoned", old.State, old.Error)
	}
}

// An owner that stalled past its lease cannot write its run back to running
// or succeeded once another run holds the key.
func TestAbandonedOwnerCannotReviveItsRun(t *testing.T) {
	store := NewMemoryStore()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	store.now = clock.Now
	stalled := newRun(Spec{Name: "onboard", IdempotencyKey: "k", MaxAttempts: 1})
	if _, _, err := store.ClaimWorkflowRun(context.Background(), stalled, time.Minute); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	if _, outcome := NewEngine(store, WithStaleAfter(time.Minute)).RunIdempotent(context.Background(),
		Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{okStep("only")}}); outcome != Executed {
		t.Fatalf("takeover outcome = %v", outcome)
	}

	stalled.State = StateSucceeded
	if err := store.SaveWorkflowRun(context.Background(), stalled); !errors.Is(err, ErrAbandoned) {
		t.Fatalf("abandoned owner saved its run: err = %v", err)
	}
	if err := store.HeartbeatWorkflowRun(context.Background(), stalled.ID); !errors.Is(err, ErrAbandoned) {
		t.Fatalf("abandoned owner heartbeated: err = %v", err)
	}
}

// A long run keeps its lease by heartbeating, so it is not taken over.
func TestHeartbeatKeepsALongRunAlive(t *testing.T) {
	engine := NewEngine(nil, WithStaleAfter(150*time.Millisecond), WithInFlightWait(0))
	release := make(chan struct{})
	started := make(chan struct{})
	spec := Spec{Name: "slow", Kind: "test", IdempotencyKey: "k", Steps: []Step{{
		Name: "slow",
		Run: func(context.Context) error {
			close(started)
			<-release
			return nil
		},
	}}}
	var first models.WorkflowRun
	done := make(chan struct{})
	go func() { first, _ = engine.RunIdempotent(context.Background(), spec); close(done) }()
	<-started
	time.Sleep(600 * time.Millisecond) // four lease periods
	if _, outcome := engine.RunIdempotent(context.Background(), spec); outcome != InProgress {
		t.Fatalf("a heartbeating run was taken over: outcome %v", outcome)
	}
	close(release)
	<-done
	if first.State != StateSucceeded {
		t.Fatalf("long run state = %s (%s)", first.State, first.Error)
	}
}

// Identical requests that arrive while a run is in flight wait for it and
// get its result, instead of an error or a second run.
func TestConcurrentIdenticalRunsExecuteOnce(t *testing.T) {
	engine := NewEngine(nil, WithInFlightWait(5*time.Second))
	var executions atomic.Int32
	spec := Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{{
		Name: "slow",
		Run: func(context.Context) error {
			executions.Add(1)
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}}}
	var wg sync.WaitGroup
	outcomes := make([]Outcome, 20)
	ids := make([]string, 20)
	for i := range outcomes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			run, outcome := engine.RunIdempotent(context.Background(), spec)
			outcomes[i], ids[i] = outcome, run.ID
		}(i)
	}
	wg.Wait()
	if n := executions.Load(); n != 1 {
		t.Fatalf("workflow executed %d times, want 1", n)
	}
	executed := 0
	for i, o := range outcomes {
		if o == Executed {
			executed++
		} else if o != Replayed {
			t.Fatalf("request %d: outcome %v, want Executed or Replayed", i, o)
		}
		if ids[i] != ids[0] {
			t.Fatalf("request %d got run %s, others %s", i, ids[i], ids[0])
		}
	}
	if executed != 1 {
		t.Fatalf("%d requests report Executed, want 1", executed)
	}
}

type flakyStore struct {
	*MemoryStore
	failClaims atomic.Int32
	failSaves  atomic.Int32
}

var errUnavailable = errors.New("database unavailable")

func (s *flakyStore) ClaimWorkflowRun(ctx context.Context, run models.WorkflowRun, staleAfter time.Duration) (models.WorkflowRun, bool, error) {
	if s.failClaims.Add(-1) >= 0 {
		return models.WorkflowRun{}, false, errUnavailable
	}
	return s.MemoryStore.ClaimWorkflowRun(ctx, run, staleAfter)
}

func (s *flakyStore) SaveWorkflowRun(ctx context.Context, run models.WorkflowRun) error {
	if s.failSaves.Add(-1) >= 0 {
		return errUnavailable
	}
	return s.MemoryStore.SaveWorkflowRun(ctx, run)
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }
