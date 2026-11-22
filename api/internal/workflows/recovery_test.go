package workflows

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

var quiet = WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

// Audit W-3: a client that disconnects mid-run used to cancel the writes that
// record the run, after its side effects were done, leaving it "running".
func TestRunCompletesWhenTheRequestIsCancelled(t *testing.T) {
	store := NewMemoryStore()
	engine := NewEngine(store, quiet)
	reqCtx, disconnect := context.WithCancel(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	sideEffects := 0
	spec := Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{
		{Name: "side-effect", Run: func(ctx context.Context) error {
			close(entered)
			<-release
			sideEffects++
			return ctx.Err()
		}},
		{Name: "after", Run: func(ctx context.Context) error { return ctx.Err() }},
	}}
	done := make(chan struct{})
	go func() { _, _ = engine.RunIdempotent(reqCtx, spec); close(done) }()
	<-entered
	disconnect()
	close(release)
	<-done

	stored, ok := engine.GetByIdempotencyKey("k")
	if !ok || stored.State != StateSucceeded {
		t.Fatalf("stored run after a client disconnect: %+v, want succeeded", stored)
	}
	if sideEffects != 1 {
		t.Fatalf("side effect ran %d times", sideEffects)
	}
}

// Audit W-3: a panicking step used to take the run (and the process) down
// with it, leaving the run "running".
func TestPanickingStepFailsItsRun(t *testing.T) {
	store := NewMemoryStore()
	engine := NewEngine(store, quiet)
	spec := Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{{
		Name: "boom",
		Run: func(context.Context) error {
			var m map[string]int
			m["x"] = 1 // nil map write
			return nil
		},
	}}}
	run, _ := engine.RunIdempotent(context.Background(), spec)
	if run.State != StateFailed || !strings.Contains(run.Error, "panic in step boom") {
		t.Fatalf("run = %s %q, want failed with the panic", run.State, run.Error)
	}
	stored, _ := store.GetWorkflowRun(context.Background(), run.ID)
	if stored.State != StateFailed {
		t.Fatalf("stored state = %s, want failed", stored.State)
	}
	if _, outcome := engine.RunIdempotent(context.Background(), Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k", Steps: []Step{okStep("fine")}}); outcome != Executed {
		t.Fatalf("key still held after a panic: outcome %v", outcome)
	}
}

// A hung step no longer hangs forever: the run timeout cancels it and the
// run is recorded as failed.
func TestRunTimeoutStopsAHungStep(t *testing.T) {
	store := NewMemoryStore()
	engine := NewEngine(store, quiet, WithRunTimeout(100*time.Millisecond))
	run := engine.Run(context.Background(), Spec{Name: "hang", Kind: "test", Steps: []Step{{
		Name: "hang",
		Run:  func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}}})
	if run.State != StateFailed || !strings.Contains(run.Error, "deadline exceeded") {
		t.Fatalf("run = %s %q, want failed on the deadline", run.State, run.Error)
	}
	stored, _ := store.GetWorkflowRun(context.Background(), run.ID)
	if stored.State != StateFailed {
		t.Fatalf("stored state = %s, want failed", stored.State)
	}
}

// Audit W-3: nothing reaped runs left behind by a crash. Runs without an
// idempotency key are never taken over by a retry, so only the reaper
// clears them.
func TestReaperFailsAbandonedRuns(t *testing.T) {
	store := NewMemoryStore()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	store.now = clock.Now
	crashed := newRun(Spec{Name: "rollout", MaxAttempts: 1})
	if err := store.SaveWorkflowRun(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}
	live := newRun(Spec{Name: "rollout", MaxAttempts: 1})
	engine := NewEngine(store, quiet, WithStaleAfter(time.Minute))

	clock.Advance(2 * time.Minute)
	if err := store.SaveWorkflowRun(context.Background(), live); err != nil { // fresh heartbeat
		t.Fatal(err)
	}
	n, err := engine.ReapStale(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("reaped %d (%v), want 1", n, err)
	}
	got, _ := store.GetWorkflowRun(context.Background(), crashed.ID)
	if got.State != StateFailed || !strings.Contains(got.Error, "abandoned") {
		t.Fatalf("crashed run = %s %q, want failed/abandoned", got.State, got.Error)
	}
	if got, _ := store.GetWorkflowRun(context.Background(), live.ID); got.State != StateRunning {
		t.Fatalf("live run was reaped: %s", got.State)
	}
}
