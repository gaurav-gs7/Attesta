package workflows

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

// recordingSleep records requested delays without waiting.
type recordingSleep struct{ delays []time.Duration }

func (r *recordingSleep) sleep(ctx context.Context, d time.Duration) bool {
	r.delays = append(r.delays, d)
	return ctx.Err() == nil
}

func failingStep(fail int, err error, calls *int) Step {
	return Step{Name: "flaky", MaxAttempts: 6, Run: func(context.Context) error {
		*calls++
		if *calls <= fail {
			return err
		}
		return nil
	}}
}

// Audit W-7: a permanent error used the same immediate retries as a
// transient one.
func TestPermanentErrorsAreNotRetried(t *testing.T) {
	rec := &recordingSleep{}
	engine := NewEngine(nil, quiet)
	engine.sleep = rec.sleep
	calls := 0
	run := engine.Run(context.Background(), Spec{Name: "w", Kind: "test",
		Steps: []Step{failingStep(10, errors.New("service already exists with different metadata"), &calls)}})
	if run.State != StateFailed || calls != 1 || len(rec.delays) != 0 {
		t.Fatalf("permanent error: state %s, %d calls, %d waits; want failed after one call", run.State, calls, len(rec.delays))
	}
	if last := run.Events[len(run.Events)-2]; last.Type != "step-not-retried" {
		t.Fatalf("expected a step-not-retried event, got %+v", run.Events)
	}
}

// Audit W-7: transient errors are retried with exponential, capped,
// jittered backoff instead of immediately.
func TestTransientErrorsBackOffExponentially(t *testing.T) {
	rec := &recordingSleep{}
	engine := NewEngine(nil, quiet, WithBackoff(Backoff{Base: 100 * time.Millisecond, Max: 400 * time.Millisecond}))
	engine.sleep = rec.sleep
	calls := 0
	run := engine.Run(context.Background(), Spec{Name: "w", Kind: "test",
		Steps: []Step{failingStep(5, Transient(errors.New("503 from registry")), &calls)}})
	if run.State != StateSucceeded || calls != 6 {
		t.Fatalf("state %s after %d calls, want succeeded on the 6th", run.State, calls)
	}
	ceilings := []time.Duration{100, 200, 400, 400, 400}
	if len(rec.delays) != len(ceilings) {
		t.Fatalf("waited %d times, want %d", len(rec.delays), len(ceilings))
	}
	for i, d := range rec.delays {
		if limit := ceilings[i] * time.Millisecond; d < 0 || d > limit {
			t.Fatalf("delay %d = %s, want within [0, %s]", i+1, d, limit)
		}
	}
	if run.Steps[0].Error != "" {
		t.Fatalf("succeeded step still carries the last error %q", run.Steps[0].Error)
	}
}

func TestBackoffJitterSpreadsDelays(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute}
	distinct := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		distinct[b.Delay(3)] = true
	}
	if len(distinct) < 40 {
		t.Fatalf("50 delays produced only %d distinct values; jitter is missing", len(distinct))
	}
}

// Retrying stops when the run's context ends, instead of sleeping through
// the remaining attempts.
func TestRetriesStopWhenTheRunTimesOut(t *testing.T) {
	engine := NewEngine(nil, quiet, WithRunTimeout(150*time.Millisecond),
		WithBackoff(Backoff{Base: time.Hour, Max: time.Hour}))
	calls := 0
	start := time.Now()
	run := engine.Run(context.Background(), Spec{Name: "w", Kind: "test",
		Steps: []Step{failingStep(10, Transient(errors.New("timeout")), &calls)}})
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("run took %s; the backoff ignored the run deadline", elapsed)
	}
	if run.State != StateFailed || calls != 1 || !strings.Contains(run.Error, "retry abandoned") {
		t.Fatalf("state %s, %d calls, error %q", run.State, calls, run.Error)
	}
}

func TestIsRetryable(t *testing.T) {
	timeout := &net.OpError{Op: "read", Err: timeoutErr{}}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{Transient(errors.New("503")), true},
		{fmt.Errorf("dial: %w", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), true},
		{fmt.Errorf("query: %w", syscall.ECONNRESET), true},
		{timeout, true},
		{errors.New("invalid spec"), false},
		{context.Canceled, false},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), false},
		{nil, false},
	} {
		if got := IsRetryable(tc.err); got != tc.want {
			t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
