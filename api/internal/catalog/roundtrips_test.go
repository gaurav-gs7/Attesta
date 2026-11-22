package catalog

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

// statementCounter counts statements sent to PostgreSQL, BEGIN and COMMIT
// included.
type statementCounter struct {
	mu    sync.Mutex
	count int
	log   []string
}

func (c *statementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	c.mu.Lock()
	c.count++
	c.log = append(c.log, strings.Join(strings.Fields(data.SQL), " "))
	c.mu.Unlock()
	return ctx
}

func (c *statementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (c *statementCounter) reset() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.count
	c.count, c.log = 0, nil
	return n
}

// Audit W-4: every save deleted and re-inserted all steps and events in a
// transaction. A one-step run cost 36 statements and onboarding 104.
func TestWorkflowPersistenceRoundTrips(t *testing.T) {
	dsn := emptyDatabase(t)
	counter := &statementCounter{}
	store, err := newPostgresStore(context.Background(), dsn, counter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	engine := workflows.NewEngine(store)
	step := func(name string) workflows.Step {
		return workflows.Step{Name: name, Run: func(context.Context) error { return nil }}
	}

	for _, tc := range []struct {
		name  string
		spec  workflows.Spec
		limit int
	}{
		{"one step, no key", workflows.Spec{Name: "slo", Kind: "test", Steps: []workflows.Step{step("evaluate")}}, 3},
		{"three steps, keyed (onboarding)", workflows.Spec{Name: "onboard", Kind: "test", IdempotencyKey: "k",
			Steps: []workflows.Step{step("persist"), step("record"), step("render")}}, 6},
	} {
		counter.reset()
		run := engine.Run(context.Background(), tc.spec)
		if run.State != workflows.StateSucceeded {
			t.Fatalf("%s: run %s %q", tc.name, run.State, run.Error)
		}
		n := counter.reset()
		t.Logf("%s: %d statements", tc.name, n)
		if n > tc.limit {
			t.Errorf("%s: %d statements, want at most %d", tc.name, n, tc.limit)
		}
		stored, err := store.GetWorkflowRun(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(stored.Steps) != len(run.Steps) || len(stored.Events) != len(run.Events) {
			t.Fatalf("%s: stored %d steps / %d events, want %d / %d", tc.name, len(stored.Steps), len(stored.Events), len(run.Steps), len(run.Events))
		}
		for i := range run.Events {
			if stored.Events[i].Type != run.Events[i].Type || stored.Events[i].Message != run.Events[i].Message {
				t.Fatalf("%s: event %d = %+v, want %+v", tc.name, i, stored.Events[i], run.Events[i])
			}
		}
		for i := range run.Steps {
			if stored.Steps[i].Name != run.Steps[i].Name || stored.Steps[i].State != run.Steps[i].State || stored.Steps[i].Attempt != run.Steps[i].Attempt {
				t.Fatalf("%s: step %d = %+v, want %+v", tc.name, i, stored.Steps[i], run.Steps[i])
			}
		}
	}
}
