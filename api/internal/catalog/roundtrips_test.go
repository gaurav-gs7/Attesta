package catalog

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gaurav-gs7/attesta/api/internal/page"
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

// Audit W-6: listing loaded each run with three queries of its own (303
// statements for 100 runs). A page now costs three, whatever its size, and
// walking the pages returns every run once, complete.
func TestListWorkflowRunsIsBatchedAndPaginated(t *testing.T) {
	dsn := emptyDatabase(t)
	counter := &statementCounter{}
	store, err := newPostgresStore(context.Background(), dsn, counter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	engine := workflows.NewEngine(store)
	ok := func(name string) workflows.Step {
		return workflows.Step{Name: name, Run: func(context.Context) error { return nil }}
	}
	const total = 250
	for i := 0; i < total; i++ {
		engine.Run(context.Background(), workflows.Spec{Name: "n", Kind: "test", Steps: []workflows.Step{ok("a"), ok("b")}})
	}

	counter.reset()
	first, next, err := store.ListWorkflowRuns(context.Background(), page.Request{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if n := counter.reset(); n != 3 {
		t.Fatalf("listing 100 runs took %d statements, want 3", n)
	}
	if len(first) != 100 || next == "" {
		t.Fatalf("first page: %d runs, next %q", len(first), next)
	}

	seen := map[string]bool{}
	req := page.Request{Limit: 100}
	for {
		runs, next, err := store.ListWorkflowRuns(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		for _, run := range runs {
			if seen[run.ID] {
				t.Fatalf("run %s on two pages", run.ID)
			}
			seen[run.ID] = true
			if len(run.Steps) != 2 || len(run.Events) != 6 || run.Steps[1].Name != "b" {
				t.Fatalf("run %s loaded incompletely: %d steps, %d events", run.ID, len(run.Steps), len(run.Events))
			}
		}
		if next == "" {
			break
		}
		if req, err = page.Parse("100", next); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != total {
		t.Fatalf("walked %d runs, want %d", len(seen), total)
	}
}
