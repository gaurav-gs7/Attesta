package catalog

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/page"
)

func withParams(t *testing.T, dsn string, kv ...string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	for i := 0; i+1 < len(kv); i += 2 {
		q.Set(kv[i], kv[i+1])
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func show(t *testing.T, store *PostgresStore, setting string) string {
	t.Helper()
	var v string
	if err := store.db.QueryRow(`SELECT current_setting($1)`, setting).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Audit D-4: no pool limits, connection lifetime or statement timeout.
func TestStoreSetsPoolLimitsAndTimeouts(t *testing.T) {
	store, _ := postgresForTest(t)
	cfg := store.pool.Config()
	if cfg.MaxConns != defaultMaxConns || cfg.MaxConnLifetime != defaultConnLifetime || cfg.MaxConnIdleTime != defaultConnIdleTime {
		t.Fatalf("pool: max %d lifetime %s idle %s", cfg.MaxConns, cfg.MaxConnLifetime, cfg.MaxConnIdleTime)
	}
	for setting, want := range map[string]string{
		"statement_timeout": "10s", "lock_timeout": "5s",
		"idle_in_transaction_session_timeout": "30s", "application_name": "attesta-api",
	} {
		if got := show(t, store, setting); got != want {
			t.Errorf("%s = %q, want %q", setting, got, want)
		}
	}
}

func TestDatabaseURLOverridesDefaults(t *testing.T) {
	store, err := NewPostgresStore(context.Background(), withParams(t, emptyDatabase(t), "pool_max_conns", "3", "statement_timeout", "200ms"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if got := store.pool.Config().MaxConns; got != 3 {
		t.Fatalf("pool_max_conns=3 gave %d", got)
	}
	_, err = store.db.Exec(`SELECT pg_sleep(1)`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("a 1s statement under a 200ms statement_timeout: %v, want query_canceled", err)
	}
}

// Listing used to issue queries while a result set was still open, holding
// two connections per request. With one connection in the pool every read
// must still finish.
func TestReadsNeedOneConnection(t *testing.T) {
	dsn := emptyDatabase(t)
	seed, err := NewPostgresStore(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := seed.CreateService(ctx, serviceWith("one-conn", []string{"db"})); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		run := runFor("")
		run.Steps = []models.WorkflowStep{{Name: "a", State: "succeeded"}}
		run.Events = []models.WorkflowEvent{{Timestamp: time.Now().UTC(), Type: "x", Message: "y"}}
		if err := seed.SaveWorkflowRun(ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	_ = seed.Close()

	store, err := NewPostgresStore(ctx, withParams(t, dsn, "pool_max_conns", "1"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 3; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _, err := store.ListServices(ctx); errs <- err }()
		go func() {
			defer wg.Done()
			_, _, err := store.ListWorkflowRuns(ctx, page.Request{Limit: 50})
			errs <- err
		}()
		go func() {
			defer wg.Done()
			_, _, err := store.ListIncidentRecords(ctx, page.Request{Limit: 50})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("read with a one-connection pool: %v", err)
		}
	}
}
