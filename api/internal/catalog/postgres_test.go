package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

// emptyDatabase creates a fresh, unmigrated database and returns its DSN, or
// skips without ATTESTA_TEST_DATABASE_URL.
func emptyDatabase(t *testing.T) string {
	t.Helper()
	adminDSN := os.Getenv("ATTESTA_TEST_DATABASE_URL")
	if adminDSN == "" {
		t.Skip("ATTESTA_TEST_DATABASE_URL is not set")
	}
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "attesta_test_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		_ = admin.Close()
	})
	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

// postgresForTest returns a migrated store on a fresh database.
func postgresForTest(t *testing.T) (*PostgresStore, string) {
	t.Helper()
	dsn := emptyDatabase(t)
	store, err := NewPostgresStore(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dsn
}

func runFor(key string) models.WorkflowRun {
	return models.WorkflowRun{
		ID: "wf-" + NewID(), Name: "onboard", Kind: "test", State: workflows.StateRunning,
		IdempotencyKey: key, Attempt: 1, MaxAttempts: 1, StartedAt: time.Now().UTC(),
	}
}

// Audit W-2: concurrent claims for one key, from separate connections, must
// produce exactly one active run.
func TestPostgresClaimIsAtomic(t *testing.T) {
	store, _ := postgresForTest(t)
	const claimers = 50
	var claimed sync.Map
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			run := runFor("same-key")
			holder, ok, err := store.ClaimWorkflowRun(context.Background(), run, time.Minute)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if ok {
				claimed.Store(run.ID, true)
			} else if holder.State != workflows.StateRunning && holder.State != workflows.StateFailed {
				t.Errorf("holder state %q", holder.State)
			}
		}()
	}
	close(start)
	wg.Wait()
	n := 0
	claimed.Range(func(any, any) bool { n++; return true })
	if n != 1 {
		t.Fatalf("%d claims succeeded for one key, want 1", n)
	}
	var active int
	if err := store.db.QueryRow(`SELECT count(*) FROM workflow_runs WHERE idempotency_key = 'same-key' AND state <> 'failed'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("%d active rows for one key, want 1", active)
	}
}

// The database itself refuses a second active run for a key, so a stalled
// owner whose run was abandoned cannot bring it back alongside the new one.
func TestPostgresRefusesASecondActiveRun(t *testing.T) {
	store, _ := postgresForTest(t)
	ctx := context.Background()
	first := runFor("k")
	if _, ok, err := store.ClaimWorkflowRun(ctx, first, time.Minute); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if _, err := store.db.Exec(`UPDATE workflow_runs SET heartbeat_at = now() - interval '1 hour' WHERE id = $1`, first.ID); err != nil {
		t.Fatal(err)
	}
	second := runFor("k")
	if _, ok, err := store.ClaimWorkflowRun(ctx, second, time.Minute); err != nil || !ok {
		t.Fatalf("takeover claim: ok=%v err=%v", ok, err)
	}

	first.State = workflows.StateSucceeded
	if err := store.SaveWorkflowRun(ctx, first); !errors.Is(err, workflows.ErrAbandoned) {
		t.Fatalf("abandoned run saved over the new holder: %v", err)
	}
	third := runFor("k")
	if err := store.SaveWorkflowRun(ctx, third); !errors.Is(err, workflows.ErrAbandoned) {
		t.Fatalf("a second active run was inserted outside a claim: %v", err)
	}
}

// Audit W-2: databases written by earlier versions hold several active runs
// per key. Migrating keeps one, retires the rest and builds the unique index.
func TestPostgresMigrationDeduplicatesActiveRuns(t *testing.T) {
	store, dsn := postgresForTest(t)
	ctx := context.Background()
	if _, err := store.db.Exec(`DROP INDEX ` + activeKeyIndex); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-time.Hour)
	for i, state := range []string{"succeeded", "succeeded", "running", "succeeded"} {
		if _, err := store.db.Exec(`
			INSERT INTO workflow_runs (id, name, kind, state, idempotency_key, started_at)
			VALUES ($1, 'onboard', 'test', $2, 'dup', $3)
		`, "wf-dup-"+string(rune('a'+i)), state, base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}

	migrated, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("migrate a database with duplicate active runs: %v", err)
	}
	defer func() { _ = migrated.Close() }()

	rows, err := migrated.db.Query(`SELECT id, state FROM workflow_runs WHERE idempotency_key = 'dup' AND state <> 'failed'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var kept []string
	for rows.Next() {
		var id, state string
		if err := rows.Scan(&id, &state); err != nil {
			t.Fatal(err)
		}
		kept = append(kept, id+"="+state)
	}
	if len(kept) != 1 || kept[0] != "wf-dup-d=succeeded" {
		t.Fatalf("kept %v, want only the newest succeeded run wf-dup-d", kept)
	}
	var exists bool
	if err := migrated.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)`, activeKeyIndex).Scan(&exists); err != nil || !exists {
		t.Fatalf("unique index missing after migration: exists=%v err=%v", exists, err)
	}
}

// API replicas starting together against an empty database migrate one
// after the other instead of racing on CREATE ... IF NOT EXISTS.
func TestPostgresConcurrentMigrations(t *testing.T) {
	dsn := emptyDatabase(t)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := NewPostgresStore(context.Background(), dsn)
			if err == nil {
				_ = s.Close()
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent migration failed: %v", err)
		}
	}
}
