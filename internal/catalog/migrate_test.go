package catalog

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/gaurav-gs7/attesta/internal/models"
)

func openDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func appliedVersions(t *testing.T, db *sql.DB) []int {
	t.Helper()
	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

// Audit D-1: the schema was one string re-executed on every start, with no
// version table, and a second copy in api/migrations that had drifted.
func TestMigrationsRecordVersionsAndRunOnce(t *testing.T) {
	dsn := emptyDatabase(t)
	db := openDB(t, dsn)
	all, err := loadMigrations(migrationFiles)
	if err != nil {
		t.Fatal(err)
	}

	// Replicas starting together: every migration is applied exactly once.
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- migrate(context.Background(), openDB(t, dsn))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrate: %v", err)
		}
	}
	if got := appliedVersions(t, db); len(got) != len(all) || got[len(got)-1] != len(all) {
		t.Fatalf("applied versions %v, want 1..%d once each", got, len(all))
	}
	// A restart applies nothing.
	if err := migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if got := appliedVersions(t, db); len(got) != len(all) {
		t.Fatalf("restart re-applied migrations: %v", got)
	}
}

func TestMigrationFailureStopsAtTheLastGoodVersion(t *testing.T) {
	db := openDB(t, emptyDatabase(t))
	fsys := fstest.MapFS{
		"migrations/0001_one.sql": {Data: []byte(`CREATE TABLE one (id INT);`)},
		"migrations/0002_bad.sql": {Data: []byte(`CREATE TABLE two (id INT); SELECT no_such_function();`)},
	}
	err := migrateFS(context.Background(), db, fsys)
	if err == nil || !strings.Contains(err.Error(), "0002_bad.sql") {
		t.Fatalf("bad migration: %v", err)
	}
	if got := appliedVersions(t, db); len(got) != 1 || got[0] != 1 {
		t.Fatalf("versions after a failed migration: %v, want [1]", got)
	}
	var two sql.NullString
	if err := db.QueryRow(`SELECT to_regclass('two')::text`).Scan(&two); err != nil || two.Valid {
		t.Fatalf("the failed migration's table survived: %v %v", two, err)
	}
	// The lock was released: a fixed migration applies.
	fsys["migrations/0002_bad.sql"] = &fstest.MapFile{Data: []byte(`CREATE TABLE two (id INT);`)}
	if err := migrateFS(context.Background(), db, fsys); err != nil {
		t.Fatal(err)
	}
}

func TestNewerSchemaIsRefused(t *testing.T) {
	dsn := emptyDatabase(t)
	db := openDB(t, dsn)
	if err := migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_migrations (version, name) VALUES (9999, '9999_future.sql')`); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPostgresStore(context.Background(), dsn); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("a binary older than the schema started: %v", err)
	}
}

func TestMigrationFilesAreWellFormed(t *testing.T) {
	if _, err := loadMigrations(migrationFiles); err != nil {
		t.Fatal(err)
	}
	gap := fstest.MapFS{"migrations/0001_a.sql": {}, "migrations/0003_c.sql": {}}
	if _, err := loadMigrations(gap); err == nil {
		t.Fatal("a gap in versions was accepted")
	}
	badName := fstest.MapFS{"migrations/1_a.sql": {}}
	if _, err := loadMigrations(badName); err == nil {
		t.Fatal("a misnamed migration was accepted")
	}
}

// Audit D-2: five tables were created and never used. Every table left is
// one the store reads or writes.
func TestSchemaHasNoUnusedTables(t *testing.T) {
	store, _ := postgresForTest(t)
	rows, err := store.db.Query(`SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	var src []byte
	for _, file := range []string{"postgres.go", "archive.go"} {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		src = append(src, b...)
	}
	for _, table := range tables {
		if table == "schema_migrations" {
			continue
		}
		if !strings.Contains(string(src), " "+table+" ") && !strings.Contains(string(src), " "+table+"(") && !strings.Contains(string(src), " "+table+"\n") {
			t.Errorf("table %s is not referenced by the store", table)
		}
	}
	// incidents and incident_events were re-created, in use, by D-5.
	for _, dropped := range []string{"slo_snapshots", "rollout_events", "readiness_checks", "incident_records", "rollouts", "deployments"} {
		for _, table := range tables {
			if table == dropped {
				t.Errorf("unused table %s still exists", dropped)
			}
		}
	}
}

// onlyMigrations is a filesystem holding the given migrations, to bring a
// test database to an older version.
func onlyMigrations(ms []migration) fstest.MapFS {
	fsys := fstest.MapFS{}
	for _, m := range ms {
		fsys["migrations/"+m.name] = &fstest.MapFile{Data: []byte(m.sql)}
	}
	return fsys
}

// The archive refuses UPDATE and DELETE in the database itself, so the
// API's own code cannot rewrite history either.
func TestDecisionArchiveIsAppendOnlyInTheDatabase(t *testing.T) {
	store, _ := postgresForTest(t)
	ctx := context.Background()
	if _, err := store.ArchiveDecision(ctx, models.ArchivedDecision{GuardUID: "g", Namespace: "n", Guard: "x", Name: "x-g-000001",
		Sequence: 1, Hash: strings.Repeat("a", 64), Record: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`UPDATE decision_archive SET hash = repeat('b', 64)`,
		`DELETE FROM decision_archive`,
	} {
		if _, err := store.db.ExecContext(ctx, stmt); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%s: %v, want refused", stmt, err)
		}
	}
}
