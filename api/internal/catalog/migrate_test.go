package catalog

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
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
