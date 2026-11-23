package catalog

import (
	"context"
	"reflect"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

func serviceWith(name string, deps []string) models.Service {
	return models.Service{
		Name: name, Team: "platform", Owner: "gaurav", Tier: "high", Language: "go",
		Namespace: "payments", Environment: "staging", SLOTarget: 99.9, SLOLatencyP95Ms: 300,
		SLOErrorRate: 1, DeploymentStrategy: "canary", Dependencies: deps,
	}
}

// Audit D-3: dependencies were stored comma-joined, so ["db,primary","cache"]
// came back as three items.
func TestDependenciesRoundTripExactly(t *testing.T) {
	store, _ := postgresForTest(t)
	ctx := context.Background()
	for name, deps := range map[string][]string{
		"commas": {"db,primary", "cache"},
		"spaces": {" padded ", "x"},
		"none":   nil,
	} {
		if _, err := store.CreateService(ctx, serviceWith("svc-"+name, deps)); err != nil {
			t.Fatal(err)
		}
		got, err := store.GetServiceByName(ctx, "svc-"+name)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Dependencies, deps) {
			t.Errorf("%s: stored %q, read back %q", name, deps, got.Dependencies)
		}
	}
	listed, err := store.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, svc := range listed {
		if svc.Name == "svc-commas" && len(svc.Dependencies) != 2 {
			t.Errorf("ListServices: %q", svc.Dependencies)
		}
	}
}

// Rows written before the array column are split as the old reader split
// them, in order.
func TestDependenciesMigrationConvertsExistingRows(t *testing.T) {
	db := openDB(t, emptyDatabase(t))
	all, err := loadMigrations(migrationFiles)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateFS(context.Background(), db, onlyMigrations(all[:2])); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO services (id, name, team, owner, language, namespace, environment, slo_target, deployment_strategy, dependencies)
		VALUES (gen_random_uuid(), 'legacy', 't', 'o', 'go', 'n', 'staging', 99.9, 'canary', 'db, cache ,,queue'),
		       (gen_random_uuid(), 'blank', 't', 'o', 'go', 'n', 'staging', 99.9, 'canary', '')`); err != nil {
		t.Fatal(err)
	}
	if err := migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	store := &PostgresStore{db: db}
	legacy, err := store.GetServiceByName(context.Background(), "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacy.Dependencies, []string{"db", "cache", "queue"}) {
		t.Fatalf("legacy row: %q", legacy.Dependencies)
	}
	blank, err := store.GetServiceByName(context.Background(), "blank")
	if err != nil || blank.Dependencies != nil {
		t.Fatalf("blank row: %q %v", blank.Dependencies, err)
	}
}
