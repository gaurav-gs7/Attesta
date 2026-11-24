package catalog

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/internal/models"
)

func bothStores(t *testing.T) map[string]func(t *testing.T) Store {
	stores := map[string]func(t *testing.T) Store{"memory": func(*testing.T) Store { return NewMemoryStore() }}
	if os.Getenv("ATTESTA_TEST_DATABASE_URL") != "" {
		stores["postgres"] = func(t *testing.T) Store { s, _ := postgresForTest(t); return s }
	}
	return stores
}

func newIncident(svc models.Service, fingerprint string) models.IncidentRecord {
	now := time.Now().UTC()
	return models.IncidentRecord{
		Incident: models.Incident{
			ID: "inc-" + NewID(), ServiceID: svc.ID, Service: svc.Name, Severity: "critical",
			Title: "HighErrorRate", Status: "active", Source: "alertmanager", Fingerprint: fingerprint,
			CreatedAt: now, OpenedBy: "alertmanager",
		},
		Service: svc,
		Signals: models.SLOStatus{Service: svc.Name, DeploymentGate: "unknown", Reason: "no data", EvaluatedAt: now},
		Events:  []models.IncidentEvent{{Timestamp: now, EventType: "opened", Description: "opened"}},
	}
}

func resolved(r models.IncidentRecord, by string) models.IncidentRecord {
	at := time.Now().UTC()
	r.Events = append(append([]models.IncidentEvent(nil), r.Events...), models.IncidentEvent{Timestamp: at, EventType: "resolved", Description: "by " + by})
	r.Incident.Status, r.Incident.ResolvedAt, r.Incident.ResolvedBy = "resolved", &at, by
	return r
}

// Audit D-5: resolving an incident overwrote the whole record with no
// concurrency check, so two concurrent resolutions both applied.
func TestIncidentUpdatesAreVersioned(t *testing.T) {
	for name, open := range bothStores(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			ctx := context.Background()
			svc, err := store.CreateService(ctx, serviceWith("payments-api", nil))
			if err != nil {
				t.Fatal(err)
			}
			created, err := store.CreateIncident(ctx, newIncident(svc, "fp-1"))
			if err != nil || created.Incident.Version != 1 {
				t.Fatalf("create: version %d, %v", created.Incident.Version, err)
			}

			// Two resolutions from the same read: exactly one applies.
			var wg sync.WaitGroup
			results := make(chan error, 2)
			for _, who := range []string{"alice", "bob"} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := store.UpdateIncident(ctx, resolved(created, who))
					results <- err
				}()
			}
			wg.Wait()
			close(results)
			var ok, conflicts int
			for err := range results {
				switch {
				case err == nil:
					ok++
				case errors.Is(err, ErrConflict):
					conflicts++
				default:
					t.Fatal(err)
				}
			}
			if ok != 1 || conflicts != 1 {
				t.Fatalf("%d applied, %d conflicts; want 1 and 1", ok, conflicts)
			}
			stored, err := store.GetIncidentRecord(ctx, created.Incident.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Incident.Version != 2 || len(stored.Events) != 2 || stored.Incident.Status != "resolved" {
				t.Fatalf("stored: version %d, %d events, %s", stored.Incident.Version, len(stored.Events), stored.Incident.Status)
			}
			if stored.Service.Name != "payments-api" || stored.Signals.Reason != "no data" {
				t.Fatalf("service or signals lost: %+v", stored)
			}

			// A stale version is refused; a missing incident is not found.
			if _, err := store.UpdateIncident(ctx, resolved(created, "carol")); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale update: %v", err)
			}
			missing := newIncident(svc, "")
			missing.Incident.Version = 1
			if _, err := store.UpdateIncident(ctx, missing); !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing incident: %v", err)
			}
		})
	}
}

func TestOneActiveIncidentPerAlertFingerprint(t *testing.T) {
	for name, open := range bothStores(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			ctx := context.Background()
			svc, err := store.CreateService(ctx, serviceWith("payments-api", nil))
			if err != nil {
				t.Fatal(err)
			}
			first, err := store.CreateIncident(ctx, newIncident(svc, "fp-1"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateIncident(ctx, newIncident(svc, "fp-1")); !errors.Is(err, ErrConflict) {
				t.Fatalf("second active incident for one fingerprint: %v", err)
			}
			for i := 0; i < 2; i++ { // incidents without a fingerprint are not limited
				if _, err := store.CreateIncident(ctx, newIncident(svc, "")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.UpdateIncident(ctx, resolved(first, "alice")); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateIncident(ctx, newIncident(svc, "fp-1")); err != nil {
				t.Fatalf("new incident after the first was resolved: %v", err)
			}
		})
	}
}

// Records written as JSON documents by earlier versions migrate into the
// tables with their timelines, and duplicate active incidents are retired.
func TestIncidentRecordsMigrate(t *testing.T) {
	db := openDB(t, emptyDatabase(t))
	ctx := context.Background()
	all, err := loadMigrations(migrationFiles)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrateFS(ctx, db, onlyMigrations(all[:3])); err != nil {
		t.Fatal(err)
	}
	old := &PostgresStore{db: db}
	svc, err := old.CreateService(ctx, serviceWith("payments-api", nil))
	if err != nil {
		t.Fatal(err)
	}
	insert := func(id, created, status, fp, events string) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO incident_records (id, service_id, service_name, incident, service, signals, events, created_at)
			VALUES ($1, $2, 'payments-api',
			        jsonb_build_object('id', $1::text, 'status', $4::text, 'fingerprint', $5::text, 'severity', 'critical',
			                           'title', 'HighErrorRate', 'createdAt', $3::text, 'source', 'alertmanager',
			                           'alertReported', jsonb_build_object('errorRatePct', 8.2)),
			        '{"name":"payments-api"}', '{"service":"payments-api","reason":"no data"}', $6::jsonb, $3::timestamptz)`,
			id, svc.ID, created, status, fp, events); err != nil {
			t.Fatal(err)
		}
	}
	insert("inc-old", "2026-11-01T10:00:00.123456789Z", "active", "fp-1",
		`[{"timestamp":"2026-11-01T10:00:00Z","eventType":"opened","description":"a"},{"timestamp":"2026-11-01T10:05:00Z","eventType":"deployment","description":"b"}]`)
	insert("inc-new", "2026-11-02T10:00:00Z", "active", "fp-1", `[]`)
	if err := migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	store := &PostgresStore{db: db}
	oldInc, err := store.GetIncidentRecord(ctx, "inc-old")
	if err != nil {
		t.Fatal(err)
	}
	if len(oldInc.Events) != 2 || oldInc.Events[1].EventType != "deployment" || oldInc.Incident.Status != "resolved" ||
		oldInc.Incident.AlertReported == nil || *oldInc.Incident.AlertReported.ErrorRatePct != 8.2 || oldInc.Signals.Reason != "no data" {
		t.Fatalf("migrated older duplicate: %+v", oldInc)
	}
	newInc, err := store.FindActiveIncident(ctx, "payments-api", "fp-1")
	if err != nil || newInc.Incident.ID != "inc-new" || newInc.Incident.Version != 1 {
		t.Fatalf("active incident after migration: %+v %v", newInc.Incident, err)
	}
}
