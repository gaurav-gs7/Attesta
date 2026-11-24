package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/workflows"
)

// Audit D-5: workflow runs, steps, events and incidents were never pruned.
func TestRetentionPrunesOnlyOldFinishedRecords(t *testing.T) {
	for name, open := range bothStores(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			ctx := context.Background()
			now := time.Now().UTC()
			old := now.Add(-40 * 24 * time.Hour)
			ancient := now.Add(-400 * 24 * time.Hour)

			save := func(state string, completed *time.Time) string {
				t.Helper()
				run := runFor("")
				run.State, run.StartedAt, run.CompletedAt = state, old.Add(-time.Minute), completed
				run.Steps = []models.WorkflowStep{{Name: "a", State: state}}
				run.Events = []models.WorkflowEvent{{Timestamp: old, Type: "x", Message: "y"}}
				if err := store.SaveWorkflowRun(ctx, run); err != nil {
					t.Fatal(err)
				}
				return run.ID
			}
			oldOK := save(workflows.StateSucceeded, &old)
			oldFailed := save(workflows.StateFailed, &old)
			running := save(workflows.StateRunning, nil)
			recent := save(workflows.StateSucceeded, &now)

			svc, err := store.CreateService(ctx, serviceWith("payments-api", nil))
			if err != nil {
				t.Fatal(err)
			}
			incident := func(fp string, resolvedAt *time.Time) string {
				t.Helper()
				r := newIncident(svc, fp)
				r.Incident.CreatedAt = ancient
				created, err := store.CreateIncident(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
				if resolvedAt != nil {
					created.Incident.Status, created.Incident.ResolvedAt = "resolved", resolvedAt
					if _, err := store.UpdateIncident(ctx, created); err != nil {
						t.Fatal(err)
					}
				}
				return created.Incident.ID
			}
			oldResolved := incident("a", &ancient)
			recentResolved := incident("b", &old)
			activeOld := incident("c", nil)

			runs, incidents, err := Prune(ctx, store, DefaultRetention, now)
			if err != nil {
				t.Fatal(err)
			}
			if runs != 2 || incidents != 1 {
				t.Fatalf("pruned %d runs and %d incidents, want 2 and 1", runs, incidents)
			}
			for id, kept := range map[string]bool{oldOK: false, oldFailed: false, running: true, recent: true} {
				if _, err := store.GetWorkflowRun(ctx, id); (err == nil) != kept {
					t.Errorf("run %s: kept=%v, want %v", id, err == nil, kept)
				}
			}
			for id, kept := range map[string]bool{oldResolved: false, recentResolved: true, activeOld: true} {
				if _, err := store.GetIncidentRecord(ctx, id); (err == nil) != kept {
					t.Errorf("incident %s: kept=%v, want %v", id, err == nil, kept)
				}
			}
			if pg, ok := store.(*PostgresStore); ok {
				var orphans int
				if err := pg.db.QueryRow(`SELECT
					(SELECT count(*) FROM workflow_steps WHERE workflow_id = ANY($1)) +
					(SELECT count(*) FROM workflow_events WHERE workflow_id = ANY($1)) +
					(SELECT count(*) FROM incident_events WHERE incident_id = $2)`,
					[]string{oldOK, oldFailed}, oldResolved).Scan(&orphans); err != nil || orphans != 0 {
					t.Fatalf("%d steps or events of pruned records left: %v", orphans, err)
				}
			}
			// Zero keeps everything.
			if runs, incidents, err := Prune(ctx, store, Retention{}, now.Add(1000*24*time.Hour)); err != nil || runs+incidents != 0 {
				t.Fatalf("zero retention pruned %d runs, %d incidents: %v", runs, incidents, err)
			}
		})
	}
}

func TestPostgresPrunesInBatches(t *testing.T) {
	store, _ := postgresForTest(t)
	if _, err := store.db.Exec(`INSERT INTO workflow_runs (id, name, kind, state, started_at, completed_at)
		SELECT 'wf-bulk-' || g, 'onboard', 'test', 'succeeded', now() - interval '90 days', now() - interval '90 days'
		FROM generate_series(1, $1) g`, 2*pruneBatch+500); err != nil {
		t.Fatal(err)
	}
	n, err := store.PruneWorkflowRuns(context.Background(), time.Now().Add(-30*24*time.Hour))
	if err != nil || n != 2*pruneBatch+500 {
		t.Fatalf("pruned %d: %v", n, err)
	}
}
