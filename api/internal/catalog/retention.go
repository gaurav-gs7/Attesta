package catalog

import (
	"context"
	"log/slog"
	"time"
)

// Retention says how long finished records are kept. Zero keeps them
// forever.
type Retention struct {
	// WorkflowRuns applies to succeeded and failed runs, by completion
	// time; running runs are never pruned. A pruned succeeded run no
	// longer holds its idempotency key, so an identical request after the
	// retention runs again (onboarding then answers 200 idempotent).
	WorkflowRuns time.Duration
	// ResolvedIncidents applies to resolved incidents, by resolution time,
	// with their timelines. Active incidents are never pruned.
	ResolvedIncidents time.Duration
}

// DefaultRetention keeps workflow runs 30 days and resolved incidents a
// year.
var DefaultRetention = Retention{WorkflowRuns: 30 * 24 * time.Hour, ResolvedIncidents: 365 * 24 * time.Hour}

// Pruner is the part of a Store the retention job uses.
type Pruner interface {
	PruneWorkflowRuns(ctx context.Context, cutoff time.Time) (int, error)
	PruneResolvedIncidents(ctx context.Context, cutoff time.Time) (int, error)
}

// Prune deletes what r no longer keeps, as of now.
func Prune(ctx context.Context, p Pruner, r Retention, now time.Time) (runs, incidents int, err error) {
	if r.WorkflowRuns > 0 {
		if runs, err = p.PruneWorkflowRuns(ctx, now.Add(-r.WorkflowRuns)); err != nil {
			return runs, 0, err
		}
	}
	if r.ResolvedIncidents > 0 {
		incidents, err = p.PruneResolvedIncidents(ctx, now.Add(-r.ResolvedIncidents))
	}
	return runs, incidents, err
}

// StartPruner runs Prune now and then every interval until ctx is done.
// Several replicas may run it: each batch skips rows another is deleting.
func StartPruner(ctx context.Context, p Pruner, r Retention, interval time.Duration, logger *slog.Logger) {
	if r.WorkflowRuns <= 0 && r.ResolvedIncidents <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			runs, incidents, err := Prune(ctx, p, r, time.Now().UTC())
			switch {
			case err != nil && ctx.Err() == nil:
				logger.Error("retention: prune failed", "error", err)
			case runs > 0 || incidents > 0:
				logger.Info("retention: pruned", "workflowRuns", runs, "incidents", incidents)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// pruneBatch bounds each DELETE, so pruning a large backlog never holds
// locks on many rows at once or runs into the statement timeout.
const pruneBatch = 1000
