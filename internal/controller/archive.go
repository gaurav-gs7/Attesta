package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/archive"
)

// Archiver sends a decision to an archive outside the cluster.
// *archive.Client implements it.
type Archiver interface {
	Archive(ctx context.Context, d attestav1.RolloutDecision) error
}

const (
	// ConditionArchived reports whether every decision has reached the
	// archive outside the cluster.
	ConditionArchived = "Archived"

	// archiveBatch is how many decisions one pass sends; a long backlog is
	// worked off over several passes.
	archiveBatch = 20
	// archiveRetry is how soon a pass that could not archive runs again.
	archiveRetry = 30 * time.Second
)

// seal links spec to head and, with a signing key, signs it.
func (r *RolloutGuardReconciler) seal(spec *attestav1.RolloutDecisionSpec, head *attestav1.DecisionRef) error {
	if err := spec.Seal(head); err != nil {
		return fmt.Errorf("seal decision: %w", err)
	}
	if r.SigningKey != nil {
		if err := spec.Sign(r.SigningKey); err != nil {
			return fmt.Errorf("sign decision: %w", err)
		}
	}
	return nil
}

// archiveDecisions sends the guard's decisions after status.archivedSequence
// to the archive, in order, and returns when to try again (0 when done or
// when there is no archive). An archive that is down never stops the guard:
// the backlog waits in the cluster, and the Archived condition says so.
func (r *RolloutGuardReconciler) archiveDecisions(ctx context.Context, guard *attestav1.RolloutGuard, status *RolloutGuardStatus) time.Duration {
	if r.Archive == nil {
		status.ArchivedSequence = 0
		removeCondition(status, ConditionArchived)
		return 0
	}
	head := status.LastDecision
	if head == nil {
		return 0
	}
	sent := 0
	for status.ArchivedSequence < head.Sequence {
		if sent == archiveBatch {
			return time.Second
		}
		seq := status.ArchivedSequence + 1
		var record attestav1.RolloutDecision
		key := types.NamespacedName{Namespace: guard.Namespace, Name: attestav1.DecisionName(guard.Name, string(guard.UID), seq)}
		if err := r.apiReader().Get(ctx, key, &record); err != nil {
			status.SetCondition(ConditionArchived, metav1.ConditionFalse, "RecordUnreadable",
				fmt.Sprintf("decision %d (%s) could not be read for the archive: %v", seq, key.Name, err))
			return archiveRetry
		}
		archiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := r.Archive.Archive(archiveCtx, record)
		cancel()
		if err != nil {
			reason := "ArchiveUnavailable"
			if errors.Is(err, archive.ErrRefused) {
				// The archive holds a different record at this position, or
				// rejects this one: worth a person's attention.
				reason = "ArchiveRefused"
				archiveRefusedTotal.Inc()
			}
			status.SetCondition(ConditionArchived, metav1.ConditionFalse, reason,
				fmt.Sprintf("%d decisions not yet archived; decision %d: %v", head.Sequence-status.ArchivedSequence, seq, err))
			return archiveRetry
		}
		status.ArchivedSequence = seq
		sent++
	}
	status.SetCondition(ConditionArchived, metav1.ConditionTrue, "UpToDate",
		fmt.Sprintf("decisions 1 to %d are in the archive", status.ArchivedSequence))
	return 0
}

func removeCondition(status *RolloutGuardStatus, conditionType string) {
	var kept []metav1.Condition
	for _, c := range status.Conditions {
		if c.Type != conditionType {
			kept = append(kept, c)
		}
	}
	status.Conditions = kept
}
