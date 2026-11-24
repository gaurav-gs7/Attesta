package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// maxEvidence is the CRD's limit on evidence items per record.
const maxEvidence = 20

// decisionDraft is a decision made in this pass, not yet recorded.
type decisionDraft struct {
	action, trigger, reason, message string
	// revision and templateHash identify the template the decision is
	// about: the one promoted, or the one rolled back or left in place.
	revision, templateHash string
	restoredTo             *attestav1.DecisionRevision
	evidence               []attestav1.DecisionEvidence
	// sealed is the record, already sealed, when the action that the draft
	// describes carried it on the Deployment update (see writeAhead).
	sealed *attestav1.RolloutDecisionSpec
}

// worthRecording reports whether the draft is a new decision. A rollback, a
// promotion and an SLO gate verdict (made once per revision) always are. A
// rollback that was skipped or failed is recorded when its reason first
// appears, not on every pass that reaches the same conclusion.
func (d *decisionDraft) worthRecording(prev, status RolloutGuardStatus) bool {
	if d == nil {
		return false
	}
	switch d.action {
	case attestav1.ActionRollbackSkipped, attestav1.ActionRollbackFailed:
		return status.Reason != prev.Reason
	}
	return true
}

// matches reports whether record already holds this decision: an earlier
// pass made it, recorded it, and lost the status write before this pass
// made it again.
func (d *decisionDraft) matches(record *attestav1.RolloutDecision, deployment *appsv1.Deployment) bool {
	if d == nil || record == nil {
		return false
	}
	s := record.Spec
	return s.Action == d.action && s.Trigger == d.trigger && s.Deployment.UID == string(deployment.UID) &&
		s.Deployment.Generation == deployment.Generation && s.Deployment.TemplateHash == d.templateHash
}

func (r *RolloutGuardReconciler) actor() attestav1.DecisionActor {
	a := r.Actor
	if a.Controller == "" {
		a.Controller = "attesta-controller"
	}
	return a
}

func (r *RolloutGuardReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// adoptRecordedDecisions moves status.lastDecision forward over records that
// an earlier pass created and then failed to point the status at, because
// the status write lost a race. It reads the cache; recordDecision handles
// the case where the cache has not seen such a record yet. It returns the
// last record adopted, or nil.
func (r *RolloutGuardReconciler) adoptRecordedDecisions(ctx context.Context, guard *attestav1.RolloutGuard, status *RolloutGuardStatus) (*attestav1.RolloutDecision, error) {
	var last *attestav1.RolloutDecision
	for {
		var next int64 = 1
		if status.LastDecision != nil {
			next = status.LastDecision.Sequence + 1
		}
		var record attestav1.RolloutDecision
		key := types.NamespacedName{Namespace: guard.Namespace, Name: attestav1.DecisionName(guard.Name, string(guard.UID), next)}
		err := r.Get(ctx, key, &record)
		if apierrors.IsNotFound(err) {
			return last, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read decision %s: %w", key.Name, err)
		}
		if err := adopt(guard, status, &record); err != nil {
			return nil, err
		}
		last = &record
	}
}

// adopt makes record the head of the guard's chain, if it is the record that
// belongs after the current head.
func adopt(guard *attestav1.RolloutGuard, status *RolloutGuardStatus, record *attestav1.RolloutDecision) error {
	head := &attestav1.DecisionRef{}
	if status.LastDecision != nil {
		head = status.LastDecision
	}
	spec := record.Spec
	hash, err := spec.ComputeHash()
	switch {
	case err != nil:
		return fmt.Errorf("decision %s: %w", record.Name, err)
	case spec.Guard != guard.Name || spec.GuardUID != string(guard.UID) ||
		spec.Sequence != head.Sequence+1 || spec.PreviousHash != head.Hash || hash != spec.Hash:
		// Something other than this controller wrote at the chain's next
		// position. Extending the chain over it would vouch for it.
		return fmt.Errorf("decision %s does not continue the chain of guard %s at sequence %d; "+
			"only the controller may create RolloutDecisions", record.Name, guard.Name, head.Sequence+1)
	}
	status.LastDecision = &attestav1.DecisionRef{Name: record.Name, Sequence: spec.Sequence, Hash: spec.Hash}
	return nil
}

// recordDecision writes the draft as the next record in the guard's chain
// and points status.lastDecision at it. The record is created before the
// status is written: a crash or a lost status write between the two leaves
// a record that the next pass adopts, never a status pointing at nothing.
func (r *RolloutGuardReconciler) recordDecision(ctx context.Context, guard *attestav1.RolloutGuard, deployment *appsv1.Deployment,
	status *RolloutGuardStatus, d *decisionDraft) error {

	spec := r.decisionSpec(guard, deployment, d)
	if d.sealed != nil {
		spec = *d.sealed
	}
	return r.createRecord(ctx, guard, status, spec, d.action, func(existing *attestav1.RolloutDecision) bool {
		return existing.Spec.Hash == spec.Hash || d.matches(existing, deployment)
	})
}

// decisionSpec is the record of the draft, not yet sealed.
func (r *RolloutGuardReconciler) decisionSpec(guard *attestav1.RolloutGuard, deployment *appsv1.Deployment, d *decisionDraft) attestav1.RolloutDecisionSpec {
	evidence := d.evidence
	if len(evidence) > maxEvidence {
		evidence = evidence[:maxEvidence]
	}
	revision := d.revision
	if revision == "" {
		revision = deployment.Annotations[revisionAnnotation]
	}
	spec := attestav1.RolloutDecisionSpec{
		Guard:     guard.Name,
		GuardUID:  string(guard.UID),
		DecidedAt: r.now().UTC().Format(time.RFC3339),
		Actor:     r.actor(),
		Deployment: attestav1.DecisionTarget{
			Name:         deployment.Name,
			UID:          string(deployment.UID),
			Generation:   deployment.Generation,
			Revision:     revision,
			TemplateHash: d.templateHash,
			Images:       images(&deployment.Spec.Template.Spec),
		},
		Trigger:    d.trigger,
		Action:     d.action,
		Reason:     d.reason,
		Message:    d.message,
		RestoredTo: d.restoredTo,
		Evidence:   evidence,
	}
	return spec
}

// createRecord writes spec as the next record of the guard's chain,
// resealing it if the chain moved, and points status.lastDecision at it.
// ours reports whether a record found at the position is this decision,
// written by an earlier attempt.
func (r *RolloutGuardReconciler) createRecord(ctx context.Context, guard *attestav1.RolloutGuard, status *RolloutGuardStatus,
	spec attestav1.RolloutDecisionSpec, action string, ours func(*attestav1.RolloutDecision) bool) error {

	for range 3 {
		if !sealedAfter(spec, status.LastDecision) {
			if err := spec.Seal(status.LastDecision); err != nil {
				return fmt.Errorf("seal decision: %w", err)
			}
		}
		record := &attestav1.RolloutDecision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      attestav1.DecisionName(guard.Name, string(guard.UID), spec.Sequence),
				Namespace: guard.Namespace,
				Labels: map[string]string{
					attestav1.DecisionGuardLabel:    guard.Name,
					attestav1.DecisionGuardUIDLabel: string(guard.UID),
				},
			},
			Spec: spec,
		}
		err := r.Create(ctx, record)
		if err == nil {
			status.LastDecision = &attestav1.DecisionRef{Name: record.Name, Sequence: spec.Sequence, Hash: spec.Hash}
			decisionsTotal.WithLabelValues(action).Inc()
			return nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("record decision %s: %w", record.Name, err)
		}
		// An earlier pass recorded at this position and lost its status
		// write, and the cache has not caught up. Adopt that record from the
		// API server and take the next position.
		var existing attestav1.RolloutDecision
		if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(record), &existing); err != nil {
			return fmt.Errorf("read decision %s: %w", record.Name, err)
		}
		if err := adopt(guard, status, &existing); err != nil {
			return err
		}
		if ours(&existing) {
			return nil
		}
	}
	return fmt.Errorf("decision chain of guard %s/%s keeps moving under this controller; is a second controller running without leader election?",
		guard.Namespace, guard.Name)
}

// sealedAfter reports whether spec is already sealed as the record after
// head.
func sealedAfter(spec attestav1.RolloutDecisionSpec, head *attestav1.DecisionRef) bool {
	if spec.Hash == "" {
		return false
	}
	if head == nil {
		return spec.Sequence == 1 && spec.PreviousHash == ""
	}
	return spec.Sequence == head.Sequence+1 && spec.PreviousHash == head.Hash
}

// decisionAnnotation carries a sealed decision on the Deployment update
// that carries out the decision, so the action and its record are written
// together: if the controller stops before creating the record, the next
// pass finds the decision on the Deployment and records it.
const decisionAnnotation = "attesta.io/decision"

// writeAhead seals d as the guard's next record and puts it on the update
// that will carry the action out. The record is created after the update
// succeeds; recoverDecision creates it if the controller stops in between.
func (r *RolloutGuardReconciler) writeAhead(guard *attestav1.RolloutGuard, deployment, updated *appsv1.Deployment,
	status *RolloutGuardStatus, d *decisionDraft) error {

	spec := r.decisionSpec(guard, deployment, d)
	if err := spec.Seal(status.LastDecision); err != nil {
		return fmt.Errorf("seal decision: %w", err)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("encode decision: %w", err)
	}
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[decisionAnnotation] = string(raw)
	d.sealed = &spec
	return nil
}

// recoverDecision records a decision that a Deployment update carried out
// but whose record was never created, because the controller stopped or the
// create failed after the update.
func (r *RolloutGuardReconciler) recoverDecision(ctx context.Context, guard *attestav1.RolloutGuard, deployment *appsv1.Deployment,
	status *RolloutGuardStatus) error {

	raw := deployment.Annotations[decisionAnnotation]
	if raw == "" {
		return nil
	}
	var spec attestav1.RolloutDecisionSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil || spec.GuardUID != string(guard.UID) {
		// Malformed, or written for another guard of the same Deployment.
		return nil
	}
	if hash, err := spec.ComputeHash(); err != nil || hash != spec.Hash {
		return nil // edited by hand; never record it as the controller's
	}
	head := status.LastDecision
	if head != nil && spec.Sequence <= head.Sequence {
		// Recorded already, unless the chain moved on without it, which
		// takes a record created by something other than this controller.
		var existing attestav1.RolloutDecision
		key := types.NamespacedName{Namespace: guard.Namespace, Name: attestav1.DecisionName(guard.Name, string(guard.UID), spec.Sequence)}
		if err := r.Get(ctx, key, &existing); err == nil && existing.Spec.Hash == spec.Hash {
			return nil
		}
		if spec.Sequence < head.Sequence {
			return nil
		}
	}
	logr.FromContextOrDiscard(ctx).Info("recording a decision the Deployment carries but no record holds",
		"deployment", deployment.Name, "action", spec.Action, "sequence", spec.Sequence)
	decisionsRecoveredTotal.Inc()
	return r.createRecord(ctx, guard, status, spec, spec.Action, func(existing *attestav1.RolloutDecision) bool {
		return existing.Spec.Hash == spec.Hash
	})
}

func images(pod *corev1.PodSpec) []string {
	var out []string
	for _, c := range pod.InitContainers {
		out = append(out, c.Image)
	}
	for _, c := range pod.Containers {
		out = append(out, c.Image)
	}
	return out
}

// deploymentEvidence is what the Deployment reported when the decision was
// made: its Progressing condition and replica counts.
func deploymentEvidence(deployment *appsv1.Deployment, at string) []attestav1.DecisionEvidence {
	progressing := "absent"
	passed := true
	for _, c := range deployment.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing {
			progressing = fmt.Sprintf("%s %s", c.Status, c.Reason)
			passed = c.Reason != "ProgressDeadlineExceeded"
		}
	}
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	st := deployment.Status
	return []attestav1.DecisionEvidence{
		{
			Name: "deployment-condition", Source: "kubernetes",
			Query:       fmt.Sprintf("deployments/%s status.conditions[type=Progressing] (observedGeneration %d)", deployment.Name, st.ObservedGeneration),
			Value:       progressing,
			Threshold:   "not ProgressDeadlineExceeded",
			Passed:      &passed,
			EvaluatedAt: at,
		},
		{
			Name: "replicas", Source: "kubernetes",
			Query:       fmt.Sprintf("deployments/%s status", deployment.Name),
			Value:       fmt.Sprintf("desired %d, updated %d, available %d, total %d", desired, st.UpdatedReplicas, st.AvailableReplicas, st.Replicas),
			EvaluatedAt: at,
		},
	}
}

// analysisEvidence is the last metric check of the template, if one ran.
func analysisEvidence(status *RolloutGuardStatus, templateHash string) []attestav1.DecisionEvidence {
	if a := status.Analysis; a != nil && a.CandidateTemplateHash == templateHash {
		return append([]attestav1.DecisionEvidence(nil), a.LastEvidence...)
	}
	return nil
}
