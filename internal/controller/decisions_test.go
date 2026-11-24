package controller

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/archive"
)

func (h *harness) decisions() []attestav1.RolloutDecision {
	h.t.Helper()
	var list attestav1.RolloutDecisionList
	if err := h.c.List(h.ctx, &list, client.InNamespace(testNamespace),
		client.MatchingLabels{attestav1.DecisionGuardUIDLabel: testGuardUID}); err != nil {
		h.t.Fatal(err)
	}
	return list.Items
}

func (h *harness) verifyChain() []attestav1.RolloutDecision {
	h.t.Helper()
	records := h.decisions()
	if err := attestav1.VerifyChain(testGuardName, testGuardUID, records, h.guard().Status.LastDecision); err != nil {
		h.t.Fatal(err)
	}
	return records
}

func bySequence(records []attestav1.RolloutDecision) map[int64]attestav1.RolloutDecisionSpec {
	out := map[int64]attestav1.RolloutDecisionSpec{}
	for _, r := range records {
		out[r.Spec.Sequence] = r.Spec
	}
	return out
}

func evidenceNamed(spec attestav1.RolloutDecisionSpec, name string) *attestav1.DecisionEvidence {
	for i := range spec.Evidence {
		if spec.Evidence[i].Name == name {
			return &spec.Evidence[i]
		}
	}
	return nil
}

func TestDecisionsFormAVerifiableChain(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile() // promotes v1
	h.startBadRollout()
	h.mustReconcile() // rolls back to v1

	records := h.verifyChain()
	if len(records) != 2 {
		t.Fatalf("got %d decisions, want 2 (promotion, rollback)", len(records))
	}
	seq := bySequence(records)

	promoted := seq[1]
	if promoted.Action != attestav1.ActionPromoted || promoted.Trigger != "RolloutComplete" ||
		promoted.Deployment.TemplateHash != "h1" || promoted.Deployment.Revision != "1" || promoted.PreviousHash != "" {
		t.Fatalf("decision 1 = %+v, want the promotion of revision 1 (h1)", promoted)
	}

	rolledBack := seq[2]
	if rolledBack.Action != attestav1.ActionRolledBack || rolledBack.Trigger != "ProgressDeadlineExceeded" {
		t.Fatalf("decision 2 = %s/%s, want RolledBack/ProgressDeadlineExceeded", rolledBack.Action, rolledBack.Trigger)
	}
	if got := rolledBack.Deployment; got.Revision != "2" || got.TemplateHash != "h2" || got.Generation != 2 ||
		got.UID != string(testUID) || strings.Join(got.Images, ",") != "payments-api:v2" {
		t.Fatalf("decision 2 is about %+v, want revision 2 (h2, v2) at generation 2", got)
	}
	if got := rolledBack.RestoredTo; got == nil || got.Revision != "1" || got.TemplateHash != "h1" {
		t.Fatalf("restoredTo = %+v, want revision 1 (h1)", got)
	}
	if rolledBack.PreviousHash != promoted.Hash {
		t.Fatal("decision 2 does not link to decision 1")
	}
	cond := evidenceNamed(rolledBack, "deployment-condition")
	if cond == nil || cond.Source != "kubernetes" || !strings.Contains(cond.Value, "ProgressDeadlineExceeded") || cond.Passed == nil || *cond.Passed {
		t.Fatalf("deployment-condition evidence = %+v, want a failed ProgressDeadlineExceeded reading", cond)
	}
	if rolledBack.Actor.Controller != "attesta-controller" || rolledBack.DecidedAt == "" {
		t.Fatalf("actor/decidedAt = %+v/%q", rolledBack.Actor, rolledBack.DecidedAt)
	}
	if head := h.guard().Status.LastDecision; head == nil || head.Sequence != 2 || head.Hash != rolledBack.Hash {
		t.Fatalf("status.lastDecision = %+v, want decision 2", head)
	}
}

// A decision the controller keeps reaching on every pass is recorded once.
func TestRepeatedSkipIsRecordedOnce(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.mustReconcile()

	// The Deployment controller adopts the healthy ReplicaSet as revision 3
	// and a stale deadline is reported at that generation.
	rs1 := h.replicaSet("payments-api-h1")
	rs1.Annotations[revisionAnnotation] = "3"
	h.updateReplicaSet(rs1)
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 3
		d.Annotations[revisionAnnotation] = "3"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 1, Conditions: progressDeadlineExceeded()}
	})
	for range 3 {
		h.mustReconcile()
	}

	records := h.verifyChain()
	if len(records) != 3 {
		t.Fatalf("got %d decisions, want 3 (promotion, rollback, one skip)", len(records))
	}
	skip := bySequence(records)[3]
	if skip.Action != attestav1.ActionRollbackSkipped || skip.Reason != reasonHealthyRevFailing || skip.Deployment.TemplateHash != "h1" {
		t.Fatalf("decision 3 = %s/%s on %s, want RollbackSkipped/%s on h1", skip.Action, skip.Reason, skip.Deployment.TemplateHash, reasonHealthyRevFailing)
	}
}

// A pass that records a decision and then loses its status write leaves a
// record the status does not point at. The next pass adopts it rather than
// recording the same decision twice or leaving a gap.
func TestDecisionRecordedBeforeALostStatusWriteIsAdopted(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "attesta.io", Resource: "rolloutguards"}, testGuardName, errors.New("stale"))
	failures := 1
	h.updateStatus = func() error {
		if failures > 0 {
			failures--
			return conflict
		}
		return nil
	}
	if err := h.reconcile(); !apierrors.IsConflict(err) {
		t.Fatalf("first pass error = %v, want the injected conflict", err)
	}
	if n := len(h.decisions()); n != 1 {
		t.Fatalf("after the lost status write there are %d records, want 1", n)
	}
	if h.guard().Status.LastDecision != nil {
		t.Fatal("status was written despite the injected conflict")
	}

	h.mustReconcile()
	records := h.verifyChain()
	if len(records) != 1 {
		t.Fatalf("got %d decisions after the retry, want the one promotion", len(records))
	}
	if got := h.field("healthyTemplateHash"); got != "h1" {
		t.Fatalf("healthyTemplateHash = %q, want h1", got)
	}
}

// When the cache has not seen the orphaned record yet, the create collides
// with it; the controller adopts it from the API server.
func TestDecisionCollisionAdoptsTheRecordAtThatPosition(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	head := h.guard().Status.LastDecision

	// Forget the status pointer, as if the write that set it was lost, and
	// hide the record from the cache read.
	g := h.guard()
	g.Status.LastDecision = nil
	g.Status.HealthyTemplateHash, g.Status.HealthyRevision, g.Status.HealthyGeneration = "", "", 0
	if err := h.c.Status().Update(h.ctx, g); err != nil {
		t.Fatal(err)
	}
	h.r.Client = hideDecisionsFromGets{h.c}
	h.r.APIReader = h.c

	h.mustReconcile()
	records := h.verifyChain()
	if len(records) != 1 || h.guard().Status.LastDecision.Hash != head.Hash {
		t.Fatalf("got %d records and head %+v, want the original single promotion", len(records), h.guard().Status.LastDecision)
	}
}

type hideDecisionsFromGets struct{ client.Client }

func (c hideDecisionsFromGets) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*attestav1.RolloutDecision); ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: "attesta.io", Resource: "rolloutdecisions"}, key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// A record at the chain's next position that this controller did not write
// is never adopted: extending the chain over it would vouch for it.
func TestForeignRecordAtTheNextPositionIsRefused(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	forged := attestav1.RolloutDecision{Spec: attestav1.RolloutDecisionSpec{
		Guard: testGuardName, GuardUID: testGuardUID, Action: attestav1.ActionPromoted, Trigger: "RolloutComplete",
	}}
	forged.Name = attestav1.DecisionName(testGuardName, testGuardUID, 1)
	forged.Namespace = testNamespace
	forged.Spec.Sequence, forged.Spec.Hash = 1, strings.Repeat("0", 64) // not the hash of its contents
	if err := h.c.Create(h.ctx, &forged); err != nil {
		t.Fatal(err)
	}
	err := h.reconcile()
	if err == nil || !strings.Contains(err.Error(), "does not continue the chain") {
		t.Fatalf("reconcile error = %v, want a refusal to adopt the forged record", err)
	}
}

func TestEditedRecordFailsVerification(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.mustReconcile()

	records := h.verifyChain()
	// The API server refuses this edit (see the envtest); the fake does not,
	// which stands in for an edit made directly in etcd.
	for i := range records {
		if records[i].Spec.Sequence == 1 {
			records[i].Spec.Deployment.Images = []string{"payments-api:v9"}
		}
	}
	err := attestav1.VerifyChain(testGuardName, testGuardUID, records, h.guard().Status.LastDecision)
	var chainErr *attestav1.ChainError
	if !errors.As(err, &chainErr) || chainErr.Sequence != 1 || !strings.Contains(err.Error(), "altered") {
		t.Fatalf("VerifyChain = %v, want record 1 reported altered", err)
	}
}

// The controller stops between rolling back and creating the record (here,
// the create fails). The rollback carried its sealed record on the
// Deployment, so the next pass records exactly that decision.
func TestARollbackIsNeverLeftWithoutItsRecord(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	failures := 1
	h.createDecision = func() error {
		if failures > 0 {
			failures--
			return errors.New("connection reset by peer")
		}
		return nil
	}
	if err := h.reconcile(); err == nil {
		t.Fatal("the failed record create was not reported")
	}
	if h.image() != "payments-api:v1" {
		t.Fatal("the rollback did not happen")
	}
	carried := h.deployment().Annotations[decisionAnnotation]
	if !strings.Contains(carried, `"action":"RolledBack"`) {
		t.Fatalf("the rollback update does not carry its decision: %q", carried)
	}
	if n := len(h.decisions()); n != 1 {
		t.Fatalf("%d records after the failed create, want only the promotion", n)
	}

	if h.field("phase") != PhaseRolledBack {
		t.Fatalf("phase %s: the rollback must reach the status even when its record does not", h.field("phase"))
	}
	// The Deployment controller adopts the healthy ReplicaSet as revision 3.
	rs1 := h.replicaSet("payments-api-h1")
	rs1.Annotations[revisionAnnotation] = "3"
	h.updateReplicaSet(rs1)
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 3
		d.Annotations[revisionAnnotation] = "3"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}
	})

	h.mustReconcile()
	records := h.verifyChain()
	if len(records) != 2 {
		t.Fatalf("%d records after recovery, want 2", len(records))
	}
	rolledBack := bySequence(records)[2]
	if rolledBack.Action != attestav1.ActionRolledBack || !strings.Contains(carried, rolledBack.Hash) {
		t.Fatalf("recovered record %s (%s) is not the one the Deployment carried", rolledBack.Action, rolledBack.Hash)
	}
	h.mustReconcile()
	if n := len(h.decisions()); n != 2 {
		t.Fatalf("recovery recorded the decision again: %d records", n)
	}
}

// A decision annotation edited by hand does not hash to what it claims and
// is never recorded as the controller's.
func TestAnEditedCarriedDecisionIsNotRecorded(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.createDecision = func() error { return errors.New("down") }
	_ = h.reconcile()
	h.createDecision = nil
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Annotations[decisionAnnotation] = strings.Replace(d.Annotations[decisionAnnotation], `"RolledBack"`, `"Promoted"`, 1)
	})
	h.mustReconcile()
	for _, r := range h.decisions() {
		if r.Spec.Sequence == 2 && r.Spec.Action == attestav1.ActionPromoted {
			t.Fatal("an edited annotation was recorded as a decision")
		}
	}
}

type fakeArchive struct {
	got  []attestav1.RolloutDecision
	fail error
}

func (f *fakeArchive) Archive(_ context.Context, d attestav1.RolloutDecision) error {
	if f.fail != nil {
		return f.fail
	}
	f.got = append(f.got, d)
	return nil
}

func TestDecisionsAreSignedAndArchivedInOrder(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	keys := attestav1.Keys{}
	keys.Add(pub)
	sink := &fakeArchive{fail: errors.New("connection refused")}
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.r.SigningKey, h.r.Archive = priv, sink

	h.mustReconcile() // promotion; the archive is down
	if c := h.condition(ConditionArchived); c == nil || c["status"] != "False" || c["reason"] != "ArchiveUnavailable" {
		t.Fatalf("Archived condition = %v", c)
	}
	if h.guard().Status.ArchivedSequence != 0 {
		t.Fatal("a decision the archive did not take counts as archived")
	}

	sink.fail = nil
	h.startBadRollout()
	h.mustReconcile() // rollback, and the backlog goes out
	records := h.verifyChain()
	if err := attestav1.VerifySignatures(testGuardName, records, keys); err != nil {
		t.Fatal(err)
	}
	if len(sink.got) != 2 || sink.got[0].Spec.Sequence != 1 || sink.got[1].Spec.Sequence != 2 || sink.got[1].Spec.Signature == nil {
		t.Fatalf("archived %d records", len(sink.got))
	}
	if g := h.guard(); g.Status.ArchivedSequence != 2 {
		t.Fatalf("archivedSequence = %d", g.Status.ArchivedSequence)
	}
	if c := h.condition(ConditionArchived); c == nil || c["status"] != "True" {
		t.Fatalf("Archived condition = %v", c)
	}

	// The next decision meets an archive that holds something else at its
	// position: reported as refused, which is what tampering looks like.
	sink.fail = fmt.Errorf("%w: 409 a different record is archived at 3", archive.ErrRefused)
	h.setDeployment(func(d *appsv1.Deployment) { delete(d.Annotations, rollbackGuardsAnnotation) })
	h.startBadRolloutAgain()
	h.mustReconcile()
	if c := h.condition(ConditionArchived); c == nil || c["reason"] != "ArchiveRefused" {
		t.Fatalf("Archived condition = %v", c)
	}
	if h.guard().Status.ArchivedSequence != 2 {
		t.Fatal("a refused decision counts as archived")
	}
}
