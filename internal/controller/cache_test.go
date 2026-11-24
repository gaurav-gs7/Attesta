package controller

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// Audit C-7: Deployment and ReplicaSet events find their guards through the
// cache index, not by listing every guard.
func TestEventsMapToTheTargetingGuardOnly(t *testing.T) {
	h := newHarness(t, nil, nil)
	other := &attestav1.RolloutGuard{
		ObjectMeta: metav1.ObjectMeta{Name: "billing-rollout", Namespace: testNamespace},
		Spec:       attestav1.RolloutGuardSpec{DeploymentRef: "billing"},
	}
	if err := h.c.Create(h.ctx, other); err != nil {
		t.Fatal(err)
	}
	want := ctrl.Request{NamespacedName: testGuardKey}
	if got := h.r.guardsForDeployment(h.ctx, h.deployment()); len(got) != 1 || got[0] != want {
		t.Fatalf("deployment event mapped to %v, want only %v", got, want)
	}
	if got := h.r.guardsForReplicaSet(h.ctx, replicaSet("payments-api-h2", 2, "h2", "payments-api:v2")); len(got) != 1 || got[0] != want {
		t.Fatalf("replicaset event mapped to %v, want only %v", got, want)
	}
	orphan := replicaSet("x", 1, "x", "x")
	orphan.OwnerReferences = nil
	if got := h.r.guardsForReplicaSet(h.ctx, orphan); len(got) != 0 {
		t.Fatalf("a ReplicaSet without a Deployment owner mapped to %v", got)
	}
}

func TestReplicaSetStatusChurnIsFiltered(t *testing.T) {
	p := replicaSetIdentityPredicate()
	old := replicaSet("payments-api-h1", 1, "h1", "payments-api:v1")
	old.ResourceVersion = "1"
	churn := old.DeepCopy()
	churn.ResourceVersion = "2"
	churn.Status.ReadyReplicas = 3
	if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: churn}) {
		t.Fatal("a status-only ReplicaSet change triggered a reconcile")
	}
	resync := old.DeepCopy()
	if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: resync}) {
		t.Fatal("a ReplicaSet resync triggered a reconcile; the Deployment resync covers it")
	}
	moved := old.DeepCopy()
	moved.ResourceVersion = "3"
	moved.Annotations[revisionAnnotation] = "4"
	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: moved}) {
		t.Fatal("a new revision did not trigger a reconcile")
	}
}

func TestGuardResyncsPassButStatusWritesDoNot(t *testing.T) {
	p := resyncPredicate()
	g := &attestav1.RolloutGuard{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5", Generation: 1}}
	if !p.Update(event.UpdateEvent{ObjectOld: g, ObjectNew: g.DeepCopy()}) {
		t.Fatal("a periodic resync was filtered")
	}
	statusWrite := g.DeepCopy()
	statusWrite.ResourceVersion = "6"
	if p.Update(event.UpdateEvent{ObjectOld: g, ObjectNew: statusWrite}) {
		t.Fatal("the controller's own status write passed the resync predicate")
	}
}

// An informer can deliver a Deployment at revision 2 before the ReplicaSet
// cache has seen ReplicaSet revision 2. Acting then would mistake the newest
// cached ReplicaSet for the current template.
func TestReconcileWaitsForReplicaSetCache(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	// The cache has not seen ReplicaSet h2 yet.
	h2 := h.replicaSet("payments-api-h2")
	if err := h.c.Delete(h.ctx, h2); err != nil {
		t.Fatal(err)
	}
	updatesBefore := h.deploymentUpdates()

	err := h.reconcile()
	if !errors.Is(err, errCacheBehind) {
		t.Fatalf("expected errCacheBehind, got %v", err)
	}
	if got := h.deploymentUpdates() - updatesBefore; got != 0 {
		t.Fatalf("rolled back on a stale ReplicaSet view: %d deployment updates", got)
	}
	res, err := h.r.Reconcile(h.ctx, ctrl.Request{NamespacedName: testGuardKey})
	if err != nil || res.RequeueAfter != staleRequeue {
		t.Fatalf("stale cache: result %+v, error %v; want a quiet requeue", res, err)
	}
	h2.ResourceVersion = ""
	h.addReplicaSet(h2) // the cache caught up
	h.mustReconcile()
	if h.image() != "payments-api:v1" {
		t.Fatalf("image = %s after the cache caught up, want payments-api:v1", h.image())
	}
}

// The status update carries the resourceVersion of the read it was decided
// on, so a reconcile working from a stale guard gets a conflict instead of
// overwriting newer status.
func TestStatusUpdateFailsOnAStaleGuard(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	stale := &staleGuardClient{Client: h.c}
	h.r.Client = stale
	stale.snapshot(h.ctx, t)
	// Another writer changes the guard after the snapshot.
	g := h.guard()
	g.Labels = map[string]string{"touched": "yes"}
	if err := h.c.Update(h.ctx, g); err != nil {
		t.Fatal(err)
	}
	err := h.reconcile()
	if !apierrors.IsConflict(err) {
		t.Fatalf("status write from a stale guard: %v, want a conflict", err)
	}
}

// staleGuardClient serves the guard as it was when snapshot was called, the
// way a cache that has not seen the latest write would.
type staleGuardClient struct {
	client.Client
	guard *attestav1.RolloutGuard
}

func (s *staleGuardClient) snapshot(ctx context.Context, t *testing.T) {
	var g attestav1.RolloutGuard
	if err := s.Client.Get(ctx, testGuardKey, &g); err != nil {
		t.Fatal(err)
	}
	s.guard = &g
}

func (s *staleGuardClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if g, ok := obj.(*attestav1.RolloutGuard); ok && key == testGuardKey && s.guard != nil {
		s.guard.DeepCopyInto(g)
		return nil
	}
	return s.Client.Get(ctx, key, obj, opts...)
}
