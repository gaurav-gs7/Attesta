package controller

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

const (
	testNamespace = "platform"
	testGuardName = "payments-api-rollout"
	testUID       = types.UID("deployment-uid")
	testGuardUID  = "0f3c9a2e-guard-uid"
)

var testGuardKey = types.NamespacedName{Namespace: testNamespace, Name: testGuardName}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := attestav1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return scheme
}

// harness runs the reconciler against controller-runtime's fake client,
// which stands in for the manager's cache and the API server: it enforces
// resourceVersion preconditions, the status subresource and the field
// indexes the reconciler reads by.
type harness struct {
	t      *testing.T
	ctx    context.Context
	r      *RolloutGuardReconciler
	c      client.Client
	events *events.FakeRecorder

	mu                    sync.Mutex
	deploymentUpdateCount int
	statusUpdates         int
	// updateDeployment, when set, answers every Deployment update instead of
	// the fake API server (nil passes the update through).
	updateDeployment func() error
	// updateStatus, when set, answers guard status writes the same way.
	updateStatus func() error
	// createDecision, when set, answers RolloutDecision creates the same way.
	createDecision func() error
}

// newHarness starts with a healthy Deployment on revision 1 (image v1) and a
// guard. rollback is the guard's spec.rollback (nil leaves it unset), and
// extraGuardFields are merged into the guard object.
func newHarness(t *testing.T, rollback map[string]any, extraGuardFields map[string]any) *harness {
	t.Helper()
	spec := map[string]any{"serviceRef": "payments-api", "strategy": "canary"}
	if rollback != nil {
		spec["rollback"] = rollback
	}
	obj := map[string]any{
		"apiVersion": "attesta.io/v1",
		"kind":       "RolloutGuard",
		"metadata":   map[string]any{"name": testGuardName, "namespace": testNamespace, "generation": int64(1), "uid": testGuardUID},
		"spec":       spec,
	}
	for k, v := range extraGuardFields {
		obj[k] = v
	}
	guard := &attestav1.RolloutGuard{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj, guard); err != nil {
		t.Fatal(err)
	}

	h := &harness{t: t, ctx: ctrl.LoggerInto(context.Background(), ctrl.Log), events: events.NewFakeRecorder(100)}
	base := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(guard, healthyDeployment(1, "payments-api:v1"), replicaSet("payments-api-h1", 1, "h1", "payments-api:v1")).
		WithStatusSubresource(&attestav1.RolloutGuard{}).
		WithIndex(&attestav1.RolloutGuard{}, TargetDeploymentIndex, guardTargetIndex).
		WithIndex(&appsv1.ReplicaSet{}, OwnerIndex, replicaSetOwnerIndex).
		Build()
	h.c = interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*attestav1.RolloutDecision); ok {
				h.mu.Lock()
				inject := h.createDecision
				h.mu.Unlock()
				if inject != nil {
					if err := inject(); err != nil {
						return err
					}
				}
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				h.mu.Lock()
				h.deploymentUpdateCount++
				inject := h.updateDeployment
				h.mu.Unlock()
				if inject != nil {
					if err := inject(); err != nil {
						return err
					}
				}
			}
			return c.Update(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "status" {
				h.mu.Lock()
				h.statusUpdates++
				inject := h.updateStatus
				h.mu.Unlock()
				if _, ok := obj.(*attestav1.RolloutGuard); ok && inject != nil {
					if err := inject(); err != nil {
						return err
					}
				}
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	h.r = &RolloutGuardReconciler{Client: h.c, Recorder: h.events}
	return h
}

func healthyDeployment(generation int64, image string) *appsv1.Deployment {
	replicas := int32(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "payments-api", Namespace: testNamespace, UID: testUID, Generation: generation,
			Annotations: map[string]string{revisionAnnotation: "1", rollbackGuardsAnnotation: testGuardName},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "payments-api"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "payments-api"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: image}}},
			},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: generation, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2},
	}
}

func replicaSet(name string, revision int, hash, image string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNamespace,
			UID:         types.UID(name),
			Labels:      map[string]string{"app": "payments-api", podTemplateHashLabel: hash},
			Annotations: map[string]string{revisionAnnotation: strconv.Itoa(revision)},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "payments-api", UID: testUID,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "payments-api"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "payments-api", podTemplateHashLabel: hash}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: image}}},
			},
		},
	}
}

// reconcile runs one pass and returns its raw error, before Reconcile turns
// stale reads into a quiet requeue.
func (h *harness) reconcile() error {
	h.t.Helper()
	_, err := h.r.reconcile(h.ctx, ctrl.Request{NamespacedName: testGuardKey})
	return err
}

func (h *harness) mustReconcile() {
	h.t.Helper()
	if err := h.reconcile(); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

func (h *harness) deployment() *appsv1.Deployment {
	h.t.Helper()
	var d appsv1.Deployment
	if err := h.c.Get(h.ctx, types.NamespacedName{Namespace: testNamespace, Name: "payments-api"}, &d); err != nil {
		h.t.Fatal(err)
	}
	return &d
}

func (h *harness) image() string {
	return h.deployment().Spec.Template.Spec.Containers[0].Image
}

// setDeployment stands in for the Deployment controller and kubectl: it
// rewrites the stored Deployment, status included.
func (h *harness) setDeployment(mutate func(*appsv1.Deployment)) {
	h.t.Helper()
	d := h.deployment()
	mutate(d)
	status := d.Status
	if err := h.c.Update(h.ctx, d); err != nil {
		h.t.Fatal(err)
	}
	// The fake API server, like a real one, ignores status on update.
	d.Status = status
	if err := h.c.Status().Update(h.ctx, d); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) replicaSet(name string) *appsv1.ReplicaSet {
	h.t.Helper()
	var rs appsv1.ReplicaSet
	if err := h.c.Get(h.ctx, types.NamespacedName{Namespace: testNamespace, Name: name}, &rs); err != nil {
		h.t.Fatal(err)
	}
	return &rs
}

func (h *harness) updateReplicaSet(rs *appsv1.ReplicaSet) {
	h.t.Helper()
	if err := h.c.Update(h.ctx, rs); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) addReplicaSet(rs *appsv1.ReplicaSet) {
	h.t.Helper()
	if err := h.c.Create(h.ctx, rs); err != nil {
		h.t.Fatal(err)
	}
}

// startBadRollout moves the Deployment to image v2 on revision 2 and reports
// ProgressDeadlineExceeded at the new generation.
func (h *harness) startBadRollout() {
	h.t.Helper()
	h.addReplicaSet(replicaSet("payments-api-h2", 2, "h2", "payments-api:v2"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 2
		d.Annotations[revisionAnnotation] = "2"
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 2, UnavailableReplicas: 1, Conditions: progressDeadlineExceeded()}
	})
}

func (h *harness) guard() *attestav1.RolloutGuard {
	h.t.Helper()
	var g attestav1.RolloutGuard
	if err := h.c.Get(h.ctx, testGuardKey, &g); err != nil {
		h.t.Fatal(err)
	}
	return &g
}

// updateGuard edits the guard's spec as a map, the way a user's manifest
// would set it, and stores it with a new generation.
func (h *harness) updateGuard(mutate func(obj map[string]any)) {
	h.t.Helper()
	g := h.guard()
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(g)
	if err != nil {
		h.t.Fatal(err)
	}
	mutate(obj)
	var next attestav1.RolloutGuard
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj, &next); err != nil {
		h.t.Fatal(err)
	}
	next.Generation = g.Generation + 1
	if err := h.c.Update(h.ctx, &next); err != nil {
		h.t.Fatal(err)
	}
}

// status is the stored status as the API server would serve it.
func (h *harness) status() map[string]any {
	h.t.Helper()
	g := h.guard()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&g.Status)
	if err != nil {
		h.t.Fatal(err)
	}
	return m
}

func (h *harness) field(name string) string {
	h.t.Helper()
	v, _ := h.status()[name].(string)
	return v
}

func (h *harness) condition(conditionType string) map[string]any {
	h.t.Helper()
	conds, _ := h.status()["conditions"].([]any)
	for _, raw := range conds {
		if c, ok := raw.(map[string]any); ok && c["type"] == conditionType {
			return c
		}
	}
	return nil
}

func (h *harness) deploymentUpdates() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.deploymentUpdateCount
}

func (h *harness) statusPatches() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.statusUpdates
}

func (h *harness) drainEvents() []string {
	var out []string
	for {
		select {
		case e := <-h.events.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestReconcileRecordsHealthyRevision(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()

	if got := h.field("phase"); got != PhaseHealthy {
		t.Fatalf("phase = %s, want %s", got, PhaseHealthy)
	}
	if got := h.field("healthyTemplateHash"); got != "h1" {
		t.Fatalf("healthyTemplateHash = %q, want h1", got)
	}
	if got := h.field("healthyRevision"); got != "1" {
		t.Fatalf("healthyRevision = %q, want 1", got)
	}
}

func TestReconcileRollsBackToLastHealthyTemplate(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile() // records v1 as healthy
	h.startBadRollout()
	h.mustReconcile()

	d := h.deployment()
	if got := d.Spec.Template.Spec.Containers[0].Image; got != "payments-api:v1" {
		t.Fatalf("image = %s, want payments-api:v1", got)
	}
	if _, ok := d.Spec.Template.Labels[podTemplateHashLabel]; ok {
		t.Fatal("restored template must not carry pod-template-hash")
	}
	for key := range d.Spec.Template.Annotations {
		if strings.HasPrefix(key, "attesta.io/") {
			t.Fatalf("restored template carries %s; audit annotations belong on the Deployment so the healthy ReplicaSet is reused", key)
		}
	}
	if d.Annotations[rollbackFromAnnotation] != "2" || d.Annotations[rollbackToAnnotation] != "1" || d.Annotations[rollbackGuardAnnotation] != "payments-api-rollout" {
		t.Fatalf("rollback annotations = %v", d.Annotations)
	}
	if got := h.field("phase"); got != PhaseRolledBack {
		t.Fatalf("phase = %s, want %s", got, PhaseRolledBack)
	}
	if got := h.field("rollbackRevision"); got != "1" {
		t.Fatalf("rollbackRevision = %q, want 1", got)
	}
	if c := h.condition(ConditionRolledBack); c == nil || c["status"] != "True" {
		t.Fatalf("RolledBack condition = %v, want True", c)
	}
}

// Audit C-3: after a rollback the Deployment controller adopts the healthy
// ReplicaSet under a new, higher revision. A stale ProgressDeadlineExceeded at
// that generation must not trigger a second rollback onto the bad revision.
func TestReconcileDoesNotRollBackTwice(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.mustReconcile()
	if h.deploymentUpdates() != 2 { // startBadRollout's own update + the rollback
		t.Fatalf("expected exactly one rollback update, got %d deployment updates", h.deploymentUpdates()-1)
	}

	rs1 := h.replicaSet("payments-api-h1")
	rs1.Annotations[revisionAnnotation] = "3"
	h.updateReplicaSet(rs1)
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 3
		d.Annotations[revisionAnnotation] = "3"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 1, Conditions: progressDeadlineExceeded()}
	})
	updatesBefore := h.deploymentUpdates()
	h.mustReconcile()
	h.mustReconcile()

	if got := h.image(); got != "payments-api:v1" {
		t.Fatalf("image = %s, want payments-api:v1 (controller rolled back onto the bad revision)", got)
	}
	if h.deploymentUpdates() != updatesBefore {
		t.Fatalf("controller updated the deployment %d more times", h.deploymentUpdates()-updatesBefore)
	}
	if got := h.field("reason"); got != reasonHealthyRevFailing {
		t.Fatalf("reason = %s, want %s", got, reasonHealthyRevFailing)
	}
}

// Audit C-1.
func TestReconcileNeverActsOnPausedDeployment(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.addReplicaSet(replicaSet("payments-api-h2", 2, "h2", "payments-api:v2"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 2
		d.Spec.Paused = true
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 2}
	})
	updatesBefore := h.deploymentUpdates()
	for i := 0; i < 3; i++ {
		h.mustReconcile()
	}
	if h.deploymentUpdates() != updatesBefore || h.image() != "payments-api:v2" {
		t.Fatalf("paused deployment was modified: image=%s", h.image())
	}
	if got := h.field("phase"); got != PhasePaused {
		t.Fatalf("phase = %s, want %s", got, PhasePaused)
	}
}

// Audit C-2.
func TestReconcileTreatsRolloutStartAsProgressing(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.addReplicaSet(replicaSet("payments-api-h2", 2, "h2", "payments-api:v2"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 2
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 2, UpdatedReplicas: 0, AvailableReplicas: 2, UnavailableReplicas: 1}
	})
	updatesBefore := h.deploymentUpdates()
	h.mustReconcile()
	if h.deploymentUpdates() != updatesBefore {
		t.Fatal("a healthy rollout's first status sync triggered a rollback")
	}
	if got := h.field("phase"); got != PhaseProgressing {
		t.Fatalf("phase = %s, want %s", got, PhaseProgressing)
	}
}

// Audit C-4.
func TestReconcileIgnoresStaleProgressDeadline(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.addReplicaSet(replicaSet("payments-api-h2", 2, "h2", "payments-api:v2"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 2
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Conditions: progressDeadlineExceeded()}
	})
	updatesBefore := h.deploymentUpdates()
	h.mustReconcile()
	if h.deploymentUpdates() != updatesBefore {
		t.Fatal("a condition from an older generation triggered a rollback")
	}
}

// Audit C-5.
func TestReconcileRollbackIsOptIn(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mustReconcile()
	h.startBadRollout()
	updatesBefore := h.deploymentUpdates()
	h.mustReconcile()
	if h.deploymentUpdates() != updatesBefore {
		t.Fatal("guard without spec.rollback.enabled rolled back")
	}
	if got := h.field("phase"); got != PhaseBlocked {
		t.Fatalf("phase = %s, want %s", got, PhaseBlocked)
	}
}

func TestReconcileWithoutHealthyRevisionDoesNotRollBack(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.startBadRollout() // the guard's first observation is already blocked
	updatesBefore := h.deploymentUpdates()
	h.mustReconcile()
	if h.deploymentUpdates() != updatesBefore {
		t.Fatal("rolled back without a recorded healthy revision")
	}
	if got := h.field("reason"); got != reasonNoHealthyRevision {
		t.Fatalf("reason = %s, want %s", got, reasonNoHealthyRevision)
	}
}

// Audit C-11.
func TestReconcileReturnsErrorWhenRollbackFails(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.updateDeployment = func() error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "payments-api", nil)
	}
	err := h.reconcile()
	if err == nil {
		t.Fatal("expected an error so the workqueue retries the rollback")
	}
	if isStaleRead(err) {
		t.Fatalf("a forbidden update is a real failure, not a stale read: %v", err)
	}
	if got := h.field("phase"); got != PhaseRollbackFailed {
		t.Fatalf("phase = %s, want %s", got, PhaseRollbackFailed)
	}
}

// Audit C-7: a rollback decided on a cached Deployment that has since changed
// fails the update precondition. That is retried quietly, not reported as a
// failed rollback.
func TestReconcileRetriesRollbackOnConflict(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.updateDeployment = func() error {
		return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "payments-api", nil)
	}
	err := h.reconcile()
	if !isStaleRead(err) {
		t.Fatalf("expected a stale-read error to requeue, got %v", err)
	}
	if got := h.field("phase"); got != PhaseBlocked {
		t.Fatalf("phase = %s, want %s", got, PhaseBlocked)
	}
	if c := h.condition(ConditionRolledBack); c != nil {
		t.Fatalf("a conflict must not record a rollback outcome: %v", c)
	}
}

// Audit C-8 and C-9.
func TestReconcileStatusAndEventsAreStable(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	firstTransition := h.condition(ConditionHealthy)["lastTransitionTime"]
	for i := 0; i < 3; i++ {
		h.mustReconcile()
	}
	if got := h.statusPatches(); got != 1 {
		t.Fatalf("status patched %d times for an unchanged deployment, want 1", got)
	}
	if got := h.condition(ConditionHealthy)["lastTransitionTime"]; got != firstTransition {
		t.Fatalf("lastTransitionTime moved from %v to %v without a state change", firstTransition, got)
	}
	if events := h.drainEvents(); len(events) != 1 {
		t.Fatalf("expected one event for one transition, got %d: %v", len(events), events)
	}
}

func TestReconcileKeepsRollbackHistoryAcrossPasses(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.mustReconcile()
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 3
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2}
	})
	h.mustReconcile()

	if got := h.field("phase"); got != PhaseProgressing {
		t.Fatalf("phase = %s, want %s", got, PhaseProgressing)
	}
	if c := h.condition(ConditionRolledBack); c == nil || c["status"] != "True" {
		t.Fatalf("RolledBack condition was dropped: %v", c)
	}
	if got := h.field("rollbackRevision"); got != "1" {
		t.Fatalf("rollbackRevision = %q, want 1", got)
	}
}

func TestReconcileRemovesLegacyStatusFields(t *testing.T) {
	legacy := map[string]any{"status": map[string]any{
		"phase":            "Healthy",
		"lastReconciledAt": "2026-07-01T00:00:00Z",
		"conditions": []any{map[string]any{
			"type": "Reconciled", "status": "True", "reason": "ReconcileComplete", "message": "RolloutGuard reconciled",
			"lastTransitionTime": "2026-07-01T00:00:00Z",
		}},
	}}
	h := newHarness(t, map[string]any{"enabled": true}, legacy)
	h.mustReconcile()
	if _, ok := h.status()["lastReconciledAt"]; ok {
		t.Fatal("lastReconciledAt survived the status patch")
	}
	if h.condition("Reconciled") != nil {
		t.Fatal("legacy Reconciled condition survived")
	}
	h.mustReconcile()
	if got := h.statusPatches(); got != 1 {
		t.Fatalf("status patched %d times, want 1", got)
	}
}

func TestReconcileReportsMissingDeployment(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	if err := h.c.Delete(h.ctx, h.deployment()); err != nil {
		t.Fatal(err)
	}
	h.mustReconcile()
	if got := h.field("reason"); got != "DeploymentMissing" {
		t.Fatalf("reason = %s, want DeploymentMissing", got)
	}
	if c := h.condition(ConditionDeploymentFound); c == nil || c["status"] != "False" {
		t.Fatalf("DeploymentFound condition = %v, want False", c)
	}
}

// Audit C-11: a failed rollback is returned as an error, so
// controller-runtime requeues it with backoff, and the retry completes it. A
// conflict is not an error: Reconcile requeues it quietly.
func TestFailedRollbackIsRetried(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	failures := 1
	h.updateDeployment = func() error {
		if failures > 0 {
			failures--
			return apierrors.NewServiceUnavailable("apiserver is restarting")
		}
		return nil
	}
	if _, err := h.r.Reconcile(h.ctx, ctrl.Request{NamespacedName: testGuardKey}); err == nil {
		t.Fatal("a failed rollback returned no error, so it would not be retried")
	}
	if got := h.field("phase"); got != PhaseRollbackFailed {
		t.Fatalf("phase = %s after a failed update, want %s", got, PhaseRollbackFailed)
	}
	if _, err := h.r.Reconcile(h.ctx, ctrl.Request{NamespacedName: testGuardKey}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if h.image() != "payments-api:v1" {
		t.Fatalf("retry did not roll back: image = %s", h.image())
	}
	if got := h.field("phase"); got != PhaseRolledBack {
		t.Fatalf("phase = %s after the retry, want %s", got, PhaseRolledBack)
	}
	events := strings.Join(h.drainEvents(), "\n")
	if !strings.Contains(events, reasonRollbackFailed) || !strings.Contains(events, reasonRollbackTriggered) {
		t.Fatalf("expected RollbackFailed then RollbackTriggered events, got:\n%s", events)
	}

	h.startBadRolloutAgain()
	h.updateDeployment = func() error {
		return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "payments-api", nil)
	}
	res, err := h.r.Reconcile(h.ctx, ctrl.Request{NamespacedName: testGuardKey})
	if err != nil || res.RequeueAfter != staleRequeue {
		t.Fatalf("conflict: result %+v, error %v; want a quiet requeue after %s", res, err, staleRequeue)
	}
}

// startBadRolloutAgain blocks a third revision after a rollback.
func (h *harness) startBadRolloutAgain() {
	h.t.Helper()
	h.addReplicaSet(replicaSet("payments-api-h3", 4, "h3", "payments-api:v3"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 4
		d.Annotations[revisionAnnotation] = "4"
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v3"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 4, Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 2, Conditions: progressDeadlineExceeded()}
	})
}
