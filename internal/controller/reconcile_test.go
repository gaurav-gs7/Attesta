package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

const (
	testNamespace = "platform"
	testGuardKey  = testNamespace + "/payments-api-rollout"
	testUID       = types.UID("deployment-uid")
)

type harness struct {
	t      *testing.T
	ctx    context.Context
	ctrl   *RolloutGuardController
	kube   *kubefake.Clientset
	dyn    *dynamicfake.FakeDynamicClient
	events *record.FakeRecorder
}

// newHarness starts with a healthy Deployment on revision 1 (image v1) and a
// guard. spec is the guard's spec; nil rollback means rollback is not set.
func newHarness(t *testing.T, rollback map[string]any, extraGuardFields map[string]any) *harness {
	t.Helper()
	spec := map[string]any{"serviceRef": "payments-api", "strategy": "canary"}
	if rollback != nil {
		spec["rollback"] = rollback
	}
	guard := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "attesta.io/v1",
		"kind":       "RolloutGuard",
		"metadata":   map[string]any{"name": "payments-api-rollout", "namespace": testNamespace, "generation": int64(1)},
		"spec":       spec,
	}}
	for k, v := range extraGuardFields {
		guard.Object[k] = v
	}
	kube := kubefake.NewSimpleClientset(
		healthyDeployment(1, "payments-api:v1"),
		replicaSet("payments-api-h1", 1, "h1", "payments-api:v1"),
	)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{rolloutGuardGVR: "RolloutGuardList"}, guard)
	events := record.NewFakeRecorder(100)
	ctrl := NewRolloutGuardControllerWithClients(kube, dyn, Options{Recorder: events})
	return &harness{t: t, ctx: context.Background(), ctrl: ctrl, kube: kube, dyn: dyn, events: events}
}

func healthyDeployment(generation int64, image string) *appsv1.Deployment {
	replicas := int32(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "payments-api", Namespace: testNamespace, UID: testUID, Generation: generation,
			Annotations: map[string]string{revisionAnnotation: "1"},
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

func (h *harness) reconcile() error {
	h.t.Helper()
	h.syncCaches()
	return h.ctrl.Reconcile(h.ctx, testGuardKey)
}

// syncCaches stands in for the informers: it loads the fake API server's
// current objects into the controller's caches, through the same indexers the
// informers use. Reconcile reads only from these caches.
func (h *harness) syncCaches() {
	h.t.Helper()
	guards, err := h.dyn.Resource(rolloutGuardGVR).Namespace(testNamespace).List(h.ctx, metav1.ListOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	var guardObjs []any
	for i := range guards.Items {
		guardObjs = append(guardObjs, &guards.Items[i])
	}
	deployments, err := h.kube.AppsV1().Deployments(testNamespace).List(h.ctx, metav1.ListOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	var deploymentObjs []any
	for i := range deployments.Items {
		deploymentObjs = append(deploymentObjs, &deployments.Items[i])
	}
	replicaSets, err := h.kube.AppsV1().ReplicaSets(testNamespace).List(h.ctx, metav1.ListOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	var rsObjs []any
	for i := range replicaSets.Items {
		rsObjs = append(rsObjs, &replicaSets.Items[i])
	}
	for _, step := range []struct {
		indexer interface{ Replace([]any, string) error }
		objs    []any
	}{
		{h.ctrl.informer.GetIndexer(), guardObjs},
		{h.ctrl.deployments.GetIndexer(), deploymentObjs},
		{h.ctrl.replicaSets.GetIndexer(), rsObjs},
	} {
		if err := step.indexer.Replace(step.objs, ""); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *harness) mustReconcile() {
	h.t.Helper()
	if err := h.reconcile(); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

func (h *harness) deployment() *appsv1.Deployment {
	h.t.Helper()
	d, err := h.kube.AppsV1().Deployments(testNamespace).Get(h.ctx, "payments-api", metav1.GetOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	return d
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
	if _, err := h.kube.AppsV1().Deployments(testNamespace).Update(h.ctx, d, metav1.UpdateOptions{}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) addReplicaSet(rs *appsv1.ReplicaSet) {
	h.t.Helper()
	if _, err := h.kube.AppsV1().ReplicaSets(testNamespace).Create(h.ctx, rs, metav1.CreateOptions{}); err != nil {
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

func (h *harness) status() map[string]any {
	h.t.Helper()
	g, err := h.dyn.Resource(rolloutGuardGVR).Namespace(testNamespace).Get(h.ctx, "payments-api-rollout", metav1.GetOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	s, _, _ := unstructured.NestedMap(g.Object, "status")
	return s
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
	n := 0
	for _, a := range h.kube.Actions() {
		if a.GetVerb() == "update" && a.GetResource().Resource == "deployments" {
			n++
		}
	}
	return n
}

func (h *harness) statusPatches() int {
	n := 0
	for _, a := range h.dyn.Actions() {
		if a.GetVerb() == "patch" && a.GetSubresource() == "status" {
			n++
		}
	}
	return n
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

	rs1, err := h.kube.AppsV1().ReplicaSets(testNamespace).Get(h.ctx, "payments-api-h1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rs1.Annotations[revisionAnnotation] = "3"
	if _, err := h.kube.AppsV1().ReplicaSets(testNamespace).Update(h.ctx, rs1, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
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
	h.kube.PrependReactor("update", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "payments-api", nil)
	})
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
	h.kube.PrependReactor("update", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "payments-api", nil)
	})
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
	if err := h.kube.AppsV1().Deployments(testNamespace).Delete(h.ctx, "payments-api", metav1.DeleteOptions{}); err != nil {
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
