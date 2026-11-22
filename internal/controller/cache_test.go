package controller

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

// Audit C-7: Deployment and ReplicaSet events find their guards through the
// informer index. Handlers make no API calls.
func TestDeploymentEventsUseTheGuardIndex(t *testing.T) {
	h := newHarness(t, nil, nil)
	other := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "attesta.io/v1", "kind": "RolloutGuard",
		"metadata": map[string]any{"name": "billing-rollout", "namespace": testNamespace},
		"spec":     map[string]any{"deploymentRef": "billing"},
	}}
	if _, err := h.dyn.Resource(rolloutGuardGVR).Namespace(testNamespace).Create(h.ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	h.syncCaches()
	h.dyn.ClearActions()
	h.kube.ClearActions()

	h.ctrl.enqueueDeployment(h.deploymentFromCache())
	if got := h.ctrl.queue.Len(); got != 1 {
		t.Fatalf("queue length = %d, want 1 (only the guard targeting payments-api)", got)
	}
	key, _ := h.ctrl.queue.Get()
	h.ctrl.queue.Done(key)
	if key != testGuardKey {
		t.Fatalf("queued %q, want %q", key, testGuardKey)
	}

	rs := replicaSet("payments-api-h2", 2, "h2", "payments-api:v2")
	h.ctrl.enqueueReplicaSetOwner(rs)
	if got := h.ctrl.queue.Len(); got != 1 {
		t.Fatalf("replicaset event queued %d guards, want 1", got)
	}

	if n := len(h.dyn.Actions()) + len(h.kube.Actions()); n != 0 {
		t.Fatalf("event handlers made %d API calls, want 0: %v %v", n, h.dyn.Actions(), h.kube.Actions())
	}
}

func (h *harness) deploymentFromCache() *appsv1.Deployment {
	h.t.Helper()
	obj, ok, err := h.ctrl.deployments.GetIndexer().GetByKey(testNamespace + "/payments-api")
	if err != nil || !ok {
		h.t.Fatalf("deployment not cached: ok=%v err=%v", ok, err)
	}
	return obj.(*appsv1.Deployment)
}

func TestReplicaSetStatusChurnDoesNotEnqueue(t *testing.T) {
	prev := replicaSet("payments-api-h1", 1, "h1", "payments-api:v1")
	prev.ResourceVersion = "10"
	next := prev.DeepCopy()
	next.ResourceVersion = "11"
	next.Status.ReadyReplicas = 2
	if replicaSetIdentityChanged(prev, next) {
		t.Fatal("a status-only ReplicaSet update should not enqueue")
	}
	next.Annotations[revisionAnnotation] = "3"
	if !replicaSetIdentityChanged(prev, next) {
		t.Fatal("a revision change must enqueue")
	}
}

// A Deployment that reports a revision the ReplicaSet cache has not seen must
// not be acted on: the newest cached ReplicaSet would be mistaken for the
// current template.
func TestReconcileWaitsForReplicaSetCache(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.syncCaches()
	if err := h.ctrl.replicaSets.GetIndexer().Delete(h.replicaSetFromCache("payments-api-h2")); err != nil {
		t.Fatal(err)
	}
	h.kube.ClearActions()

	err := h.ctrl.Reconcile(h.ctx, testGuardKey)
	if !errors.Is(err, errCacheBehind) {
		t.Fatalf("expected errCacheBehind, got %v", err)
	}
	if got := h.deploymentUpdates(); got != 0 {
		t.Fatalf("rolled back on a stale ReplicaSet view: %d deployment updates", got)
	}
	h.mustReconcile() // caches caught up
	if h.image() != "payments-api:v1" {
		t.Fatalf("image = %s after the cache caught up, want payments-api:v1", h.image())
	}
}

func (h *harness) replicaSetFromCache(name string) any {
	h.t.Helper()
	obj, ok, err := h.ctrl.replicaSets.GetIndexer().GetByKey(testNamespace + "/" + name)
	if err != nil || !ok {
		h.t.Fatalf("replicaset %s not cached", name)
	}
	return obj
}

// The status patch carries the cached resourceVersion, so a reconcile working
// from a stale guard gets a conflict instead of overwriting newer status.
func TestStatusPatchCarriesResourceVersionPrecondition(t *testing.T) {
	h := newHarness(t, nil, map[string]any{"metadata": map[string]any{
		"name": "payments-api-rollout", "namespace": testNamespace, "generation": int64(1), "resourceVersion": "42",
	}})
	var payload []byte
	h.dyn.PrependReactor("patch", "rolloutguards", func(a k8stesting.Action) (bool, runtime.Object, error) {
		payload = a.(k8stesting.PatchAction).GetPatch()
		return false, nil, nil
	})
	h.mustReconcile()
	var body struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decode patch %s: %v", payload, err)
	}
	if body.Metadata.ResourceVersion != "42" {
		t.Fatalf("status patch precondition = %q, want 42: %s", body.Metadata.ResourceVersion, payload)
	}
}

// Steady state against informers: resyncs reconcile every guard, but after
// the initial LIST and WATCH the controller makes no reads at all.
func TestSteadyStateMakesNoReads(t *testing.T) {
	if testing.Short() {
		t.Skip("runs informers through several one-second resyncs")
	}
	guard := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "attesta.io/v1", "kind": "RolloutGuard",
		"metadata": map[string]any{"name": "payments-api-rollout", "namespace": testNamespace, "generation": int64(1)},
		"spec":     map[string]any{"serviceRef": "payments-api"},
	}}
	kube := kubefake.NewSimpleClientset(
		healthyDeployment(1, "payments-api:v1"),
		replicaSet("payments-api-h1", 1, "h1", "payments-api:v1"),
	)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{rolloutGuardGVR: "RolloutGuardList"}, guard)
	ctrl := NewRolloutGuardControllerWithClients(kube, dyn, Options{Recorder: record.NewFakeRecorder(100), Resync: time.Second})

	before := successfulReconciles(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(ctx, 1) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		// Read the tracker directly so the poll is not recorded as an action.
		obj, err := dyn.Tracker().Get(rolloutGuardGVR, testNamespace, "payments-api-rollout")
		if u, ok := obj.(*unstructured.Unstructured); err == nil && ok && u.Object["status"] != nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("guard was never reconciled")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// client-go clamps informer resync to at least one second.
	time.Sleep(3500 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	reads := map[string]int{}
	for _, a := range append(kube.Actions(), dyn.Actions()...) {
		switch a.GetVerb() {
		case "get", "list":
			reads[a.GetVerb()+" "+a.GetResource().Resource]++
		}
	}
	want := map[string]int{"list rolloutguards": 1, "list deployments": 1, "list replicasets": 1}
	if reconciles := successfulReconciles(t) - before; reconciles < 3 {
		t.Fatalf("only %v reconciles ran; resyncs did not drive the controller", reconciles)
	}
	for k, v := range reads {
		if want[k] != v {
			t.Fatalf("reads = %v, want only the initial lists %v", reads, want)
		}
	}
}

func successfulReconciles(t *testing.T) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(reconcileTotal)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "result" && l.GetValue() == "success" {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// Audit C-10: Run returns only after in-flight reconciles finish, so the
// caller can hand leadership over without two replicas overlapping.
func TestRunWaitsForInFlightReconcile(t *testing.T) {
	guard := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "attesta.io/v1", "kind": "RolloutGuard",
		"metadata": map[string]any{"name": "payments-api-rollout", "namespace": testNamespace, "generation": int64(1)},
		"spec":     map[string]any{"serviceRef": "payments-api"},
	}}
	kube := kubefake.NewSimpleClientset(
		healthyDeployment(1, "payments-api:v1"),
		replicaSet("payments-api-h1", 1, "h1", "payments-api:v1"),
	)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{rolloutGuardGVR: "RolloutGuardList"}, guard)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	dyn.PrependReactor("patch", "rolloutguards", func(k8stesting.Action) (bool, runtime.Object, error) {
		once.Do(func() { close(entered) })
		<-release // a status write that is slow to return
		return false, nil, nil
	})
	ctrl := NewRolloutGuardControllerWithClients(kube, dyn, Options{Recorder: record.NewFakeRecorder(100)})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(ctx, 1) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile never reached the status patch")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned (%v) while a reconcile was still in flight", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the reconcile finished")
	}
}
