package controller

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// canaryHarness is a guard with spec.canary over the usual payments-api
// Deployment (2 replicas, revision 1, hash h1), a clock, and a fake
// Prometheus. Steps are 10% and 50%, each held 1m.
func canaryHarness(t *testing.T, canary map[string]any) (*harness, *clock, *fakeProm) {
	t.Helper()
	h := newHarness(t, nil, nil)
	if canary == nil {
		canary = map[string]any{}
	}
	if canary["steps"] == nil {
		canary["steps"] = []any{map[string]any{"weight": int64(10), "pause": "1m"}, map[string]any{"weight": int64(50), "pause": "1m"}}
	}
	h.updateGuard(func(obj map[string]any) { obj["spec"].(map[string]any)["canary"] = canary })
	clk := &clock{t: time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)}
	prom := &fakeProm{byHash: map[string]sample{"h1": healthySample(), "h2": healthySample()}}
	h.r.Now = clk.now
	return h, clk, prom
}

func (h *harness) primary() *appsv1.Deployment {
	h.t.Helper()
	var d appsv1.Deployment
	if err := h.c.Get(h.ctx, types.NamespacedName{Namespace: testNamespace, Name: "payments-api-primary"}, &d); err != nil {
		h.t.Fatal(err)
	}
	return &d
}

// primaryRolledOut stands in for the Deployment controller finishing the
// primary's rollout.
func (h *harness) primaryRolledOut() {
	h.t.Helper()
	p := h.primary()
	n := *p.Spec.Replicas
	p.Status = appsv1.DeploymentStatus{ObservedGeneration: p.Generation, Replicas: n, UpdatedReplicas: n, ReadyReplicas: n, AvailableReplicas: n}
	if err := h.c.Status().Update(h.ctx, p); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) canaryReplicas() int32 { return *h.deployment().Spec.Replicas }

func (h *harness) canaryStatus() *attestav1.CanaryStatus { return h.guard().Status.Canary }

// canaryAvailable stands in for the canary's pods becoming available.
func (h *harness) canaryAvailable() {
	h.setDeployment(func(d *appsv1.Deployment) {
		n := *d.Spec.Replicas
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: n, UpdatedReplicas: n, ReadyReplicas: n, AvailableReplicas: n}
	})
}

// newRevision changes the guarded Deployment's template to v2 (hash h2).
func (h *harness) newRevision() {
	h.addReplicaSet(replicaSet("payments-api-h2", 2, "h2", "payments-api:v2"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation++
		d.Annotations[revisionAnnotation] = "2"
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation}
	})
}

func actions(records []attestav1.RolloutDecision) string {
	out := make([]string, len(records))
	for _, r := range records {
		out[r.Spec.Sequence-1] = r.Spec.Action
	}
	return strings.Join(out, ",")
}

// initialise takes a canary guard from nothing to Idle on revision 1.
func initialise(t *testing.T, h *harness) {
	t.Helper()
	h.mustReconcile() // creates the primary
	p := h.primary()
	if *p.Spec.Replicas != 2 || p.Spec.Template.Labels[trackLabel] != "primary" || p.Spec.Selector.MatchLabels[trackLabel] != "primary" ||
		p.Annotations[primaryOfAnnotation] != "payments-api" || p.Spec.Template.Spec.Containers[0].Image != "payments-api:v1" {
		t.Fatalf("primary %+v", p.Spec)
	}
	h.mustReconcile()
	if h.canaryStatus().Phase != CanaryInitializing || h.canaryReplicas() != 2 {
		t.Fatal("scaled the canary down before the primary was available")
	}
	h.primaryRolledOut()
	h.mustReconcile()
	if c := h.canaryStatus(); c.Phase != CanaryIdle || c.PrimaryTemplateHash != "h1" || h.canaryReplicas() != 0 {
		t.Fatalf("after initialisation: %+v, canary replicas %d", c, h.canaryReplicas())
	}
}

func TestCanaryStepsThroughAndPromotes(t *testing.T) {
	h, clk, _ := canaryHarness(t, nil)
	initialise(t, h)

	h.newRevision()
	h.mustReconcile()
	c := h.canaryStatus()
	// Without a router the share is the canary's fraction of the pods:
	// one canary pod beside two primary pods.
	if c.Phase != CanaryProgressing || c.Step != 0 || c.CanaryTemplateHash != "h2" || h.canaryReplicas() != 1 || c.Weight != 33 {
		t.Fatalf("after start: %+v, canary replicas %d", c, h.canaryReplicas())
	}

	h.mustReconcile()
	if h.canaryStatus().Step != 0 {
		t.Fatal("advanced before the canary's pods were available")
	}
	h.canaryAvailable()
	clk.advance(30 * time.Second)
	h.mustReconcile()
	if h.canaryStatus().Step != 0 {
		t.Fatal("advanced before the step's pause")
	}
	clk.advance(31 * time.Second)
	h.mustReconcile()
	if c := h.canaryStatus(); c.Step != 1 || h.canaryReplicas() != 2 || c.Weight != 50 {
		t.Fatalf("after step 1: %+v, canary replicas %d", c, h.canaryReplicas())
	}

	h.canaryAvailable()
	clk.advance(61 * time.Second)
	h.mustReconcile()
	if c := h.canaryStatus(); c.Phase != CanaryPromoting || h.primary().Spec.Template.Spec.Containers[0].Image != "payments-api:v2" {
		t.Fatalf("not promoting into the primary: %+v", c)
	}
	// The API server bumps the primary's generation for the new template;
	// the fake client does not.
	p := h.primary()
	p.Generation++
	if err := h.c.Update(h.ctx, p); err != nil {
		t.Fatal(err)
	}
	h.mustReconcile()
	if h.canaryStatus().Phase != CanaryPromoting || h.canaryReplicas() != 2 {
		t.Fatal("finished promotion before the primary rolled out")
	}
	h.primaryRolledOut()
	h.mustReconcile()
	c = h.canaryStatus()
	if c.Phase != CanaryIdle || c.PrimaryTemplateHash != "h2" || h.canaryReplicas() != 0 || h.field("healthyTemplateHash") != "h2" {
		t.Fatalf("after promotion: %+v, canary replicas %d", c, h.canaryReplicas())
	}
	if got := actions(h.verifyChain()); got != "Promoted,CanaryStarted,StepAdvanced,Promoted" {
		t.Fatalf("decisions %s", got)
	}

	// At rest it stays at rest.
	clk.advance(time.Hour)
	h.mustReconcile()
	if h.canaryReplicas() != 0 || len(h.decisions()) != 4 {
		t.Fatal("an idle canary did something")
	}
}

func TestCanaryFailingItsChecksIsAborted(t *testing.T) {
	h, clk, prom := canaryHarness(t, nil)
	h.r.Prometheus = prom
	h.updateGuard(func(obj map[string]any) { obj["spec"].(map[string]any)["checks"] = checks(nil) })
	initialise(t, h)

	prom.set("h2", failingSample())
	h.newRevision()
	h.mustReconcile()
	h.canaryAvailable()
	for i := 0; i < 3 && h.canaryStatus().Phase == CanaryProgressing; i++ {
		clk.advance(31 * time.Second)
		h.mustReconcile()
	}
	c := h.canaryStatus()
	if c.Phase != CanaryAborted || c.AbortedTemplateHash != "h2" || h.canaryReplicas() != 0 {
		t.Fatalf("after failing checks: %+v, canary replicas %d", c, h.canaryReplicas())
	}
	if h.primary().Spec.Template.Spec.Containers[0].Image != "payments-api:v1" {
		t.Fatal("an aborted canary reached the primary")
	}
	records := h.verifyChain()
	aborted := bySequence(records)[int64(len(records))]
	if aborted.Action != attestav1.ActionCanaryAborted || aborted.Trigger != "AnalysisFailed" || aborted.RestoredTo.TemplateHash != "h1" {
		t.Fatalf("decision %s/%s", aborted.Action, aborted.Trigger)
	}
	if ev := evidenceNamed(aborted, "error-rate"); ev == nil || ev.Passed == nil || *ev.Passed {
		t.Fatalf("abort evidence %+v", ev)
	}

	// The aborted revision gets no traffic again; a new one does.
	clk.advance(time.Hour)
	h.mustReconcile()
	if h.canaryReplicas() != 0 {
		t.Fatal("the aborted revision was started again")
	}
}

func TestCanaryRoutesWithGatewayAPI(t *testing.T) {
	h, clk, _ := canaryHarness(t, map[string]any{"trafficRouting": map[string]any{"gatewayAPI": map[string]any{"httpRoute": "payments", "port": int64(8080)}}})
	route := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
		"metadata": map[string]any{"name": "payments", "namespace": testNamespace},
		"spec": map[string]any{"rules": []any{map[string]any{"backendRefs": []any{
			map[string]any{"name": "payments-api-primary", "port": int64(8080)},
			map[string]any{"name": "payments-api-canary", "port": int64(8080)},
		}}}},
	}}
	if err := h.c.Create(h.ctx, route); err != nil {
		t.Fatal(err)
	}
	weights := func() (primary, canary int64) {
		t.Helper()
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(httpRouteGVK)
		if err := h.c.Get(h.ctx, types.NamespacedName{Namespace: testNamespace, Name: "payments"}, got); err != nil {
			t.Fatal(err)
		}
		rules, _, _ := unstructured.NestedSlice(got.Object, "spec", "rules")
		refs := rules[0].(map[string]any)["backendRefs"].([]any)
		return refs[0].(map[string]any)["weight"].(int64), refs[1].(map[string]any)["weight"].(int64)
	}
	initialise(t, h)
	if p, c := weights(); p != 100 || c != 0 {
		t.Fatalf("at rest the route sends %d/%d", p, c)
	}
	var svc corev1.Service
	if err := h.c.Get(h.ctx, types.NamespacedName{Namespace: testNamespace, Name: "payments-api-primary"}, &svc); err != nil ||
		svc.Spec.Selector[trackLabel] != "primary" || svc.Spec.Ports[0].Port != 8080 {
		t.Fatalf("primary service %+v (%v)", svc.Spec, err)
	}

	h.newRevision()
	h.mustReconcile()
	if p, c := weights(); p != 90 || c != 10 || h.canaryStatus().Weight != 10 {
		t.Fatalf("first step routes %d/%d", p, c)
	}
	if err := h.c.Get(h.ctx, types.NamespacedName{Namespace: testNamespace, Name: "payments-api-canary"}, &svc); err != nil ||
		svc.Spec.Selector[podTemplateHashLabel] != "h2" {
		t.Fatalf("canary service selects %v", svc.Spec.Selector)
	}
	// The canary has the capacity for its share: ceil(2 x 10%) = 1 pod.
	if h.canaryReplicas() != 1 {
		t.Fatalf("canary replicas %d", h.canaryReplicas())
	}
	h.canaryAvailable()
	clk.advance(61 * time.Second)
	h.mustReconcile()
	if p, c := weights(); p != 50 || c != 50 {
		t.Fatalf("second step routes %d/%d", p, c)
	}
}

func TestCanaryWaitsForTheSLOGate(t *testing.T) {
	h, clk, _ := canaryHarness(t, nil)
	source := &fakeSLO{signals: measured(4)}
	h.r.SLO = source
	h.updateGuard(func(obj map[string]any) {
		obj["spec"].(map[string]any)["sloGate"] = map[string]any{"availabilityTarget": 99.9, "recheckInterval": "30s"}
	})
	initialise(t, h)

	h.newRevision()
	h.mustReconcile()
	if c := h.canaryStatus(); c.Phase != CanaryWaiting || h.canaryReplicas() != 0 {
		t.Fatalf("started a canary while the budget was spent: %+v", c)
	}
	clk.advance(10 * time.Second)
	h.mustReconcile()
	if len(source.asked) != 1 {
		t.Fatal("measured again before the recheck interval")
	}
	source.signals = measured(70)
	clk.advance(30 * time.Second)
	h.mustReconcile()
	if c := h.canaryStatus(); c.Phase != CanaryProgressing || h.canaryReplicas() != 1 {
		t.Fatalf("did not start after the budget recovered: %+v", c)
	}
	records := h.verifyChain()
	if got := actions(records); got != "Promoted,Paused,CanaryStarted" {
		t.Fatalf("decisions %s", got)
	}
	if started := bySequence(records)[3]; started.Trigger != "SLOGateAllowed" {
		t.Fatalf("start trigger %s", started.Trigger)
	}
}

// Every canary change needs the guarded Deployment's consent.
func TestCanaryNeedsConsent(t *testing.T) {
	h, _, _ := canaryHarness(t, nil)
	h.setDeployment(func(d *appsv1.Deployment) { delete(d.Annotations, rollbackGuardsAnnotation) })
	h.mustReconcile()
	var list appsv1.DeploymentList
	if err := h.c.List(h.ctx, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || h.field("reason") != reasonNotAuthorized {
		t.Fatalf("%d deployments, reason %s", len(list.Items), h.field("reason"))
	}
}
