package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/prometheus"
	"github.com/gaurav-gs7/attesta/internal/slo"
)

// fakeSLO answers with fixed signals and remembers what it was asked.
type fakeSLO struct {
	signals slo.Signals
	err     error
	asked   []models.Service
}

func (f *fakeSLO) Signals(_ context.Context, s models.Service) (slo.Signals, error) {
	f.asked = append(f.asked, s)
	return f.signals, f.err
}

func f64(v float64) *float64 { return &v }

// signals with every value measured; budget is the remaining error budget.
func measured(budget float64) slo.Signals {
	return slo.Signals{
		ErrorRatePct: f64(0.1), LatencyP95Ms: f64(120), AvailabilityPct: f64(99.95), ErrorBudgetRemainingPct: f64(budget),
		BurnRates: map[string]*float64{"5m": f64(1), "30m": f64(1), "1h": f64(1), "6h": f64(1)},
		Queries:   map[string]string{"p95": "p95-query", "errors:30d": "e30d", "total:30d": "t30d", "errors:5m": "e5m", "total:5m": "t5m"},
	}
}

// gateHarness has a guard with spec.sloGate, a recorded healthy revision 1,
// and revision 2 rolling out (not yet complete).
func gateHarness(t *testing.T, source *fakeSLO, gate map[string]any) *harness {
	t.Helper()
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.updateGuard(func(obj map[string]any) { obj["spec"].(map[string]any)["sloGate"] = gate })
	h.r.SLO = source
	h.mustReconcile() // promotes revision 1
	h.addReplicaSet(replicaSet("payments-api-h2", 2, "h2", "payments-api:v2"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 2
		d.Annotations[revisionAnnotation] = "2"
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 2}
	})
	return h
}

func lastDecision(h *harness) attestav1.RolloutDecisionSpec {
	h.t.Helper()
	records := h.verifyChain()
	return bySequence(records)[int64(len(records))]
}

func TestSLOGatePausesARevisionWhileTheBudgetIsSpent(t *testing.T) {
	source := &fakeSLO{signals: measured(4)}
	h := gateHarness(t, source, map[string]any{"availabilityTarget": 99.9, "latencyP95TargetMs": int64(250)})
	h.mustReconcile()

	d := h.deployment()
	if !d.Spec.Paused || d.Annotations[pausedByAnnotation] != testGuardName || !strings.Contains(d.Annotations[pausedReasonAnnotation], "error budget remaining 4.0%") {
		t.Fatalf("deployment paused=%v annotations=%v, want paused by the gate", d.Spec.Paused, d.Annotations)
	}
	if got := source.asked; len(got) != 1 || got[0].Name != "payments-api" || got[0].SLOTarget != 99.9 || got[0].SLOLatencyP95Ms != 250 {
		t.Fatalf("SLOs measured for %+v", got)
	}
	if h.field("phase") != PhasePaused || h.field("reason") != reasonSLOGatePaused {
		t.Fatalf("phase/reason = %s/%s", h.field("phase"), h.field("reason"))
	}
	if c := h.condition(ConditionSLOGate); c == nil || c["status"] != "False" || c["reason"] != "Blocked" {
		t.Fatalf("SLOGate condition = %v", c)
	}

	paused := lastDecision(h)
	if paused.Action != attestav1.ActionPaused || paused.Trigger != "SLOGateBlocked" || paused.Deployment.TemplateHash != "h2" {
		t.Fatalf("decision = %s/%s on %s", paused.Action, paused.Trigger, paused.Deployment.TemplateHash)
	}
	budget := evidenceNamed(paused, "error-budget-remaining")
	if budget == nil || budget.Value != "4%" || budget.Passed == nil || *budget.Passed || !strings.Contains(budget.Query, "e30d") {
		t.Fatalf("budget evidence = %+v", budget)
	}

	// Still paused on the next passes, with the gate's reason, and not
	// measured again.
	h.mustReconcile()
	if h.field("reason") != reasonSLOGatePaused || len(source.asked) != 1 {
		t.Fatalf("reason %s after %d measurements", h.field("reason"), len(source.asked))
	}

	// Someone resumes the rollout. The gate has had its say on revision 2.
	h.setDeployment(func(d *appsv1.Deployment) { d.Spec.Paused = false })
	updates := h.deploymentUpdates()
	h.mustReconcile()
	h.mustReconcile()
	if h.deployment().Spec.Paused || h.deploymentUpdates() != updates || len(source.asked) != 1 {
		t.Fatal("the gate paused revision 2 again after it was resumed")
	}

	// A later pause is the person's: the gate never resumes it.
	h.setDeployment(func(d *appsv1.Deployment) { d.Spec.Paused = true })
	h.r.Now = func() time.Time { return time.Now().Add(time.Hour) }
	source.signals = measured(80)
	h.mustReconcile()
	if !h.deployment().Spec.Paused {
		t.Fatal("the gate resumed a pause it did not make")
	}
}

func TestSLOGateLetsARevisionThroughWithinBudget(t *testing.T) {
	source := &fakeSLO{signals: measured(80)}
	h := gateHarness(t, source, map[string]any{"availabilityTarget": 99.9})
	records := len(h.decisions())
	h.mustReconcile()
	if h.deployment().Spec.Paused {
		t.Fatal("paused a revision while the SLOs were met")
	}
	if c := h.condition(ConditionSLOGate); c == nil || c["status"] != "True" {
		t.Fatalf("SLOGate condition = %v", c)
	}
	if len(h.decisions()) != records {
		t.Fatal("an allowed revision was recorded as a decision")
	}
}

func TestSLOGateOnUnknownData(t *testing.T) {
	for _, tc := range []struct {
		onUnknown  string
		wantPaused bool
	}{{"", false}, {"Proceed", false}, {"Pause", true}} {
		t.Run("onUnknown="+tc.onUnknown, func(t *testing.T) {
			source := &fakeSLO{err: errors.New("connection refused")}
			gate := map[string]any{"availabilityTarget": 99.9}
			if tc.onUnknown != "" {
				gate["onUnknown"] = tc.onUnknown
			}
			h := gateHarness(t, source, gate)
			h.mustReconcile()
			if got := h.deployment().Spec.Paused; got != tc.wantPaused {
				t.Fatalf("paused = %v, want %v", got, tc.wantPaused)
			}
			if c := h.condition(ConditionSLOGate); c == nil || c["status"] != "Unknown" || !strings.Contains(c["message"].(string), "connection refused") {
				t.Fatalf("SLOGate condition = %v", c)
			}
		})
	}
}

func TestSLOGateNeedsTheDeploymentsConsent(t *testing.T) {
	source := &fakeSLO{signals: measured(4)}
	h := gateHarness(t, source, map[string]any{"availabilityTarget": 99.9})
	h.setDeployment(func(d *appsv1.Deployment) { delete(d.Annotations, rollbackGuardsAnnotation) })
	h.mustReconcile()
	h.mustReconcile()
	if h.deployment().Spec.Paused {
		t.Fatal("paused a Deployment that did not name the guard")
	}
	skipped := lastDecision(h)
	if skipped.Action != attestav1.ActionPauseSkipped || skipped.Reason != reasonNotAuthorized {
		t.Fatalf("decision = %s/%s, want PauseSkipped/%s", skipped.Action, skipped.Reason, reasonNotAuthorized)
	}
	var skips int
	for _, r := range h.decisions() {
		if r.Spec.Action == attestav1.ActionPauseSkipped {
			skips++
		}
	}
	if skips != 1 {
		t.Fatalf("%d PauseSkipped records, want 1", skips)
	}
}

// A revision that metric analysis blocks is rolled back; the gate does not
// pause it first.
func TestSLOGateStaysOutOfABlockedRollout(t *testing.T) {
	source := &fakeSLO{signals: measured(4)}
	h := gateHarness(t, source, map[string]any{"availabilityTarget": 99.9})
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 2, Conditions: progressDeadlineExceeded()}
	})
	h.mustReconcile()
	if h.deployment().Spec.Paused || h.image() != "payments-api:v1" || len(source.asked) != 0 {
		t.Fatalf("paused=%v image=%s measured=%d, want a rollback and no gate", h.deployment().Spec.Paused, h.image(), len(source.asked))
	}
}

// Analysis and the SLO status build their PromQL from the same metric
// names, so a service instrumented with other names is measured the same
// way by both.
func TestAnalysisUsesTheConfiguredMetricNames(t *testing.T) {
	cfg := AnalysisConfig{ServiceLabel: "payments-api", RevisionLabel: "pod_template_hash", Window: 300e9,
		Metrics: prometheus.Metrics{Requests: "rpc_total", Duration: "rpc_seconds", ServiceLabel: "app", StatusLabel: "code", ErrorStatus: "Unavailable|Internal"}}
	requests, errs, p95 := cfg.queries("h2")
	for got, want := range map[string]string{
		requests: `sum(increase(rpc_total{app="payments-api",pod_template_hash="h2"}[300s]))`,
		errs:     `sum(increase(rpc_total{app="payments-api",pod_template_hash="h2",code=~"Unavailable|Internal"}[300s]))`,
		p95:      `histogram_quantile(0.95, sum by (le) (rate(rpc_seconds_bucket{app="payments-api",pod_template_hash="h2"}[300s]))) * 1000`,
	} {
		if got != want {
			t.Errorf("query\n got %s\nwant %s", got, want)
		}
	}
}

func TestSLOGateThresholdsArePerGuard(t *testing.T) {
	// 4% of the budget left: blocked by the default 10% floor, allowed
	// by a guard that sets 2%.
	source := &fakeSLO{signals: measured(4)}
	h := gateHarness(t, source, map[string]any{"availabilityTarget": 99.9, "minErrorBudgetPct": 2.0})
	h.mustReconcile()
	if h.deployment().Spec.Paused {
		t.Fatal("paused at 4% remaining with a 2% floor")
	}

	// A slow burn of 3x over 6h and 30m: allowed by the default 6x, blocked
	// by a guard that sets 2x.
	burning := measured(80)
	burning.BurnRates["6h"], burning.BurnRates["30m"] = f64(3), f64(3)
	h = gateHarness(t, &fakeSLO{signals: burning}, map[string]any{"availabilityTarget": 99.9, "slowBurnRate": 2.0})
	h.mustReconcile()
	if !h.deployment().Spec.Paused || !strings.Contains(h.deployment().Annotations[pausedReasonAnnotation], "at or above 2.0x") {
		t.Fatalf("not paused on a 3x slow burn with a 2x threshold: %v", h.deployment().Annotations)
	}
	ev := evidenceNamed(lastDecision(h), "burn-rate-6h")
	if ev == nil || !strings.HasPrefix(ev.Threshold, "< 2x") || ev.Passed == nil || *ev.Passed {
		t.Fatalf("burn-rate evidence does not carry the guard's threshold: %+v", ev)
	}
}

func TestSLOGateResumesWhenTheSLOsRecover(t *testing.T) {
	source := &fakeSLO{signals: measured(4)}
	h := gateHarness(t, source, map[string]any{"availabilityTarget": 99.9, "recheckInterval": "30s"})
	clk := &clock{t: time.Now().UTC()}
	h.r.Now = clk.now
	h.mustReconcile()
	if !h.deployment().Spec.Paused {
		t.Fatal("not paused")
	}

	// Before the interval: not measured again.
	clk.advance(10 * time.Second)
	h.mustReconcile()
	if len(source.asked) != 1 {
		t.Fatalf("measured %d times before the recheck interval", len(source.asked))
	}

	// After it, still out of budget: stays paused, nothing new recorded.
	records := len(h.decisions())
	clk.advance(30 * time.Second)
	h.mustReconcile()
	if !h.deployment().Spec.Paused || len(source.asked) != 2 || len(h.decisions()) != records {
		t.Fatalf("paused=%v measured=%d records=%d", h.deployment().Spec.Paused, len(source.asked), len(h.decisions()))
	}
	if c := h.condition(ConditionSLOGate); c == nil || c["reason"] != "StillBlocked" {
		t.Fatalf("SLOGate condition = %v", c)
	}

	// The budget recovers: resumed, and on record.
	source.signals = measured(60)
	clk.advance(31 * time.Second)
	h.mustReconcile()
	d := h.deployment()
	if d.Spec.Paused || d.Annotations[resumedAtAnnotation] == "" {
		t.Fatal("not resumed after the SLOs recovered")
	}
	resumed := lastDecision(h)
	if resumed.Action != attestav1.ActionResumed || resumed.Trigger != "SLOGateAllowed" || resumed.Deployment.TemplateHash != "h2" {
		t.Fatalf("decision %s/%s on %s", resumed.Action, resumed.Trigger, resumed.Deployment.TemplateHash)
	}
	if ev := evidenceNamed(resumed, "error-budget-remaining"); ev == nil || ev.Value != "60%" || !*ev.Passed {
		t.Fatalf("resume evidence %+v", ev)
	}
	if g := h.guard().Status.SLOGate; g == nil || g.Paused || !g.Resumed {
		t.Fatalf("status.sloGate = %+v", g)
	}

	// And it stays resumed.
	clk.advance(time.Minute)
	h.mustReconcile()
	if h.deployment().Spec.Paused {
		t.Fatal("paused the resumed revision again")
	}
}

func TestSLOGateLeavesResumingToAPersonWhenAskedTo(t *testing.T) {
	source := &fakeSLO{signals: measured(4)}
	h := gateHarness(t, source, map[string]any{"availabilityTarget": 99.9, "resume": "Manual"})
	clk := &clock{t: time.Now().UTC()}
	h.r.Now = clk.now
	h.mustReconcile()
	source.signals = measured(60)
	clk.advance(10 * time.Minute)
	h.mustReconcile()
	if !h.deployment().Spec.Paused || len(source.asked) != 1 {
		t.Fatal("resumed a manual-resume gate")
	}
	if !strings.Contains(h.field("message"), "kubectl rollout resume") {
		t.Fatalf("message does not say how to resume: %s", h.field("message"))
	}
}

// switchableSLO is a fakeSLO whose signals change while a manager runs.
type switchableSLO struct {
	mu      sync.Mutex
	signals slo.Signals
}

func (s *switchableSLO) set(signals slo.Signals) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signals = signals
}

func (s *switchableSLO) Signals(context.Context, models.Service) (slo.Signals, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.signals, nil
}
