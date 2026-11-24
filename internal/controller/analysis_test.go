package controller

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// sample is what the fake Prometheus reports for one candidate revision.
type sample struct {
	requests, errors, p95 float64
	noP95                 bool
}

// fakeProm answers analysis queries per pod_template_hash.
type fakeProm struct {
	mu      sync.Mutex
	byHash  map[string]sample
	err     error
	queries []string
}

var hashInQuery = regexp.MustCompile(`pod_template_hash="([^"]+)"`)

func (f *fakeProm) Query(_ context.Context, q string) (float64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q)
	if f.err != nil {
		return 0, false, f.err
	}
	hash := "service-wide"
	if m := hashInQuery.FindStringSubmatch(q); m != nil {
		hash = m[1]
	}
	s, ok := f.byHash[hash]
	if !ok {
		return 0, false, nil
	}
	switch {
	case strings.Contains(q, "histogram_quantile"):
		return s.p95, !s.noP95, nil
	case strings.Contains(q, `status=~"5.."`):
		return s.errors, true, nil
	default:
		return s.requests, true, nil
	}
}

func (f *fakeProm) set(hash string, s sample) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byHash[hash] = s
}

func (f *fakeProm) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queries)
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }
func healthySample() sample              { return sample{requests: 1000, errors: 2, p95: 120} }
func failingSample() sample              { return sample{requests: 1000, errors: 100, p95: 120} }
func lowTrafficSample() sample           { return sample{requests: 5, errors: 5, p95: 900} }
func checks(extra map[string]any) map[string]any {
	m := map[string]any{"errorRateThreshold": 1.0, "latencyP95ThresholdMs": int64(300), "interval": "30s", "window": "5m", "minRequests": int64(20), "successfulChecks": int64(3)}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// newAnalysisHarness returns a harness whose guard sets spec.checks and
// whose controller has a fake Prometheus and clock.
func newAnalysisHarness(t *testing.T, rollback map[string]any, checkSpec map[string]any, withProm bool) (*harness, *fakeProm, *clock) {
	t.Helper()
	h := newHarness(t, rollback, nil)
	h.updateGuard(func(obj map[string]any) {
		obj["spec"].(map[string]any)["checks"] = checkSpec
	})
	prom := &fakeProm{byHash: map[string]sample{"h1": healthySample()}}
	clk := &clock{t: time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)}
	if withProm {
		h.r.Prometheus = prom
	}
	h.r.Now = clk.now
	return h, prom, clk
}

func (h *harness) analysis(field string) any {
	a, _ := h.status()["analysis"].(map[string]any)
	if a == nil {
		return nil
	}
	return a[field]
}

// promote reconciles until the initial revision passes successfulChecks (3).
func promote(t *testing.T, h *harness, clk *clock) {
	t.Helper()
	for i := 0; i < 3; i++ {
		h.mustReconcile()
		clk.advance(30 * time.Second)
	}
	if got := h.field("healthyTemplateHash"); got != "h1" {
		t.Fatalf("initial revision not promoted after 3 passing checks: healthyTemplateHash=%q phase=%s", got, h.field("phase"))
	}
}

// rollOutV2 completes a rollout to v2 (revision 2, hash h2) as far as
// Kubernetes is concerned.
func rollOutV2(h *harness) {
	h.addReplicaSet(replicaSet("payments-api-h2", 2, "h2", "payments-api:v2"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 2
		d.Annotations[revisionAnnotation] = "2"
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}
	})
}

func TestAnalysisPromotesOnlyAfterSuccessfulChecks(t *testing.T) {
	h, prom, clk := newAnalysisHarness(t, map[string]any{"enabled": true}, checks(nil), true)

	h.mustReconcile()
	if got := h.field("phase"); got != PhaseVerifying {
		t.Fatalf("phase = %s, want %s while checks accumulate", got, PhaseVerifying)
	}
	if got := h.field("healthyTemplateHash"); got != "" {
		t.Fatalf("revision recorded healthy after one check: %q", got)
	}
	q := strings.Join(prom.queries, "\n")
	for _, want := range []string{`service="payments-api"`, `pod_template_hash="h1"`, "[300s]"} {
		if !strings.Contains(q, want) {
			t.Fatalf("queries missing %s:\n%s", want, q)
		}
	}

	queries := prom.count()
	h.mustReconcile() // inside the interval: no new check
	if prom.count() != queries {
		t.Fatal("analysis ran again before the interval elapsed")
	}

	clk.advance(30 * time.Second)
	h.mustReconcile()
	clk.advance(30 * time.Second)
	h.mustReconcile()
	if got := h.field("healthyTemplateHash"); got != "h1" {
		t.Fatalf("healthyTemplateHash = %q after 3 passes", got)
	}
	if got := h.field("phase"); got != PhaseHealthy {
		t.Fatalf("phase = %s, want %s", got, PhaseHealthy)
	}

	h.mustReconcile()
	if h.status()["analysis"] != nil {
		t.Fatal("analysis status should clear once the Deployment is on the healthy revision")
	}
	if c := h.condition(ConditionAnalysis); c == nil || c["reason"] != "OnHealthyRevision" {
		t.Fatalf("Analysis condition = %v", c)
	}
}

// Audit C-2 and C-5: a rollout Kubernetes considers complete is rolled back
// once its own metrics fail consecutiveFailures checks in a row.
func TestAnalysisRollsBackAfterConsecutiveFailures(t *testing.T) {
	h, prom, clk := newAnalysisHarness(t, map[string]any{"enabled": true, "consecutiveFailures": int64(2)}, checks(nil), true)
	promote(t, h, clk)
	prom.set("h2", failingSample())
	rollOutV2(h)

	h.mustReconcile()
	if h.image() != "payments-api:v2" {
		t.Fatal("rolled back after a single failed check")
	}
	if got := h.analysis("consecutiveFailures"); got != int64(1) {
		t.Fatalf("consecutiveFailures = %v, want 1", got)
	}
	if got := h.field("phase"); got != PhaseVerifying {
		t.Fatalf("phase = %s, want %s", got, PhaseVerifying)
	}
	h.mustReconcile() // same interval: must not count twice
	if got := h.analysis("consecutiveFailures"); got != int64(1) {
		t.Fatalf("a reconcile inside the interval counted as a check: %v", got)
	}

	clk.advance(30 * time.Second)
	h.mustReconcile()
	d := h.deployment()
	if got := d.Spec.Template.Spec.Containers[0].Image; got != "payments-api:v1" {
		t.Fatalf("image = %s, want payments-api:v1 after two consecutive failures", got)
	}
	if d.Annotations[rollbackReasonAnnotation] != "AnalysisFailed" || d.Annotations[rollbackToAnnotation] != "1" {
		t.Fatalf("rollback annotations = %v", d.Annotations)
	}
	if msg := h.field("message"); !strings.Contains(msg, "error rate 10.00% > 1.00%") {
		t.Fatalf("status message does not carry the measured cause: %q", msg)
	}
	if got := h.field("phase"); got != PhaseRolledBack {
		t.Fatalf("phase = %s, want %s", got, PhaseRolledBack)
	}

	// The next pass moves the phase on; the RolledBack condition keeps the cause.
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 3
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 2}
	})
	h.mustReconcile()
	c := h.condition(ConditionRolledBack)
	msg, _ := c["message"].(string)
	if c["status"] != "True" || !strings.Contains(msg, "AnalysisFailed") || !strings.Contains(msg, "error rate 10.00% > 1.00%") {
		t.Fatalf("RolledBack condition lost the cause: %v", c)
	}
}

func TestAnalysisFailuresMustBeConsecutive(t *testing.T) {
	h, prom, clk := newAnalysisHarness(t, map[string]any{"enabled": true, "consecutiveFailures": int64(2)}, checks(nil), true)
	promote(t, h, clk)
	rollOutV2(h)
	for _, s := range []sample{failingSample(), healthySample(), failingSample()} {
		prom.set("h2", s)
		h.mustReconcile()
		clk.advance(30 * time.Second)
	}
	if h.image() != "payments-api:v2" {
		t.Fatal("rolled back on failures that were not consecutive")
	}
	if got := h.analysis("consecutiveFailures"); got != int64(1) {
		t.Fatalf("consecutiveFailures = %v, want 1 (reset by the pass)", got)
	}
}

func TestAnalysisInconclusiveNeverActs(t *testing.T) {
	cases := map[string]func(*fakeProm){
		"low traffic":            func(p *fakeProm) { p.set("h2", lowTrafficSample()) },
		"no series":              func(*fakeProm) {},
		"prometheus down":        func(p *fakeProm) { p.err = errors.New("connection refused") },
		"latency not measurable": func(p *fakeProm) { p.set("h2", sample{requests: 1000, errors: 1, noP95: true}) },
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			h, prom, clk := newAnalysisHarness(t, map[string]any{"enabled": true, "consecutiveFailures": int64(1)}, checks(nil), true)
			promote(t, h, clk)
			rollOutV2(h)
			setup(prom)
			for i := 0; i < 5; i++ {
				h.mustReconcile()
				clk.advance(30 * time.Second)
			}
			if h.image() != "payments-api:v2" {
				t.Fatal("rolled back on inconclusive data")
			}
			if got := h.field("healthyTemplateHash"); got != "h1" {
				t.Fatalf("promoted on inconclusive data: healthyTemplateHash=%q", got)
			}
			if got := h.analysis("lastResult"); got != AnalysisInconclusive {
				t.Fatalf("lastResult = %v, want %s", got, AnalysisInconclusive)
			}
			if c := h.condition(ConditionAnalysis); c == nil || c["status"] != "Unknown" {
				t.Fatalf("Analysis condition = %v, want Unknown", c)
			}
		})
	}
}

func TestAnalysisFailureWithRollbackDisabledOnlyBlocks(t *testing.T) {
	h, prom, clk := newAnalysisHarness(t, map[string]any{"enabled": false, "consecutiveFailures": int64(1)}, checks(nil), true)
	promote(t, h, clk)
	prom.set("h2", failingSample())
	rollOutV2(h)
	h.mustReconcile()
	if h.image() != "payments-api:v2" {
		t.Fatal("rolled back with rollback disabled")
	}
	if got := h.field("reason"); got != "AnalysisFailed" {
		t.Fatalf("reason = %s, want AnalysisFailed", got)
	}
}

func TestAnalysisWithoutPrometheusKeepsDeadlineBehaviour(t *testing.T) {
	h, _, _ := newAnalysisHarness(t, map[string]any{"enabled": true}, checks(nil), false)
	h.mustReconcile()
	if got := h.field("healthyTemplateHash"); got != "h1" {
		t.Fatalf("without Prometheus the complete rollout should be recorded healthy, got %q", got)
	}
	if c := h.condition(ConditionAnalysis); c == nil || c["reason"] != "PrometheusNotConfigured" {
		t.Fatalf("Analysis condition = %v", c)
	}
}

func TestAnalysisInvalidChecksAreReported(t *testing.T) {
	h, prom, _ := newAnalysisHarness(t, map[string]any{"enabled": true}, checks(map[string]any{"revisionLabel": "pod-template-hash", "interval": "1s"}), true)
	h.mustReconcile()
	c := h.condition(ConditionAnalysis)
	if c == nil || c["reason"] != "InvalidChecks" {
		t.Fatalf("Analysis condition = %v", c)
	}
	msg, _ := c["message"].(string)
	if !strings.Contains(msg, "revisionLabel") || !strings.Contains(msg, "interval") {
		t.Fatalf("message should name each invalid field: %q", msg)
	}
	if prom.count() != 0 {
		t.Fatal("queried Prometheus with invalid checks")
	}
}

func TestAnalysisServiceWideWhenRevisionLabelEmpty(t *testing.T) {
	h, prom, _ := newAnalysisHarness(t, map[string]any{"enabled": true}, checks(map[string]any{"revisionLabel": ""}), true)
	prom.set("service-wide", healthySample())
	h.mustReconcile()
	if q := strings.Join(prom.queries, "\n"); strings.Contains(q, "pod_template_hash") || !strings.Contains(q, `service="payments-api"`) {
		t.Fatalf("expected service-wide queries:\n%s", q)
	}
}

func guardFrom(t *testing.T, spec map[string]any) *attestav1.RolloutGuard {
	t.Helper()
	var g attestav1.RolloutGuard
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(map[string]any{"spec": spec}, &g); err != nil {
		t.Fatal(err)
	}
	return &g
}

func TestAnalysisConfigDefaultsAndParsing(t *testing.T) {
	cfg := analysisConfigFrom(guardFrom(t, map[string]any{"checks": map[string]any{"errorRateThreshold": int64(2)}}), "svc")
	if cfg.Err != nil || !cfg.Enabled() || *cfg.ErrorRateThresholdPct != 2 {
		t.Fatalf("integer threshold not parsed: %+v", cfg)
	}
	if cfg.Window != defaultAnalysisWindow || cfg.Interval != defaultAnalysisInterval || cfg.SuccessfulChecks != 3 ||
		cfg.ConsecutiveFailures != 2 || cfg.RevisionLabel != "pod_template_hash" || cfg.MinRequests != 20 {
		t.Fatalf("defaults not applied: %+v", cfg)
	}

	if off := analysisConfigFrom(guardFrom(t, map[string]any{}), "svc"); off.Enabled() {
		t.Fatal("analysis must be off without thresholds")
	}

	// The CRD schema rejects these at apply time; a guard stored before the
	// schema is reported instead of analysed.
	bad := analysisConfigFrom(guardFrom(t, map[string]any{
		"checks":   map[string]any{"errorRateThreshold": 150.0, "latencyP95ThresholdMs": -1.0, "window": "1s", "successfulChecks": int64(0)},
		"rollback": map[string]any{"consecutiveFailures": int64(0)},
	}), "svc")
	if bad.Err == nil {
		t.Fatal("expected validation errors")
	}
	for _, want := range []string{"errorRateThreshold", "latencyP95ThresholdMs", "window", "successfulChecks", "consecutiveFailures"} {
		if !strings.Contains(bad.Err.Error(), want) {
			t.Fatalf("validation error missing %s: %v", want, bad.Err)
		}
	}
}

func TestAnalyzeAvailabilityThreshold(t *testing.T) {
	ninety := 99.5
	cfg := AnalysisConfig{MinAvailabilityPct: &ninety, Window: time.Minute, MinRequests: 1, ServiceLabel: "svc", RevisionLabel: "pod_template_hash"}
	prom := &fakeProm{byHash: map[string]sample{"x": {requests: 1000, errors: 10}}}
	got := analyze(context.Background(), prom, cfg, "x")
	if got.Result != AnalysisFailed || !strings.Contains(got.Message, "availability 99.00% < 99.50%") {
		t.Fatalf("got %+v", got)
	}
}
