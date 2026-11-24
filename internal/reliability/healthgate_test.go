package reliability

import (
	"errors"
	"testing"

	"github.com/gaurav-gs7/attesta/internal/models"
)

func f(v float64) *float64 { return &v }

// Audit S-3: thresholds were fixed at 300ms and 1%, and p99 was compared to
// a p95 objective. A service registered with 800ms and 5% was rolled back at
// 450ms and 2%.
func TestPolicyComesFromTheServiceRecord(t *testing.T) {
	policy := PolicyFor(models.Service{SLOLatencyP95Ms: 800, SLOErrorRate: 5})
	if policy.LatencyP95Ms != 800 || policy.ErrorRatePct != 5 {
		t.Fatalf("policy = %+v, want the service's 800ms / 5%%", policy)
	}
	for _, tc := range []struct {
		m      Metrics
		action string
	}{
		{Metrics{P95LatencyMs: 450, ErrorRate: 2, Requests: 100}, "promote"},
		{Metrics{P95LatencyMs: 900, ErrorRate: 2, Requests: 100}, "rollback"},
		{Metrics{P95LatencyMs: 450, ErrorRate: 6, Requests: 100}, "rollback"},
	} {
		if got := EvaluateCanary(policy, tc.m); got.Action != tc.action {
			t.Errorf("%+v: %s (%s), want %s", tc.m, got.Action, got.Reason, tc.action)
		}
	}
	if d := PolicyFor(models.Service{}); d.LatencyP95Ms != 300 || d.ErrorRatePct != 1 {
		t.Fatalf("defaults for an unset service = %+v", d)
	}
}

func TestMetricsRejectP99AndNonsense(t *testing.T) {
	for _, m := range []Metrics{
		{P99LatencyMs: f(450), Requests: 100},
		{P95LatencyMs: -1},
		{ErrorRate: 101},
		{Requests: -5},
	} {
		if err := m.Validate(); !errors.Is(err, ErrInvalidMetrics) {
			t.Errorf("Validate(%+v) = %v, want ErrInvalidMetrics", m, err)
		}
	}
	if err := (Metrics{P95LatencyMs: 200, ErrorRate: 0.5, Requests: 10}).Validate(); err != nil {
		t.Fatalf("valid metrics rejected: %v", err)
	}
}

// Audit S-4, as reproduced: a canary failing every request, with zero
// successes, was "healthy: true, action: continue" and could never collect
// the 50 successes needed to be judged.
func TestSampleSizeCountsRequestsNotSuccesses(t *testing.T) {
	policy := PolicyFor(models.Service{}) // 300ms, 1%, 50 requests
	for _, tc := range []struct {
		name    string
		m       Metrics
		action  string
		healthy bool
	}{
		{"all failing, low volume", Metrics{ErrorRate: 100, Requests: 10}, ActionRollback, false},
		{"one failure in 10 is already over 1% of 50", Metrics{ErrorRate: 10, Requests: 10}, ActionRollback, false},
		{"healthy, low volume", Metrics{P95LatencyMs: 900, ErrorRate: 0, Requests: 10}, ActionWait, false},
		{"no traffic yet", Metrics{}, ActionWait, false},
		{"healthy, full sample", Metrics{P95LatencyMs: 120, ErrorRate: 0.5, Requests: 100}, ActionPromote, true},
		{"failing, full sample", Metrics{P95LatencyMs: 120, ErrorRate: 2, Requests: 100}, ActionRollback, false},
	} {
		got := EvaluateCanary(policy, tc.m)
		if got.Action != tc.action || got.Healthy != tc.healthy {
			t.Errorf("%s: %s healthy=%v (%s), want %s healthy=%v", tc.name, got.Action, got.Healthy, got.Reason, tc.action, tc.healthy)
		}
	}
	if err := (Metrics{SuccessCount: new(int)}).Validate(); !errors.Is(err, ErrInvalidMetrics) {
		t.Fatalf("successCount accepted: %v", err)
	}
}
