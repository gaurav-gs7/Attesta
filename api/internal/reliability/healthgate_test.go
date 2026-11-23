package reliability

import (
	"errors"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/models"
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
		{Metrics{P95LatencyMs: 450, ErrorRate: 2, SuccessCount: 100}, "promote"},
		{Metrics{P95LatencyMs: 900, ErrorRate: 2, SuccessCount: 100}, "rollback"},
		{Metrics{P95LatencyMs: 450, ErrorRate: 6, SuccessCount: 100}, "rollback"},
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
		{P99LatencyMs: f(450), SuccessCount: 100},
		{P95LatencyMs: -1},
		{ErrorRate: 101},
		{SuccessCount: -5},
	} {
		if err := m.Validate(); !errors.Is(err, ErrInvalidMetrics) {
			t.Errorf("Validate(%+v) = %v, want ErrInvalidMetrics", m, err)
		}
	}
	if err := (Metrics{P95LatencyMs: 200, ErrorRate: 0.5, SuccessCount: 10}).Validate(); err != nil {
		t.Fatalf("valid metrics rejected: %v", err)
	}
}
