package slo

import (
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

func f(v float64) *float64 { return &v }

var svc = models.Service{Name: "payments-api", SLOTarget: 99.9, SLOLatencyP95Ms: 300, SLOErrorRate: 1}

func healthy() Signals {
	return Signals{
		RequestRatePerSec:       f(25),
		ErrorRatePct:            f(0.2),
		LatencyP95Ms:            f(180),
		BurnRate:                f(0.5),
		AvailabilityPct:         f(99.95),
		ErrorBudgetRemainingPct: f(60),
	}
}

func TestEvaluateGate(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*Signals)
		wantGate string
		inReason string
	}{
		{"all signals within targets", func(*Signals) {}, models.GateAllowed, "within targets"},
		{"budget below 10%", func(s *Signals) { s.ErrorBudgetRemainingPct = f(4) }, models.GateBlocked, "error budget"},
		{"p95 over target", func(s *Signals) { s.LatencyP95Ms = f(450) }, models.GateBlocked, "p95 latency"},
		{"error rate over threshold", func(s *Signals) { s.ErrorRatePct = f(2.5) }, models.GateBlocked, "error rate"},
		{"burn rate at limit", func(s *Signals) { s.BurnRate = f(2) }, models.GateBlocked, "burn rate"},
		{"latency not measured", func(s *Signals) { s.LatencyP95Ms = nil }, models.GateUnknown, "p95 latency not measured"},
		{"budget not measured", func(s *Signals) { s.ErrorBudgetRemainingPct = nil }, models.GateUnknown, "error budget not measured"},
		{"explicit missing reason kept", func(s *Signals) {
			s.ErrorRatePct = nil
			s.Missing = []string{"error rate: no requests in the last 5m"}
		}, models.GateUnknown, "no requests in the last 5m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signals := healthy()
			tc.mutate(&signals)
			got := Evaluate(svc, signals)
			if got.DeploymentGate != tc.wantGate || !strings.Contains(got.Reason, tc.inReason) {
				t.Fatalf("gate=%s reason=%q, want gate=%s reason containing %q", got.DeploymentGate, got.Reason, tc.wantGate, tc.inReason)
			}
			if got.DataSource != models.SLOSourcePrometheus || got.SignalWindow != SignalWindow || got.BudgetWindow != BudgetWindow {
				t.Fatalf("status must name its source and windows: %+v", got)
			}
		})
	}
}

func TestUnknownCarriesNoMeasurements(t *testing.T) {
	got := Unknown(svc, models.SLOSourceNone, "no Prometheus configured")
	if got.DeploymentGate != models.GateUnknown || got.DataSource != models.SLOSourceNone {
		t.Fatalf("unexpected status %+v", got)
	}
	if got.ErrorRateCurrentPct != nil || got.LatencyP95CurrentMs != nil || got.AvailabilityCurrent != nil ||
		got.BurnRate != nil || got.ErrorBudgetRemainingPct != nil || got.RequestRatePerSec != nil {
		t.Fatalf("unknown status must not carry measurements: %+v", got)
	}
}
