// Package slo evaluates a service's SLOs from measured signals and decides
// whether a rollout may start. It never invents data: a signal that could not
// be measured is nil, and the gate is then "unknown", which callers treat as
// blocked.
package slo

import (
	"fmt"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

// Windows used by the Prometheus queries and reported in every status.
const (
	SignalWindow   = "5m"
	BurnRateWindow = "1h"
	BudgetWindow   = "30d"
)

// Gate thresholds.
const (
	MinBudgetRemainingPct = 10.0
	MaxBurnRate           = 2.0
)

// Signals are measured values. A nil field was not measurable (no data, no
// traffic, or a NaN result) and makes the gate unknown.
type Signals struct {
	RequestRatePerSec       *float64
	ErrorRatePct            *float64
	LatencyP95Ms            *float64
	BurnRate                *float64
	AvailabilityPct         *float64
	ErrorBudgetRemainingPct *float64
	// Missing explains each nil field, for example "no requests in the last 5m".
	Missing []string
}

// Unknown returns a status with no measurements and an unknown gate.
func Unknown(service models.Service, source, reason string) models.SLOStatus {
	status := base(service, source)
	status.DeploymentGate = models.GateUnknown
	status.Reason = reason
	return status
}

// Evaluate applies the gate to measured signals.
func Evaluate(service models.Service, signals Signals) models.SLOStatus {
	status := base(service, models.SLOSourcePrometheus)
	status.SignalWindow = SignalWindow
	status.BurnRateWindow = BurnRateWindow
	status.BudgetWindow = BudgetWindow
	status.RequestRatePerSec = signals.RequestRatePerSec
	status.ErrorRateCurrentPct = signals.ErrorRatePct
	status.LatencyP95CurrentMs = signals.LatencyP95Ms
	status.BurnRate = signals.BurnRate
	status.AvailabilityCurrent = signals.AvailabilityPct
	status.ErrorBudgetRemainingPct = signals.ErrorBudgetRemainingPct

	missing := append([]string(nil), signals.Missing...)
	need := func(v *float64, name string) {
		if v == nil && !mentions(missing, name) {
			missing = append(missing, name+" not measured")
		}
	}
	need(signals.ErrorRatePct, "error rate")
	need(signals.LatencyP95Ms, "p95 latency")
	need(signals.BurnRate, "burn rate")
	need(signals.ErrorBudgetRemainingPct, "error budget")
	if len(missing) > 0 {
		status.DeploymentGate = models.GateUnknown
		status.Reason = "SLO state unknown, deployment gate fails closed: " + strings.Join(missing, "; ")
		return status
	}

	switch {
	case *signals.ErrorBudgetRemainingPct < MinBudgetRemainingPct:
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("error budget remaining %.1f%% over %s is below %.0f%%", *signals.ErrorBudgetRemainingPct, BudgetWindow, MinBudgetRemainingPct)
	case *signals.LatencyP95Ms > float64(status.LatencyP95TargetMs):
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("p95 latency %.0fms over %s exceeds the %dms target", *signals.LatencyP95Ms, SignalWindow, status.LatencyP95TargetMs)
	case *signals.ErrorRatePct > status.ErrorRateThresholdPct:
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("error rate %.2f%% over %s exceeds the %.2f%% threshold", *signals.ErrorRatePct, SignalWindow, status.ErrorRateThresholdPct)
	case *signals.BurnRate >= MaxBurnRate:
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("burn rate %.2fx over %s is at or above %.1fx", *signals.BurnRate, BurnRateWindow, MaxBurnRate)
	default:
		status.DeploymentGate = models.GateAllowed
		status.Reason = fmt.Sprintf("measured SLOs are within targets and %.1f%% of the %s error budget remains", *signals.ErrorBudgetRemainingPct, BudgetWindow)
	}
	return status
}

func base(service models.Service, source string) models.SLOStatus {
	target := service.SLOTarget
	if target == 0 {
		target = 99.9
	}
	latencyTarget := service.SLOLatencyP95Ms
	if latencyTarget == 0 {
		latencyTarget = 300
	}
	errorThreshold := service.SLOErrorRate
	if errorThreshold == 0 {
		errorThreshold = 1
	}
	return models.SLOStatus{
		Service:               service.Name,
		DataSource:            source,
		AvailabilityTarget:    target,
		LatencyP95TargetMs:    latencyTarget,
		ErrorRateThresholdPct: errorThreshold,
		EvaluatedAt:           time.Now().UTC(),
	}
}

func mentions(items []string, name string) bool {
	for _, item := range items {
		if strings.HasPrefix(item, name) {
			return true
		}
	}
	return false
}
