// Package slo evaluates a service's SLOs from measured signals and decides
// whether a rollout may start. It never invents data: a signal that could not
// be measured is nil, and the gate is then "unknown", which callers treat as
// blocked.
package slo

import (
	"fmt"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/internal/models"
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
	// FastBurnRate and SlowBurnRate are the multi-window, multi-burn-rate
	// thresholds from the Google SRE workbook. The fast pair (1h and 5m) at
	// 14.4x spends 2% of a 30-day budget in an hour; the slow pair (6h and
	// 30m) at 6x spends 5% in six hours. A pair blocks only when both of its
	// windows are over: the long window shows the burn is significant, the
	// short one that it is still happening.
	FastBurnRate = 14.4
	SlowBurnRate = 6.0
	// MinRequests is the fewest requests a window must hold for its error
	// rate, latency or budget to be judged. Below it a single request decides
	// the answer, so the signal counts as not measured and the gate is
	// unknown.
	MinRequests = 20
)

// Window lengths in seconds, for turning a request rate into a count.
const (
	SignalWindowSeconds = 5 * 60
)

// BurnWindow is one window of the multi-window burn-rate checks.
type BurnWindow struct {
	Name    string
	Seconds float64
}

// BurnWindows are the windows the burn rate is measured over: the short and
// long window of the fast and the slow pair.
var BurnWindows = []BurnWindow{{"5m", 5 * 60}, {"30m", 30 * 60}, {"1h", 60 * 60}, {"6h", 6 * 60 * 60}}

// EnoughRequests reports whether a window with this many requests can be
// judged.
func EnoughRequests(count float64) bool { return count >= MinRequests }

// Signals are measured values. A nil field was not measurable (no data, no
// traffic, or a NaN result) and makes the gate unknown.
type Signals struct {
	RequestRatePerSec *float64
	ErrorRatePct      *float64
	LatencyP95Ms      *float64
	// BurnRates is the burn rate over each of BurnWindows, by name.
	BurnRates               map[string]*float64
	AvailabilityPct         *float64
	ErrorBudgetRemainingPct *float64
	// Missing explains each nil field, for example "no requests in the last 5m".
	Missing []string
	// Queries is the PromQL each value came from, by key: "p95",
	// "total:<window>" and "errors:<window>" (rates for the burn windows,
	// counts for the budget window). Decision records quote them.
	Queries map[string]string
}

// Unknown returns a status with no measurements and an unknown gate.
func Unknown(service models.Service, source, reason string) models.SLOStatus {
	status := base(service, source)
	status.DeploymentGate = models.GateUnknown
	status.Reason = reason
	return status
}

// Thresholds are the gate's limits. The defaults are the SRE workbook's;
// a RolloutGuard's spec.sloGate, or the API's ATTESTA_SLO_* settings,
// override them.
type Thresholds struct {
	// MinBudgetRemainingPct blocks when less of the budget remains.
	MinBudgetRemainingPct float64
	// FastBurnRate blocks when the 1h and 5m burn rates both reach it.
	FastBurnRate float64
	// SlowBurnRate blocks when the 6h and 30m burn rates both reach it.
	SlowBurnRate float64
}

// DefaultThresholds are used where nothing overrides them.
var DefaultThresholds = Thresholds{MinBudgetRemainingPct: MinBudgetRemainingPct, FastBurnRate: FastBurnRate, SlowBurnRate: SlowBurnRate}

// Validate rejects thresholds that cannot mean anything.
func (t Thresholds) Validate() error {
	if t.MinBudgetRemainingPct < 0 || t.MinBudgetRemainingPct > 100 {
		return fmt.Errorf("minimum error budget %g%% is not between 0 and 100", t.MinBudgetRemainingPct)
	}
	if t.FastBurnRate <= 0 || t.SlowBurnRate <= 0 {
		return fmt.Errorf("burn-rate thresholds must be positive, got %g and %g", t.FastBurnRate, t.SlowBurnRate)
	}
	return nil
}

// Evaluate applies the gate, with the default thresholds, to measured
// signals.
func Evaluate(service models.Service, signals Signals) models.SLOStatus {
	return EvaluateWith(service, signals, DefaultThresholds)
}

// EvaluateWith applies the gate with the given thresholds.
func EvaluateWith(service models.Service, signals Signals, t Thresholds) models.SLOStatus {
	status := base(service, models.SLOSourcePrometheus)
	status.SignalWindow = SignalWindow
	status.BurnRateWindow = BurnRateWindow
	status.BudgetWindow = BudgetWindow
	status.RequestRatePerSec = signals.RequestRatePerSec
	status.ErrorRateCurrentPct = signals.ErrorRatePct
	status.LatencyP95CurrentMs = signals.LatencyP95Ms
	status.BurnRate = signals.BurnRates["1h"]
	status.BurnRates = signals.BurnRates
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
	for _, w := range BurnWindows {
		need(signals.BurnRates[w.Name], "burn rate "+w.Name)
	}
	need(signals.ErrorBudgetRemainingPct, "error budget")
	if len(missing) > 0 {
		status.DeploymentGate = models.GateUnknown
		status.Reason = "SLO state unknown, deployment gate fails closed: " + strings.Join(missing, "; ")
		return status
	}

	switch {
	case *signals.ErrorBudgetRemainingPct < t.MinBudgetRemainingPct:
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("error budget remaining %.1f%% over %s is below %g%%", *signals.ErrorBudgetRemainingPct, BudgetWindow, t.MinBudgetRemainingPct)
	case *signals.LatencyP95Ms > float64(status.LatencyP95TargetMs):
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("p95 latency %.0fms over %s exceeds the %dms target", *signals.LatencyP95Ms, SignalWindow, status.LatencyP95TargetMs)
	case *signals.ErrorRatePct > status.ErrorRateThresholdPct:
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("error rate %.2f%% over %s exceeds the %.2f%% threshold", *signals.ErrorRatePct, SignalWindow, status.ErrorRateThresholdPct)
	case burning(signals, "1h", "5m", t.FastBurnRate):
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("fast burn: burn rate %.1fx over 1h and %.1fx over 5m are both at or above %.1fx",
			*signals.BurnRates["1h"], *signals.BurnRates["5m"], t.FastBurnRate)
	case burning(signals, "6h", "30m", t.SlowBurnRate):
		status.DeploymentGate = models.GateBlocked
		status.Reason = fmt.Sprintf("slow burn: burn rate %.1fx over 6h and %.1fx over 30m are both at or above %.1fx",
			*signals.BurnRates["6h"], *signals.BurnRates["30m"], t.SlowBurnRate)
	default:
		status.DeploymentGate = models.GateAllowed
		status.Reason = fmt.Sprintf("measured SLOs are within targets and %.1f%% of the %s error budget remains", *signals.ErrorBudgetRemainingPct, BudgetWindow)
	}
	return status
}

func burning(s Signals, long, short string, threshold float64) bool {
	return *s.BurnRates[long] >= threshold && *s.BurnRates[short] >= threshold
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
