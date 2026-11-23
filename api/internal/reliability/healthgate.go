// Package reliability judges a canary against the service's own SLO.
package reliability

import (
	"errors"
	"fmt"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

// Policy is the gate a canary must pass. It is built from the service
// record, so a service registered with an 800ms p95 objective is judged
// against 800ms, not a fixed default.
type Policy struct {
	LatencyP95Ms float64 `json:"latencyP95Ms"`
	ErrorRatePct float64 `json:"errorRatePct"`
	// MinRequests is how many requests, failed ones included, the canary
	// must have served before it can be promoted.
	MinRequests int `json:"minRequests"`
}

// DefaultMinRequests is the sample needed before promotion.
const DefaultMinRequests = 50

// PolicyFor builds the gate from the service's registered objectives,
// falling back to the same defaults the SLO gate uses for unset fields.
func PolicyFor(service models.Service) Policy {
	p := Policy{LatencyP95Ms: float64(service.SLOLatencyP95Ms), ErrorRatePct: service.SLOErrorRate, MinRequests: DefaultMinRequests}
	if p.LatencyP95Ms <= 0 {
		p.LatencyP95Ms = 300
	}
	if p.ErrorRatePct <= 0 {
		p.ErrorRatePct = 1
	}
	return p
}

// Metrics are the canary's measurements, supplied by the caller. Latency is
// the p95, the percentile the service's objective is defined on.
type Metrics struct {
	P95LatencyMs float64 `json:"p95LatencyMs"`
	// ErrorRate is the percentage of requests that failed.
	ErrorRate float64 `json:"errorRate"`
	// Requests is how many requests the canary served, failed ones
	// included.
	Requests int `json:"requests"`
	// P99LatencyMs and SuccessCount are accepted only to reject them with a
	// clear message: p99 used to be compared against a p95 objective, and
	// counting successes let a canary that only fails wait forever.
	P99LatencyMs *float64 `json:"p99LatencyMs,omitempty"`
	SuccessCount *int     `json:"successCount,omitempty"`
}

// ErrInvalidMetrics is returned by Validate.
var ErrInvalidMetrics = errors.New("invalid canary metrics")

// Validate rejects metrics that cannot be judged against the policy.
func (m Metrics) Validate() error {
	switch {
	case m.P99LatencyMs != nil:
		return fmt.Errorf("%w: p99LatencyMs is not accepted because the service's latency objective is a p95; send p95LatencyMs", ErrInvalidMetrics)
	case m.SuccessCount != nil:
		return fmt.Errorf("%w: successCount is not accepted; send requests, the total including failures", ErrInvalidMetrics)
	case m.P95LatencyMs < 0:
		return fmt.Errorf("%w: p95LatencyMs must not be negative", ErrInvalidMetrics)
	case m.ErrorRate < 0 || m.ErrorRate > 100:
		return fmt.Errorf("%w: errorRate is a percentage between 0 and 100", ErrInvalidMetrics)
	case m.Requests < 0:
		return fmt.Errorf("%w: requests must not be negative", ErrInvalidMetrics)
	}
	return nil
}

// Decision actions.
const (
	ActionPromote  = "promote"
	ActionWait     = "wait"
	ActionRollback = "rollback"
	ActionHold     = "hold"
)

// Decision is the gate's verdict. Healthy is true only when the canary has
// been shown healthy (promote); a canary still waiting for traffic is not.
type Decision struct {
	Healthy bool   `json:"healthy"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
}

// EvaluateCanary judges metrics against policy.
//
// Below policy.MinRequests a healthy-looking canary waits for more traffic,
// but a failing one does not: if it has already failed more requests than
// the error threshold allows in a full sample, no amount of further traffic
// can bring it back under, so it is rolled back now.
func EvaluateCanary(policy Policy, metrics Metrics) Decision {
	failed := float64(metrics.Requests) * metrics.ErrorRate / 100
	if metrics.Requests < policy.MinRequests {
		if allowed := policy.ErrorRatePct / 100 * float64(policy.MinRequests); failed > allowed {
			return Decision{
				Action: ActionRollback,
				Reason: fmt.Sprintf("%.0f of %d requests failed, more than the %.1f the %.2f%% threshold allows in a sample of %d",
					failed, metrics.Requests, allowed, policy.ErrorRatePct, policy.MinRequests),
			}
		}
		return Decision{
			Action: ActionWait,
			Reason: fmt.Sprintf("waiting for more traffic: %d of %d requests", metrics.Requests, policy.MinRequests),
		}
	}
	if metrics.P95LatencyMs > policy.LatencyP95Ms {
		return Decision{
			Action: ActionRollback,
			Reason: fmt.Sprintf("p95 latency %.2fms exceeds the service's %.0fms objective", metrics.P95LatencyMs, policy.LatencyP95Ms),
		}
	}
	if metrics.ErrorRate > policy.ErrorRatePct {
		return Decision{
			Action: ActionRollback,
			Reason: fmt.Sprintf("error rate %.2f%% exceeds the service's %.2f%% threshold", metrics.ErrorRate, policy.ErrorRatePct),
		}
	}
	return Decision{Healthy: true, Action: ActionPromote, Reason: "canary is within the service's SLO thresholds"}
}
