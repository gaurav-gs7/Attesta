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
	// MinSuccesses is how many successful requests are needed before a
	// healthy canary is promoted.
	MinSuccesses int `json:"minSuccesses"`
}

// DefaultMinSuccesses is the sample needed before promotion.
const DefaultMinSuccesses = 50

// PolicyFor builds the gate from the service's registered objectives,
// falling back to the same defaults the SLO gate uses for unset fields.
func PolicyFor(service models.Service) Policy {
	p := Policy{LatencyP95Ms: float64(service.SLOLatencyP95Ms), ErrorRatePct: service.SLOErrorRate, MinSuccesses: DefaultMinSuccesses}
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
	ErrorRate    float64 `json:"errorRate"`
	SuccessCount int     `json:"successCount"`
	// P99LatencyMs is accepted only to reject it with a clear message: it
	// used to be compared against a p95 objective.
	P99LatencyMs *float64 `json:"p99LatencyMs,omitempty"`
}

// ErrInvalidMetrics is returned by Validate.
var ErrInvalidMetrics = errors.New("invalid canary metrics")

// Validate rejects metrics that cannot be judged against the policy.
func (m Metrics) Validate() error {
	switch {
	case m.P99LatencyMs != nil:
		return fmt.Errorf("%w: p99LatencyMs is not accepted because the service's latency objective is a p95; send p95LatencyMs", ErrInvalidMetrics)
	case m.P95LatencyMs < 0:
		return fmt.Errorf("%w: p95LatencyMs must not be negative", ErrInvalidMetrics)
	case m.ErrorRate < 0 || m.ErrorRate > 100:
		return fmt.Errorf("%w: errorRate is a percentage between 0 and 100", ErrInvalidMetrics)
	case m.SuccessCount < 0:
		return fmt.Errorf("%w: successCount must not be negative", ErrInvalidMetrics)
	}
	return nil
}

type Decision struct {
	Healthy bool   `json:"healthy"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
}

// EvaluateCanary judges metrics against policy.
func EvaluateCanary(policy Policy, metrics Metrics) Decision {
	if metrics.SuccessCount < policy.MinSuccesses {
		return Decision{
			Healthy: true,
			Action:  "continue",
			Reason:  fmt.Sprintf("waiting for more samples: %d/%d", metrics.SuccessCount, policy.MinSuccesses),
		}
	}
	if metrics.P95LatencyMs > policy.LatencyP95Ms {
		return Decision{
			Healthy: false,
			Action:  "rollback",
			Reason:  fmt.Sprintf("p95 latency %.2fms exceeds the service's %.0fms objective", metrics.P95LatencyMs, policy.LatencyP95Ms),
		}
	}
	if metrics.ErrorRate > policy.ErrorRatePct {
		return Decision{
			Healthy: false,
			Action:  "rollback",
			Reason:  fmt.Sprintf("error rate %.2f%% exceeds the service's %.2f%% threshold", metrics.ErrorRate, policy.ErrorRatePct),
		}
	}
	return Decision{Healthy: true, Action: "promote", Reason: "canary is within the service's SLO thresholds"}
}
