package controller

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/prometheus"
	"github.com/gaurav-gs7/attesta/internal/promql"
)

// Analysis results.
const (
	AnalysisPassed       = "Passed"
	AnalysisFailed       = "Failed"
	AnalysisInconclusive = "Inconclusive"
)

// Defaults for spec.checks and spec.rollback.
const (
	defaultAnalysisWindow      = 5 * time.Minute
	defaultAnalysisInterval    = 30 * time.Second
	defaultMinRequests         = 20
	defaultSuccessfulChecks    = 3
	defaultConsecutiveFailures = 2
	defaultRevisionLabel       = "pod_template_hash"
	minAnalysisInterval        = 5 * time.Second
	minAnalysisWindow          = 10 * time.Second
)

var promLabelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// AnalysisConfig is a guard's metric analysis, read from spec.checks and
// spec.rollback.consecutiveFailures. Analysis is enabled when at least one
// threshold is set.
type AnalysisConfig struct {
	ErrorRateThresholdPct *float64
	LatencyP95ThresholdMs *float64
	MinAvailabilityPct    *float64
	Window                time.Duration
	Interval              time.Duration
	MinRequests           float64
	SuccessfulChecks      int
	ConsecutiveFailures   int
	// ServiceLabel is the value of the "service" label the metrics carry.
	ServiceLabel string
	// RevisionLabel is the metric label holding the pod-template-hash, so
	// queries see only the candidate revision. Empty means service-wide.
	RevisionLabel string
	// Metrics names the series queried; empty names take the defaults.
	Metrics prometheus.Metrics
	// Err is set when spec.checks is invalid; analysis is then not run.
	Err error
}

func (c AnalysisConfig) Enabled() bool {
	return c.ErrorRateThresholdPct != nil || c.LatencyP95ThresholdMs != nil || c.MinAvailabilityPct != nil
}

func analysisConfigFrom(guard *attestav1.RolloutGuard, serviceLabel string) AnalysisConfig {
	cfg := AnalysisConfig{
		Window:              defaultAnalysisWindow,
		Interval:            defaultAnalysisInterval,
		MinRequests:         defaultMinRequests,
		SuccessfulChecks:    defaultSuccessfulChecks,
		ConsecutiveFailures: defaultConsecutiveFailures,
		ServiceLabel:        serviceLabel,
		RevisionLabel:       defaultRevisionLabel,
	}
	var problems []string
	duration := func(raw string, fallback, minimum time.Duration, field string) time.Duration {
		if raw == "" {
			return fallback
		}
		d, err := time.ParseDuration(raw)
		if err != nil || d < minimum {
			problems = append(problems, fmt.Sprintf("spec.%s must be a duration of at least %s", field, minimum))
			return fallback
		}
		return d
	}

	if checks := guard.Spec.Checks; checks != nil {
		cfg.ErrorRateThresholdPct = checks.ErrorRateThreshold
		cfg.LatencyP95ThresholdMs = checks.LatencyP95ThresholdMs
		cfg.MinAvailabilityPct = checks.MinAvailability
		cfg.Window = duration(checks.Window, defaultAnalysisWindow, minAnalysisWindow, "checks.window")
		cfg.Interval = duration(checks.Interval, defaultAnalysisInterval, minAnalysisInterval, "checks.interval")
		if checks.MinRequests != nil {
			cfg.MinRequests = *checks.MinRequests
		}
		if checks.SuccessfulChecks != nil {
			cfg.SuccessfulChecks = int(*checks.SuccessfulChecks)
		}
		if checks.RevisionLabel != nil {
			cfg.RevisionLabel = *checks.RevisionLabel
		}
	}
	if rb := guard.Spec.Rollback; rb != nil && rb.ConsecutiveFailures != nil {
		cfg.ConsecutiveFailures = int(*rb.ConsecutiveFailures)
	}

	// The CRD schema enforces these ranges; they are checked again so a guard
	// stored before the schema existed is reported rather than acted on.
	inRange := func(v *float64, name string, lo, hi float64) {
		if v != nil && (*v < lo || *v > hi) {
			problems = append(problems, fmt.Sprintf("spec.checks.%s must be between %g and %g", name, lo, hi))
		}
	}
	inRange(cfg.ErrorRateThresholdPct, "errorRateThreshold", 0, 100)
	inRange(cfg.MinAvailabilityPct, "minAvailability", 0, 100)
	if cfg.LatencyP95ThresholdMs != nil && *cfg.LatencyP95ThresholdMs <= 0 {
		problems = append(problems, "spec.checks.latencyP95ThresholdMs must be positive")
	}
	if cfg.MinRequests < 0 {
		problems = append(problems, "spec.checks.minRequests must not be negative")
	}
	if cfg.SuccessfulChecks < 1 {
		problems = append(problems, "spec.checks.successfulChecks must be at least 1")
	}
	if cfg.ConsecutiveFailures < 1 {
		problems = append(problems, "spec.rollback.consecutiveFailures must be at least 1")
	}
	if cfg.RevisionLabel != "" && !promLabelName.MatchString(cfg.RevisionLabel) {
		problems = append(problems, "spec.checks.revisionLabel must be a Prometheus label name")
	}
	if len(problems) > 0 {
		cfg.Err = errors.New(strings.Join(problems, "; "))
	}
	return cfg
}

// AnalysisResult is one check of the candidate revision.
type AnalysisResult struct {
	Result       string
	Message      string
	Requests     *float64
	ErrorRatePct *float64
	LatencyP95Ms *float64
	// Evidence is each query sent and what it returned, for the decision
	// record. EvaluatedAt is left for the caller to fill.
	Evidence []attestav1.DecisionEvidence
}

func (r *AnalysisResult) evidence(name, query, value, threshold string, passed *bool) {
	r.Evidence = append(r.Evidence, attestav1.DecisionEvidence{
		Name: name, Source: "prometheus", Query: query, Value: value, Threshold: threshold, Passed: passed,
	})
}

func formatValue(v float64) string { return strconv.FormatFloat(v, 'g', 6, 64) }

func boolPtr(b bool) *bool { return &b }

// queries returns the request, error and p95 queries for a candidate, built
// by the same functions as the API's SLO status and the SLO gate.
func (c AnalysisConfig) queries(candidateHash string) (requests, errs, p95 string) {
	m := c.Metrics.WithDefaults()
	var extra []string
	if c.RevisionLabel != "" {
		extra = []string{c.RevisionLabel, candidateHash}
	}
	sel := m.Selector(c.ServiceLabel, extra...)
	window := fmt.Sprintf("%ds", int(c.Window.Seconds()))
	return m.RequestIncrease(sel, window), m.RequestIncrease(m.ErrorSelector(sel), window), m.LatencyP95Ms(sel, window)
}

// analyze checks the candidate revision once. It never fails on missing
// data: too little traffic, no series, NaN or an unreachable Prometheus give
// Inconclusive, which neither promotes nor rolls back.
func analyze(ctx context.Context, q promql.Querier, cfg AnalysisConfig, candidateHash string) AnalysisResult {
	reqQuery, errQuery, p95Query := cfg.queries(candidateHash)
	window := cfg.Window.String()

	requests, found, err := q.Query(ctx, reqQuery)
	if err != nil {
		result := AnalysisResult{Result: AnalysisInconclusive, Message: fmt.Sprintf("Prometheus query failed: %v", err)}
		result.evidence("requests", reqQuery, "error: "+err.Error(), "", nil)
		return result
	}
	if !found {
		requests = 0
	}
	result := AnalysisResult{Requests: &requests}
	enough := requests >= cfg.MinRequests && requests > 0
	result.evidence("requests", reqQuery, formatValue(requests), ">= "+formatValue(cfg.MinRequests), boolPtr(enough))
	if !enough {
		result.Result = AnalysisInconclusive
		result.Message = fmt.Sprintf("%.0f requests to the candidate in the last %s; at least %.0f are needed", requests, window, cfg.MinRequests)
		return result
	}

	needErrors := cfg.ErrorRateThresholdPct != nil || cfg.MinAvailabilityPct != nil
	if needErrors {
		errs, found, err := q.Query(ctx, errQuery)
		if err != nil {
			result.Result = AnalysisInconclusive
			result.Message = fmt.Sprintf("Prometheus query failed: %v", err)
			result.evidence("errors", errQuery, "error: "+err.Error(), "", nil)
			return result
		}
		value := formatValue(errs)
		if !found {
			errs = 0 // traffic exists and no 5xx series: zero errors
			value = "absent (0)"
		}
		result.evidence("errors", errQuery, value, "", nil)
		rate := errs / requests * 100
		result.ErrorRatePct = &rate
		if cfg.ErrorRateThresholdPct != nil {
			result.evidence("error-rate", "100 * errors / requests", formatValue(rate)+"%",
				"<= "+formatValue(*cfg.ErrorRateThresholdPct)+"%", boolPtr(rate <= *cfg.ErrorRateThresholdPct))
		}
		if cfg.MinAvailabilityPct != nil {
			result.evidence("availability", "100 - error-rate", formatValue(100-rate)+"%",
				">= "+formatValue(*cfg.MinAvailabilityPct)+"%", boolPtr(100-rate >= *cfg.MinAvailabilityPct))
		}
	}
	if cfg.LatencyP95ThresholdMs != nil {
		threshold := "<= " + formatValue(*cfg.LatencyP95ThresholdMs) + "ms"
		p95, found, err := q.Query(ctx, p95Query)
		if err != nil {
			result.Result = AnalysisInconclusive
			result.Message = fmt.Sprintf("Prometheus query failed: %v", err)
			result.evidence("latency-p95", p95Query, "error: "+err.Error(), threshold, nil)
			return result
		}
		if !found {
			result.Result = AnalysisInconclusive
			result.Message = fmt.Sprintf("no latency histogram data for the candidate in the last %s", window)
			result.evidence("latency-p95", p95Query, "absent", threshold, nil)
			return result
		}
		result.LatencyP95Ms = &p95
		result.evidence("latency-p95", p95Query, formatValue(p95)+"ms", threshold, boolPtr(p95 <= *cfg.LatencyP95ThresholdMs))
	}

	var failures []string
	if cfg.ErrorRateThresholdPct != nil && *result.ErrorRatePct > *cfg.ErrorRateThresholdPct {
		failures = append(failures, fmt.Sprintf("error rate %.2f%% > %.2f%%", *result.ErrorRatePct, *cfg.ErrorRateThresholdPct))
	}
	if cfg.MinAvailabilityPct != nil && 100-*result.ErrorRatePct < *cfg.MinAvailabilityPct {
		failures = append(failures, fmt.Sprintf("availability %.2f%% < %.2f%%", 100-*result.ErrorRatePct, *cfg.MinAvailabilityPct))
	}
	if cfg.LatencyP95ThresholdMs != nil && *result.LatencyP95Ms > *cfg.LatencyP95ThresholdMs {
		failures = append(failures, fmt.Sprintf("p95 latency %.0fms > %.0fms", *result.LatencyP95Ms, *cfg.LatencyP95ThresholdMs))
	}
	if len(failures) > 0 {
		result.Result = AnalysisFailed
		result.Message = fmt.Sprintf("over %s and %.0f requests: %s", window, requests, strings.Join(failures, ", "))
		return result
	}
	result.Result = AnalysisPassed
	result.Message = fmt.Sprintf("over %s and %.0f requests: all checks within thresholds", window, requests)
	return result
}
