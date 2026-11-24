package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/internal/catalog"
	"github.com/gaurav-gs7/attesta/internal/httpapi"
	"github.com/gaurav-gs7/attesta/internal/slo"
)

// RateLimits reads ATTESTA_RATE_LIMIT_{IP,PRINCIPAL}_{RPS,BURST} over
// httpapi.DefaultRateLimits. An RPS of 0 disables that limit.
// ATTESTA_TRUST_FORWARDED_FOR=true keys the per-IP limit on
// X-Forwarded-For; set it only behind a proxy that sets the header.
func RateLimits() (httpapi.RateLimits, error) {
	limits := httpapi.DefaultRateLimits
	var err error
	if limits.PerIP.Rate, err = floatEnv("ATTESTA_RATE_LIMIT_IP_RPS", limits.PerIP.Rate); err != nil {
		return limits, err
	}
	if limits.PerIP.Burst, err = intEnv("ATTESTA_RATE_LIMIT_IP_BURST", limits.PerIP.Burst); err != nil {
		return limits, err
	}
	if limits.PerPrincipal.Rate, err = floatEnv("ATTESTA_RATE_LIMIT_PRINCIPAL_RPS", limits.PerPrincipal.Rate); err != nil {
		return limits, err
	}
	if limits.PerPrincipal.Burst, err = intEnv("ATTESTA_RATE_LIMIT_PRINCIPAL_BURST", limits.PerPrincipal.Burst); err != nil {
		return limits, err
	}
	limits.TrustForwardedFor = boolEnv("ATTESTA_TRUST_FORWARDED_FOR")
	return limits, nil
}

// RequestTimeout reads ATTESTA_REQUEST_TIMEOUT (a Go duration) over
// httpapi.DefaultRequestTimeout. It must stay below the server's 30s
// write timeout.
func RequestTimeout() (time.Duration, error) {
	raw := os.Getenv("ATTESTA_REQUEST_TIMEOUT")
	if raw == "" {
		return httpapi.DefaultRequestTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 || d >= 30*time.Second {
		return 0, fmt.Errorf("ATTESTA_REQUEST_TIMEOUT=%q: want a duration above 0 and below 30s", raw)
	}
	return d, nil
}

func floatEnv(key string, fallback float64) (float64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%s=%q: want a number of requests per second, 0 to disable", key, raw)
	}
	return v, nil
}

func intEnv(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return 0, fmt.Errorf("%s=%q: want a positive whole number", key, raw)
	}
	return v, nil
}

// Retention reads ATTESTA_WORKFLOW_RETENTION and
// ATTESTA_INCIDENT_RETENTION over catalog.DefaultRetention: a Go duration
// or a whole number of days ("30d"); "0" keeps records forever.
func Retention() (catalog.Retention, error) {
	r := catalog.DefaultRetention
	var err error
	if r.WorkflowRuns, err = retentionEnv("ATTESTA_WORKFLOW_RETENTION", r.WorkflowRuns); err != nil {
		return r, err
	}
	if r.ResolvedIncidents, err = retentionEnv("ATTESTA_INCIDENT_RETENTION", r.ResolvedIncidents); err != nil {
		return r, err
	}
	return r, nil
}

func retentionEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	if days, ok := strings.CutSuffix(raw, "d"); ok {
		if n, err := strconv.Atoi(days); err == nil && n >= 0 {
			return time.Duration(n) * 24 * time.Hour, nil
		}
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 || (d > 0 && d < time.Hour) {
		return 0, fmt.Errorf("%s=%q: want a duration of at least 1h, a number of days such as 30d, or 0 to keep forever", key, raw)
	}
	return d, nil
}

// SLOThresholds reads ATTESTA_SLO_MIN_ERROR_BUDGET_PCT,
// ATTESTA_SLO_FAST_BURN_RATE and ATTESTA_SLO_SLOW_BURN_RATE over the
// defaults (10, 14.4, 6). They set the gate the API reports; a guard's
// spec.sloGate sets its own.
func SLOThresholds() (slo.Thresholds, error) {
	t := slo.DefaultThresholds
	for key, v := range map[string]*float64{
		"ATTESTA_SLO_MIN_ERROR_BUDGET_PCT": &t.MinBudgetRemainingPct,
		"ATTESTA_SLO_FAST_BURN_RATE":       &t.FastBurnRate,
		"ATTESTA_SLO_SLOW_BURN_RATE":       &t.SlowBurnRate,
	} {
		raw := os.Getenv(key)
		if raw == "" {
			continue
		}
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return t, fmt.Errorf("%s=%q: want a number", key, raw)
		}
		*v = f
	}
	if err := t.Validate(); err != nil {
		return t, fmt.Errorf("ATTESTA_SLO_*: %w", err)
	}
	return t, nil
}
