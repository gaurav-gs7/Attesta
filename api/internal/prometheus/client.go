package prometheus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/slo"
	"github.com/gaurav-gs7/attesta/internal/promql"
)

type Client struct {
	baseURL string
	q       *promql.Client
}

func NewClient(baseURL string) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	return &Client{baseURL: baseURL, q: promql.NewClient(baseURL)}
}

// withHTTPClient replaces the HTTP client, for tests.
func (c *Client) withHTTPClient(h *http.Client) *Client {
	c.q.WithHTTPClient(h)
	return c
}

// Signals measures a service's SLIs. A transport or query error returns an
// error. Missing data, zero traffic or a NaN result is not an error: the
// affected signal is nil and Signals.Missing says why, so the gate reports
// "unknown" instead of treating silence as health.
func (c *Client) Signals(ctx context.Context, service models.Service) (slo.Signals, error) {
	if c.baseURL == "" {
		return slo.Signals{}, errors.New("prometheus URL is empty")
	}
	var out slo.Signals
	sel := fmt.Sprintf("service=%q", service.Name)
	errSel := fmt.Sprintf(`service=%q,status=~"5.."`, service.Name)

	rate := func(selector, window string) (float64, bool, error) {
		return c.query(ctx, fmt.Sprintf(`sum(rate(http_requests_total{%s}[%s]))`, selector, window))
	}
	increase := func(selector, window string) (float64, bool, error) {
		return c.query(ctx, fmt.Sprintf(`sum(increase(http_requests_total{%s}[%s]))`, selector, window))
	}

	// Request rate and error rate over the signal window.
	total, ok, err := rate(sel, slo.SignalWindow)
	if err != nil {
		return slo.Signals{}, fmt.Errorf("query request rate: %w", err)
	}
	if ok {
		out.RequestRatePerSec = ptr(total)
	}
	enough5m := ok && slo.EnoughRequests(total*slo.SignalWindowSeconds)
	switch {
	case !ok || total == 0:
		out.Missing = append(out.Missing, fmt.Sprintf("error rate: no requests for service %q in the last %s", service.Name, slo.SignalWindow))
	case !enough5m:
		out.Missing = append(out.Missing, fmt.Sprintf("error rate: about %.0f requests in the last %s, fewer than the %d needed to judge it",
			total*slo.SignalWindowSeconds, slo.SignalWindow, slo.MinRequests))
	default:
		errs, found, err := rate(errSel, slo.SignalWindow)
		if err != nil {
			return slo.Signals{}, fmt.Errorf("query error rate: %w", err)
		}
		if !found {
			errs = 0 // traffic exists and no 5xx series: zero errors
		}
		out.ErrorRatePct = ptr(math.Max(0, errs/total*100))
	}

	// p95 latency over the signal window, only with enough traffic for a
	// percentile to mean something.
	if !enough5m {
		out.Missing = append(out.Missing, fmt.Sprintf("p95 latency: fewer than %d requests in the last %s", slo.MinRequests, slo.SignalWindow))
	} else {
		p95, ok, err := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{%s}[%s]))) * 1000`, sel, slo.SignalWindow))
		if err != nil {
			return slo.Signals{}, fmt.Errorf("query p95 latency: %w", err)
		}
		if ok {
			out.LatencyP95Ms = ptr(p95)
		} else {
			out.Missing = append(out.Missing, fmt.Sprintf("p95 latency: no latency histogram data in the last %s", slo.SignalWindow))
		}
	}

	allowedBadPct := 100 - service.SLOTarget
	if allowedBadPct <= 0 {
		out.Missing = append(out.Missing, "burn rate: an SLO target of 100% leaves no error budget", "error budget: an SLO target of 100% leaves no error budget")
		return out, nil
	}

	// Burn rate over the burn-rate window.
	total1h, ok, err := rate(sel, slo.BurnRateWindow)
	if err != nil {
		return slo.Signals{}, fmt.Errorf("query burn-rate request rate: %w", err)
	}
	if !ok || !slo.EnoughRequests(total1h*slo.BurnRateWindowSeconds) {
		out.Missing = append(out.Missing, fmt.Sprintf("burn rate: fewer than %d requests in the last %s", slo.MinRequests, slo.BurnRateWindow))
	} else {
		errs1h, found, err := rate(errSel, slo.BurnRateWindow)
		if err != nil {
			return slo.Signals{}, fmt.Errorf("query burn-rate error rate: %w", err)
		}
		if !found {
			errs1h = 0
		}
		out.BurnRate = ptr((errs1h / total1h * 100) / allowedBadPct)
	}

	// Availability and remaining error budget over the budget window.
	total30d, ok, err := increase(sel, slo.BudgetWindow)
	if err != nil {
		return slo.Signals{}, fmt.Errorf("query budget request count: %w", err)
	}
	if !ok || !slo.EnoughRequests(total30d) {
		out.Missing = append(out.Missing, fmt.Sprintf("error budget: fewer than %d requests in the last %s", slo.MinRequests, slo.BudgetWindow))
	} else {
		errs30d, found, err := increase(errSel, slo.BudgetWindow)
		if err != nil {
			return slo.Signals{}, fmt.Errorf("query budget error count: %w", err)
		}
		if !found {
			errs30d = 0
		}
		badPct := math.Max(0, errs30d/total30d*100)
		out.AvailabilityPct = ptr(100 - badPct)
		// Negative means the budget is overspent; it is reported as measured.
		out.ErrorBudgetRemainingPct = ptr(math.Min(100, (allowedBadPct-badPct)/allowedBadPct*100))
	}
	return out, nil
}

// query runs an instant query through the shared client. found is false
// when the result is empty or the sample is NaN or infinite.
func (c *Client) query(ctx context.Context, query string) (value float64, found bool, err error) {
	return c.q.Query(ctx, query)
}

func ptr(v float64) *float64 { return &v }
