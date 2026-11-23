// Package prometheus measures a service's SLIs for the deployment gate.
package prometheus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/slo"
	"github.com/gaurav-gs7/attesta/internal/promql"
)

// Metrics names the series the SLIs are computed from. The defaults match
// the services Attesta generates; services instrumented differently set
// their own names.
type Metrics struct {
	// Requests is a counter of HTTP requests.
	Requests string
	// Duration is a histogram of request durations in seconds; its
	// _bucket series is queried.
	Duration string
	// ServiceLabel holds the service name on both series.
	ServiceLabel string
	// StatusLabel holds the HTTP status code on the request counter.
	StatusLabel string
	// ErrorStatus is a regular expression matched against StatusLabel to
	// count failed requests.
	ErrorStatus string
}

// DefaultMetrics are the names the generated services expose.
var DefaultMetrics = Metrics{
	Requests:     "http_requests_total",
	Duration:     "http_request_duration_seconds",
	ServiceLabel: "service",
	StatusLabel:  "status",
	ErrorStatus:  "5..",
}

var (
	metricName = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelName  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// Validate rejects names that are not valid Prometheus metric or label
// names, so configuration cannot inject PromQL.
func (m Metrics) Validate() error {
	for name, v := range map[string]string{"requests metric": m.Requests, "duration metric": m.Duration} {
		if !metricName.MatchString(v) {
			return fmt.Errorf("invalid Prometheus %s %q", name, v)
		}
	}
	for name, v := range map[string]string{"service label": m.ServiceLabel, "status label": m.StatusLabel} {
		if !labelName.MatchString(v) {
			return fmt.Errorf("invalid Prometheus %s %q", name, v)
		}
	}
	if _, err := regexp.Compile(m.ErrorStatus); err != nil || strings.ContainsAny(m.ErrorStatus, "\"`\\\n") {
		return fmt.Errorf("invalid error status pattern %q", m.ErrorStatus)
	}
	return nil
}

// Config configures the client.
type Config struct {
	URL string
	promql.Options
	// Metrics defaults to DefaultMetrics for any empty field.
	Metrics Metrics
	// Concurrency caps the queries in flight for one evaluation; 0 means 6.
	Concurrency int
}

type Client struct {
	q           promql.Querier
	m           Metrics
	concurrency int
}

// New builds the shared client: one per process, reused across requests.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("prometheus URL is empty")
	}
	m := cfg.Metrics
	fill := func(v *string, def string) {
		if *v == "" {
			*v = def
		}
	}
	fill(&m.Requests, DefaultMetrics.Requests)
	fill(&m.Duration, DefaultMetrics.Duration)
	fill(&m.ServiceLabel, DefaultMetrics.ServiceLabel)
	fill(&m.StatusLabel, DefaultMetrics.StatusLabel)
	fill(&m.ErrorStatus, DefaultMetrics.ErrorStatus)
	if err := m.Validate(); err != nil {
		return nil, err
	}
	q, err := promql.New(cfg.URL, cfg.Options)
	if err != nil {
		return nil, err
	}
	c := &Client{q: q, m: m, concurrency: cfg.Concurrency}
	if c.concurrency <= 0 {
		c.concurrency = 6
	}
	return c, nil
}

// NewClient returns a client with default settings for baseURL.
func NewClient(baseURL string) *Client {
	c, err := New(Config{URL: baseURL})
	if err != nil {
		return &Client{q: errQuerier{err}, m: DefaultMetrics, concurrency: 1}
	}
	return c
}

type errQuerier struct{ err error }

func (e errQuerier) Query(context.Context, string) (float64, bool, error) { return 0, false, e.err }

// withHTTPClient replaces the HTTP client, for tests.
func (c *Client) withHTTPClient(h *http.Client) *Client {
	if q, ok := c.q.(*promql.Client); ok {
		q.WithHTTPClient(h)
	}
	return c
}

type sample struct {
	value float64
	found bool
}

// Signals measures a service's SLIs. A transport or query error returns an
// error. Missing data, too little traffic or a NaN result is not an error:
// the affected signal is nil and Signals.Missing says why, so the gate
// reports "unknown" instead of treating silence as health.
//
// All queries run concurrently, each with its own timeout, so one slow
// query does not use up the time of the others.
func (c *Client) Signals(ctx context.Context, service models.Service) (slo.Signals, error) {
	sel := fmt.Sprintf("%s=%q", c.m.ServiceLabel, service.Name)
	errSel := fmt.Sprintf(`%s=%q,%s=~%q`, c.m.ServiceLabel, service.Name, c.m.StatusLabel, c.m.ErrorStatus)
	rate := func(selector, window string) string {
		return fmt.Sprintf(`sum(rate(%s{%s}[%s]))`, c.m.Requests, selector, window)
	}
	increase := func(selector, window string) string {
		return fmt.Sprintf(`sum(increase(%s{%s}[%s]))`, c.m.Requests, selector, window)
	}
	queries := map[string]string{
		"p95":                        fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(%s_bucket{%s}[%s]))) * 1000`, c.m.Duration, sel, slo.SignalWindow),
		"total:" + slo.BudgetWindow:  increase(sel, slo.BudgetWindow),
		"errors:" + slo.BudgetWindow: increase(errSel, slo.BudgetWindow),
	}
	windows := []string{slo.SignalWindow}
	for _, w := range slo.BurnWindows {
		if w.Name != slo.SignalWindow {
			windows = append(windows, w.Name)
		}
	}
	for _, w := range windows {
		queries["total:"+w] = rate(sel, w)
		queries["errors:"+w] = rate(errSel, w)
	}
	results, err := c.run(ctx, queries)
	if err != nil {
		return slo.Signals{}, err
	}

	var out slo.Signals
	// Request rate and error rate over the signal window.
	total := results["total:"+slo.SignalWindow]
	if total.found {
		out.RequestRatePerSec = ptr(total.value)
	}
	count5m := total.value * slo.SignalWindowSeconds
	enough5m := total.found && slo.EnoughRequests(count5m)
	switch {
	case !total.found || total.value == 0:
		out.Missing = append(out.Missing, fmt.Sprintf("error rate: no requests for service %q in the last %s", service.Name, slo.SignalWindow))
	case !enough5m:
		out.Missing = append(out.Missing, fmt.Sprintf("error rate: about %.0f requests in the last %s, fewer than the %d needed to judge it",
			count5m, slo.SignalWindow, slo.MinRequests))
	default:
		out.ErrorRatePct = ptr(math.Max(0, errorsOrZero(results["errors:"+slo.SignalWindow])/total.value*100))
	}

	// p95 latency, only with enough traffic for a percentile to mean
	// something.
	switch p95 := results["p95"]; {
	case !enough5m:
		out.Missing = append(out.Missing, fmt.Sprintf("p95 latency: fewer than %d requests in the last %s", slo.MinRequests, slo.SignalWindow))
	case !p95.found:
		out.Missing = append(out.Missing, fmt.Sprintf("p95 latency: no latency histogram data in the last %s", slo.SignalWindow))
	default:
		out.LatencyP95Ms = ptr(p95.value)
	}

	allowedBadPct := 100 - service.SLOTarget
	if allowedBadPct <= 0 {
		out.Missing = append(out.Missing, "burn rate 5m: an SLO target of 100% leaves no error budget", "error budget: an SLO target of 100% leaves no error budget")
		return out, nil
	}

	// Burn rate over each window of the multi-window checks.
	out.BurnRates = map[string]*float64{}
	for _, w := range slo.BurnWindows {
		totalW := results["total:"+w.Name]
		if !totalW.found || !slo.EnoughRequests(totalW.value*w.Seconds) {
			out.Missing = append(out.Missing, fmt.Sprintf("burn rate %s: fewer than %d requests in the last %s", w.Name, slo.MinRequests, w.Name))
			continue
		}
		out.BurnRates[w.Name] = ptr((errorsOrZero(results["errors:"+w.Name]) / totalW.value * 100) / allowedBadPct)
	}

	// Availability and remaining error budget over the budget window.
	total30d := results["total:"+slo.BudgetWindow]
	if !total30d.found || !slo.EnoughRequests(total30d.value) {
		out.Missing = append(out.Missing, fmt.Sprintf("error budget: fewer than %d requests in the last %s", slo.MinRequests, slo.BudgetWindow))
	} else {
		badPct := math.Max(0, errorsOrZero(results["errors:"+slo.BudgetWindow])/total30d.value*100)
		out.AvailabilityPct = ptr(100 - badPct)
		// Negative means the budget is overspent; it is reported as measured.
		out.ErrorBudgetRemainingPct = ptr(math.Min(100, (allowedBadPct-badPct)/allowedBadPct*100))
	}
	return out, nil
}

// errorsOrZero reads an error count: traffic with no error series means
// zero errors.
func errorsOrZero(s sample) float64 {
	if !s.found {
		return 0
	}
	return s.value
}

// run executes the queries concurrently, at most c.concurrency at a time,
// and returns the first error.
func (c *Client) run(ctx context.Context, queries map[string]string) (map[string]sample, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		results  = make(map[string]sample, len(queries))
		firstErr error
		wg       sync.WaitGroup
		slots    = make(chan struct{}, c.concurrency)
	)
	for key, query := range queries {
		wg.Add(1)
		go func(key, query string) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				return
			}
			value, found, err := c.q.Query(ctx, query)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("query %s: %w", key, err)
					cancel()
				}
				return
			}
			results[key] = sample{value, found}
		}(key, query)
	}
	wg.Wait()
	if firstErr == nil && len(results) < len(queries) {
		firstErr = fmt.Errorf("prometheus queries did not complete: %w", context.Cause(ctx))
	}
	return results, firstErr
}

func ptr(v float64) *float64 { return &v }
