package prometheus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/slo"
)

// maxResponseBytes caps a single query response.
const maxResponseBytes = 1 << 20

type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		http:    &http.Client{Timeout: 5 * time.Second},
	}
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
	if !ok || total == 0 {
		out.Missing = append(out.Missing, fmt.Sprintf("error rate: no requests for service %q in the last %s", service.Name, slo.SignalWindow))
	} else {
		errs, found, err := rate(errSel, slo.SignalWindow)
		if err != nil {
			return slo.Signals{}, fmt.Errorf("query error rate: %w", err)
		}
		if !found {
			errs = 0 // traffic exists and no 5xx series: zero errors
		}
		out.ErrorRatePct = ptr(math.Max(0, errs/total*100))
	}

	// p95 latency over the signal window.
	p95, ok, err := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{%s}[%s]))) * 1000`, sel, slo.SignalWindow))
	if err != nil {
		return slo.Signals{}, fmt.Errorf("query p95 latency: %w", err)
	}
	if ok {
		out.LatencyP95Ms = ptr(p95)
	} else {
		out.Missing = append(out.Missing, fmt.Sprintf("p95 latency: no latency histogram data in the last %s", slo.SignalWindow))
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
	if !ok || total1h == 0 {
		out.Missing = append(out.Missing, fmt.Sprintf("burn rate: no requests in the last %s", slo.BurnRateWindow))
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
	if !ok || total30d == 0 {
		out.Missing = append(out.Missing, fmt.Sprintf("error budget: no requests in the last %s", slo.BudgetWindow))
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

// query runs an instant query and returns its first sample. found is false
// when the result is empty or the sample is NaN or infinite.
func (c *Client) query(ctx context.Context, query string) (value float64, found bool, err error) {
	values := url.Values{}
	values.Set("query", query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/query?"+values.Encode(), nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, false, fmt.Errorf("prometheus returned %s", resp.Status)
	}
	var payload struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return 0, false, err
	}
	if payload.Status != "success" {
		if payload.Error == "" {
			payload.Error = "query failed"
		}
		return 0, false, errors.New(payload.Error)
	}
	if len(payload.Data.Result) == 0 {
		return 0, false, nil
	}
	if len(payload.Data.Result[0].Value) < 2 {
		return 0, false, fmt.Errorf("query returned no scalar result")
	}
	raw, ok := payload.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, false, fmt.Errorf("query scalar result was not a string")
	}
	value, err = strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse scalar result %q: %w", raw, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false, nil
	}
	return value, true, nil
}

func ptr(v float64) *float64 { return &v }
