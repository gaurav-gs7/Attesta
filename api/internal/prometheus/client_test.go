package prometheus

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

var service = models.Service{Name: "payments-api", SLOTarget: 99.9}

// fakePrometheus answers each query from values keyed by a substring of the
// query. A missing key returns an empty result; "NaN" returns a NaN sample.
func fakePrometheus(values map[string]string) *Client {
	client := NewClient("http://prometheus.test")
	client.withHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		query := r.URL.Query().Get("query")
		key := classify(query)
		value, ok := values[key]
		if !ok {
			return prometheusResponse(`{"status":"success","data":{"result":[]}}`), nil
		}
		return prometheusResponse(fmt.Sprintf(`{"status":"success","data":{"result":[{"value":[1710000000,"%s"]}]}}`, value)), nil
	})})
	return client
}

func classify(query string) string {
	errs := strings.Contains(query, `status=~"5.."`)
	switch {
	case strings.Contains(query, "histogram_quantile"):
		return "p95"
	case strings.Contains(query, "[5m]") && errs:
		return "err5m"
	case strings.Contains(query, "[5m]"):
		return "total5m"
	case strings.Contains(query, "[30m]") && errs:
		return "err30m"
	case strings.Contains(query, "[30m]"):
		return "total30m"
	case strings.Contains(query, "[6h]") && errs:
		return "err6h"
	case strings.Contains(query, "[6h]"):
		return "total6h"
	case strings.Contains(query, "[1h]") && errs:
		return "err1h"
	case strings.Contains(query, "[1h]"):
		return "total1h"
	case strings.Contains(query, "[30d]") && errs:
		return "err30d"
	case strings.Contains(query, "[30d]"):
		return "total30d"
	}
	return "unknown"
}

func full() map[string]string {
	return map[string]string{
		"total5m": "100", "err5m": "2",
		"p95":      "250",
		"total30m": "100", "err30m": "0.05",
		"total1h": "100", "err1h": "0.05",
		"total6h": "100", "err6h": "0.05",
		"total30d": "1000000", "err30d": "400",
	}
}

func approx(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: not measured, want %v", name, want)
	}
	if math.Abs(*got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", name, *got, want)
	}
}

func TestSignalsComputesEachSignalOverItsWindow(t *testing.T) {
	signals, err := fakePrometheus(full()).Signals(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}
	approx(t, "request rate", signals.RequestRatePerSec, 100)
	approx(t, "error rate (5m)", signals.ErrorRatePct, 2)
	approx(t, "p95", signals.LatencyP95Ms, 250)
	approx(t, "burn rate (1h)", signals.BurnRates["1h"], 0.5) // 0.05% errors / 0.1% allowed
	for _, w := range []string{"5m", "30m", "6h"} {
		if signals.BurnRates[w] == nil {
			t.Fatalf("burn rate %s not measured", w)
		}
	}
	approx(t, "availability (30d)", signals.AvailabilityPct, 99.96)
	approx(t, "budget remaining (30d)", signals.ErrorBudgetRemainingPct, 60) // 0.04% of 0.1% spent
	if len(signals.Missing) != 0 {
		t.Fatalf("unexpected missing signals: %v", signals.Missing)
	}
}

func TestSignalsTreatsMissingErrorSeriesAsZeroErrors(t *testing.T) {
	values := full()
	delete(values, "err5m")
	delete(values, "err1h")
	delete(values, "err30d")
	signals, err := fakePrometheus(values).Signals(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}
	approx(t, "error rate", signals.ErrorRatePct, 0)
	approx(t, "budget remaining", signals.ErrorBudgetRemainingPct, 100)
}

// Audit S-2: a service with no series at all must not look healthy.
func TestSignalsWithNoDataAreNotMeasured(t *testing.T) {
	signals, err := fakePrometheus(map[string]string{}).Signals(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}
	if signals.ErrorRatePct != nil || signals.LatencyP95Ms != nil || signals.BurnRates["1h"] != nil ||
		signals.AvailabilityPct != nil || signals.ErrorBudgetRemainingPct != nil {
		t.Fatalf("no data must leave every signal unmeasured, got %+v", signals)
	}
	if len(signals.Missing) != 7 { // error rate, p95, four burn windows, budget
		t.Fatalf("expected a reason for each missing signal, got %v", signals.Missing)
	}
}

func TestSignalsWithZeroTrafficAreNotMeasured(t *testing.T) {
	values := full()
	values["total5m"] = "0"
	signals, err := fakePrometheus(values).Signals(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}
	approx(t, "request rate", signals.RequestRatePerSec, 0)
	if signals.ErrorRatePct != nil {
		t.Fatalf("error rate with zero traffic must be unmeasured, got %v", *signals.ErrorRatePct)
	}
}

// Audit S-1: histogram_quantile returns NaN for an idle service.
func TestSignalsTreatsNaNAsNotMeasured(t *testing.T) {
	values := full()
	values["p95"] = "NaN"
	signals, err := fakePrometheus(values).Signals(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}
	if signals.LatencyP95Ms != nil {
		t.Fatalf("NaN p95 must be unmeasured, got %v", *signals.LatencyP95Ms)
	}
	values["p95"] = "+Inf"
	signals, _ = fakePrometheus(values).Signals(context.Background(), service)
	if signals.LatencyP95Ms != nil {
		t.Fatalf("+Inf p95 must be unmeasured, got %v", *signals.LatencyP95Ms)
	}
}

func TestSignalsReportsOverspentBudget(t *testing.T) {
	values := full()
	values["err30d"] = "2500" // 0.25% bad against 0.1% allowed
	signals, err := fakePrometheus(values).Signals(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}
	approx(t, "budget remaining", signals.ErrorBudgetRemainingPct, -150)
}

func TestSignalsFailsWhenPrometheusUnavailable(t *testing.T) {
	_, err := NewClient("http://127.0.0.1:1").Signals(context.Background(), service)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSignalsFailsOnQueryError(t *testing.T) {
	client := NewClient("http://prometheus.test")
	client.withHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return prometheusResponse(`{"status":"error","error":"parse error"}`), nil
	})})
	if _, err := client.Signals(context.Background(), service); err == nil || !strings.Contains(err.Error(), "parse error") {
		t.Fatalf("expected the query error, got %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func prometheusResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

// Audit S-2: a handful of requests decided the gate, so one lucky or
// unlucky request in five minutes passed or failed a rollout. Below
// slo.MinRequests a window is not measured.
func TestSignalsBelowMinimumVolumeAreNotMeasured(t *testing.T) {
	// 0.05 req/s is 15 requests in 5m, 180 in 1h; 10 requests in 30d.
	signals, err := fakePrometheus(map[string]string{
		"total5m": "0.05", "err5m": "0", "p95": "40",
		"total1h": "0.05", "err1h": "0",
		"total30d": "10", "err30d": "0",
	}).Signals(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}
	if signals.ErrorRatePct != nil || signals.LatencyP95Ms != nil || signals.ErrorBudgetRemainingPct != nil {
		t.Fatalf("low-volume windows were judged: %+v", signals)
	}
	if signals.RequestRatePerSec == nil || signals.BurnRates["1h"] == nil {
		t.Fatalf("the request rate and the 1h burn rate (180 requests) are measurable: %+v", signals)
	}
	if got := strings.Join(signals.Missing, "; "); !strings.Contains(got, "fewer than 20") {
		t.Fatalf("missing reasons do not name the volume floor: %s", got)
	}
}

// slowQuerier answers every query after a delay and records the queries.
type slowQuerier struct {
	delay   time.Duration
	mu      sync.Mutex
	queries []string
}

func (s *slowQuerier) Query(ctx context.Context, q string) (float64, bool, error) {
	s.mu.Lock()
	s.queries = append(s.queries, q)
	s.mu.Unlock()
	select {
	case <-time.After(s.delay):
		return 100, true, nil
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

// Audit S-7: the queries ran one after another under one shared deadline,
// so a slow Prometheus made later queries fail for lack of time.
func TestSignalsQueriesRunConcurrently(t *testing.T) {
	q := &slowQuerier{delay: 200 * time.Millisecond}
	c := &Client{q: q, m: DefaultMetrics, concurrency: 6}
	start := time.Now()
	if _, err := c.Signals(context.Background(), service); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	sequential := time.Duration(len(q.queries)) * q.delay
	if elapsed > sequential/2 {
		t.Fatalf("%d queries of %s took %s; sequential would be %s", len(q.queries), q.delay, elapsed, sequential)
	}
}

// Audit S-7: metric and label names were fixed in code.
func TestMetricNamesAreConfigurable(t *testing.T) {
	q := &slowQuerier{}
	c, err := New(Config{URL: "http://prometheus.test", Metrics: Metrics{
		Requests: "istio_requests_total", Duration: "istio_request_duration_milliseconds",
		ServiceLabel: "destination_service_name", StatusLabel: "response_code",
	}})
	if err != nil {
		t.Fatal(err)
	}
	c.q = q
	if _, err := c.Signals(context.Background(), service); err != nil {
		t.Fatal(err)
	}
	all := strings.Join(q.queries, "\n")
	for _, want := range []string{`istio_requests_total{destination_service_name="payments-api"}`, `response_code=~"5.."`, "istio_request_duration_milliseconds_bucket"} {
		if !strings.Contains(all, want) {
			t.Fatalf("queries do not use %q:\n%s", want, all)
		}
	}
	if strings.Contains(all, "http_requests_total") {
		t.Fatalf("default metric name still queried:\n%s", all)
	}
	for _, bad := range []Metrics{{Requests: `x{a="b"} or vector(1)`}, {ServiceLabel: "has-dash"}, {ErrorStatus: `5.."} or up{x="`}} {
		if _, err := New(Config{URL: "http://p", Metrics: bad}); err == nil {
			t.Fatalf("invalid metric config %+v accepted", bad)
		}
	}
}
