package prometheus

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"

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
		"p95":     "250",
		"total1h": "100", "err1h": "0.05",
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
	approx(t, "burn rate (1h)", signals.BurnRate, 0.5) // 0.05% errors / 0.1% allowed
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
	if signals.ErrorRatePct != nil || signals.LatencyP95Ms != nil || signals.BurnRate != nil ||
		signals.AvailabilityPct != nil || signals.ErrorBudgetRemainingPct != nil {
		t.Fatalf("no data must leave every signal unmeasured, got %+v", signals)
	}
	if len(signals.Missing) != 4 {
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
