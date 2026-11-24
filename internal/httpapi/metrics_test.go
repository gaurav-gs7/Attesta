package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/internal/catalog"
)

func scrape(t *testing.T, server *Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: %d", rec.Code)
	}
	return rec.Body.String()
}

func seriesCount(body, name string) int {
	n := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+"{") {
			n++
		}
	}
	return n
}

// Audit X-3, as reproduced: 2,000 unauthenticated requests to distinct
// paths created 2,001 series, and the map never shrank.
func TestRequestMetricsHaveBoundedLabels(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore())
	routes := server.Routes()
	send := func(method, path string) {
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	}
	for i := 0; i < 2000; i++ {
		send(http.MethodGet, fmt.Sprintf("/nope/%d", i))                       // unmatched, no token needed
		send(http.MethodGet, fmt.Sprintf("/api/v1/guards/ns-%d/svc-%d", i, i)) // unauthenticated, ID-bearing
		send(fmt.Sprintf("M%d", i), "/livez")                                  // made-up method
	}
	body := scrape(t, server)
	if n := seriesCount(body, "attesta_http_requests_total"); n > 10 {
		t.Fatalf("6,000 requests with unique paths and methods made %d request series:\n%s", n, body)
	}
	if strings.Contains(body, "svc-1") || strings.Contains(body, "/nope/") || strings.Contains(body, "M1") {
		t.Fatal("client-chosen path or method values reached a label")
	}
	if !strings.Contains(body, `route="other"`) {
		t.Fatalf("unmatched requests are not bucketed as other:\n%s", body)
	}
}

func TestRequestMetricsUseTheMatchedPattern(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore())
	authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody)
	authorizedRequest(server, http.MethodGet, "/api/v1/guards/payments/payments-api-rollout", nil)
	body := scrape(t, server)
	for _, want := range []string{
		`attesta_http_requests_total{method="POST",route="/api/v1/services",status="201"} 1`,
		`attesta_http_requests_total{method="GET",route="/api/v1/guards/{namespace}/{name}",status="503"} 1`,
		"attesta_http_request_duration_seconds_bucket",
		"attesta_uptime_seconds",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics lacks %q:\n%s", want, body)
		}
	}
}
