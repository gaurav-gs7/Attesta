package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
	promclient "github.com/gaurav-gs7/attesta/api/internal/prometheus"
	"github.com/gaurav-gs7/attesta/internal/promql"
)

// A misconfigured Prometheus client keeps the gate closed and says why,
// instead of failing on every query with a transport error.
func TestMisconfiguredPrometheusFailsClosed(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore(), WithPrometheus(promclient.Config{
		URL: "https://prometheus.example", Options: promql.Options{CAFile: "/does/not/exist.pem"},
	}))
	onboard(t, server)
	_, out, body := getSLO(t, server)
	if out.SLO.DeploymentGate != models.GateUnknown || !strings.Contains(out.SLO.Reason, "misconfigured") {
		t.Fatalf("expected an unknown gate naming the misconfiguration, got %s", body)
	}
	if rec := startRollout(server); rec.Code != http.StatusConflict {
		t.Fatalf("rollout with a misconfigured Prometheus: %d, want 409", rec.Code)
	}
}
