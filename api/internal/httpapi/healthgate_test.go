package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/reliability"
)

type gateResponse struct {
	Decision reliability.Decision `json:"decision"`
	Policy   reliability.Policy   `json:"policy"`
	Error    string               `json:"error"`
}

func healthGate(t *testing.T, server *Server, service, body string) (int, gateResponse) {
	t.Helper()
	rec := authorizedRequest(server, http.MethodPost, "/api/v1/services/"+service+"/health-gate", []byte(body))
	var out gateResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// Audit S-3, as reproduced: a service registered with an 800ms p95 and a 5%
// error threshold was told to roll back at 450ms and 2%.
func TestHealthGateUsesTheServiceObjectives(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore())
	body := `{"name":"slow-api","team":"t","owner":"o","language":"go","environment":"staging","deploymentStrategy":"canary","sloLatencyP95Ms":800,"sloErrorRate":5}`
	if code := authorizedRequest(server, http.MethodPost, "/api/v1/services", []byte(body)).Code; code != http.StatusCreated {
		t.Fatalf("onboard: %d", code)
	}
	code, out := healthGate(t, server, "slow-api", `{"p95LatencyMs":450,"errorRate":2,"successCount":100}`)
	if code != http.StatusOK || out.Decision.Action != "promote" || out.Policy.LatencyP95Ms != 800 || out.Policy.ErrorRatePct != 5 {
		t.Fatalf("450ms / 2%% against 800ms / 5%%: %d %+v", code, out)
	}
	if _, out := healthGate(t, server, "slow-api", `{"p95LatencyMs":850,"errorRate":2,"successCount":100}`); out.Decision.Action != "rollback" {
		t.Fatalf("850ms against 800ms: %+v", out.Decision)
	}
	if code, out := healthGate(t, server, "slow-api", `{"p99LatencyMs":450,"errorRate":2,"successCount":100}`); code != http.StatusBadRequest {
		t.Fatalf("p99 input: %d %+v, want 400", code, out)
	}
}
