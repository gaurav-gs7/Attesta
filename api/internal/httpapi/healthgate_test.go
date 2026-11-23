package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/reliability"
	"github.com/gaurav-gs7/attesta/api/internal/rollouts"
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
	code, out := healthGate(t, server, "slow-api", `{"p95LatencyMs":450,"errorRate":2,"requests":100}`)
	if code != http.StatusOK || out.Decision.Action != "promote" || out.Policy.LatencyP95Ms != 800 || out.Policy.ErrorRatePct != 5 {
		t.Fatalf("450ms / 2%% against 800ms / 5%%: %d %+v", code, out)
	}
	if _, out := healthGate(t, server, "slow-api", `{"p95LatencyMs":850,"errorRate":2,"requests":100}`); out.Decision.Action != "rollback" {
		t.Fatalf("850ms against 800ms: %+v", out.Decision)
	}
	if code, out := healthGate(t, server, "slow-api", `{"p99LatencyMs":450,"errorRate":2,"requests":100}`); code != http.StatusBadRequest {
		t.Fatalf("p99 input: %d %+v, want 400", code, out)
	}
}

// Audit S-4 through the API: a canary failing every request is rolled back
// (or held, with rollbackOnFailure false) instead of waiting forever.
func TestHealthGateRollsBackAFailingCanaryAtLowVolume(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore())
	if code := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody).Code; code != http.StatusCreated {
		t.Fatalf("onboard: %d", code)
	}
	code, out := healthGate(t, server, "payments-api", `{"p95LatencyMs":40,"errorRate":100,"requests":8}`)
	if code != http.StatusOK || out.Decision.Action != reliability.ActionRollback || out.Decision.Healthy {
		t.Fatalf("8 requests, all failed: %d %+v, want a rollback decision", code, out.Decision)
	}
	if code, out := healthGate(t, server, "payments-api", `{"p95LatencyMs":40,"errorRate":100,"successCount":0}`); code != http.StatusBadRequest {
		t.Fatalf("successCount input: %d %+v, want 400", code, out)
	}
}

// Audit S-5: with rollbackOnFailure false, a breach is a hold. This checks
// it with a rollout actually in flight, the case where a rollback request
// would otherwise be recorded.
func TestHealthGateHoldsInFlightRolloutWhenRollbackIsOff(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore(), WithPrometheusURL(prometheusStub(t, healthyPrometheus())))
	body := strings.Replace(string(onboardingBody), `"deploymentStrategy":"canary"`, `"deploymentStrategy":"canary","rollbackOnFailure":false`, 1)
	if code := authorizedRequest(server, http.MethodPost, "/api/v1/services", []byte(body)).Code; code != http.StatusCreated {
		t.Fatalf("onboard: %d", code)
	}
	code, started := call(t, server, http.MethodPost, svc+"/rollouts", rolloutBody)
	if code != http.StatusAccepted {
		t.Fatalf("start: %d", code)
	}
	code, out := healthGate(t, server, "payments-api", `{"p95LatencyMs":900,"errorRate":5,"requests":500}`)
	if code != http.StatusOK || out.Decision.Action != reliability.ActionHold {
		t.Fatalf("breach with rollbackOnFailure false: %d %+v, want 200 hold", code, out.Decision)
	}
	if _, got := call(t, server, http.MethodGet, svc+"/rollouts/"+started.Rollout.ID, ""); got.Rollout.State != rollouts.StateStarted {
		t.Fatalf("rollout moved to %s although rollbackOnFailure is false", got.Rollout.State)
	}
}
