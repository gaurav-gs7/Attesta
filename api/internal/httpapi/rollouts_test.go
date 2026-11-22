package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/rollouts"
)

const rolloutBody = `{"version":"v2","image":"ghcr.io/acme/payments-api:v2","gitSha":"0123abc"}`

type rolloutResponse struct {
	Rollout models.Rollout `json:"rollout"`
	Error   string         `json:"error"`
}

func rolloutServer(t *testing.T, store catalog.Store) *Server {
	t.Helper()
	server := newTestServer(t, store, WithPrometheusURL(prometheusStub(t, healthyPrometheus())))
	if code := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody).Code; code != http.StatusCreated {
		t.Fatalf("onboard: %d", code)
	}
	return server
}

func call(t *testing.T, server *Server, method, path, body string) (int, rolloutResponse) {
	t.Helper()
	var b []byte
	if body != "" {
		b = []byte(body)
	}
	rec := authorizedRequest(server, method, path, b)
	var out rolloutResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

const svc = "/api/v1/services/payments-api"

// Audit W-8: startRollout ignored JSON errors and had no body limit.
func TestStartRolloutValidatesInput(t *testing.T) {
	server := rolloutServer(t, catalog.NewMemoryStore())
	for name, body := range map[string]string{
		"not JSON":      `{"version":`,
		"unknown field": `{"version":"v2","image":"app:v2","replicas":3}`,
		"no image":      `{"version":"v2"}`,
		"no version":    `{"image":"app:v2"}`,
		"bad image":     `{"version":"v2","image":"app:v2; curl evil"}`,
		"bad sha":       `{"version":"v2","image":"app:v2","gitSha":"zzz"}`,
		"oversized":     `{"version":"v2","image":"app:v2","gitSha":"` + strings.Repeat("a", 2<<20) + `"}`,
	} {
		if code, out := call(t, server, http.MethodPost, svc+"/rollouts", body); code != http.StatusBadRequest {
			t.Errorf("%s: %d %q, want 400", name, code, out.Error)
		}
	}
}

// Audit W-8: a rollout is an entity with its image and git SHA, a state
// machine, and one deployment history row however many times it changes.
func TestRolloutLifecycle(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			server := rolloutServer(t, open(t))

			code, started := call(t, server, http.MethodPost, svc+"/rollouts", rolloutBody)
			if code != http.StatusAccepted || started.Rollout.State != rollouts.StateStarted ||
				started.Rollout.Image != "ghcr.io/acme/payments-api:v2" || started.Rollout.GitSHA != "0123abc" {
				t.Fatalf("start: %d %+v", code, started)
			}
			id := started.Rollout.ID

			if code, _ := call(t, server, http.MethodPost, svc+"/rollouts", rolloutBody); code != http.StatusConflict {
				t.Fatalf("second start while one is active: %d, want 409", code)
			}
			if code, out := call(t, server, http.MethodPost, svc+"/rollouts/"+id+"/resume", ""); code != http.StatusConflict ||
				out.Rollout.State != rollouts.StateStarted {
				t.Fatalf("resume a started rollout: %d %+v, want 409", code, out)
			}
			for _, step := range []struct{ action, want string }{
				{"pause", rollouts.StatePaused},
				{"resume", rollouts.StateStarted},
				{"complete", rollouts.StateCompleted},
			} {
				code, out := call(t, server, http.MethodPost, svc+"/rollouts/"+id+"/"+step.action, `{"reason":"test `+step.action+`"}`)
				if code != http.StatusOK || out.Rollout.State != step.want {
					t.Fatalf("%s: %d %+v, want %s", step.action, code, out, step.want)
				}
			}

			code, got := call(t, server, http.MethodGet, svc+"/rollouts/latest", "")
			if code != http.StatusOK || got.Rollout.ID != id || len(got.Rollout.Events) != 4 || got.Rollout.Events[1].Reason != "test pause" {
				t.Fatalf("latest: %d %+v", code, got.Rollout)
			}

			// One history row for the rollout, plus the scaffold, and the
			// latest deployment is the rollout, not a pause record.
			rec := authorizedRequest(server, http.MethodGet, svc+"/deployments", nil)
			var history struct {
				Deployments []models.Deployment `json:"deployments"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &history); err != nil {
				t.Fatal(err)
			}
			if len(history.Deployments) != 2 || history.Deployments[0].RolloutID != id ||
				history.Deployments[0].Status != "succeeded" || history.Deployments[0].Image != "ghcr.io/acme/payments-api:v2" {
				t.Fatalf("deployment history: %+v", history.Deployments)
			}
			status := authorizedRequest(server, http.MethodGet, svc+"/status", nil)
			if !strings.Contains(status.Body.String(), `"rolloutId":"`+id+`"`) {
				t.Fatalf("latest deployment is not the rollout: %s", status.Body.String())
			}

			// A new rollout may start now; rolling back the first, by its ID,
			// leaves the second alone.
			code, second := call(t, server, http.MethodPost, svc+"/rollouts", `{"version":"v3","image":"ghcr.io/acme/payments-api:v3"}`)
			if code != http.StatusAccepted {
				t.Fatalf("start after complete: %d", code)
			}
			if code, out := call(t, server, http.MethodPost, svc+"/rollouts/"+id+"/rollback", ""); code != http.StatusOK || out.Rollout.ID != id ||
				out.Rollout.State != rollouts.StateRollbackRequested {
				t.Fatalf("rollback by id: %d %+v", code, out)
			}
			if _, out := call(t, server, http.MethodGet, svc+"/rollouts/"+second.Rollout.ID, ""); out.Rollout.State != rollouts.StateStarted {
				t.Fatalf("rolling back %s changed %s to %s", id, second.Rollout.ID, out.Rollout.State)
			}
		})
	}
}

// Audit W-8: pause wrote a deployment row named pause-<raw path> for any ID.
func TestRolloutActionsRequireAnExistingRollout(t *testing.T) {
	store := catalog.NewMemoryStore()
	server := rolloutServer(t, store)
	for _, id := range []string{"00000000-0000-4000-8000-000000000000", "..%2F..%2Fetc", "latest", "x"} {
		if code, _ := call(t, server, http.MethodPost, svc+"/rollouts/"+id+"/pause", ""); code != http.StatusNotFound {
			t.Fatalf("pause %q: %d, want 404", id, code)
		}
	}
	rec := authorizedRequest(server, http.MethodGet, svc+"/deployments", nil)
	if strings.Contains(rec.Body.String(), "pause-") || strings.Count(rec.Body.String(), `"id"`) != 1 {
		t.Fatalf("a rejected action wrote history: %s", rec.Body.String())
	}
}

type rolloutStoreDown struct{ catalog.Store }

func (rolloutStoreDown) CreateRollout(context.Context, models.Rollout, models.Deployment) (models.Rollout, error) {
	return models.Rollout{}, errors.New("connection refused")
}

// Audit W-8: a database failure was reported as 409, like a closed gate.
func TestStartRolloutReportsStoreFailuresAs500(t *testing.T) {
	server := rolloutServer(t, rolloutStoreDown{catalog.NewMemoryStore()})
	if code, _ := call(t, server, http.MethodPost, svc+"/rollouts", rolloutBody); code != http.StatusInternalServerError {
		t.Fatalf("start with the store down: %d, want 500", code)
	}
}

// Concurrent starts for one service: exactly one wins, on both stores.
func TestConcurrentRolloutStartsAdmitOne(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			server := rolloutServer(t, open(t))
			codes := make([]int, 10)
			var wg sync.WaitGroup
			for i := range codes {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					codes[i], _ = call(t, server, http.MethodPost, svc+"/rollouts", rolloutBody)
				}(i)
			}
			wg.Wait()
			count := map[int]int{}
			for _, c := range codes {
				count[c]++
			}
			if count[http.StatusAccepted] != 1 || count[http.StatusConflict] != len(codes)-1 {
				t.Fatalf("status codes %v, want one 202 and the rest 409", count)
			}
		})
	}
}

// The health gate marks the active rollout rollback-requested, and with no
// rollout in flight it reports the decision without writing anything.
func TestHealthGateRequestsRollbackOfTheActiveRollout(t *testing.T) {
	server := rolloutServer(t, catalog.NewMemoryStore())
	breach := `{"p99LatencyMs":450,"errorRate":0.2,"successCount":100}`
	if rec := authorizedRequest(server, http.MethodPost, svc+"/health-gate", []byte(breach)); rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"rollout":null`) {
		t.Fatalf("breach with no rollout: %d %s, want 200 with no rollout", rec.Code, rec.Body.String())
	}
	_, started := call(t, server, http.MethodPost, svc+"/rollouts", rolloutBody)
	code, out := call(t, server, http.MethodPost, svc+"/health-gate", breach)
	if code != http.StatusAccepted || out.Rollout.ID != started.Rollout.ID || out.Rollout.State != rollouts.StateRollbackRequested ||
		!strings.Contains(out.Rollout.Events[len(out.Rollout.Events)-1].Reason, "health gate") {
		t.Fatalf("breach during a rollout: %d %+v", code, out.Rollout)
	}
}
