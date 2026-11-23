package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/auth"
	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/templates"
)

func tokenServer(t *testing.T, logs *logBuffer) (*Server, catalog.Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	body := fmt.Sprintf(`{"tokens":[
		{"name":"viewer-bot","role":"viewer","sha256":%q},
		{"name":"ci-deployer","role":"operator","sha256":%q},
		{"name":"gaurav","role":"admin","sha256":%q}]}`,
		auth.HashToken("tok-view"), auth.HashToken("tok-ops"), auth.HashToken("tok-admin"))
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	tokens, err := auth.NewTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	store := catalog.NewMemoryStore()
	root := filepath.Clean("../../..")
	opts := []Option{WithAuthenticator(tokens), WithPrometheusURL(prometheusStub(t, healthyPrometheus()))}
	if logs != nil {
		opts = append(opts, WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	}
	return NewServer(store, templatesFor(t, root), opts...), store
}

func as(server *Server, token, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	return rec
}

// Audit X-4: one shared token meant no identity, no roles, and no actor in
// the audit trail.
func TestRolesAreEnforcedPerRoute(t *testing.T) {
	server, _ := tokenServer(t, nil)
	if rec := as(server, "", http.MethodGet, "/api/v1/services", ""); rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := as(server, "tok-wrong", http.MethodGet, "/api/v1/services", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", rec.Code)
	}
	if rec := as(server, "tok-ops", http.MethodPost, "/api/v1/services", string(onboardingBody)); rec.Code != http.StatusForbidden ||
		!strings.Contains(rec.Body.String(), "needs admin") {
		t.Fatalf("operator onboarding: %d %s, want 403", rec.Code, rec.Body.String())
	}
	if rec := as(server, "tok-admin", http.MethodPost, "/api/v1/services", string(onboardingBody)); rec.Code != http.StatusCreated {
		t.Fatalf("admin onboarding: %d %s", rec.Code, rec.Body.String())
	}
	if rec := as(server, "tok-view", http.MethodGet, "/api/v1/services/payments-api", ""); rec.Code != http.StatusOK {
		t.Fatalf("viewer read: %d", rec.Code)
	}
	if rec := as(server, "tok-view", http.MethodPost, svc+"/rollouts", rolloutBody); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer rollout: %d, want 403", rec.Code)
	}
	if rec := as(server, "tok-ops", http.MethodPost, svc+"/rollouts", rolloutBody); rec.Code != http.StatusAccepted {
		t.Fatalf("operator rollout: %d %s", rec.Code, rec.Body.String())
	}
	if rec := as(server, "", http.MethodGet, "/livez", ""); rec.Code != http.StatusOK {
		t.Fatalf("probes must stay open: %d", rec.Code)
	}
}

// Every run, rollout transition, incident and access log line names the
// caller.
func TestActorIsRecordedEverywhere(t *testing.T) {
	logs := &logBuffer{}
	server, store := tokenServer(t, logs)
	onboard := as(server, "tok-admin", http.MethodPost, "/api/v1/services", string(onboardingBody))
	var created struct {
		Workflow struct {
			ID    string `json:"id"`
			Actor string `json:"actor"`
		} `json:"workflow"`
	}
	_ = json.Unmarshal(onboard.Body.Bytes(), &created)
	if created.Workflow.Actor != "gaurav" {
		t.Fatalf("onboarding run actor = %q, want gaurav", created.Workflow.Actor)
	}
	if run, err := store.GetWorkflowRun(context.Background(), created.Workflow.ID); err != nil || run.Actor != "gaurav" {
		t.Fatalf("stored run actor = %q (%v)", run.Actor, err)
	}

	start := as(server, "tok-ops", http.MethodPost, svc+"/rollouts", rolloutBody)
	var started rolloutResponse
	_ = json.Unmarshal(start.Body.Bytes(), &started)
	pause := as(server, "tok-admin", http.MethodPost, svc+"/rollouts/"+started.Rollout.ID+"/pause", "")
	var paused rolloutResponse
	_ = json.Unmarshal(pause.Body.Bytes(), &paused)
	if ev := paused.Rollout.Events; len(ev) != 2 || ev[0].Actor != "ci-deployer" || ev[1].Actor != "gaurav" {
		t.Fatalf("rollout events: %+v, want started by ci-deployer and paused by gaurav", ev)
	}

	inc := as(server, "tok-ops", http.MethodPost, "/api/v1/incidents", `{"service":"payments-api","severity":"sev2","title":"t"}`)
	if !strings.Contains(inc.Body.String(), `"openedBy":"ci-deployer"`) {
		t.Fatalf("incident does not name its opener: %s", inc.Body.String())
	}

	if access := find(logs.records(t), "request"); access == nil || access["actor"] == "" {
		t.Fatalf("access log has no actor: %v", access)
	}
}

// With no authenticator and no explicit insecure mode, the API refuses
// every request instead of serving it unauthenticated.
func TestNoAuthenticatorFailsClosed(t *testing.T) {
	root := filepath.Clean("../../..")
	server := NewServer(catalog.NewMemoryStore(), templatesFor(t, root))
	if rec := as(server, "", http.MethodPost, "/api/v1/services", string(onboardingBody)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unconfigured auth: %d, want 401", rec.Code)
	}
	insecure := NewServer(catalog.NewMemoryStore(), templatesFor(t, root), WithInsecureNoAuth())
	rec := as(insecure, "", http.MethodPost, "/api/v1/services", string(onboardingBody))
	if rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), `"actor":"anonymous"`) {
		t.Fatalf("explicit insecure mode: %d %s", rec.Code, rec.Body.String())
	}
}

func templatesFor(t *testing.T, root string) *templates.Generator {
	t.Helper()
	return templates.NewGenerator(filepath.Join(root, "templates"))
}
