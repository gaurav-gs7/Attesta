package httpapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/templates"
	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

var onboardingBody = []byte(`{
	"name":"payments-api",
	"team":"platform",
	"owner":"gaurav",
	"language":"go",
	"environment":"staging",
	"slo":"99.9",
	"deploymentStrategy":"canary"
}`)

// storesUnderTest returns the in-memory store and, when
// ATTESTA_TEST_DATABASE_URL points at a PostgreSQL server, a store on a fresh
// database there.
func storesUnderTest(t *testing.T) map[string]func(*testing.T) catalog.Store {
	stores := map[string]func(*testing.T) catalog.Store{
		"memory": func(*testing.T) catalog.Store { return catalog.NewMemoryStore() },
	}
	if dsn := os.Getenv("ATTESTA_TEST_DATABASE_URL"); dsn != "" {
		stores["postgres"] = func(t *testing.T) catalog.Store { return freshPostgresStore(t, dsn) }
	}
	return stores
}

func freshPostgresStore(t *testing.T, adminDSN string) *catalog.PostgresStore {
	t.Helper()
	ctx := context.Background()
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "attesta_test_" + hex.EncodeToString(b[:])
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	store, err := catalog.NewPostgresStore(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.Close()
		_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return store
}

func newTestServer(t *testing.T, store catalog.Store, opts ...Option) *Server {
	t.Helper()
	root := filepath.Clean("../../..")
	opts = append([]Option{WithAPIToken("secret")}, opts...)
	return NewServer(store, templates.NewGenerator(filepath.Join(root, "templates"), t.TempDir()), opts...)
}

// failingStore fails the next N claims, or exactly the Nth workflow save,
// and otherwise behaves.
type failingStore struct {
	catalog.Store
	failClaims atomic.Int32
	failSaveAt atomic.Int32
	saves      atomic.Int32
}

var errStoreDown = errors.New("connection refused")

func (s *failingStore) ClaimWorkflowRun(ctx context.Context, run models.WorkflowRun, staleAfter time.Duration) (models.WorkflowRun, bool, error) {
	if s.failClaims.Add(-1) >= 0 {
		return models.WorkflowRun{}, false, errStoreDown
	}
	return s.Store.ClaimWorkflowRun(ctx, run, staleAfter)
}

func (s *failingStore) SaveWorkflowRun(ctx context.Context, run models.WorkflowRun) error {
	if n := s.saves.Add(1); n == s.failSaveAt.Load() {
		return errStoreDown
	}
	return s.Store.SaveWorkflowRun(ctx, run)
}

func decode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return payload
}

// Audit W-1, as reproduced: one failed workflow write gave 500 on the first
// request and on every retry, including after a restart, although the store
// was healthy again.
func TestOnboardingRecoversAfterATransientStoreFailure(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		for _, tc := range []struct {
			name string
			fail func(*failingStore)
		}{
			{"first write fails", func(s *failingStore) { s.failClaims.Store(1) }},
			// Save 2 starts the second step and records the first step's
			// success, after the service row was written.
			{"write after the service row fails", func(s *failingStore) { s.failSaveAt.Store(2) }},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				base := open(t)
				store := &failingStore{Store: base}
				tc.fail(store)
				server := newTestServer(t, store)

				first := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody)
				if first.Code != http.StatusInternalServerError {
					t.Fatalf("first request: %d %s, want 500", first.Code, first.Body.String())
				}

				retry := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody)
				if retry.Code != http.StatusCreated && retry.Code != http.StatusOK {
					t.Fatalf("retry with a healthy store: %d %s", retry.Code, retry.Body.String())
				}

				restarted := newTestServer(t, base)
				again := authorizedRequest(restarted, http.MethodPost, "/api/v1/services", onboardingBody)
				if again.Code != http.StatusOK || decode(t, again.Body.Bytes())["idempotent"] != true {
					t.Fatalf("after restart: %d %s, want an idempotent 200", again.Code, again.Body.String())
				}

				deployments := authorizedRequest(restarted, http.MethodGet, "/api/v1/services/payments-api/deployments", nil)
				var payload struct {
					Deployments []models.Deployment `json:"deployments"`
				}
				if err := json.Unmarshal(deployments.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				if len(payload.Deployments) != 1 {
					t.Fatalf("re-driven onboarding recorded %d initial deployments, want 1", len(payload.Deployments))
				}
			})
		}
	}
}

// Audit W-1: a run left "running" by a crashed process is taken over once its
// heartbeat is stale, instead of holding the key forever.
func TestOnboardingTakesOverAStaleRun(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			crashed := models.WorkflowRun{
				ID: "wf-crashed", Name: "service-onboarding", Kind: "attesta.service.onboarding",
				State: workflows.StateRunning, Service: "payments-api", Attempt: 1, MaxAttempts: 1,
				StartedAt: time.Now().UTC(), IdempotencyKey: onboardingKeyFor(t),
			}
			if _, claimed, err := store.ClaimWorkflowRun(context.Background(), crashed, time.Hour); err != nil || !claimed {
				t.Fatalf("seed: claimed=%v err=%v", claimed, err)
			}

			busy := newTestServer(t, store, WithWorkflowOptions(workflows.WithInFlightWait(0)))
			inFlight := authorizedRequest(busy, http.MethodPost, "/api/v1/services", onboardingBody)
			if inFlight.Code != http.StatusConflict || inFlight.Header().Get("Retry-After") == "" {
				t.Fatalf("while the crashed run's lease is live: %d %s, want 409 with Retry-After", inFlight.Code, inFlight.Body.String())
			}

			time.Sleep(1100 * time.Millisecond)
			server := newTestServer(t, store, WithWorkflowOptions(workflows.WithStaleAfter(time.Second), workflows.WithInFlightWait(0)))
			taken := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody)
			if taken.Code != http.StatusCreated {
				t.Fatalf("after the lease expired: %d %s, want 201", taken.Code, taken.Body.String())
			}
			old, err := store.GetWorkflowRun(context.Background(), "wf-crashed")
			if err != nil || old.State != workflows.StateFailed {
				t.Fatalf("crashed run: %+v %v, want failed", old, err)
			}
		})
	}
}

func onboardingKeyFor(t *testing.T) string {
	t.Helper()
	service, err := requestToService(mustRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	return serviceOnboardingKey(service)
}

func mustRequest(t *testing.T) models.CreateServiceRequest {
	t.Helper()
	var req models.CreateServiceRequest
	if err := json.Unmarshal(onboardingBody, &req); err != nil {
		t.Fatal(err)
	}
	return req
}
