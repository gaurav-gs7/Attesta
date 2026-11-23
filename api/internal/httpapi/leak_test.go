package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
)

// errLeaky is what pgx reports when the database is down: it names the
// user, host and port.
var errLeaky = errors.New(`failed to connect to user=attesta database=attesta: 10.0.4.17:5432 (db.internal): dial error: connection refused`)

var leakMarkers = []string{"10.0.4.17", "5432", "db.internal", "user=attesta", "dial error"}

// leakyStore fails the calls below with errLeaky once broken is set.
type leakyStore struct {
	catalog.Store
	broken bool
}

func (s *leakyStore) fail() bool { return s.broken }

func (s *leakyStore) ListServices(ctx context.Context) ([]models.Service, error) {
	if s.fail() {
		return nil, errLeaky
	}
	return s.Store.ListServices(ctx)
}

func (s *leakyStore) CreateService(ctx context.Context, svc models.Service) (models.Service, error) {
	if s.fail() {
		return models.Service{}, errLeaky
	}
	return s.Store.CreateService(ctx, svc)
}

func (s *leakyStore) GetServiceByName(ctx context.Context, name string) (models.Service, error) {
	if s.fail() {
		return models.Service{}, errLeaky
	}
	return s.Store.GetServiceByName(ctx, name)
}

func (s *leakyStore) Ping(context.Context) error {
	if s.fail() {
		return errLeaky
	}
	return nil
}

// Audit X-5: internal errors went to the client verbatim, leaking the
// database user, host and port.
func TestInternalErrorsDoNotReachTheClient(t *testing.T) {
	store := &leakyStore{Store: catalog.NewMemoryStore()}
	logs := &logBuffer{}
	server := newTestServer(t, store, WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	store.broken = true

	requests := []struct {
		method, path string
		body         []byte
	}{
		{http.MethodGet, "/api/v1/services", nil},
		{http.MethodGet, "/api/v1/services/payments-api", nil},
		{http.MethodGet, "/api/v1/services/payments-api/status", nil},
		{http.MethodPost, "/api/v1/services", onboardingBody},
		{http.MethodGet, "/readyz", nil},
	}
	for _, rq := range requests {
		rec := authorizedRequest(server, rq.method, rq.path, rq.body)
		if rec.Code < 500 {
			t.Errorf("%s %s: %d, want a 5xx with the store down", rq.method, rq.path, rec.Code)
		}
		for _, marker := range leakMarkers {
			if strings.Contains(rec.Body.String(), marker) {
				t.Errorf("%s %s leaked %q: %s", rq.method, rq.path, marker, rec.Body.String())
			}
		}
		if rq.path == "/readyz" {
			continue
		}
		var body struct {
			RequestID string `json:"requestId"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body.RequestID == "" || body.RequestID != rec.Header().Get(RequestIDHeader) {
			t.Errorf("%s %s: requestId %q, header %q; the client needs the ID to quote", rq.method, rq.path, body.RequestID, rec.Header().Get(RequestIDHeader))
		}
		if !loggedWithRequestID(t, logs, body.RequestID) {
			t.Errorf("%s %s: the error detail was not logged under request %s", rq.method, rq.path, body.RequestID)
		}
	}
}

func loggedWithRequestID(t *testing.T, logs *logBuffer, id string) bool {
	for _, rec := range logs.records(t) {
		if rec["request_id"] == id && strings.Contains(rec["error"].(string), "10.0.4.17:5432") {
			return true
		}
	}
	return false
}

// A Prometheus failure names the server it tried; the SLO reason does not.
func TestPrometheusErrorsDoNotReachTheClient(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream prom-0.monitoring.svc:9090 unavailable", http.StatusBadGateway)
	}))
	defer prom.Close()
	server := newTestServer(t, catalog.NewMemoryStore(), WithPrometheusURL(prom.URL),
		WithLogger(slog.New(slog.NewJSONHandler(&logBuffer{}, nil))))
	if rec := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody); rec.Code != http.StatusCreated {
		t.Fatalf("onboard: %d %s", rec.Code, rec.Body.String())
	}
	rec := authorizedRequest(server, http.MethodGet, "/api/v1/services/payments-api/slos", nil)
	host := strings.TrimPrefix(prom.URL, "http://")
	for _, marker := range []string{host, "prom-0", "9090", "502"} {
		if strings.Contains(rec.Body.String(), marker) {
			t.Errorf("SLO response leaked %q: %s", marker, rec.Body.String())
		}
	}
	if !strings.Contains(rec.Body.String(), "Prometheus query failed") {
		t.Errorf("SLO response does not say the query failed: %s", rec.Body.String())
	}
}
