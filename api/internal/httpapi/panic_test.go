package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

// logBuffer collects JSON log records.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("log line is not JSON: %s", sc.Text())
		}
		out = append(out, rec)
	}
	return out
}

type panickingStore struct{ catalog.Store }

func (panickingStore) ListServices(context.Context) ([]models.Service, error) {
	var services []models.Service
	return services[len(services) : len(services)+1], nil // slice bounds out of range
}

func find(records []map[string]any, msg string) map[string]any {
	for _, r := range records {
		if r["msg"] == msg {
			return r
		}
	}
	return nil
}

// Audit W-11: the recoverer answered 500 without logging the panic, and
// requests had no ID to tie a response to a log line.
func TestPanicsAreLoggedWithStackAndRequestID(t *testing.T) {
	logs := &logBuffer{}
	server := newTestServer(t, panickingStore{catalog.NewMemoryStore()},
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))

	rec := authorizedRequest(server, http.MethodGet, "/api/v1/services", nil)
	id := rec.Header().Get(RequestIDHeader)
	if rec.Code != http.StatusInternalServerError || id == "" || !strings.Contains(rec.Body.String(), id) {
		t.Fatalf("panic response: %d id %q body %s; want 500 carrying the request ID", rec.Code, id, rec.Body.String())
	}
	records := logs.records(t)
	panicked := find(records, "handler panicked")
	if panicked == nil {
		t.Fatalf("panic was not logged: %v", records)
	}
	if panicked["request_id"] != id || !strings.Contains(panicked["panic"].(string), "out of range") ||
		!strings.Contains(panicked["stack"].(string), "ListServices") {
		t.Fatalf("panic log lacks value, stack or request ID: %v", panicked)
	}
	access := find(records, "request")
	if access == nil || access["request_id"] != id || access["status"] != float64(500) {
		t.Fatalf("access log: %v; want status 500 with the same request ID", access)
	}
}

func TestRequestIDIsTakenFromTheCallerOrGenerated(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore(), WithLogger(slog.New(slog.NewJSONHandler(&logBuffer{}, nil))))
	send := func(header string) string {
		req := httptest.NewRequest(http.MethodGet, "/livez", nil)
		if header != "" {
			req.Header.Set(RequestIDHeader, header)
		}
		rec := httptest.NewRecorder()
		server.Routes().ServeHTTP(rec, req)
		return rec.Header().Get(RequestIDHeader)
	}
	if got := send("trace-abc.123"); got != "trace-abc.123" {
		t.Fatalf("caller's request ID not kept: %q", got)
	}
	for _, bad := range []string{"has space", strings.Repeat("a", 200), "x\ny"} {
		if got := send(bad); got == bad || len(got) != 32 {
			t.Fatalf("malformed request ID %q echoed as %q; want a generated one", bad, got)
		}
	}
	if a, b := send(""), send(""); a == b || a == "" {
		t.Fatalf("generated request IDs %q and %q", a, b)
	}
}

// A panic inside a workflow step is logged with the ID of the request that
// started the run.
func TestWorkflowStepPanicLogCarriesTheRequestID(t *testing.T) {
	logs := &logBuffer{}
	server := newTestServer(t, catalog.NewMemoryStore(), WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	ctx := context.WithValue(context.Background(), requestIDKey{}, "req-42")
	server.workflows.Run(ctx, workflowsSpecThatPanics())
	rec := find(logs.records(t), "workflow step panicked")
	if rec == nil || rec["request_id"] != "req-42" {
		t.Fatalf("step panic log: %v; want request_id req-42", rec)
	}
}

func workflowsSpecThatPanics() workflows.Spec {
	return workflows.Spec{Name: "boom", Kind: "test", Steps: []workflows.Step{{
		Name: "boom",
		Run: func(context.Context) error {
			var steps []workflows.Step
			_ = steps[len(steps)]
			return nil
		},
	}}}
}
