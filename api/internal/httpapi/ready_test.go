package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
)

type pingStore struct {
	catalog.Store
	ping func(context.Context) error
}

func (p pingStore) Ping(ctx context.Context) error { return p.ping(ctx) }

func probe(server *Server, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// Audit W-9: /readyz answered ready with the database down.
func TestReadyzChecksTheStore(t *testing.T) {
	down := pingStore{catalog.NewMemoryStore(), func(context.Context) error { return errors.New("connection refused") }}
	server := newTestServer(t, down)
	if rec := probe(server, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with the store down: %d %s, want 503", rec.Code, rec.Body.String())
	}
	if rec := probe(server, "/livez"); rec.Code != http.StatusOK {
		t.Fatalf("/livez must not depend on the store: %d", rec.Code)
	}

	hung := pingStore{catalog.NewMemoryStore(), func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	start := time.Now()
	if rec := probe(newTestServer(t, hung), "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with a hung store: %d, want 503", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > readyTimeout+time.Second {
		t.Fatalf("/readyz took %s with a hung store; the timeout is %s", elapsed, readyTimeout)
	}

	for name, open := range storesUnderTest(t) {
		if rec := probe(newTestServer(t, open(t)), "/readyz"); rec.Code != http.StatusOK {
			t.Fatalf("%s: /readyz with a healthy store: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}
