package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthRejectsWrongToken(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	for _, token := range []string{"", "secre", "secret-but-longer", "SECRET"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/services", nil)
		if token != "" {
			req.Header.Set("X-Attesta-Token", token)
		}
		rec := httptest.NewRecorder()
		server.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: expected 401, got %d", token, rec.Code)
		}
	}
}

func TestAuthAcceptsCorrectToken(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	rec := authorizedRequest(server, http.MethodGet, "/api/v1/services", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with the configured token, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestProbesDoNotRequireToken(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	for _, path := range []string{"/livez", "/readyz", "/metrics"} {
		rec := httptest.NewRecorder()
		server.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: expected 200 without a token, got %d", path, rec.Code)
		}
	}
}
