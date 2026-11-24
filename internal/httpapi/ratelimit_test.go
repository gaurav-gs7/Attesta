package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/internal/catalog"
	"github.com/gaurav-gs7/attesta/internal/models"
)

func send(h http.Handler, from, token string, header ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/services", nil)
	req.RemoteAddr = from + ":40000"
	if token != "" {
		req.Header.Set("X-Attesta-Token", token)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Add(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Audit X-6: nothing limited how fast one client could call the API.
func TestPerIPLimitStopsUnauthenticatedFloods(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore(),
		WithRateLimits(RateLimits{PerIP: Limit{Rate: 0.001, Burst: 3}}))
	h := server.Routes()
	for i := 0; i < 3; i++ {
		if rec := send(h, "198.51.100.7", ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("request %d: %d, want 401 inside the burst", i, rec.Code)
		}
	}
	rec := send(h, "198.51.100.7", "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over the burst: %d, want 429", rec.Code)
	}
	if after, err := strconv.Atoi(rec.Header().Get("Retry-After")); err != nil || after < 1 {
		t.Fatalf("Retry-After = %q, want whole seconds", rec.Header().Get("Retry-After"))
	}
	if !strings.Contains(rec.Body.String(), rec.Header().Get(RequestIDHeader)) {
		t.Fatalf("429 body lacks the request ID: %s", rec.Body.String())
	}
	if rec := send(h, "198.51.100.8", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("another address: %d, want its own bucket", rec.Code)
	}
	probe := httptest.NewRequest(http.MethodGet, "/livez", nil)
	probe.RemoteAddr = "198.51.100.7:40000"
	live := httptest.NewRecorder()
	h.ServeHTTP(live, probe)
	if live.Code != http.StatusOK {
		t.Fatalf("/livez from a limited address: %d; probes must not be limited", live.Code)
	}
}

func TestPerPrincipalLimitSpansAddresses(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore(),
		WithRateLimits(RateLimits{PerPrincipal: Limit{Rate: 0.001, Burst: 2}}))
	h := server.Routes()
	for i := 0; i < 2; i++ {
		if rec := send(h, fmt.Sprintf("203.0.113.%d", i), "secret"); rec.Code != http.StatusOK {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
	}
	if rec := send(h, "203.0.113.99", "secret"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("same principal from a new address: %d, want 429", rec.Code)
	}
}

func TestForwardedForIsUsedOnlyWhenTrusted(t *testing.T) {
	for _, trust := range []bool{false, true} {
		server := newTestServer(t, catalog.NewMemoryStore(),
			WithRateLimits(RateLimits{PerIP: Limit{Rate: 0.001, Burst: 1}, TrustForwardedFor: trust}))
		h := server.Routes()
		send(h, "10.0.0.1", "", "X-Forwarded-For", "192.0.2.1")
		// A client behind the same proxy, and a client forging the header.
		code := send(h, "10.0.0.1", "", "X-Forwarded-For", "192.0.2.1, 192.0.2.2").Code
		if trust && code != http.StatusUnauthorized {
			t.Errorf("trusted: a different client behind the proxy got %d, want its own bucket", code)
		}
		if !trust && code != http.StatusTooManyRequests {
			t.Errorf("untrusted: X-Forwarded-For changed the key (%d)", code)
		}
	}
}

func TestLimiterTableIsBounded(t *testing.T) {
	l := newLimiter(Limit{Rate: 1, Burst: 1})
	clock := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return clock }
	for i := 0; i < maxLimiterKeys+500; i++ {
		l.allow(strconv.Itoa(i))
	}
	if n := len(l.buckets); n > maxLimiterKeys {
		t.Fatalf("%d buckets, cap %d", n, maxLimiterKeys)
	}
	// The 500 keys past the cap shared one overflow bucket and drained it.
	if ok, _ := l.allow("new"); ok {
		t.Fatal("a new key past the cap got a fresh bucket")
	}
	clock = clock.Add(limiterIdle + time.Minute)
	if ok, _ := l.allow("after-idle"); !ok {
		t.Fatal("idle buckets were not reclaimed")
	}
	if n := len(l.buckets); n != 1 {
		t.Fatalf("%d buckets after the idle sweep, want 1", n)
	}
}

type hangingStore struct{ catalog.Store }

func (hangingStore) ListServices(ctx context.Context) ([]models.Service, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRequestTimeoutCancelsSlowStoreCalls(t *testing.T) {
	server := newTestServer(t, hangingStore{catalog.NewMemoryStore()}, WithRequestTimeout(50*time.Millisecond))
	start := time.Now()
	rec := authorizedRequest(server, http.MethodGet, "/api/v1/services", nil)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("request took %s; the timeout is 50ms", elapsed)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("timed-out request: %d", rec.Code)
	}
}

// The DNS-label cap reaches the API: 63 characters pass, 64 do not.
func TestServiceNameLengthIsCapped(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore())
	for _, n := range []int{63, 64} {
		name := "a" + strings.Repeat("b", n-1)
		body := strings.Replace(string(onboardingBody), `"name":"payments-api"`, `"name":"`+name+`"`, 1)
		if body == string(onboardingBody) {
			t.Fatal("onboardingBody has no payments-api name to replace")
		}
		code := authorizedRequest(server, http.MethodPost, "/api/v1/services", []byte(body)).Code
		if n == 63 && code != http.StatusCreated {
			t.Errorf("63-character name: %d, want 201", code)
		}
		if n == 64 && code != http.StatusUnprocessableEntity {
			t.Errorf("64-character name: %d, want 422", code)
		}
	}
}
