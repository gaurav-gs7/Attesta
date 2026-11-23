package promql

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func server(t *testing.T, status int, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" || r.URL.Query().Get("query") == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL + "/")
}

func vector(v string) string {
	return fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,%q]}]}}`, v)
}

func TestQuery(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		want      float64
		wantFound bool
		wantErr   string
	}{
		{"sample", 200, vector("42.5"), 42.5, true, ""},
		{"empty result", 200, `{"status":"success","data":{"resultType":"vector","result":[]}}`, 0, false, ""},
		{"NaN is not a sample", 200, vector("NaN"), 0, false, ""},
		{"Inf is not a sample", 200, vector("+Inf"), 0, false, ""},
		{"query error", 200, `{"status":"error","error":"bad_data: parse error"}`, 0, false, "parse error"},
		{"HTTP error", 503, `unavailable`, 0, false, "503"},
		{"unparsable value", 200, vector("abc"), 0, false, "parse scalar"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, found, err := server(t, tc.status, tc.body).Query(context.Background(), `up`)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want || found != tc.wantFound {
				t.Fatalf("got (%v, %v, %v), want (%v, %v, nil)", got, found, err, tc.want, tc.wantFound)
			}
		})
	}
}

func TestQueryRequiresURL(t *testing.T) {
	if _, _, err := NewClient("").Query(context.Background(), "up"); err == nil {
		t.Fatal("expected an error for an empty base URL")
	}
}

func okBody(w http.ResponseWriter) {
	_, _ = w.Write([]byte(`{"status":"success","data":{"result":[{"value":[1,"42"]}]}}`))
}

// Audit S-7: the client had no auth or TLS options.
func TestClientSendsConfiguredAuth(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		okBody(w)
	}))
	defer srv.Close()
	for _, tc := range []struct {
		opts Options
		want string
	}{
		{Options{BearerToken: "s3cret"}, "Bearer s3cret"},
		{Options{Username: "attesta", Password: "pw"}, "Basic YXR0ZXN0YTpwdw=="},
		{Options{}, ""},
	} {
		c, err := New(srv.URL, tc.opts)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Query(context.Background(), "up"); err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("Authorization = %q, want %q", got, tc.want)
		}
	}
	if _, err := New(srv.URL, Options{BearerToken: "a", Username: "b"}); err == nil {
		t.Fatal("bearer token and basic auth together were accepted")
	}
}

func TestClientTrustsConfiguredCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { okBody(w) }))
	defer srv.Close()
	if _, _, err := NewClient(srv.URL).Query(context.Background(), "up"); err == nil {
		t.Fatal("a self-signed server was trusted without a CA file")
	}
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caFile, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := New(srv.URL, Options{CAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, err := c.Query(context.Background(), "up"); err != nil || !ok || v != 42 {
		t.Fatalf("query through the configured CA: %v %v %v", v, ok, err)
	}
}

// Each query has its own timeout instead of sharing one deadline.
func TestClientTimesOutEachQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()
	c, _ := New(srv.URL, Options{QueryTimeout: 100 * time.Millisecond})
	start := time.Now()
	if _, _, err := c.Query(context.Background(), "up"); err == nil {
		t.Fatal("a hung query succeeded")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("query took %s with a 100ms timeout", elapsed)
	}
}

func TestClientRejectsOversizedResponses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[{"value":[1,"1"],"pad":"` + strings.Repeat("x", 2<<20) + `"}]}}`))
	}))
	defer srv.Close()
	if _, _, err := NewClient(srv.URL).Query(context.Background(), "up"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized response: %v, want a size error", err)
	}
}
