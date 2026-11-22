package promql

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
