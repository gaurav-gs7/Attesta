package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/internal/scaffold"
)

var update = flag.Bool("update", false, "rewrite the golden files")

// recorded is one request the CLI sent.
type recorded struct {
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Token  string          `json:"token,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
}

// fakeAPI records requests and answers each with {"ok":true}, or with the
// scaffold archive for .../scaffold.
func fakeAPI(t *testing.T) *[]recorded {
	t.Helper()
	var got []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec := recorded{Method: r.Method, Path: r.URL.Path, Token: r.Header.Get("X-Attesta-Token")}
		if len(body) > 0 {
			rec.Body = body
		}
		got = append(got, rec)
		if strings.HasSuffix(r.URL.Path, "/scaffold") {
			w.Header().Set("Content-Type", "application/gzip")
			_ = scaffold.WriteTarGz(w, "payments-api", []scaffold.File{{Path: "Dockerfile", Content: []byte("FROM scratch\n")}})
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("ATTESTA_API_URL", srv.URL)
	t.Setenv("ATTESTA_API_TOKEN", "test-token")
	stdout = io.Discard
	t.Cleanup(func() { stdout = os.Stdout })
	return &got
}

// golden compares the requests a command sent with testdata/<name>.golden.
// Run go test ./cmd/attesta -update to rewrite them after a deliberate
// change to what the CLI sends.
func golden(t *testing.T, name string, got []recorded) {
	t.Helper()
	for i := range got {
		if len(got[i].Body) > 0 {
			var buf bytes.Buffer
			if err := json.Indent(&buf, got[i].Body, "", "  "); err != nil {
				t.Fatalf("request body is not JSON: %s", got[i].Body)
			}
			got[i].Body = buf.Bytes()
		}
	}
	out, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, '\n')
	path := filepath.Join("testdata", name+".golden")
	if *update {
		if err := os.WriteFile(path, out, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if !bytes.Equal(out, want) {
		t.Fatalf("%s: requests differ from the golden file\n--- got\n%s\n--- want\n%s", name, out, want)
	}
}

// Audit B-4: the CLI had no tests, and copied the API's request types.
// These pin exactly what each command sends.
func TestCommandsSendTheAPIRequests(t *testing.T) {
	cases := map[string][]string{
		"service-init": {"service", "init", "payments-api", "--owner", "payments-platform", "--team", "platform",
			"--tier", "critical", "--language", "go", "--slo-availability", "99.9", "--slo-latency-p95", "300ms",
			"--deployment", "canary", "--pager", "payments-oncall", "--output", ""},
		"incident-create":   {"incident", "create", "--service", "payments-api", "--alert", "HighErrorRate", "--error-rate", "8.2"},
		"incident-no-flags": {"incident", "create", "payments-api"},
		"rollback":          {"rollback", "--name", "payments-api"},
		"check":             {"check", "payments-api"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			got := fakeAPI(t)
			if err := run(args); err != nil {
				t.Fatal(err)
			}
			golden(t, name, *got)
		})
	}
}

func TestServiceInitWritesTheScaffold(t *testing.T) {
	got := fakeAPI(t)
	out := t.TempDir()
	err := run([]string{"service", "init", "payments-api", "--owner", "gaurav", "--output", out})
	if err != nil {
		t.Fatal(err)
	}
	if len(*got) != 2 || (*got)[1].Path != "/api/v1/services/payments-api/scaffold" {
		t.Fatalf("requests: %+v", *got)
	}
	if b, err := os.ReadFile(filepath.Join(out, "payments-api", "Dockerfile")); err != nil || string(b) != "FROM scratch\n" {
		t.Fatalf("scaffold not written: %q %v", b, err)
	}
}

func TestBadInvocationsAreErrors(t *testing.T) {
	fakeAPI(t)
	for _, args := range [][]string{
		nil,
		{"nope"},
		{"service"},
		{"health-gate", "--name", "x"},
		{"onboard", "--name", "x"},
		{"service", "scaffold", "../etc"},
	} {
		if err := run(args); err == nil {
			t.Errorf("%q: accepted", args)
		}
	}
	if err := run(nil); !errors.Is(err, errUsage) {
		t.Errorf("no command: %v, want the usage", err)
	}
}

func TestAPIErrorsAreReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"service not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()
	t.Setenv("ATTESTA_API_URL", srv.URL)
	stdout = io.Discard
	defer func() { stdout = os.Stdout }()
	err := run([]string{"check", "nope"})
	if err == nil || !strings.Contains(err.Error(), "404") || !strings.Contains(err.Error(), "service not found") {
		t.Fatalf("error = %v", err)
	}
}
