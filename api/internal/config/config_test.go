package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateRequiresTokenByDefault(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "")
	if err := Load().Validate(); !errors.Is(err, ErrNoAuth) {
		t.Fatalf("expected ErrNoAuth without a token, got %v", err)
	}
}

func TestValidateAcceptsToken(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "secret")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "")
	if err := Load().Validate(); err != nil {
		t.Fatalf("expected a configured token to validate, got %v", err)
	}
}

func TestValidateAllowsExplicitInsecureMode(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "true")
	if err := Load().Validate(); err != nil {
		t.Fatalf("expected explicit insecure mode to validate, got %v", err)
	}
}

func TestInsecureFlagRejectsNonBooleanValues(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "yes please")
	if err := Load().Validate(); !errors.Is(err, ErrNoAuth) {
		t.Fatalf("expected a non-boolean flag to be ignored, got %v", err)
	}
}

func TestPrometheusConfigReadsSecretsFromFiles(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ATTESTA_PROMETHEUS_URL", "https://prometheus.example")
	t.Setenv("ATTESTA_PROMETHEUS_BEARER_TOKEN_FILE", token)
	t.Setenv("ATTESTA_PROMETHEUS_QUERY_TIMEOUT", "750ms")
	t.Setenv("ATTESTA_PROMETHEUS_REQUESTS_METRIC", "istio_requests_total")
	cfg, err := Load().Prometheus()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BearerToken != "s3cret" || cfg.QueryTimeout != 750*time.Millisecond || cfg.Metrics.Requests != "istio_requests_total" {
		t.Fatalf("config = %+v", cfg)
	}

	t.Setenv("ATTESTA_PROMETHEUS_BEARER_TOKEN", "also-set")
	if _, err := Load().Prometheus(); err == nil {
		t.Fatal("token and token file together were accepted")
	}
	t.Setenv("ATTESTA_PROMETHEUS_BEARER_TOKEN", "")
	t.Setenv("ATTESTA_PROMETHEUS_QUERY_TIMEOUT", "soon")
	if _, err := Load().Prometheus(); err == nil {
		t.Fatal("a malformed timeout was accepted")
	}
}
