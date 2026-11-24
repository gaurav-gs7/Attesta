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

func TestValidateAcceptsEachAuthMethodAndChecksPairs(t *testing.T) {
	for _, c := range []Config{
		{TokensFile: "/etc/attesta/tokens.json"},
		{KubernetesAuth: true, RoleBindingsFile: "/etc/attesta/bindings.json"},
		{APIToken: "x"},
		{InsecureNoAuth: true},
	} {
		if err := c.Validate(); err != nil {
			t.Errorf("%+v: %v", c, err)
		}
	}
	for _, c := range []Config{
		{},
		{KubernetesAuth: true},
		{APIToken: "x", TLSCertFile: "/tls.crt"},
	} {
		if err := c.Validate(); err == nil {
			t.Errorf("%+v: accepted", c)
		}
	}
}

func TestRateLimitsFromEnvironment(t *testing.T) {
	t.Setenv("ATTESTA_RATE_LIMIT_IP_RPS", "0")
	t.Setenv("ATTESTA_RATE_LIMIT_PRINCIPAL_RPS", "5")
	t.Setenv("ATTESTA_RATE_LIMIT_PRINCIPAL_BURST", "7")
	t.Setenv("ATTESTA_TRUST_FORWARDED_FOR", "true")
	limits, err := RateLimits()
	if err != nil {
		t.Fatal(err)
	}
	if limits.PerIP.Rate != 0 || limits.PerPrincipal.Rate != 5 || limits.PerPrincipal.Burst != 7 || !limits.TrustForwardedFor {
		t.Fatalf("limits = %+v", limits)
	}
	t.Setenv("ATTESTA_RATE_LIMIT_IP_RPS", "fast")
	if _, err := RateLimits(); err == nil {
		t.Fatal("a non-numeric rate was accepted")
	}
	t.Setenv("ATTESTA_REQUEST_TIMEOUT", "45s")
	if _, err := RequestTimeout(); err == nil {
		t.Fatal("a request timeout above the write timeout was accepted")
	}
}

func TestRetentionFromEnvironment(t *testing.T) {
	t.Setenv("ATTESTA_WORKFLOW_RETENTION", "7d")
	t.Setenv("ATTESTA_INCIDENT_RETENTION", "0")
	r, err := Retention()
	if err != nil || r.WorkflowRuns != 7*24*time.Hour || r.ResolvedIncidents != 0 {
		t.Fatalf("retention %+v, %v", r, err)
	}
	t.Setenv("ATTESTA_WORKFLOW_RETENTION", "5m")
	if _, err := Retention(); err == nil {
		t.Fatal("a retention under an hour was accepted")
	}
}

func TestImageRegistryIsValidated(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "x")
	t.Setenv("ATTESTA_IMAGE_REGISTRY", "https://ghcr.io/acme")
	if err := Load().Validate(); err == nil {
		t.Fatal("a registry URL with a scheme was accepted")
	}
	t.Setenv("ATTESTA_IMAGE_REGISTRY", "ghcr.io/acme")
	if err := Load().Validate(); err != nil {
		t.Fatal(err)
	}
}
