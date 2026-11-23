package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/prometheus"
)

// ErrNoAuth is returned by Validate when no API token is configured and
// unauthenticated access has not been explicitly allowed.
var ErrNoAuth = errors.New("ATTESTA_API_TOKEN is not set; refusing to start an unauthenticated API " +
	"(set ATTESTA_INSECURE_NO_AUTH=true for local development only)")

type Config struct {
	Addr           string
	DatabaseURL    string
	TemplateDir    string
	OutputDir      string
	APIToken       string
	InsecureNoAuth bool
	MaxBodyMB      int64
	PrometheusURL  string
}

func Load() Config {
	return Config{
		Addr:           env("ATTESTA_ADDR", ":8080"),
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		TemplateDir:    env("ATTESTA_TEMPLATE_DIR", "templates"),
		OutputDir:      env("ATTESTA_OUTPUT_DIR", "generated/services"),
		APIToken:       os.Getenv("ATTESTA_API_TOKEN"),
		InsecureNoAuth: boolEnv("ATTESTA_INSECURE_NO_AUTH"),
		MaxBodyMB:      1,
		PrometheusURL:  os.Getenv("ATTESTA_PROMETHEUS_URL"),
	}
}

// Validate rejects configurations that would expose an unauthenticated API.
func (c Config) Validate() error {
	if c.APIToken == "" && !c.InsecureNoAuth {
		return ErrNoAuth
	}
	return nil
}

// Prometheus reads the Prometheus client configuration:
//
//	ATTESTA_PROMETHEUS_URL
//	ATTESTA_PROMETHEUS_BEARER_TOKEN or ATTESTA_PROMETHEUS_BEARER_TOKEN_FILE
//	ATTESTA_PROMETHEUS_USERNAME and ATTESTA_PROMETHEUS_PASSWORD(_FILE)
//	ATTESTA_PROMETHEUS_CA_FILE
//	ATTESTA_PROMETHEUS_QUERY_TIMEOUT (a Go duration, default 3s)
//	ATTESTA_PROMETHEUS_REQUESTS_METRIC, _DURATION_METRIC, _SERVICE_LABEL,
//	_STATUS_LABEL, _ERROR_STATUS (default http_requests_total,
//	http_request_duration_seconds, service, status, 5..)
//
// Secrets are best passed as files (a mounted Secret) rather than as
// environment variables.
func (c Config) Prometheus() (prometheus.Config, error) {
	cfg := prometheus.Config{URL: c.PrometheusURL}
	var err error
	if cfg.BearerToken, err = secret("ATTESTA_PROMETHEUS_BEARER_TOKEN"); err != nil {
		return cfg, err
	}
	cfg.Username = os.Getenv("ATTESTA_PROMETHEUS_USERNAME")
	if cfg.Password, err = secret("ATTESTA_PROMETHEUS_PASSWORD"); err != nil {
		return cfg, err
	}
	cfg.CAFile = os.Getenv("ATTESTA_PROMETHEUS_CA_FILE")
	if raw := os.Getenv("ATTESTA_PROMETHEUS_QUERY_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return cfg, fmt.Errorf("ATTESTA_PROMETHEUS_QUERY_TIMEOUT must be a positive duration such as 3s, got %q", raw)
		}
		cfg.QueryTimeout = d
	}
	cfg.Metrics = prometheus.Metrics{
		Requests:     os.Getenv("ATTESTA_PROMETHEUS_REQUESTS_METRIC"),
		Duration:     os.Getenv("ATTESTA_PROMETHEUS_DURATION_METRIC"),
		ServiceLabel: os.Getenv("ATTESTA_PROMETHEUS_SERVICE_LABEL"),
		StatusLabel:  os.Getenv("ATTESTA_PROMETHEUS_STATUS_LABEL"),
		ErrorStatus:  os.Getenv("ATTESTA_PROMETHEUS_ERROR_STATUS"),
	}
	return cfg, nil
}

// secret reads NAME, or the file named by NAME_FILE.
func secret(name string) (string, error) {
	path := os.Getenv(name + "_FILE")
	if path == "" {
		return os.Getenv(name), nil
	}
	if os.Getenv(name) != "" {
		return "", fmt.Errorf("set %s or %s_FILE, not both", name, name)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s_FILE: %w", name, err)
	}
	return strings.TrimSpace(string(b)), nil
}

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func boolEnv(key string) bool {
	value, err := strconv.ParseBool(os.Getenv(key))
	return err == nil && value
}
