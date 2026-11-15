package config

import (
	"errors"
	"os"
	"strconv"
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
