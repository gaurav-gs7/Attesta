package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/gaurav-gs7/attesta/api/internal/auth"
	"github.com/gaurav-gs7/attesta/api/internal/prometheus"
	"github.com/gaurav-gs7/attesta/api/internal/templates"
)

// ErrNoAuth is returned by Validate when no API token is configured and
// unauthenticated access has not been explicitly allowed.
var ErrNoAuth = errors.New("no authentication configured: set ATTESTA_API_TOKENS_FILE, ATTESTA_AUTH_KUBERNETES=true " +
	"or ATTESTA_API_TOKEN; refusing to start an unauthenticated API (set ATTESTA_INSECURE_NO_AUTH=true for local development only)")

type Config struct {
	Addr           string
	DatabaseURL    string
	TemplateDir    string
	APIToken       string
	InsecureNoAuth bool
	// TokensFile lists API tokens by hash, each with a name and role.
	TokensFile string
	// KubernetesAuth enables Kubernetes TokenReview, with roles from
	// RoleBindingsFile and tokens required to carry Audiences.
	KubernetesAuth   bool
	RoleBindingsFile string
	Audiences        []string
	// TLSCertFile and TLSKeyFile make the API serve HTTPS.
	TLSCertFile   string
	TLSKeyFile    string
	MaxBodyMB     int64
	PrometheusURL string
	// ImageRegistry is where generated services' images live, as
	// host[:port][/path]; each service's image is <registry>/<name>.
	ImageRegistry string
}

func Load() Config {
	return Config{
		Addr:             env("ATTESTA_ADDR", ":8080"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		TemplateDir:      env("ATTESTA_TEMPLATE_DIR", "templates"),
		APIToken:         os.Getenv("ATTESTA_API_TOKEN"),
		InsecureNoAuth:   boolEnv("ATTESTA_INSECURE_NO_AUTH"),
		TokensFile:       os.Getenv("ATTESTA_API_TOKENS_FILE"),
		KubernetesAuth:   boolEnv("ATTESTA_AUTH_KUBERNETES"),
		RoleBindingsFile: os.Getenv("ATTESTA_AUTH_ROLE_BINDINGS_FILE"),
		Audiences:        list(os.Getenv("ATTESTA_AUTH_AUDIENCES")),
		TLSCertFile:      os.Getenv("ATTESTA_TLS_CERT_FILE"),
		TLSKeyFile:       os.Getenv("ATTESTA_TLS_KEY_FILE"),
		MaxBodyMB:        1,
		PrometheusURL:    os.Getenv("ATTESTA_PROMETHEUS_URL"),
		ImageRegistry:    env("ATTESTA_IMAGE_REGISTRY", templates.DefaultImageRegistry),
	}
}

// Validate rejects configurations that would expose an unauthenticated API.
func (c Config) Validate() error {
	if c.APIToken == "" && c.TokensFile == "" && !c.KubernetesAuth && !c.InsecureNoAuth {
		return ErrNoAuth
	}
	if c.KubernetesAuth && c.RoleBindingsFile == "" {
		return errors.New("ATTESTA_AUTH_KUBERNETES needs ATTESTA_AUTH_ROLE_BINDINGS_FILE to map users and groups to roles")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("set both ATTESTA_TLS_CERT_FILE and ATTESTA_TLS_KEY_FILE, or neither")
	}
	if c.ImageRegistry != "" {
		if err := templates.ValidateImageRegistry(c.ImageRegistry); err != nil {
			return fmt.Errorf("ATTESTA_IMAGE_REGISTRY: %w", err)
		}
	}
	return nil
}

// Authenticator builds the chain of configured authenticators: the tokens
// file, Kubernetes TokenReview, then the legacy shared token. It returns
// nil when none is configured. newKube is called only for Kubernetes auth.
func (c Config) Authenticator(newKube func() (kubernetes.Interface, error)) (auth.Authenticator, error) {
	var chain auth.Chain
	if c.TokensFile != "" {
		tokens, err := auth.NewTokenFile(c.TokensFile)
		if err != nil {
			return nil, err
		}
		chain = append(chain, tokens)
	}
	if c.KubernetesAuth {
		bindings, err := auth.LoadBindings(c.RoleBindingsFile)
		if err != nil {
			return nil, err
		}
		client, err := newKube()
		if err != nil {
			return nil, fmt.Errorf("kubernetes client for token review: %w", err)
		}
		chain = append(chain, auth.NewTokenReview(client, bindings, c.Audiences))
	}
	if c.APIToken != "" {
		chain = append(chain, auth.NewStaticToken(c.APIToken, auth.RoleAdmin))
	}
	if len(chain) == 0 {
		return nil, nil
	}
	return chain, nil
}

func list(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
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
