package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/catalog"
	"github.com/gaurav-gs7/attesta/internal/cluster"
	"github.com/gaurav-gs7/attesta/internal/config"
	"github.com/gaurav-gs7/attesta/internal/httpapi"
	"github.com/gaurav-gs7/attesta/internal/prometheus"
	"github.com/gaurav-gs7/attesta/internal/templates"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}
	authenticator, err := cfg.Authenticator(func() (kubernetes.Interface, error) {
		rc, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		return kubernetes.NewForConfig(rc)
	})
	if err != nil {
		log.Fatalf("authentication: %v", err)
	}
	if authenticator == nil {
		log.Printf("WARNING: ATTESTA_INSECURE_NO_AUTH is set; /api/ routes accept unauthenticated requests as an anonymous admin")
	} else if cfg.APIToken != "" {
		log.Printf("WARNING: ATTESTA_API_TOKEN is one shared admin identity; prefer ATTESTA_API_TOKENS_FILE or ATTESTA_AUTH_KUBERNETES")
	}
	if cfg.TLSCertFile == "" {
		log.Printf("WARNING: serving plain HTTP; set ATTESTA_TLS_CERT_FILE and ATTESTA_TLS_KEY_FILE unless TLS is terminated in front of the API")
	}

	ctx := context.Background()
	store, closer, err := catalog.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open catalog store: %v", err)
	}
	defer closer()

	promCfg, err := cfg.Prometheus()
	if err != nil {
		log.Fatalf("prometheus configuration: %v", err)
	}
	if promCfg.URL != "" {
		// Fail at startup on a bad CA file, conflicting auth or invalid
		// metric names, rather than on the first SLO check.
		if _, err := prometheus.New(promCfg); err != nil {
			log.Fatalf("prometheus configuration: %v", err)
		}
	}

	limits, err := config.RateLimits()
	if err != nil {
		log.Fatalf("rate limits: %v", err)
	}
	requestTimeout, err := config.RequestTimeout()
	if err != nil {
		log.Fatalf("request timeout: %v", err)
	}
	retention, err := config.Retention()
	if err != nil {
		log.Fatalf("retention: %v", err)
	}

	if cfg.ImageRegistry == templates.DefaultImageRegistry {
		log.Printf("WARNING: ATTESTA_IMAGE_REGISTRY is not set; generated services push to the placeholder %s", templates.DefaultImageRegistry)
	}
	generator := templates.NewGenerator(cfg.TemplateDir, templates.WithImageRegistry(cfg.ImageRegistry))
	opts := []httpapi.Option{
		httpapi.WithMaxBodyBytes(cfg.MaxBodyMB * 1024 * 1024),
		httpapi.WithPrometheus(promCfg),
		httpapi.WithRateLimits(limits),
		httpapi.WithRequestTimeout(requestTimeout),
	}
	if authenticator != nil {
		opts = append(opts, httpapi.WithAuthenticator(authenticator))
	} else {
		opts = append(opts, httpapi.WithInsecureNoAuth())
	}
	if cfg.ClusterRead {
		// $KUBECONFIG, then the in-cluster service account. The API only
		// reads: see deploy/api/cluster-read-rbac.yaml.
		restCfg, err := ctrlconfig.GetConfig()
		if err != nil {
			log.Fatalf("ATTESTA_CLUSTER_READ: %v", err)
		}
		restCfg.UserAgent = "attesta-api"
		reader, err := cluster.New(restCfg)
		if err != nil {
			log.Fatalf("ATTESTA_CLUSTER_READ: %v", err)
		}
		opts = append(opts, httpapi.WithCluster(reader))
	} else {
		log.Printf("ATTESTA_CLUSTER_READ is not set; the API cannot show RolloutGuards or the controller's decisions")
	}
	if cfg.DecisionKeysFile != "" {
		raw, err := os.ReadFile(cfg.DecisionKeysFile)
		if err != nil {
			log.Fatalf("ATTESTA_DECISION_PUBLIC_KEYS_FILE: %v", err)
		}
		keys, err := attestav1.ParsePublicKeys(raw)
		if err != nil {
			log.Fatalf("ATTESTA_DECISION_PUBLIC_KEYS_FILE: %v", err)
		}
		log.Printf("decision records must be signed by one of %v", keys.IDs())
		opts = append(opts, httpapi.WithDecisionKeys(keys))
	} else {
		log.Printf("ATTESTA_DECISION_PUBLIC_KEYS_FILE is not set; decision signatures are not checked")
	}
	handler := httpapi.NewServer(store, generator, opts...)

	reaperCtx, stopReaper := context.WithCancel(ctx)
	defer stopReaper()
	handler.StartWorkflowReaper(reaperCtx)
	catalog.StartPruner(reaperCtx, store, retention, time.Hour, slog.Default())

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("attesta api listening on %s", cfg.Addr)
		if cfg.TLSCertFile != "" {
			server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
			errCh <- server.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
			return
		}
		errCh <- server.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown: %v", err)
	}
}
