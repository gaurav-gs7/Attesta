package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/archive"
	"github.com/gaurav-gs7/attesta/internal/controller"
	"github.com/gaurav-gs7/attesta/internal/prometheus"
	"github.com/gaurav-gs7/attesta/internal/promql"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

type options struct {
	namespace        string
	resync           time.Duration
	workers          int
	metricsAddr      string
	healthAddr       string
	leaderElect      bool
	leaderElectionNS string
	leaderElectionID string
	leaseDuration    time.Duration
	renewDeadline    time.Duration
	retryPeriod      time.Duration
	shutdownTimeout  time.Duration
	showVersion      bool
	prometheusURL    string
	promTokenFile    string
	promCAFile       string
	promTimeout      time.Duration
	metrics          prometheus.Metrics
	signingKeyFile   string
	archiveURL       string
	archiveTokenFile string
}

func main() {
	o := registerFlags(flag.CommandLine)
	flag.Parse()

	if o.showVersion {
		fmt.Println(version)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))
	if err := run(ctrl.SetupSignalHandler(), *o); err != nil {
		logger.Error("attesta-controller exited", "error", err)
		os.Exit(1)
	}
}

// registerFlags defines the controller's flags on fs, which main passes as
// the default flag set: controller-runtime registers --kubeconfig there, and
// a second definition of any flag panics at startup.
func registerFlags(fs *flag.FlagSet) *options {
	var o options
	// --kubeconfig is registered by controller-runtime's config package on
	// the default flag set; defining it here too panics at startup.
	fs.StringVar(&o.namespace, "namespace", "", "namespace to watch; empty watches all namespaces")
	fs.DurationVar(&o.resync, "resync", 30*time.Second, "interval at which every RolloutGuard is re-evaluated")
	fs.IntVar(&o.workers, "workers", 2, "concurrent reconcile workers")
	fs.StringVar(&o.metricsAddr, "metrics-addr", ":8080", "address for the Prometheus /metrics endpoint; \"0\" disables it")
	fs.StringVar(&o.healthAddr, "health-addr", ":8081", "address for /healthz and /readyz")
	fs.BoolVar(&o.leaderElect, "leader-elect", true, "use a Lease so only one replica reconciles at a time")
	fs.StringVar(&o.leaderElectionNS, "leader-election-namespace", "", "namespace for the leader election Lease; defaults to $POD_NAMESPACE, then attesta-system")
	fs.StringVar(&o.leaderElectionID, "leader-election-id", "attesta-controller", "name of the leader election Lease")
	fs.DurationVar(&o.leaseDuration, "lease-duration", 15*time.Second, "leader election lease duration")
	fs.DurationVar(&o.renewDeadline, "renew-deadline", 10*time.Second, "leader election renew deadline")
	fs.DurationVar(&o.retryPeriod, "retry-period", 2*time.Second, "leader election retry period")
	fs.DurationVar(&o.shutdownTimeout, "shutdown-timeout", 30*time.Second, "time reconciles get to finish on shutdown before the lease is released")
	fs.StringVar(&o.prometheusURL, "prometheus-url", "", "Prometheus base URL for guards that set spec.checks or spec.sloGate; empty disables both")
	fs.StringVar(&o.promTokenFile, "prometheus-bearer-token-file", "", "file holding a bearer token for Prometheus (for example a mounted Secret)")
	fs.StringVar(&o.promCAFile, "prometheus-ca-file", "", "PEM bundle to trust for Prometheus's TLS certificate")
	fs.DurationVar(&o.promTimeout, "prometheus-query-timeout", promql.DefaultQueryTimeout, "timeout for each Prometheus query")
	fs.StringVar(&o.signingKeyFile, "signing-key-file", "", "PEM PKCS#8 Ed25519 private key that signs every decision record (openssl genpkey -algorithm ed25519); empty leaves records unsigned")
	fs.StringVar(&o.archiveURL, "archive-url", "", "URL every decision record is POSTed to, in order, such as attesta-api's /api/v1/decision-archive; empty keeps records in the cluster only")
	fs.StringVar(&o.archiveTokenFile, "archive-token-file", "", "file holding a bearer token for --archive-url")
	d := prometheus.DefaultMetrics
	fs.StringVar(&o.metrics.Requests, "prometheus-requests-metric", d.Requests, "counter of HTTP requests (as ATTESTA_PROMETHEUS_REQUESTS_METRIC for the API)")
	fs.StringVar(&o.metrics.Duration, "prometheus-duration-metric", d.Duration, "histogram of request durations in seconds")
	fs.StringVar(&o.metrics.ServiceLabel, "prometheus-service-label", d.ServiceLabel, "label holding the service name")
	fs.StringVar(&o.metrics.StatusLabel, "prometheus-status-label", d.StatusLabel, "label holding the HTTP status code")
	fs.StringVar(&o.metrics.ErrorStatus, "prometheus-error-status", d.ErrorStatus, "regular expression matching failed requests' status label")
	fs.BoolVar(&o.showVersion, "version", false, "print the version and exit")
	return &o
}

// Scheme holds the types the controller reads and writes.
func Scheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := attestav1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return scheme
}

func run(ctx context.Context, o options) error {
	// --kubeconfig, then $KUBECONFIG, in-cluster config, ~/.kube/config.
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes config: %w", err)
	}
	cfg.UserAgent = "attesta-controller/" + version

	leaderNS := o.leaderElectionNS
	if leaderNS == "" {
		leaderNS = os.Getenv("POD_NAMESPACE")
	}
	if leaderNS == "" {
		leaderNS = "attesta-system"
	}
	cacheOpts := cache.Options{SyncPeriod: &o.resync}
	if o.namespace != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{o.namespace: {}}
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: Scheme(),
		Cache:  cacheOpts,
		// Decision records only accumulate, and the controller reads one by
		// name only when a status write was lost. Reading them live keeps
		// them out of memory and the controller's rights at get and create.
		Client:                  client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&attestav1.RolloutDecision{}}}},
		Metrics:                 metricsserver.Options{BindAddress: o.metricsAddr},
		HealthProbeBindAddress:  o.healthAddr,
		LeaderElection:          o.leaderElect,
		LeaderElectionID:        o.leaderElectionID,
		LeaderElectionNamespace: leaderNS,
		// The lease is released only after reconciles have stopped (within
		// GracefulShutdownTimeout), so a standby never starts while this
		// replica is still inside a reconcile, possibly mid-rollback.
		LeaderElectionReleaseOnCancel: true,
		LeaseDuration:                 &o.leaseDuration,
		RenewDeadline:                 &o.renewDeadline,
		RetryPeriod:                   &o.retryPeriod,
		GracefulShutdownTimeout:       &o.shutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}
	controller.RegisterMetrics(metrics.Registry)

	instance := os.Getenv("POD_NAME")
	if instance == "" {
		instance, _ = os.Hostname()
	}
	reconciler := &controller.RolloutGuardReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Recorder:  mgr.GetEventRecorder("attesta-controller"),
		Actor:     attestav1.DecisionActor{Controller: "attesta-controller/" + version, Instance: instance},
	}
	if o.prometheusURL != "" {
		promOpts := promql.Options{CAFile: o.promCAFile, QueryTimeout: o.promTimeout}
		if o.promTokenFile != "" {
			token, err := os.ReadFile(o.promTokenFile)
			if err != nil {
				return fmt.Errorf("read --prometheus-bearer-token-file: %w", err)
			}
			promOpts.BearerToken = strings.TrimSpace(string(token))
		}
		prom, err := promql.New(o.prometheusURL, promOpts)
		if err != nil {
			return fmt.Errorf("configure Prometheus: %w", err)
		}
		// The SLO gate measures with the API's client and metric names.
		slos, err := prometheus.New(prometheus.Config{URL: o.prometheusURL, Options: promOpts, Metrics: o.metrics})
		if err != nil {
			return fmt.Errorf("configure Prometheus: %w", err)
		}
		reconciler.Prometheus, reconciler.SLO, reconciler.Metrics = prom, slos, o.metrics
	}
	if o.signingKeyFile != "" {
		raw, err := os.ReadFile(o.signingKeyFile)
		if err != nil {
			return fmt.Errorf("read --signing-key-file: %w", err)
		}
		if reconciler.SigningKey, err = attestav1.ParsePrivateKey(raw); err != nil {
			return fmt.Errorf("--signing-key-file: %w", err)
		}
		ctrl.Log.Info("signing decision records", "keyID", attestav1.KeyID(reconciler.SigningKey.Public().(ed25519.PublicKey)))
	} else {
		ctrl.Log.Info("decision records are not signed; set --signing-key-file")
	}
	if o.archiveURL != "" {
		token := ""
		if o.archiveTokenFile != "" {
			raw, err := os.ReadFile(o.archiveTokenFile)
			if err != nil {
				return fmt.Errorf("read --archive-token-file: %w", err)
			}
			token = string(raw)
		}
		reconciler.Archive = archive.New(o.archiveURL, token)
	}
	if err := reconciler.SetupWithManager(ctx, mgr, o.workers); err != nil {
		return fmt.Errorf("set up the RolloutGuard controller: %w", err)
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return err
	}
	// Ready once the caches have synced. Standby replicas sync their caches
	// too, so a rolling update of the controller can proceed.
	if err := mgr.AddReadyzCheck("caches", func(*http.Request) error {
		waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if !mgr.GetCache().WaitForCacheSync(waitCtx) {
			return errors.New("caches not synced")
		}
		return nil
	}); err != nil {
		return err
	}

	ctrl.Log.Info("starting attesta-controller", "version", version, "namespace", o.namespace,
		"leaderElect", o.leaderElect, "metricAnalysis", o.prometheusURL != "")
	return mgr.Start(ctx)
}
