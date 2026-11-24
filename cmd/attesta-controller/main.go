package main

import (
	"context"
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/controller"
	"github.com/gaurav-gs7/attesta/internal/promql"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

type options struct {
	kubeconfig       string
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
}

func main() {
	var o options
	flag.StringVar(&o.kubeconfig, "kubeconfig", "", "path to kubeconfig; defaults to $KUBECONFIG, in-cluster config, then ~/.kube/config")
	flag.StringVar(&o.namespace, "namespace", "", "namespace to watch; empty watches all namespaces")
	flag.DurationVar(&o.resync, "resync", 30*time.Second, "interval at which every RolloutGuard is re-evaluated")
	flag.IntVar(&o.workers, "workers", 2, "concurrent reconcile workers")
	flag.StringVar(&o.metricsAddr, "metrics-addr", ":8080", "address for the Prometheus /metrics endpoint; \"0\" disables it")
	flag.StringVar(&o.healthAddr, "health-addr", ":8081", "address for /healthz and /readyz")
	flag.BoolVar(&o.leaderElect, "leader-elect", true, "use a Lease so only one replica reconciles at a time")
	flag.StringVar(&o.leaderElectionNS, "leader-election-namespace", "", "namespace for the leader election Lease; defaults to $POD_NAMESPACE, then attesta-system")
	flag.StringVar(&o.leaderElectionID, "leader-election-id", "attesta-controller", "name of the leader election Lease")
	flag.DurationVar(&o.leaseDuration, "lease-duration", 15*time.Second, "leader election lease duration")
	flag.DurationVar(&o.renewDeadline, "renew-deadline", 10*time.Second, "leader election renew deadline")
	flag.DurationVar(&o.retryPeriod, "retry-period", 2*time.Second, "leader election retry period")
	flag.DurationVar(&o.shutdownTimeout, "shutdown-timeout", 30*time.Second, "time reconciles get to finish on shutdown before the lease is released")
	flag.StringVar(&o.prometheusURL, "prometheus-url", "", "Prometheus base URL for metric analysis of guards that set spec.checks; empty disables analysis")
	flag.StringVar(&o.promTokenFile, "prometheus-bearer-token-file", "", "file holding a bearer token for Prometheus (for example a mounted Secret)")
	flag.StringVar(&o.promCAFile, "prometheus-ca-file", "", "PEM bundle to trust for Prometheus's TLS certificate")
	flag.DurationVar(&o.promTimeout, "prometheus-query-timeout", promql.DefaultQueryTimeout, "timeout for each Prometheus query")
	flag.BoolVar(&o.showVersion, "version", false, "print the version and exit")
	flag.Parse()

	if o.showVersion {
		fmt.Println(version)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))
	if err := run(ctrl.SetupSignalHandler(), o); err != nil {
		logger.Error("attesta-controller exited", "error", err)
		os.Exit(1)
	}
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
	cfg, err := restConfig(o.kubeconfig)
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
		Scheme:                  Scheme(),
		Cache:                   cacheOpts,
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

	reconciler := &controller.RolloutGuardReconciler{
		Client:   mgr.GetClient(),
		Recorder: mgr.GetEventRecorder("attesta-controller"),
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
		reconciler.Prometheus = prom
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

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return ctrl.GetConfig()
}
