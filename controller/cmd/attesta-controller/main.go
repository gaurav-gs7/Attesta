package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

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
	showVersion      bool
	prometheusURL    string
	promTokenFile    string
	promCAFile       string
	promTimeout      time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.kubeconfig, "kubeconfig", "", "path to kubeconfig; defaults to in-cluster config, then ~/.kube/config")
	flag.StringVar(&o.namespace, "namespace", "", "namespace to watch; empty watches all namespaces")
	flag.DurationVar(&o.resync, "resync", 30*time.Second, "periodic RolloutGuard resync interval")
	flag.IntVar(&o.workers, "workers", 2, "concurrent reconcile workers")
	flag.StringVar(&o.metricsAddr, "metrics-addr", ":8080", "address for the Prometheus /metrics endpoint; empty disables it")
	flag.StringVar(&o.healthAddr, "health-addr", ":8081", "address for /healthz and /readyz")
	flag.BoolVar(&o.leaderElect, "leader-elect", true, "use a Lease so only one replica reconciles at a time")
	flag.StringVar(&o.leaderElectionNS, "leader-election-namespace", "", "namespace for the leader election Lease; defaults to $POD_NAMESPACE, then attesta-system")
	flag.StringVar(&o.leaderElectionID, "leader-election-id", "attesta-controller", "name of the leader election Lease")
	flag.DurationVar(&o.leaseDuration, "lease-duration", 15*time.Second, "leader election lease duration")
	flag.DurationVar(&o.renewDeadline, "renew-deadline", 10*time.Second, "leader election renew deadline")
	flag.DurationVar(&o.retryPeriod, "retry-period", 2*time.Second, "leader election retry period")
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
	if err := run(o, logger); err != nil {
		logger.Error("attesta-controller exited", "error", err)
		os.Exit(1)
	}
}

func run(o options, logger *slog.Logger) error {
	cfg, err := kubeConfig(o.kubeconfig)
	if err != nil {
		return fmt.Errorf("load Kubernetes config: %w", err)
	}
	cfg.UserAgent = "attesta-controller/" + version

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	controller.RegisterMetrics(registry)

	opts := controller.Options{
		Namespace: o.namespace,
		Resync:    o.resync,
		Logger:    logger,
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
		client, err := promql.New(o.prometheusURL, promOpts)
		if err != nil {
			return fmt.Errorf("configure Prometheus: %w", err)
		}
		opts.Prometheus = client
	}
	ctrl, err := controller.NewRolloutGuardController(cfg, opts)
	if err != nil {
		return fmt.Errorf("create rollout guard controller: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Standby replicas report ready so a rolling update of the controller
	// Deployment can proceed; the leader reports ready once its caches sync.
	var leading atomic.Bool
	ready := func() bool { return !leading.Load() || ctrl.HasSynced() }

	servers := []*http.Server{healthServer(o.healthAddr, ready)}
	if o.metricsAddr != "" {
		servers = append(servers, metricsServer(o.metricsAddr, registry))
	}
	serverErr := make(chan error, len(servers))
	for _, srv := range servers {
		go func(srv *http.Server) {
			logger.Info("serving", "addr", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serverErr <- fmt.Errorf("serve %s: %w", srv.Addr, err)
			}
		}(srv)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, srv := range servers {
			_ = srv.Shutdown(shutdownCtx)
		}
	}()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		select {
		case err := <-serverErr:
			logger.Error("http server failed", "error", err)
			cancelRun()
		case <-runCtx.Done():
		}
	}()

	logger.Info("starting attesta-controller", "version", version, "namespace", o.namespace, "leaderElect", o.leaderElect, "metricAnalysis", o.prometheusURL != "")
	if !o.leaderElect {
		leading.Store(true)
		return ctrl.Run(runCtx, o.workers)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	identity, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("leader election identity: %w", err)
	}
	return runWithLeaderElection(runCtx, kube, identity, o, logger, &leading, func(leaderCtx context.Context) error {
		return ctrl.Run(leaderCtx, o.workers)
	})
}

// runWithLeaderElection runs work only while this replica holds the Lease.
//
// On shutdown the Lease is released only after work has returned. client-go's
// ReleaseOnCancel releases as soon as its context is cancelled, without
// waiting for OnStartedLeading to finish, so a standby could start
// reconciling while this replica is still inside a reconcile, possibly mid
// rollback. Here the elector runs on its own context, which is cancelled once
// work has stopped (or after renewDeadline, if it does not stop).
//
// If the Lease is lost instead, work's context is cancelled and an error is
// returned so the process exits and restarts with fresh caches.
func runWithLeaderElection(ctx context.Context, kube kubernetes.Interface, identity string, o options, logger *slog.Logger, leading *atomic.Bool, work func(context.Context) error) error {
	ns := o.leaderElectionNS
	if ns == "" {
		ns = os.Getenv("POD_NAMESPACE")
	}
	if ns == "" {
		ns = "attesta-system"
	}
	lock, err := resourcelock.New(resourcelock.LeasesResourceLock, ns, o.leaderElectionID,
		kube.CoreV1(), kube.CoordinationV1(), resourcelock.ResourceLockConfig{Identity: identity})
	if err != nil {
		return fmt.Errorf("leader election lock: %w", err)
	}

	electorCtx, cancelElector := context.WithCancel(context.Background())
	defer cancelElector()
	workStarted, workDone := make(chan struct{}), make(chan struct{})
	// mu guards the hand-off between the elector's OnStartedLeading goroutine
	// and this function's return: work never starts after RunOrDie returned.
	var (
		mu       sync.Mutex
		started  bool
		returned bool
	)
	go func() {
		select {
		case <-ctx.Done():
		case <-electorCtx.Done():
			return
		}
		select {
		case <-workStarted:
			select {
			case <-workDone:
			case <-time.After(o.renewDeadline):
				logger.Warn("work did not stop before the renew deadline; releasing the lease anyway")
			}
		default:
		}
		cancelElector()
	}()

	var workErr error
	lostLease := false
	leaderelection.RunOrDie(electorCtx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   o.leaseDuration,
		RenewDeadline:   o.renewDeadline,
		RetryPeriod:     o.retryPeriod,
		ReleaseOnCancel: true,
		Name:            o.leaderElectionID,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				mu.Lock()
				if returned {
					mu.Unlock()
					return
				}
				started = true
				mu.Unlock()
				close(workStarted)
				defer close(workDone)
				// Work stops on shutdown or on losing the lease, whichever
				// comes first.
				workCtx, cancelWork := context.WithCancel(leaderCtx)
				defer cancelWork()
				stop := context.AfterFunc(ctx, cancelWork)
				defer stop()
				leading.Store(true)
				logger.Info("acquired leader lease", "identity", identity, "lease", ns+"/"+o.leaderElectionID)
				workErr = work(workCtx)
			},
			OnStoppedLeading: func() {
				leading.Store(false)
				if ctx.Err() == nil {
					lostLease = true
				}
				logger.Info("stopped leading", "identity", identity)
			},
			OnNewLeader: func(current string) {
				if current != identity {
					logger.Info("following leader", "leader", current)
				}
			},
		},
	})
	// RunOrDie does not wait for OnStartedLeading. Wait for work here, so
	// workErr is safe to read and nothing is still reconciling on return.
	mu.Lock()
	returned = true
	wasStarted := started
	mu.Unlock()
	if wasStarted {
		select {
		case <-workDone:
		case <-time.After(o.renewDeadline):
			return errors.New("work did not stop after leadership ended")
		}
	}
	if workErr != nil {
		return workErr
	}
	if lostLease {
		// Exit so the pod restarts with fresh caches rather than reconciling
		// alongside the new leader.
		return errors.New("lost leader lease")
	}
	return nil
}

func healthServer(addr string, ready func() bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(w, "caches not synced", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func metricsServer(addr string, registry *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func kubeConfig(path string) (*rest.Config, error) {
	if path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	return clientcmd.BuildConfigFromFlags("", filepath.Join(home, ".kube", "config"))
}
