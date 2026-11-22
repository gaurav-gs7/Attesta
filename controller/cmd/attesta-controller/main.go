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

	ctrl, err := controller.NewRolloutGuardController(cfg, controller.Options{
		Namespace: o.namespace,
		Resync:    o.resync,
		Logger:    logger,
	})
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

	logger.Info("starting attesta-controller", "version", version, "namespace", o.namespace, "leaderElect", o.leaderElect)
	if !o.leaderElect {
		leading.Store(true)
		return ctrl.Run(runCtx, o.workers)
	}
	return runWithLeaderElection(runCtx, cfg, o, logger, &leading, func(leaderCtx context.Context) error {
		return ctrl.Run(leaderCtx, o.workers)
	})
}

func runWithLeaderElection(ctx context.Context, cfg *rest.Config, o options, logger *slog.Logger, leading *atomic.Bool, work func(context.Context) error) error {
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	identity, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("leader election identity: %w", err)
	}
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

	var workErr error
	lostLease := false
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   o.leaseDuration,
		RenewDeadline:   o.renewDeadline,
		RetryPeriod:     o.retryPeriod,
		ReleaseOnCancel: true,
		Name:            o.leaderElectionID,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				leading.Store(true)
				logger.Info("acquired leader lease", "identity", identity, "lease", ns+"/"+o.leaderElectionID)
				workErr = work(leaderCtx)
			},
			OnStoppedLeading: func() {
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
