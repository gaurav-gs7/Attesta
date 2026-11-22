package controller

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/util/workqueue"
)

// Metrics exported by the controller. They are created unregistered so tests
// can use them freely; RegisterMetrics adds them to a registry.
var (
	reconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "reconcile_total",
		Help:      "RolloutGuard reconciles by result (success, error, or requeue after a stale cache read or write conflict).",
	}, []string{"result"})

	reconcileDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "reconcile_duration_seconds",
		Help:      "Time taken by one RolloutGuard reconcile.",
		Buckets:   prometheus.ExponentialBuckets(0.005, 2, 12),
	})

	rollbackTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "rollback_total",
		Help:      "Rollback decisions for blocked rollouts by outcome (triggered, failed, skipped_no_healthy_revision, skipped_already_healthy, skipped_reverted, skipped_healthy_revision_gone).",
	}, []string{"outcome"})

	analysisTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "analysis_total",
		Help:      "Metric analysis checks of rolling-out revisions by result (Passed, Failed, Inconclusive).",
	}, []string{"result"})

	phaseTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "phase_transitions_total",
		Help:      "RolloutGuard phase transitions by the phase entered.",
	}, []string{"phase"})
)

var registerOnce sync.Once

// RegisterMetrics registers the controller's metrics and client-go workqueue
// metrics with reg. It is safe to call more than once; only the first call
// has an effect.
func RegisterMetrics(reg prometheus.Registerer) {
	registerOnce.Do(func() {
		reg.MustRegister(reconcileTotal, reconcileDuration, rollbackTotal, phaseTransitions, analysisTotal)
		workqueue.SetProvider(newWorkqueueMetricsProvider(reg))
	})
}

// workqueueMetricsProvider exports client-go workqueue metrics under the same
// names controller-runtime uses, so existing dashboards work.
type workqueueMetricsProvider struct {
	adds, retries                          *prometheus.CounterVec
	depthGauge, unfinished, longestRunning *prometheus.GaugeVec
	latency, workDuration                  *prometheus.HistogramVec
}

func newWorkqueueMetricsProvider(reg prometheus.Registerer) *workqueueMetricsProvider {
	p := &workqueueMetricsProvider{
		depthGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: "workqueue", Name: "depth", Help: "Current depth of the workqueue.",
		}, []string{"name"}),
		adds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: "workqueue", Name: "adds_total", Help: "Total adds handled by the workqueue.",
		}, []string{"name"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Subsystem: "workqueue", Name: "queue_duration_seconds", Help: "Seconds an item waits in the workqueue before being processed.",
			Buckets: prometheus.ExponentialBuckets(10e-9, 10, 12),
		}, []string{"name"}),
		workDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Subsystem: "workqueue", Name: "work_duration_seconds", Help: "Seconds spent processing an item from the workqueue.",
			Buckets: prometheus.ExponentialBuckets(10e-9, 10, 12),
		}, []string{"name"}),
		unfinished: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: "workqueue", Name: "unfinished_work_seconds", Help: "Seconds of work in progress that has not been observed by work_duration.",
		}, []string{"name"}),
		longestRunning: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem: "workqueue", Name: "longest_running_processor_seconds", Help: "Seconds the longest running processor has been running.",
		}, []string{"name"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Subsystem: "workqueue", Name: "retries_total", Help: "Total retries handled by the workqueue.",
		}, []string{"name"}),
	}
	reg.MustRegister(p.depthGauge, p.adds, p.latency, p.workDuration, p.unfinished, p.longestRunning, p.retries)
	return p
}

func (p *workqueueMetricsProvider) NewDepthMetric(name string) workqueue.GaugeMetric {
	return p.depthGauge.WithLabelValues(name)
}

func (p *workqueueMetricsProvider) NewAddsMetric(name string) workqueue.CounterMetric {
	return p.adds.WithLabelValues(name)
}

func (p *workqueueMetricsProvider) NewLatencyMetric(name string) workqueue.HistogramMetric {
	return p.latency.WithLabelValues(name)
}

func (p *workqueueMetricsProvider) NewWorkDurationMetric(name string) workqueue.HistogramMetric {
	return p.workDuration.WithLabelValues(name)
}

func (p *workqueueMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeMetric {
	return p.unfinished.WithLabelValues(name)
}

func (p *workqueueMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeMetric {
	return p.longestRunning.WithLabelValues(name)
}

func (p *workqueueMetricsProvider) NewRetriesMetric(name string) workqueue.CounterMetric {
	return p.retries.WithLabelValues(name)
}
