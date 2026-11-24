package controller

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics exported by the controller, beside controller-runtime's own
// (controller_runtime_reconcile_total, controller_runtime_reconcile_time_seconds,
// workqueue_*). They are created unregistered so tests can use them freely;
// RegisterMetrics adds them to a registry.
var (
	rollbackTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "rollback_total",
		Help:      "Rollback decisions for blocked rollouts by outcome (triggered, failed, skipped_not_authorized, skipped_no_healthy_revision, skipped_already_healthy, skipped_reverted, skipped_healthy_revision_gone).",
	}, []string{"outcome"})

	analysisTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "analysis_total",
		Help:      "Metric analysis checks of rolling-out revisions by result (Passed, Failed, Inconclusive).",
	}, []string{"result"})

	decisionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "decisions_recorded_total",
		Help:      "RolloutDecision records written, by action (Promoted, RolledBack, RollbackFailed, RollbackSkipped, Paused).",
	}, []string{"action"})

	phaseTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "attesta",
		Subsystem: "rolloutguard",
		Name:      "phase_transitions_total",
		Help:      "RolloutGuard phase transitions by the phase entered.",
	}, []string{"phase"})
)

var registerOnce sync.Once

// RegisterMetrics registers the controller's metrics with reg, normally
// controller-runtime's metrics.Registry. Only the first call has an effect.
func RegisterMetrics(reg prometheus.Registerer) {
	registerOnce.Do(func() {
		reg.MustRegister(rollbackTotal, phaseTransitions, analysisTotal, decisionsTotal)
	})
}
