package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/slo"
)

// SLOSource measures a service's SLOs. *prometheus.Client, which the API's
// SLO status uses too, implements it.
type SLOSource interface {
	Signals(ctx context.Context, service models.Service) (slo.Signals, error)
}

const (
	// ConditionSLOGate reports the SLO gate's verdict on the latest revision.
	ConditionSLOGate = "SLOGate"

	reasonSLOGatePaused = "SLOGatePaused"

	pausedByAnnotation     = "attesta.io/paused-by-guard"
	pausedAtAnnotation     = "attesta.io/paused-at"
	pausedReasonAnnotation = "attesta.io/paused-reason"

	onUnknownPause = "Pause"
)

// applySLOGate checks the service's SLOs once for each new revision, and
// pauses the rollout when the gate blocks it. It runs only while a revision
// other than the healthy one is rolling out and nothing else has decided
// about it: a revision that analysis blocks is rolled back, not paused.
//
// The verdict comes from slo.Evaluate on signals from the same Prometheus
// client the API's SLO status uses, so the API and the controller cannot
// disagree about whether a service is within its SLOs.
func (r *RolloutGuardReconciler) applySLOGate(ctx context.Context, guard *attestav1.RolloutGuard, spec RolloutGuardSpec,
	deployment *appsv1.Deployment, decided bool, status *RolloutGuardStatus) (*decisionDraft, error) {

	gate := guard.Spec.SLOGate
	switch {
	case gate == nil:
		status.SLOGate = nil
		meta.RemoveStatusCondition(&status.Conditions, ConditionSLOGate)
		return nil, nil
	case r.SLO == nil:
		status.SetCondition(ConditionSLOGate, metav1.ConditionUnknown, "PrometheusNotConfigured",
			"spec.sloGate is set but the controller has no --prometheus-url; rollouts are not gated")
		return nil, nil
	case decided, deployment.Spec.Paused, deployment.Status.ObservedGeneration < deployment.Generation:
		return nil, nil
	case status.HealthyTemplateHash == "":
		// The first rollout the guard sees has no healthy revision to keep
		// serving; pausing it would only leave the service half deployed.
		return nil, nil
	}
	current, _, err := r.ownedReplicaSets(ctx, deployment)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, nil
	}
	hash, revision := current.Labels[podTemplateHashLabel], current.Annotations[revisionAnnotation]
	if hash == status.HealthyTemplateHash || (status.SLOGate != nil && status.SLOGate.TemplateHash == hash) {
		return nil, nil
	}

	service := models.Service{
		Name:            spec.ServiceLabel(deployment.Name),
		SLOTarget:       gate.AvailabilityTarget,
		SLOLatencyP95Ms: 300,
		SLOErrorRate:    1,
	}
	if gate.LatencyP95TargetMs != nil {
		service.SLOLatencyP95Ms = int(*gate.LatencyP95TargetMs)
	}
	if gate.ErrorRateThreshold != nil {
		service.SLOErrorRate = *gate.ErrorRateThreshold
	}
	at := r.now().UTC().Format(time.RFC3339)
	signals, err := r.SLO.Signals(ctx, service)
	var verdict models.SLOStatus
	var evidence []attestav1.DecisionEvidence
	if err != nil {
		verdict = slo.Unknown(service, models.SLOSourcePrometheus, fmt.Sprintf("SLOs could not be measured: %v", err))
		evidence = []attestav1.DecisionEvidence{{Name: "prometheus", Source: "prometheus", Value: "error: " + err.Error(), EvaluatedAt: at}}
	} else {
		verdict = slo.Evaluate(service, signals)
		evidence = sloEvidence(service, signals, at)
	}

	result := &attestav1.SLOGateStatus{TemplateHash: hash, Revision: revision, Gate: verdict.DeploymentGate, Reason: verdict.Reason, EvaluatedAt: at}
	status.SLOGate = result
	pause := verdict.DeploymentGate == models.GateBlocked || (verdict.DeploymentGate == models.GateUnknown && gate.OnUnknown == onUnknownPause)
	switch verdict.DeploymentGate {
	case models.GateAllowed:
		status.SetCondition(ConditionSLOGate, metav1.ConditionTrue, "Allowed", fmt.Sprintf("revision %s: %s", revision, verdict.Reason))
	case models.GateBlocked:
		status.SetCondition(ConditionSLOGate, metav1.ConditionFalse, "Blocked", fmt.Sprintf("revision %s: %s", revision, verdict.Reason))
	default:
		action := "proceeding (spec.sloGate.onUnknown is Proceed)"
		if pause {
			action = "pausing (spec.sloGate.onUnknown is Pause)"
		}
		status.SetCondition(ConditionSLOGate, metav1.ConditionUnknown, "Unknown", fmt.Sprintf("revision %s: %s; %s", revision, verdict.Reason, action))
	}
	sloGateTotal.WithLabelValues(verdict.DeploymentGate).Inc()
	if !pause {
		return nil, nil
	}

	d := &decisionDraft{
		trigger:      "SLOGate" + titleGate(verdict.DeploymentGate),
		revision:     revision,
		templateHash: hash,
		evidence:     append(evidence, deploymentEvidence(deployment, at)...),
	}
	if !rollbackAuthorized(deployment, guard.Name) {
		d.action, d.reason = attestav1.ActionPauseSkipped, reasonNotAuthorized
		d.message = fmt.Sprintf("the SLO gate would pause revision %s (%s), but %s", revision, verdict.Reason, notAuthorizedMessage(deployment, guard.Name))
		return d, nil
	}

	if err := r.ensureCurrent(ctx, guard); err != nil {
		return nil, err
	}
	updated := deployment.DeepCopy()
	updated.Spec.Paused = true
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[pausedByAnnotation] = guard.Name
	updated.Annotations[pausedAtAnnotation] = at
	updated.Annotations[pausedReasonAnnotation] = verdict.Reason
	message := pausedMessage(deployment.Name, revision, verdict.Reason)
	d.action, d.reason, d.message = attestav1.ActionPaused, reasonSLOGatePaused, message
	if err := r.writeAhead(guard, deployment, updated, status, d); err != nil {
		return nil, err
	}
	if err := r.Update(ctx, updated); err != nil {
		// Decided on a view that may be stale; decide again.
		return nil, fmt.Errorf("pause deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}
	result.Paused = true
	status.Phase = PhasePaused
	status.Reason = reasonSLOGatePaused
	status.Message = message
	return d, nil
}

func pausedMessage(deployment, revision, reason string) string {
	return fmt.Sprintf("the SLO gate paused the rollout of revision %s: %s. "+
		"Resume it (kubectl rollout resume deployment/%s) once the SLOs allow, or to override; the gate does not pause the same revision twice",
		revision, reason, deployment)
}

func titleGate(gate string) string {
	switch gate {
	case models.GateBlocked:
		return "Blocked"
	case models.GateUnknown:
		return "Unknown"
	}
	return "Allowed"
}

// sloEvidence turns the measured signals into evidence, quoting the PromQL
// each value came from.
func sloEvidence(service models.Service, s slo.Signals, at string) []attestav1.DecisionEvidence {
	q := s.Queries
	allowedBad := 100 - service.SLOTarget
	var out []attestav1.DecisionEvidence
	add := func(name, query string, value *float64, unit, threshold string, passed func(float64) bool) {
		e := attestav1.DecisionEvidence{Name: name, Source: "prometheus", Query: query, Value: "absent", Threshold: threshold, EvaluatedAt: at}
		if value != nil {
			e.Value = formatValue(*value) + unit
			e.Passed = boolPtr(passed(*value))
		}
		out = append(out, e)
	}
	ratio := func(window string) string {
		return fmt.Sprintf("100 * (%s) / (%s)", q["errors:"+window], q["total:"+window])
	}
	add("error-budget-remaining",
		fmt.Sprintf("100 * (%g - %s) / %g", allowedBad, ratio(slo.BudgetWindow), allowedBad),
		s.ErrorBudgetRemainingPct, "%", fmt.Sprintf(">= %g%%", slo.MinBudgetRemainingPct),
		func(v float64) bool { return v >= slo.MinBudgetRemainingPct })
	add("error-rate", ratio(slo.SignalWindow), s.ErrorRatePct, "%", fmt.Sprintf("<= %g%%", service.SLOErrorRate),
		func(v float64) bool { return v <= service.SLOErrorRate })
	add("latency-p95", q["p95"], s.LatencyP95Ms, "ms", fmt.Sprintf("<= %dms", service.SLOLatencyP95Ms),
		func(v float64) bool { return v <= float64(service.SLOLatencyP95Ms) })
	for _, w := range slo.BurnWindows {
		limit := slo.FastBurnRate
		if w.Name == "6h" || w.Name == "30m" {
			limit = slo.SlowBurnRate
		}
		add("burn-rate-"+w.Name, fmt.Sprintf("%s / %g", ratio(w.Name), allowedBad), s.BurnRates[w.Name], "x",
			fmt.Sprintf("< %gx (blocks when its pair is over too)", limit), func(v float64) bool { return v < limit })
	}
	return out
}
