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
//
// A revision it paused is measured again every recheckInterval and, with
// spec.sloGate.resume Automatic, resumed once the gate allows it.
func (r *RolloutGuardReconciler) applySLOGate(ctx context.Context, guard *attestav1.RolloutGuard, spec RolloutGuardSpec,
	deployment *appsv1.Deployment, decided bool, status *RolloutGuardStatus) (*decisionDraft, time.Duration, error) {

	gate := guard.Spec.SLOGate
	if status.SLOGate != nil && status.SLOGate.Paused && !deployment.Spec.Paused {
		// Someone resumed the rollout. That is the override: this revision
		// is theirs now, and a later pause is theirs too, not the gate's.
		status.SLOGate.Paused = false
	}
	switch {
	case gate == nil:
		status.SLOGate = nil
		meta.RemoveStatusCondition(&status.Conditions, ConditionSLOGate)
		return nil, 0, nil
	case r.SLO == nil:
		status.SetCondition(ConditionSLOGate, metav1.ConditionUnknown, "PrometheusNotConfigured",
			"spec.sloGate is set but the controller has no --prometheus-url; rollouts are not gated")
		return nil, 0, nil
	case deployment.Spec.Paused && deployment.Annotations[pausedByAnnotation] == guard.Name && status.SLOGate != nil && status.SLOGate.Paused:
		return r.recheckPaused(ctx, guard, spec, deployment, status)
	case decided, deployment.Spec.Paused, deployment.Status.ObservedGeneration < deployment.Generation:
		return nil, 0, nil
	case status.HealthyTemplateHash == "":
		// The first rollout the guard sees has no healthy revision to keep
		// serving; pausing it would only leave the service half deployed.
		return nil, 0, nil
	}
	current, _, err := r.ownedReplicaSets(ctx, deployment)
	if err != nil {
		return nil, 0, err
	}
	if current == nil {
		return nil, 0, nil
	}
	hash, revision := current.Labels[podTemplateHashLabel], current.Annotations[revisionAnnotation]
	if hash == status.HealthyTemplateHash || (status.SLOGate != nil && status.SLOGate.TemplateHash == hash) {
		return nil, 0, nil
	}

	at := r.now().UTC().Format(time.RFC3339)
	verdict, evidence := r.measureSLOs(ctx, gate, spec.ServiceLabel(deployment.Name), at)

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
		return nil, 0, nil
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
		return d, 0, nil
	}

	if err := r.ensureCurrent(ctx, guard); err != nil {
		return nil, 0, err
	}
	updated := deployment.DeepCopy()
	updated.Spec.Paused = true
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[pausedByAnnotation] = guard.Name
	updated.Annotations[pausedAtAnnotation] = at
	updated.Annotations[pausedReasonAnnotation] = verdict.Reason
	message := pausedMessage(gate, deployment.Name, revision, verdict.Reason)
	d.action, d.reason, d.message = attestav1.ActionPaused, reasonSLOGatePaused, message
	if err := r.writeAhead(guard, deployment, updated, status, d); err != nil {
		return nil, 0, err
	}
	if err := r.Update(ctx, updated); err != nil {
		// Decided on a view that may be stale; decide again.
		return nil, 0, fmt.Errorf("pause deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}
	result.Paused = true
	status.Phase = PhasePaused
	status.Reason = reasonSLOGatePaused
	status.Message = message
	return d, recheckInterval(gate), nil
}

// measureSLOs measures the service and applies the guard's thresholds.
func (r *RolloutGuardReconciler) measureSLOs(ctx context.Context, gate *attestav1.SLOGate, serviceLabel, at string) (models.SLOStatus, []attestav1.DecisionEvidence) {
	service := models.Service{Name: serviceLabel, SLOTarget: gate.AvailabilityTarget, SLOLatencyP95Ms: 300, SLOErrorRate: 1}
	if gate.LatencyP95TargetMs != nil {
		service.SLOLatencyP95Ms = int(*gate.LatencyP95TargetMs)
	}
	if gate.ErrorRateThreshold != nil {
		service.SLOErrorRate = *gate.ErrorRateThreshold
	}
	thresholds := gateThresholds(gate)
	signals, err := r.SLO.Signals(ctx, service)
	if err != nil {
		return slo.Unknown(service, models.SLOSourcePrometheus, fmt.Sprintf("SLOs could not be measured: %v", err)),
			[]attestav1.DecisionEvidence{{Name: "prometheus", Source: "prometheus", Value: "error: " + err.Error(), EvaluatedAt: at}}
	}
	return slo.EvaluateWith(service, signals, thresholds), sloEvidence(service, signals, thresholds, at)
}

func gateThresholds(gate *attestav1.SLOGate) slo.Thresholds {
	t := slo.DefaultThresholds
	if gate.MinErrorBudgetPct != nil {
		t.MinBudgetRemainingPct = *gate.MinErrorBudgetPct
	}
	if gate.FastBurnRate != nil {
		t.FastBurnRate = *gate.FastBurnRate
	}
	if gate.SlowBurnRate != nil {
		t.SlowBurnRate = *gate.SlowBurnRate
	}
	return t
}

const (
	defaultRecheckInterval = time.Minute
	minRecheckInterval     = 10 * time.Second
	resumeManual           = "Manual"
	reasonSLOGateResumed   = "SLOGateResumed"
	resumedAtAnnotation    = "attesta.io/resumed-at"
)

func recheckInterval(gate *attestav1.SLOGate) time.Duration {
	d, err := time.ParseDuration(gate.RecheckInterval)
	switch {
	case gate.RecheckInterval == "" || err != nil:
		return defaultRecheckInterval
	case d < minRecheckInterval:
		return minRecheckInterval
	}
	return d
}

// recheckPaused measures a revision the gate paused again, at most every
// recheckInterval, and resumes it once the gate allows it, unless resuming
// is left to a person.
func (r *RolloutGuardReconciler) recheckPaused(ctx context.Context, guard *attestav1.RolloutGuard, spec RolloutGuardSpec,
	deployment *appsv1.Deployment, status *RolloutGuardStatus) (*decisionDraft, time.Duration, error) {

	gate, paused := guard.Spec.SLOGate, status.SLOGate
	status.Reason = reasonSLOGatePaused
	status.Message = pausedMessage(gate, deployment.Name, paused.Revision, paused.Reason)
	if gate.Resume == resumeManual {
		return nil, 0, nil
	}
	interval := recheckInterval(gate)
	now := r.now().UTC()
	if last, err := time.Parse(time.RFC3339, paused.EvaluatedAt); err == nil && now.Sub(last) < interval {
		return nil, interval - now.Sub(last), nil
	}
	at := now.Format(time.RFC3339)
	verdict, evidence := r.measureSLOs(ctx, gate, spec.ServiceLabel(deployment.Name), at)
	sloGateTotal.WithLabelValues(verdict.DeploymentGate).Inc()
	paused.Gate, paused.Reason, paused.EvaluatedAt = verdict.DeploymentGate, verdict.Reason, at
	if verdict.DeploymentGate != models.GateAllowed {
		status.Message = pausedMessage(gate, deployment.Name, paused.Revision, verdict.Reason)
		status.SetCondition(ConditionSLOGate, metav1.ConditionFalse, "StillBlocked",
			fmt.Sprintf("revision %s is paused: %s; measuring again in %s", paused.Revision, verdict.Reason, interval))
		return nil, interval, nil
	}
	if !rollbackAuthorized(deployment, guard.Name) {
		status.SetCondition(ConditionSLOGate, metav1.ConditionFalse, reasonNotAuthorized,
			fmt.Sprintf("the SLOs allow revision %s again, but %s", paused.Revision, notAuthorizedMessage(deployment, guard.Name)))
		return nil, interval, nil
	}
	if err := r.ensureCurrent(ctx, guard); err != nil {
		return nil, 0, err
	}
	message := fmt.Sprintf("the SLO gate resumed the rollout of revision %s: %s", paused.Revision, verdict.Reason)
	d := &decisionDraft{
		action: attestav1.ActionResumed, trigger: "SLOGateAllowed", reason: reasonSLOGateResumed, message: message,
		revision: paused.Revision, templateHash: paused.TemplateHash,
		evidence: append(evidence, deploymentEvidence(deployment, at)...),
	}
	updated := deployment.DeepCopy()
	updated.Spec.Paused = false
	updated.Annotations[resumedAtAnnotation] = at
	delete(updated.Annotations, pausedByAnnotation)
	if err := r.writeAhead(guard, deployment, updated, status, d); err != nil {
		return nil, 0, err
	}
	if err := r.Update(ctx, updated); err != nil {
		return nil, 0, fmt.Errorf("resume deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}
	paused.Paused, paused.Resumed = false, true
	status.Phase, status.Reason, status.Message = PhaseProgressing, reasonSLOGateResumed, message
	status.SetCondition(ConditionSLOGate, metav1.ConditionTrue, "Resumed", message)
	return d, 0, nil
}

func pausedMessage(gate *attestav1.SLOGate, deployment, revision, reason string) string {
	if gate != nil && gate.Resume == resumeManual {
		return fmt.Sprintf("the SLO gate paused the rollout of revision %s: %s. "+
			"Resume it (kubectl rollout resume deployment/%s) once the SLOs allow, or to override; the gate does not pause the same revision twice",
			revision, reason, deployment)
	}
	return fmt.Sprintf("the SLO gate paused the rollout of revision %s: %s. "+
		"It resumes the rollout when the SLOs allow (measured every %s); kubectl rollout resume deployment/%s overrides it now",
		revision, reason, recheckInterval(gate), deployment)
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
func sloEvidence(service models.Service, s slo.Signals, t slo.Thresholds, at string) []attestav1.DecisionEvidence {
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
		s.ErrorBudgetRemainingPct, "%", fmt.Sprintf(">= %g%%", t.MinBudgetRemainingPct),
		func(v float64) bool { return v >= t.MinBudgetRemainingPct })
	add("error-rate", ratio(slo.SignalWindow), s.ErrorRatePct, "%", fmt.Sprintf("<= %g%%", service.SLOErrorRate),
		func(v float64) bool { return v <= service.SLOErrorRate })
	add("latency-p95", q["p95"], s.LatencyP95Ms, "ms", fmt.Sprintf("<= %dms", service.SLOLatencyP95Ms),
		func(v float64) bool { return v <= float64(service.SLOLatencyP95Ms) })
	for _, w := range slo.BurnWindows {
		limit := t.FastBurnRate
		if w.Name == "6h" || w.Name == "30m" {
			limit = t.SlowBurnRate
		}
		add("burn-rate-"+w.Name, fmt.Sprintf("%s / %g", ratio(w.Name), allowedBad), s.BurnRates[w.Name], "x",
			fmt.Sprintf("< %gx (blocks when its pair is over too)", limit), func(v float64) bool { return v < limit })
	}
	return out
}
