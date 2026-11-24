package controller

import (
	"context"
	"fmt"
	"math"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/models"
)

// Canary phases, in status.canary.phase.
const (
	CanaryInitializing = "Initializing"
	CanaryIdle         = "Idle"
	CanaryWaiting      = "Waiting"
	CanaryProgressing  = "Progressing"
	CanaryPromoting    = "Promoting"
	CanaryAborted      = "Aborted"
)

const (
	// trackLabel marks the primary's pods, so its selector and Service do
	// not take the canary's.
	trackLabel = "attesta.io/track"
	// primaryOfAnnotation on the primary Deployment names the guarded one.
	primaryOfAnnotation = "attesta.io/primary-of"
	// noCanary is the canary Service's pod-template-hash while no revision
	// is in a canary: it matches no pod.
	noCanary = "none"

	defaultStepPause = time.Minute
	canaryPoll       = 5 * time.Second
	defaultRoutePort = 80
)

var httpRouteGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}

func primaryName(d *appsv1.Deployment) string       { return d.Name + "-primary" }
func canaryServiceName(d *appsv1.Deployment) string { return d.Name + "-canary" }

// canaryRun holds what one pass of a progressive rollout works with.
type canaryRun struct {
	r          *RolloutGuardReconciler
	guard      *attestav1.RolloutGuard
	spec       RolloutGuardSpec
	deployment *appsv1.Deployment
	primary    *appsv1.Deployment
	status     *RolloutGuardStatus
	c          *attestav1.CanaryStatus
	desired    int32
	now        time.Time
}

// reconcileCanary runs a guard with spec.canary. The guarded Deployment is
// the canary; <name>-primary, which the controller creates from it, serves
// the last promoted revision.
func (r *RolloutGuardReconciler) reconcileCanary(ctx context.Context, guard *attestav1.RolloutGuard, spec RolloutGuardSpec,
	deployment *appsv1.Deployment, prev RolloutGuardStatus, adopted *attestav1.RolloutDecision) (ctrl.Result, error) {

	status := prev
	status.ObservedGeneration = guard.Generation
	status.DeploymentGeneration = deployment.Generation
	status.ServiceRef, status.DeploymentRef, status.Strategy = spec.ServiceRef, deployment.Name, "canary"
	status.RollbackEnabled = spec.RollbackEnabled
	status.SetCondition(ConditionDeploymentFound, metav1.ConditionTrue, "DeploymentFound", fmt.Sprintf("deployment %s found", deployment.Name))
	if status.Canary == nil {
		status.Canary = &attestav1.CanaryStatus{Phase: CanaryInitializing, Step: -1}
	}

	// Every change a canary makes is a write to the guarded Deployment;
	// it needs the same consent as a rollback.
	if !rollbackAuthorized(deployment, guard.Name) {
		msg := notAuthorizedMessage(deployment, guard.Name)
		status.Phase, status.Reason, status.Message = PhaseBlocked, reasonNotAuthorized, msg
		status.SetCondition(ConditionRollbackAuthorized, metav1.ConditionFalse, reasonNotAuthorized, msg)
		return ctrl.Result{}, r.finish(ctx, guard, prev, status, nil)
	}
	status.SetCondition(ConditionRollbackAuthorized, metav1.ConditionTrue, "DeploymentConsents",
		fmt.Sprintf("deployment %s lists this guard in %s", deployment.Name, rollbackGuardsAnnotation))

	run := &canaryRun{r: r, guard: guard, spec: spec, deployment: deployment, status: &status, c: status.Canary, now: r.now().UTC()}
	decision, requeue, err := run.step(ctx)
	if err != nil {
		if isStaleRead(err) {
			return ctrl.Result{}, err
		}
		status.SetCondition("Canary", metav1.ConditionFalse, "Error", err.Error())
		_ = r.finish(ctx, guard, prev, status, nil)
		return ctrl.Result{}, err
	}
	run.report()
	if decision.worthRecording(prev, status) && !decision.matches(adopted, deployment) {
		if err := r.recordDecision(ctx, guard, deployment, &status, decision); err != nil {
			return ctrl.Result{}, r.finish(ctx, guard, prev, status, err)
		}
	}
	if retry := r.archiveDecisions(ctx, guard, &status); retry > 0 && (requeue == 0 || retry < requeue) {
		requeue = retry
	}
	if err := r.finish(ctx, guard, prev, status, nil); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// step advances the state machine by at most one decision.
func (cr *canaryRun) step(ctx context.Context) (*decisionDraft, time.Duration, error) {
	r, c, d := cr.r, cr.c, cr.deployment
	current, _, err := r.ownedReplicaSets(ctx, d)
	if err != nil {
		return nil, 0, err
	}
	if current == nil {
		c.Message = "waiting for the Deployment controller to create the first ReplicaSet"
		return nil, canaryPoll, nil
	}
	hash, revision := current.Labels[podTemplateHashLabel], current.Annotations[revisionAnnotation]

	var primary appsv1.Deployment
	err = r.Get(ctx, types.NamespacedName{Namespace: d.Namespace, Name: primaryName(d)}, &primary)
	switch {
	case apierrors.IsNotFound(err):
		return nil, canaryPoll, cr.createPrimary(ctx, hash, revision)
	case err != nil:
		return nil, 0, err
	}
	cr.primary = &primary
	cr.desired = 1
	if primary.Spec.Replicas != nil {
		cr.desired = *primary.Spec.Replicas
	}
	if err := cr.ensureServices(ctx); err != nil {
		return nil, 0, err
	}

	switch c.Phase {
	case CanaryInitializing:
		if !deploymentComplete(&primary) {
			c.Message = fmt.Sprintf("waiting for %s to be available", primary.Name)
			return nil, canaryPoll, nil
		}
		return cr.settle(ctx, attestav1.ActionPromoted, "CanaryInitialized", hash, revision,
			fmt.Sprintf("%s serves revision %s; %s is the canary and scaled to zero until its template changes", primary.Name, revision, d.Name))

	case CanaryIdle, CanaryAborted, CanaryWaiting:
		if hash == c.PrimaryTemplateHash || hash == c.AbortedTemplateHash {
			if err := cr.ensureAtRest(ctx); err != nil {
				return nil, 0, err
			}
			return nil, 0, nil
		}
		return cr.start(ctx, hash, revision)

	case CanaryProgressing:
		if hash != c.CanaryTemplateHash {
			return cr.abort(ctx, "Superseded", fmt.Sprintf("the template changed to revision %s during the canary of revision %s", revision, c.CanaryRevision))
		}
		return cr.progress(ctx)

	case CanaryPromoting:
		return cr.promote(ctx)
	}
	return nil, 0, fmt.Errorf("unknown canary phase %q", c.Phase)
}

// createPrimary copies the guarded Deployment into <name>-primary.
func (cr *canaryRun) createPrimary(ctx context.Context, hash, revision string) error {
	d := cr.deployment
	replicas := int32(1)
	if d.Spec.Replicas != nil && *d.Spec.Replicas > 0 {
		replicas = *d.Spec.Replicas
	}
	primary := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: primaryName(d), Namespace: d.Namespace,
			Labels:          managedLabels(d),
			Annotations:     map[string]string{primaryOfAnnotation: d.Name},
			OwnerReferences: ownedBy(d),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas:                &replicas,
			Selector:                primarySelector(d),
			Template:                primaryTemplate(d),
			Strategy:                d.Spec.Strategy,
			MinReadySeconds:         d.Spec.MinReadySeconds,
			ProgressDeadlineSeconds: d.Spec.ProgressDeadlineSeconds,
			RevisionHistoryLimit:    d.Spec.RevisionHistoryLimit,
		},
	}
	if err := cr.r.Create(ctx, primary); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s: %w", primary.Name, err)
	}
	cr.c.Phase, cr.c.PrimaryTemplateHash, cr.c.PrimaryRevision, cr.c.Step = CanaryInitializing, hash, revision, -1
	cr.c.Message = fmt.Sprintf("created %s from revision %s", primary.Name, revision)
	return nil
}

// managedLabels label what the controller creates for a canary. The
// guarded Deployment's own labels are not copied: a GitOps tool that tracks
// its resources by label would claim the primary as its own and prune it.
func managedLabels(d *appsv1.Deployment) map[string]string {
	return map[string]string{"app.kubernetes.io/managed-by": "attesta-controller", primaryOfAnnotation: d.Name}
}

// ownedBy makes an object go when the guarded Deployment goes. Not a
// controller reference, and not blocking deletion: neither is needed, and
// both take rights some clusters do not grant.
func ownedBy(d *appsv1.Deployment) []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: d.Name, UID: d.UID}}
}

func primarySelector(d *appsv1.Deployment) *metav1.LabelSelector {
	sel := d.Spec.Selector.DeepCopy()
	if sel.MatchLabels == nil {
		sel.MatchLabels = map[string]string{}
	}
	sel.MatchLabels[trackLabel] = "primary"
	return sel
}

func primaryTemplate(d *appsv1.Deployment) corev1.PodTemplateSpec {
	t := *d.Spec.Template.DeepCopy()
	if t.Labels == nil {
		t.Labels = map[string]string{}
	}
	delete(t.Labels, podTemplateHashLabel)
	t.Labels[trackLabel] = "primary"
	return t
}

// ensureServices keeps <name>-primary and <name>-canary Services for
// Gateway API routing: the first selects the primary's pods, the second
// the canary revision's by its pod-template-hash.
func (cr *canaryRun) ensureServices(ctx context.Context) error {
	gw := cr.gateway()
	if gw == nil {
		return nil
	}
	d := cr.deployment
	port := int32(defaultRoutePort)
	if gw.Port != nil {
		port = *gw.Port
	}
	target := intstr.FromInt32(port)
	if cs := d.Spec.Template.Spec.Containers; len(cs) > 0 && len(cs[0].Ports) > 0 {
		target = intstr.FromInt32(cs[0].Ports[0].ContainerPort)
	}
	canaryHash := noCanary
	if cr.c.Phase == CanaryProgressing || cr.c.Phase == CanaryPromoting {
		canaryHash = cr.c.CanaryTemplateHash
	}
	for name, selector := range map[string]map[string]string{
		primaryName(d):       withLabel(d.Spec.Selector.MatchLabels, trackLabel, "primary"),
		canaryServiceName(d): withLabel(d.Spec.Selector.MatchLabels, podTemplateHashLabel, canaryHash),
	} {
		var svc corev1.Service
		err := cr.r.Get(ctx, types.NamespacedName{Namespace: d.Namespace, Name: name}, &svc)
		if apierrors.IsNotFound(err) {
			svc = corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: d.Namespace, Labels: managedLabels(d),
					OwnerReferences: ownedBy(d)},
				Spec: corev1.ServiceSpec{Selector: selector, Ports: []corev1.ServicePort{{Name: "http", Port: port, TargetPort: target}}},
			}
			if err := cr.r.Create(ctx, &svc); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create service %s: %w", name, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if !equalLabels(svc.Spec.Selector, selector) {
			svc.Spec.Selector = selector
			if err := cr.r.Update(ctx, &svc); err != nil {
				return fmt.Errorf("update service %s: %w", name, err)
			}
		}
	}
	return nil
}

func withLabel(base map[string]string, key, value string) map[string]string {
	out := map[string]string{key: value}
	for k, v := range base {
		if k != key {
			out[k] = v
		}
	}
	return out
}

func equalLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (cr *canaryRun) gateway() *attestav1.GatewayAPIRouting {
	if tr := cr.guard.Spec.Canary.TrafficRouting; tr != nil {
		return tr.GatewayAPI
	}
	return nil
}

// canaryReplicas is how many pods the canary runs at weight w, and the
// share of traffic that gives.
func (cr *canaryRun) canaryReplicas(w int32) (replicas, actual int32) {
	n := float64(cr.desired)
	if cr.gateway() != nil {
		// The route sets the share; the canary needs the capacity for it.
		return max(1, int32(math.Ceil(n*float64(w)/100))), w
	}
	// Without a router, the share is the canary's fraction of the pods.
	replicas = max(1, int32(math.Round(n*float64(w)/float64(100-w))))
	return replicas, int32(math.Round(100 * float64(replicas) / (float64(replicas) + n)))
}

// setWeight routes w percent to the canary: the HTTPRoute's weights, and
// the canary's replicas, which carry the decision (see writeAhead). With
// a router, capacity goes up before traffic and traffic down before
// capacity.
func (cr *canaryRun) setWeight(ctx context.Context, w int32, d *decisionDraft) error {
	replicas, actual := cr.canaryReplicas(w)
	if w == 0 {
		replicas, actual = 0, 0
	}
	up := cr.deployment.Spec.Replicas == nil || replicas >= *cr.deployment.Spec.Replicas
	if !up {
		if err := cr.route(ctx, w); err != nil {
			return err
		}
	}
	if err := cr.scaleCanary(ctx, replicas, d); err != nil {
		return err
	}
	if up {
		if err := cr.route(ctx, w); err != nil {
			return err
		}
	}
	cr.c.Weight = actual
	return nil
}

// scaleCanary sets the guarded Deployment's replicas, carrying d.
func (cr *canaryRun) scaleCanary(ctx context.Context, replicas int32, d *decisionDraft) error {
	r := cr.r
	if err := r.ensureCurrent(ctx, cr.guard); err != nil {
		return err
	}
	updated := cr.deployment.DeepCopy()
	updated.Spec.Replicas = &replicas
	if d != nil {
		if err := r.writeAhead(cr.guard, cr.deployment, updated, cr.status, d); err != nil {
			return err
		}
	}
	if err := r.Update(ctx, updated); err != nil {
		if d != nil {
			d.sealed = nil
		}
		return fmt.Errorf("scale %s to %d: %w", cr.deployment.Name, replicas, err)
	}
	cr.deployment = updated
	return nil
}

// route sets the HTTPRoute's backendRef weights; without Gateway API
// routing it does nothing.
func (cr *canaryRun) route(ctx context.Context, w int32) error {
	gw := cr.gateway()
	if gw == nil {
		return nil
	}
	d := cr.deployment
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(httpRouteGVK)
	if err := cr.r.Get(ctx, types.NamespacedName{Namespace: d.Namespace, Name: gw.HTTPRoute}, route); err != nil {
		return fmt.Errorf("read HTTPRoute %s: %w", gw.HTTPRoute, err)
	}
	rules, _, _ := unstructured.NestedSlice(route.Object, "spec", "rules")
	foundPrimary, foundCanary, changed := false, false, false
	for i := range rules {
		rule, ok := rules[i].(map[string]any)
		if !ok {
			continue
		}
		refs, _ := rule["backendRefs"].([]any)
		for j := range refs {
			ref, ok := refs[j].(map[string]any)
			if !ok {
				continue
			}
			var weight int64
			switch ref["name"] {
			case primaryName(d):
				weight, foundPrimary = int64(100-w), true
			case canaryServiceName(d):
				weight, foundCanary = int64(w), true
			default:
				continue
			}
			if old, ok := ref["weight"].(int64); !ok || old != weight {
				ref["weight"], changed = weight, true
			}
		}
	}
	if !foundPrimary || !foundCanary {
		return fmt.Errorf("HTTPRoute %s must send traffic to backendRefs %s and %s", gw.HTTPRoute, primaryName(d), canaryServiceName(d))
	}
	if !changed {
		return nil
	}
	if err := unstructured.SetNestedSlice(route.Object, rules, "spec", "rules"); err != nil {
		return err
	}
	if err := cr.r.Update(ctx, route); err != nil {
		return fmt.Errorf("set HTTPRoute %s weights: %w", gw.HTTPRoute, err)
	}
	return nil
}

// ensureAtRest keeps the primary serving everything between rollouts.
func (cr *canaryRun) ensureAtRest(ctx context.Context) error {
	if cr.deployment.Spec.Replicas != nil && *cr.deployment.Spec.Replicas == 0 {
		return cr.route(ctx, 0)
	}
	if err := cr.route(ctx, 0); err != nil {
		return err
	}
	return cr.scaleCanary(ctx, 0, nil)
}

// start gives a new revision its first share of traffic, once the SLO gate
// allows it.
func (cr *canaryRun) start(ctx context.Context, hash, revision string) (*decisionDraft, time.Duration, error) {
	c, at := cr.c, cr.now.Format(time.RFC3339)
	trigger := "NewRevision"
	if gate := cr.guard.Spec.SLOGate; gate != nil && cr.r.SLO != nil {
		waited := c.Phase == CanaryWaiting && c.CanaryTemplateHash == hash
		if waited {
			if last, err := time.Parse(time.RFC3339, c.StepStartedAt); err == nil && cr.now.Sub(last) < recheckInterval(gate) {
				return nil, recheckInterval(gate) - cr.now.Sub(last), nil
			}
		}
		verdict, evidence := cr.r.measureSLOs(ctx, gate, cr.spec.ServiceLabel(cr.deployment.Name), at)
		sloGateTotal.WithLabelValues(verdict.DeploymentGate).Inc()
		cr.status.SLOGate = &attestav1.SLOGateStatus{TemplateHash: hash, Revision: revision, Gate: verdict.DeploymentGate, Reason: verdict.Reason, EvaluatedAt: at}
		hold := verdict.DeploymentGate == models.GateBlocked || (verdict.DeploymentGate == models.GateUnknown && gate.OnUnknown == onUnknownPause)
		if hold {
			first := !waited
			c.Phase, c.CanaryTemplateHash, c.CanaryRevision, c.StepStartedAt = CanaryWaiting, hash, revision, at
			c.Message = fmt.Sprintf("revision %s waits for the SLO gate: %s", revision, verdict.Reason)
			cr.status.SLOGate.Paused = true
			status := metav1.ConditionFalse
			if verdict.DeploymentGate == models.GateUnknown {
				status = metav1.ConditionUnknown
			}
			cr.status.SetCondition(ConditionSLOGate, status, "Holding", c.Message)
			if !first {
				return nil, recheckInterval(gate), nil
			}
			// Nothing changes in the cluster: the canary is still at zero.
			return &decisionDraft{
				action: attestav1.ActionPaused, trigger: "SLOGate" + titleGate(verdict.DeploymentGate), reason: reasonSLOGatePaused,
				message: c.Message, revision: revision, templateHash: hash, evidence: evidence,
			}, recheckInterval(gate), nil
		}
		cr.status.SetCondition(ConditionSLOGate, metav1.ConditionTrue, "Allowed", fmt.Sprintf("revision %s: %s", revision, verdict.Reason))
		if waited {
			trigger = "SLOGateAllowed"
			cr.status.SLOGate.Resumed = true
		}
	}

	first := cr.guard.Spec.Canary.Steps[0].Weight
	c.Phase, c.CanaryTemplateHash, c.CanaryRevision, c.Step, c.StepStartedAt = CanaryProgressing, hash, revision, 0, at
	cr.status.Analysis = &AnalysisStatus{CandidateTemplateHash: hash, CandidateRevision: revision, CandidateGeneration: cr.deployment.Generation}
	d := &decisionDraft{action: attestav1.ActionCanaryStarted, trigger: trigger, reason: "CanaryStarted",
		message: fmt.Sprintf("revision %s gets %d%% of traffic", revision, first), revision: revision, templateHash: hash}
	d.evidence = cr.weightEvidence(first, at)
	if err := cr.ensureServices(ctx); err != nil {
		return nil, 0, err
	}
	if err := cr.setWeight(ctx, first, d); err != nil {
		return nil, 0, err
	}
	c.Message = d.message
	return d, canaryPoll, nil
}

// progress holds the current step until its pause has passed, the canary's
// pods are available and its metrics pass; then it moves to the next step,
// or to promotion after the last.
func (cr *canaryRun) progress(ctx context.Context) (*decisionDraft, time.Duration, error) {
	c, d, at := cr.c, cr.deployment, cr.now.Format(time.RFC3339)
	steps := cr.guard.Spec.Canary.Steps

	if ev := EvaluateDeploymentRollout(d); ev.Blocked {
		return cr.abort(ctx, ev.Reason, ev.Message)
	}
	analysisRequeue, failed, passed, err := cr.analyse(ctx)
	if err != nil {
		return nil, 0, err
	}
	if failed {
		a := cr.status.Analysis
		return cr.abort(ctx, "AnalysisFailed", fmt.Sprintf("revision %s at %d%% failed %d consecutive metric checks: %s", c.CanaryRevision, c.Weight, a.ConsecutiveFailures, a.LastMessage))
	}

	pause := stepPause(steps[c.Step])
	started, _ := time.Parse(time.RFC3339, c.StepStartedAt)
	wait := pause - cr.now.Sub(started)
	available := d.Status.ObservedGeneration >= d.Generation && d.Spec.Replicas != nil && d.Status.AvailableReplicas >= *d.Spec.Replicas
	switch {
	case !available:
		c.Message = fmt.Sprintf("step %d (%d%%): waiting for the canary's pods (%d of %d available)", c.Step+1, c.Weight, d.Status.AvailableReplicas, ptrValue(d.Spec.Replicas))
		return nil, canaryPoll, nil
	case wait > 0:
		c.Message = fmt.Sprintf("step %d (%d%%): holding for %s", c.Step+1, c.Weight, wait.Round(time.Second))
		return nil, minPositive(wait, analysisRequeue), nil
	case !passed:
		c.Message = fmt.Sprintf("step %d (%d%%): waiting for a passing metric check", c.Step+1, c.Weight)
		return nil, minPositive(analysisRequeue, canaryPoll), nil
	}

	evidence := append(analysisEvidence(cr.status, c.CanaryTemplateHash), deploymentEvidence(d, at)...)
	if int(c.Step) == len(steps)-1 {
		return cr.beginPromotion(ctx)
	}
	next := steps[c.Step+1].Weight
	draft := &decisionDraft{action: attestav1.ActionStepAdvanced, trigger: "StepPassed", reason: "StepAdvanced",
		message:  fmt.Sprintf("revision %s passed step %d at %d%%; it now gets %d%%", c.CanaryRevision, c.Step+1, c.Weight, next),
		revision: c.CanaryRevision, templateHash: c.CanaryTemplateHash}
	draft.evidence = append(evidence, cr.weightEvidence(next, at)...)
	if err := cr.setWeight(ctx, next, draft); err != nil {
		return nil, 0, err
	}
	c.Step++
	c.StepStartedAt, c.Message = at, draft.message
	return draft, canaryPoll, nil
}

// analyse runs a metric check of the canary when one is due. It reports
// whether the canary has failed enough checks in a row to abort, and
// whether the latest check passed (true when no checks are configured).
func (cr *canaryRun) analyse(ctx context.Context) (requeue time.Duration, failed, passed bool, err error) {
	cfg := analysisConfigFrom(cr.guard, cr.spec.ServiceLabel(cr.deployment.Name))
	cfg.Metrics = cr.r.Metrics
	switch {
	case !cfg.Enabled():
		return 0, false, true, nil
	case cfg.Err != nil:
		return 0, false, false, fmt.Errorf("spec.checks: %w", cfg.Err)
	case cr.r.Prometheus == nil:
		cr.status.SetCondition(ConditionAnalysis, metav1.ConditionUnknown, "PrometheusNotConfigured",
			"spec.checks is set but the controller has no --prometheus-url; steps advance on time alone")
		return 0, false, true, nil
	}
	a := cr.status.Analysis
	if a == nil || a.CandidateTemplateHash != cr.c.CanaryTemplateHash {
		a = &AnalysisStatus{CandidateTemplateHash: cr.c.CanaryTemplateHash, CandidateRevision: cr.c.CanaryRevision}
		cr.status.Analysis = a
	}
	last, perr := time.Parse(time.RFC3339, a.LastCheckTime)
	if perr != nil || cr.now.Sub(last) >= cfg.Interval {
		result := analyze(ctx, cr.r.Prometheus, cfg, a.CandidateTemplateHash)
		analysisTotal.WithLabelValues(result.Result).Inc()
		a.LastCheckTime = cr.now.Format(time.RFC3339)
		a.LastResult, a.LastMessage = result.Result, result.Message
		a.Requests, a.ErrorRatePct, a.LatencyP95Ms = result.Requests, result.ErrorRatePct, result.LatencyP95Ms
		for i := range result.Evidence {
			result.Evidence[i].EvaluatedAt = a.LastCheckTime
		}
		a.LastEvidence = result.Evidence
		switch result.Result {
		case AnalysisPassed:
			a.ConsecutivePasses++
			a.ConsecutiveFailures = 0
		case AnalysisFailed:
			a.ConsecutiveFailures++
			a.ConsecutivePasses = 0
		}
		last = cr.now
	}
	condition := metav1.ConditionUnknown
	switch a.LastResult {
	case AnalysisPassed:
		condition = metav1.ConditionTrue
	case AnalysisFailed:
		condition = metav1.ConditionFalse
	}
	cr.status.SetCondition(ConditionAnalysis, condition, analysisReason(a.LastResult),
		fmt.Sprintf("canary revision %s at %d%%: %s", cr.c.CanaryRevision, cr.c.Weight, a.LastMessage))
	return cfg.Interval - cr.now.Sub(last), int(a.ConsecutiveFailures) >= cfg.ConsecutiveFailures, a.LastResult == AnalysisPassed, nil
}

// beginPromotion copies the canary's template into the primary.
func (cr *canaryRun) beginPromotion(ctx context.Context) (*decisionDraft, time.Duration, error) {
	updated := cr.primary.DeepCopy()
	updated.Spec.Template = primaryTemplate(cr.deployment)
	if err := cr.r.ensureCurrent(ctx, cr.guard); err != nil {
		return nil, 0, err
	}
	if err := cr.r.Update(ctx, updated); err != nil {
		return nil, 0, fmt.Errorf("promote into %s: %w", updated.Name, err)
	}
	cr.c.Phase = CanaryPromoting
	cr.c.Message = fmt.Sprintf("revision %s passed every step; rolling it out to %s", cr.c.CanaryRevision, updated.Name)
	return nil, canaryPoll, nil
}

// promote finishes once the primary runs the promoted revision: all
// traffic to the primary, the canary back to zero.
func (cr *canaryRun) promote(ctx context.Context) (*decisionDraft, time.Duration, error) {
	p := cr.primary
	if p.Status.ObservedGeneration < p.Generation || !deploymentComplete(p) {
		cr.c.Message = fmt.Sprintf("waiting for %s to roll out revision %s (%d of %d updated and available)",
			p.Name, cr.c.CanaryRevision, p.Status.UpdatedReplicas, cr.desired)
		return nil, canaryPoll, nil
	}
	return cr.settle(ctx, attestav1.ActionPromoted, "CanaryPassed", cr.c.CanaryTemplateHash, cr.c.CanaryRevision,
		fmt.Sprintf("revision %s passed every step and is promoted: %s serves it, and the canary is back at zero", cr.c.CanaryRevision, p.Name))
}

// settle routes everything to the primary, scales the canary to zero and
// records the primary's revision as the healthy one.
func (cr *canaryRun) settle(ctx context.Context, action, trigger, hash, revision, message string) (*decisionDraft, time.Duration, error) {
	at := cr.now.Format(time.RFC3339)
	d := &decisionDraft{action: action, trigger: trigger, reason: "Promoted", message: message, revision: revision, templateHash: hash}
	d.evidence = append(analysisEvidence(cr.status, hash), cr.weightEvidence(0, at)...)
	if err := cr.setWeight(ctx, 0, d); err != nil {
		return nil, 0, err
	}
	c := cr.c
	c.Phase, c.PrimaryTemplateHash, c.PrimaryRevision = CanaryIdle, hash, revision
	c.CanaryTemplateHash, c.CanaryRevision, c.Step, c.Weight, c.StepStartedAt = "", "", -1, 0, ""
	c.Message = message
	cr.status.HealthyTemplateHash, cr.status.HealthyRevision, cr.status.HealthyGeneration = hash, revision, cr.deployment.Generation
	cr.status.Analysis = nil
	return d, 0, nil
}

// abort takes the canary out of traffic and back to zero. The primary is
// untouched: it never stopped serving the last promoted revision.
func (cr *canaryRun) abort(ctx context.Context, trigger, why string) (*decisionDraft, time.Duration, error) {
	c, at := cr.c, cr.now.Format(time.RFC3339)
	message := fmt.Sprintf("canary of revision %s aborted at %d%%: %s; %s serves all traffic", c.CanaryRevision, c.Weight, why, primaryName(cr.deployment))
	d := &decisionDraft{action: attestav1.ActionCanaryAborted, trigger: trigger, reason: "CanaryAborted", message: message,
		revision: c.CanaryRevision, templateHash: c.CanaryTemplateHash, restoredTo: &attestav1.DecisionRevision{Revision: c.PrimaryRevision, TemplateHash: c.PrimaryTemplateHash}}
	d.evidence = append(append(analysisEvidence(cr.status, c.CanaryTemplateHash), deploymentEvidence(cr.deployment, at)...), cr.weightEvidence(0, at)...)
	if err := cr.setWeight(ctx, 0, d); err != nil {
		return nil, 0, err
	}
	canaryAbortedTotal.Inc()
	c.Phase, c.AbortedTemplateHash = CanaryAborted, c.CanaryTemplateHash
	c.Step, c.Weight, c.StepStartedAt, c.Message = -1, 0, "", message
	return d, 0, nil
}

func (cr *canaryRun) weightEvidence(w int32, at string) []attestav1.DecisionEvidence {
	replicas, actual := cr.canaryReplicas(w)
	if w == 0 {
		replicas, actual = 0, 0
	}
	if gw := cr.gateway(); gw != nil {
		return []attestav1.DecisionEvidence{{Name: "traffic-weight", Source: "kubernetes",
			Query: fmt.Sprintf("httproutes/%s backendRefs %s, %s", gw.HTTPRoute, primaryName(cr.deployment), canaryServiceName(cr.deployment)),
			Value: fmt.Sprintf("%d%% canary (%d pods), %d%% primary (%d pods)", actual, replicas, 100-actual, cr.desired), EvaluatedAt: at}}
	}
	return []attestav1.DecisionEvidence{{Name: "traffic-weight", Source: "kubernetes",
		Query: fmt.Sprintf("deployments/%s and %s replicas", cr.deployment.Name, primaryName(cr.deployment)),
		Value: fmt.Sprintf("about %d%% canary: %d canary pods, %d primary pods", actual, replicas, cr.desired), EvaluatedAt: at}}
}

// report sets the guard's phase and conditions from the canary's.
func (cr *canaryRun) report() {
	c, s := cr.c, cr.status
	s.Reason, s.Message = "Canary"+c.Phase, c.Message
	switch c.Phase {
	case CanaryIdle:
		s.Phase = PhaseHealthy
	case CanaryAborted:
		s.Phase = PhaseRolledBack
	case CanaryWaiting:
		s.Phase = PhasePaused
	default:
		s.Phase = PhaseProgressing
	}
	if cr.primary != nil {
		s.DesiredReplicas, s.UpdatedReplicas, s.AvailableReplicas = cr.desired, cr.primary.Status.UpdatedReplicas, cr.primary.Status.AvailableReplicas
	}
	status := metav1.ConditionTrue
	if c.Phase == CanaryAborted {
		status = metav1.ConditionFalse
	}
	s.SetCondition("Canary", status, c.Phase, c.Message)
	healthy := metav1.ConditionFalse
	if c.Phase == CanaryIdle {
		healthy = metav1.ConditionTrue
	}
	s.SetCondition(ConditionHealthy, healthy, s.Reason, s.Message)
}

func deploymentComplete(d *appsv1.Deployment) bool {
	want := int32(1)
	if d.Spec.Replicas != nil {
		want = *d.Spec.Replicas
	}
	st := d.Status
	return st.ObservedGeneration >= d.Generation && st.UpdatedReplicas == want && st.Replicas == want && st.AvailableReplicas == want
}

func stepPause(s attestav1.CanaryStep) time.Duration {
	if d, err := time.ParseDuration(s.Pause); err == nil && d >= 0 {
		return d
	}
	return defaultStepPause
}

func ptrValue(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func minPositive(a, b time.Duration) time.Duration {
	switch {
	case a <= 0:
		return b
	case b <= 0 || a < b:
		return a
	}
	return b
}
