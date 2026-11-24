package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/prometheus"
	"github.com/gaurav-gs7/attesta/internal/promql"
)

const (
	revisionAnnotation        = "deployment.kubernetes.io/revision"
	podTemplateHashLabel      = "pod-template-hash"
	rollbackFromAnnotation    = "attesta.io/rollback-from-revision"
	rollbackToAnnotation      = "attesta.io/rollback-to-revision"
	rollbackAtAnnotation      = "attesta.io/rollback-at"
	rollbackGuardAnnotation   = "attesta.io/rollback-by-guard"
	rollbackReasonAnnotation  = "attesta.io/rollback-reason"
	reasonNoHealthyRevision   = "NoHealthyRevision"
	reasonHealthyRevisionGone = "HealthyRevisionGone"
	reasonHealthyRevFailing   = "HealthyRevisionFailing"
	reasonRollbackTriggered   = "RollbackTriggered"
	reasonRollbackFailed      = "RollbackFailed"
	reasonRollbackReverted    = "RollbackReverted"
	reasonNotAuthorized       = "RollbackNotAuthorized"

	// rollbackGuardsAnnotation on a Deployment lists, comma-separated, the
	// RolloutGuards allowed to roll it back. Setting it requires update rights
	// on the Deployment, which is what stops a user who can only create
	// guards from using the controller's own Deployment rights.
	rollbackGuardsAnnotation = "attesta.io/rollback-guards"

	argoTrackingAnnotation = "argocd.argoproj.io/tracking-id"
	fluxKustomizeLabel     = "kustomize.toolkit.fluxcd.io/name"
	fluxHelmLabel          = "helm.toolkit.fluxcd.io/name"

	// TargetDeploymentIndex is a cache index of guards by the
	// "namespace/name" of the Deployment they target.
	TargetDeploymentIndex = "attesta.io/target-deployment"
	// OwnerIndex is a cache index of ReplicaSets by the UID of the
	// Deployment that owns them.
	OwnerIndex = "attesta.io/owner-deployment"

	// staleRequeue is how soon a reconcile that lost a race with another
	// writer, or ran on a cache that trails the API server, runs again.
	staleRequeue = time.Second
)

// errCacheBehind means the ReplicaSet cache has not yet seen the revision the
// Deployment reports. The key is requeued; acting on the older view could
// pick the wrong candidate or rollback target.
var errCacheBehind = errors.New("replicaset cache is behind the deployment revision")

// errGuardStale means the cached guard is older than the API server's. The
// key is requeued rather than acting on a status the controller has already
// moved past; after the action, the status write would fail and the record
// of what was done would be lost with it.
var errGuardStale = errors.New("rollout guard cache is behind the API server")

// RolloutGuardReconciler reconciles RolloutGuards against their Deployments.
// Every read goes through the manager's cache; every write carries the
// resourceVersion it was decided on, so a decision made on a stale view
// fails with a conflict and is made again.
type RolloutGuardReconciler struct {
	client.Client
	Recorder events.EventRecorder
	// Prometheus enables metric analysis for guards that set spec.checks.
	// Nil disables it; guards then rely on ProgressDeadlineExceeded only.
	Prometheus promql.Querier
	// Metrics names the request and latency series; empty names take the
	// defaults the API uses too.
	Metrics prometheus.Metrics
	// SLO measures a service's SLOs for guards that set spec.sloGate. Nil
	// disables the gate.
	SLO SLOSource
	// Actor identifies this controller in the decision records it writes.
	Actor attestav1.DecisionActor
	// APIReader reads around the cache. It is used only to adopt a decision
	// record the cache has not seen yet; nil means the client.
	APIReader client.Reader
	// Now overrides the clock. Tests use it.
	Now func() time.Time
}

// SetupWithManager registers the cache indexes and watches.
func (r *RolloutGuardReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager, workers int) error {
	if err := IndexFields(ctx, mgr.GetFieldIndexer()); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("rolloutguard").
		// The controller's own status writes do not change the guard's
		// generation; skip them so a status update does not trigger another
		// pass. Periodic resyncs (same resourceVersion) still reconcile.
		For(&attestav1.RolloutGuard{}, builder.WithPredicates(predicate.Or[client.Object](
			predicate.GenerationChangedPredicate{}, resyncPredicate()))).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.guardsForDeployment)).
		// ReplicaSet status churns with every pod change. Only a new revision
		// or owner matters to the guard.
		Watches(&appsv1.ReplicaSet{}, handler.EnqueueRequestsFromMapFunc(r.guardsForReplicaSet),
			builder.WithPredicates(replicaSetIdentityPredicate())).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers}).
		Complete(r)
}

// IndexFields adds the indexes the reconciler and its event mapping read.
func IndexFields(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &attestav1.RolloutGuard{}, TargetDeploymentIndex, guardTargetIndex); err != nil {
		return fmt.Errorf("index rollout guards: %w", err)
	}
	if err := indexer.IndexField(ctx, &appsv1.ReplicaSet{}, OwnerIndex, replicaSetOwnerIndex); err != nil {
		return fmt.Errorf("index replicasets: %w", err)
	}
	return nil
}

func guardTargetIndex(obj client.Object) []string {
	guard, ok := obj.(*attestav1.RolloutGuard)
	if !ok {
		return nil
	}
	return []string{guard.Namespace + "/" + rolloutGuardSpecFrom(guard).DeploymentName(guard.Name)}
}

func replicaSetOwnerIndex(obj client.Object) []string {
	rs, ok := obj.(*appsv1.ReplicaSet)
	if !ok {
		return nil
	}
	if owner := deploymentOwner(rs.OwnerReferences); owner != nil {
		return []string{string(owner.UID)}
	}
	return nil
}

func deploymentOwner(refs []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Kind == "Deployment" && refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	// Fall back to any Deployment owner; isOwnedByDeployment accepts these too.
	for i := range refs {
		if refs[i].Kind == "Deployment" {
			return &refs[i]
		}
	}
	return nil
}

// resyncPredicate passes the periodic resync, which redelivers an object
// unchanged, so every guard is re-evaluated on the manager's sync period.
func resyncPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetResourceVersion() == e.ObjectNew.GetResourceVersion()
		},
	}
}

func replicaSetIdentityPredicate() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		p, ok1 := e.ObjectOld.(*appsv1.ReplicaSet)
		n, ok2 := e.ObjectNew.(*appsv1.ReplicaSet)
		if !ok1 || !ok2 {
			return true
		}
		if p.ResourceVersion == n.ResourceVersion {
			return false // periodic resync; the Deployment resync covers it
		}
		return p.Annotations[revisionAnnotation] != n.Annotations[revisionAnnotation] ||
			p.Labels[podTemplateHashLabel] != n.Labels[podTemplateHashLabel] ||
			len(p.OwnerReferences) != len(n.OwnerReferences)
	}}
}

// guardsForDeployment maps a Deployment event to the guards targeting it,
// through the cache index; event handling never calls the API server.
func (r *RolloutGuardReconciler) guardsForDeployment(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.guardsFor(ctx, obj.GetNamespace(), obj.GetName())
}

func (r *RolloutGuardReconciler) guardsForReplicaSet(ctx context.Context, obj client.Object) []reconcile.Request {
	rs, ok := obj.(*appsv1.ReplicaSet)
	if !ok {
		return nil
	}
	owner := deploymentOwner(rs.OwnerReferences)
	if owner == nil {
		return nil
	}
	return r.guardsFor(ctx, rs.Namespace, owner.Name)
}

func (r *RolloutGuardReconciler) guardsFor(ctx context.Context, namespace, deployment string) []reconcile.Request {
	var guards attestav1.RolloutGuardList
	if err := r.List(ctx, &guards, client.InNamespace(namespace),
		client.MatchingFields{TargetDeploymentIndex: namespace + "/" + deployment}); err != nil {
		logr.FromContextOrDiscard(ctx).Error(err, "look up rollout guards for deployment", "namespace", namespace, "deployment", deployment)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(guards.Items))
	for _, g := range guards.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: g.Namespace, Name: g.Name}})
	}
	return requests
}

func (r *RolloutGuardReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile evaluates one RolloutGuard against its Deployment.
//
// Rollback is deliberately narrow. It happens only when rollback is enabled
// and the Deployment consents, the Deployment is Blocked at its observed
// generation, a healthy template has been recorded, and the current template
// is not that healthy template. The last rule is what stops the controller
// from rolling "back" to a bad revision after its own rollback created a
// newer revision number.
func (r *RolloutGuardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.reconcile(ctx, req)
	if isStaleRead(err) {
		// The cache or a write lost a race with another writer. Retry on a
		// fresher view; this is routine, not a failure.
		logr.FromContextOrDiscard(ctx).V(1).Info("requeue on a stale read", "error", err.Error())
		return ctrl.Result{RequeueAfter: staleRequeue}, nil
	}
	return result, err
}

// isStaleRead reports errors that mean "retry on a fresher view": an
// optimistic-concurrency conflict on a write, or a ReplicaSet cache that has
// not caught up with the Deployment.
func isStaleRead(err error) bool {
	return err != nil && (apierrors.IsConflict(err) || errors.Is(err, errCacheBehind) || errors.Is(err, errGuardStale))
}

// ensureCurrent reads the guard from the API server, past the cache, before
// the controller changes a Deployment. Acting is rare, so the extra read is
// cheap; deciding on a stale status (an old healthy template, a missing
// rollback) is not.
func (r *RolloutGuardReconciler) ensureCurrent(ctx context.Context, guard *attestav1.RolloutGuard) error {
	var live metav1.PartialObjectMetadata
	live.SetGroupVersionKind(attestav1.GroupVersion.WithKind("RolloutGuard"))
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(guard), &live); err != nil {
		return fmt.Errorf("read rollout guard %s/%s: %w", guard.Namespace, guard.Name, err)
	}
	if live.ResourceVersion != guard.ResourceVersion {
		return fmt.Errorf("guard %s/%s is at resourceVersion %s, cache has %s: %w",
			guard.Namespace, guard.Name, live.ResourceVersion, guard.ResourceVersion, errGuardStale)
	}
	return nil
}

func (r *RolloutGuardReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var guard attestav1.RolloutGuard
	if err := r.Get(ctx, req.NamespacedName, &guard); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	spec := rolloutGuardSpecFrom(&guard)
	prev := statusFromGuard(&guard)
	adopted, err := r.adoptRecordedDecisions(ctx, &guard, &prev)
	if err != nil {
		return ctrl.Result{}, err
	}
	deploymentName := spec.DeploymentName(guard.Name)

	var deployment appsv1.Deployment
	err = r.Get(ctx, types.NamespacedName{Namespace: guard.Namespace, Name: deploymentName}, &deployment)
	if apierrors.IsNotFound(err) {
		status := statusForMissingDeployment(prev, &guard, spec, deploymentName)
		return ctrl.Result{}, r.finish(ctx, &guard, prev, status, nil)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	evaluation := EvaluateDeploymentRollout(&deployment)
	status := nextStatus(prev, &guard, spec, &deployment, evaluation)
	if spec.RollbackEnabled {
		if rollbackAuthorized(&deployment, guard.Name) {
			status.SetCondition(ConditionRollbackAuthorized, metav1.ConditionTrue, "DeploymentConsents",
				fmt.Sprintf("deployment %s lists this guard in %s", deployment.Name, rollbackGuardsAnnotation))
		} else {
			status.SetCondition(ConditionRollbackAuthorized, metav1.ConditionFalse, reasonNotAuthorized, notAuthorizedMessage(&deployment, guard.Name))
		}
	} else {
		meta.RemoveStatusCondition(&status.Conditions, ConditionRollbackAuthorized)
	}

	promote, blocked := evaluation.Healthy, evaluation.Blocked
	requeueAfter, err := r.applyAnalysis(ctx, &guard, spec, &deployment, evaluation, &status, &promote, &blocked)
	if err != nil {
		return ctrl.Result{}, err
	}

	decision, err := r.applySLOGate(ctx, &guard, spec, &deployment, promote || blocked, &status)
	if err != nil {
		return ctrl.Result{}, err
	}
	if deployment.Spec.Paused && deployment.Annotations[pausedByAnnotation] == guard.Name && status.SLOGate != nil && status.SLOGate.Paused {
		status.Reason = reasonSLOGatePaused
		status.Message = pausedMessage(deployment.Name, status.SLOGate.Revision, status.SLOGate.Reason)
	}

	var actionErr error
	switch {
	case decision != nil:
		// The gate paused the rollout, or would have; nothing else acts on
		// this pass.
	case promote:
		if err := r.recordHealthyRevision(ctx, &deployment, &status); err != nil {
			return ctrl.Result{}, err
		}
		decision = r.promotion(&deployment, prev, &status)
	case blocked && spec.RollbackEnabled:
		decision, actionErr = r.rollback(ctx, &guard, &deployment, &status)
	}
	if decision.worthRecording(prev, status) && !decision.matches(adopted, &deployment) {
		// The record goes in before the status that points at it.
		if err := r.recordDecision(ctx, &guard, &deployment, &status, decision); err != nil {
			return ctrl.Result{}, errors.Join(actionErr, err)
		}
	}
	if err := r.finish(ctx, &guard, prev, status, actionErr); err != nil {
		return ctrl.Result{}, err
	}
	// Keep analysing on schedule rather than waiting for the next event or
	// resync.
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// applyAnalysis runs metric analysis for the revision rolling out, if the
// guard configures it. It can hold promotion (the rollout is complete but has
// not passed enough checks) or block the rollout (enough consecutive
// failures). It returns when to analyse next.
func (r *RolloutGuardReconciler) applyAnalysis(ctx context.Context, guard *attestav1.RolloutGuard, spec RolloutGuardSpec,
	deployment *appsv1.Deployment, evaluation RolloutEvaluation, status *RolloutGuardStatus, promote, blocked *bool) (time.Duration, error) {

	cfg := analysisConfigFrom(guard, spec.ServiceLabel(deployment.Name))
	cfg.Metrics = r.Metrics
	switch {
	case !cfg.Enabled():
		status.Analysis = nil
		meta.RemoveStatusCondition(&status.Conditions, ConditionAnalysis)
		return 0, nil
	case cfg.Err != nil:
		status.Analysis = nil
		status.SetCondition(ConditionAnalysis, metav1.ConditionFalse, "InvalidChecks", cfg.Err.Error())
		return 0, nil
	case r.Prometheus == nil:
		status.Analysis = nil
		status.SetCondition(ConditionAnalysis, metav1.ConditionFalse, "PrometheusNotConfigured",
			"spec.checks is set but the controller has no --prometheus-url; only ProgressDeadlineExceeded is acted on")
		return 0, nil
	case deployment.Status.ObservedGeneration < deployment.Generation || deployment.Spec.Paused:
		return 0, nil
	}

	// Identify the candidate: the Deployment's current template. The lookup
	// lists ReplicaSets only when the Deployment's generation has moved.
	a := status.Analysis
	if a == nil || a.CandidateGeneration != deployment.Generation {
		current, _, err := r.ownedReplicaSets(ctx, deployment)
		if err != nil {
			return 0, err
		}
		if current == nil {
			return cfg.Interval, nil
		}
		hash, revision := current.Labels[podTemplateHashLabel], current.Annotations[revisionAnnotation]
		if a == nil || a.CandidateTemplateHash != hash {
			a = &AnalysisStatus{CandidateTemplateHash: hash}
		}
		a.CandidateRevision, a.CandidateGeneration = revision, deployment.Generation
	}
	if a.CandidateTemplateHash == status.HealthyTemplateHash {
		status.Analysis = nil
		status.SetCondition(ConditionAnalysis, metav1.ConditionTrue, "OnHealthyRevision",
			fmt.Sprintf("deployment is on the healthy revision %s; nothing to analyse", status.HealthyRevision))
		return 0, nil
	}
	status.Analysis = a

	now := r.now().UTC()
	last, err := time.Parse(time.RFC3339, a.LastCheckTime)
	if err != nil || now.Sub(last) >= cfg.Interval {
		result := analyze(ctx, r.Prometheus, cfg, a.CandidateTemplateHash)
		analysisTotal.WithLabelValues(result.Result).Inc()
		a.LastCheckTime = now.Format(time.RFC3339)
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
		// Inconclusive results leave both counters as they are.
	}

	conditionStatus := metav1.ConditionUnknown
	switch a.LastResult {
	case AnalysisPassed:
		conditionStatus = metav1.ConditionTrue
	case AnalysisFailed:
		conditionStatus = metav1.ConditionFalse
	}
	status.SetCondition(ConditionAnalysis, conditionStatus, analysisReason(a.LastResult),
		fmt.Sprintf("revision %s: %s (%d consecutive failures of %d allowed, %d consecutive passes of %d needed)",
			a.CandidateRevision, a.LastMessage, a.ConsecutiveFailures, cfg.ConsecutiveFailures, a.ConsecutivePasses, cfg.SuccessfulChecks))

	switch {
	case int(a.ConsecutiveFailures) >= cfg.ConsecutiveFailures:
		*blocked, *promote = true, false
		status.Phase = PhaseBlocked
		status.Reason = "AnalysisFailed"
		status.Message = fmt.Sprintf("revision %s failed %d consecutive metric checks: %s", a.CandidateRevision, a.ConsecutiveFailures, a.LastMessage)
		status.SetCondition(ConditionHealthy, metav1.ConditionFalse, status.Reason, status.Message)
		status.SetCondition(ConditionBlocked, metav1.ConditionTrue, status.Reason, status.Message)
	case evaluation.Healthy && int(a.ConsecutivePasses) < cfg.SuccessfulChecks:
		*promote = false
		status.Phase = PhaseVerifying
		status.Reason = "AnalysisPending"
		status.Message = fmt.Sprintf("revision %s is rolled out; %d of %d metric checks passed so far", a.CandidateRevision, a.ConsecutivePasses, cfg.SuccessfulChecks)
		status.SetCondition(ConditionHealthy, metav1.ConditionFalse, status.Reason, status.Message)
	}
	return cfg.Interval, nil
}

func analysisReason(result string) string {
	switch result {
	case AnalysisPassed:
		return "ChecksPassed"
	case AnalysisFailed:
		return "ChecksFailed"
	case AnalysisInconclusive:
		return "Inconclusive"
	}
	return "Pending"
}

// finish writes the status if it changed, emits an event on phase or reason
// transitions, and returns actionErr so failed actions are retried.
func (r *RolloutGuardReconciler) finish(ctx context.Context, guard *attestav1.RolloutGuard, prev, status RolloutGuardStatus, actionErr error) error {
	if statusChanged(guard, status) {
		updated := guard.DeepCopy()
		updated.Status = status
		// The update carries the resourceVersion of the cached read, so a
		// stale view never overwrites newer status such as a just-recorded
		// rollback.
		if err := r.Status().Update(ctx, updated); err != nil {
			return errors.Join(actionErr, err)
		}
	}
	if status.Phase != prev.Phase || status.Reason != prev.Reason {
		eventType := corev1.EventTypeNormal
		switch status.Phase {
		case PhaseBlocked, PhaseRolledBack, PhaseRollbackFailed:
			eventType = corev1.EventTypeWarning
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(guard, nil, eventType, status.Reason, "Reconcile", "%s", status.Message)
		}
		if status.Phase != prev.Phase {
			phaseTransitions.WithLabelValues(status.Phase).Inc()
		}
		logr.FromContextOrDiscard(ctx).Info("rollout guard transition",
			"deployment", status.DeploymentRef, "phase", status.Phase, "reason", status.Reason)
	}
	return actionErr
}

// recordHealthyRevision remembers the template of a fully rolled out
// Deployment. It lists ReplicaSets only when the Deployment's generation has
// moved since the last recorded healthy state.
func (r *RolloutGuardReconciler) recordHealthyRevision(ctx context.Context, deployment *appsv1.Deployment, status *RolloutGuardStatus) error {
	if status.HealthyGeneration == deployment.Generation && status.HealthyTemplateHash != "" {
		return nil
	}
	current, _, err := r.ownedReplicaSets(ctx, deployment)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	status.HealthyRevision = current.Annotations[revisionAnnotation]
	status.HealthyTemplateHash = current.Labels[podTemplateHashLabel]
	status.HealthyGeneration = deployment.Generation
	if status.RollbackTemplateHash != "" && status.HealthyTemplateHash != status.RollbackTemplateHash {
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, "NewRevisionHealthy",
			fmt.Sprintf("revision %s rolled out healthy after the last rollback", status.HealthyRevision))
		// A new revision is healthy, so the rolled-back template is no longer
		// the one to protect against.
		status.RollbackFromTemplateHash = ""
	}
	return nil
}

// promotion is the decision to record a newly healthy template, or nil if
// the healthy template did not change in this pass.
func (r *RolloutGuardReconciler) promotion(deployment *appsv1.Deployment, prev RolloutGuardStatus, status *RolloutGuardStatus) *decisionDraft {
	if status.HealthyTemplateHash == "" || status.HealthyTemplateHash == prev.HealthyTemplateHash {
		return nil
	}
	at := r.now().UTC().Format(time.RFC3339)
	evidence := analysisEvidence(status, status.HealthyTemplateHash)
	trigger := "RolloutComplete"
	if len(evidence) > 0 {
		trigger = "AnalysisPassed"
	}
	return &decisionDraft{
		action:       attestav1.ActionPromoted,
		trigger:      trigger,
		reason:       status.Reason,
		message:      fmt.Sprintf("revision %s (template %s) is the healthy revision a rollback restores: %s", status.HealthyRevision, status.HealthyTemplateHash, status.Message),
		revision:     status.HealthyRevision,
		templateHash: status.HealthyTemplateHash,
		evidence:     append(evidence, deploymentEvidence(deployment, at)...),
	}
}

// rollback restores the last recorded healthy template. It returns the
// decision it made, if any, and an error only for failures worth retrying;
// decisions not to act are recorded in the status and return nil.
func (r *RolloutGuardReconciler) rollback(ctx context.Context, guard *attestav1.RolloutGuard, deployment *appsv1.Deployment, status *RolloutGuardStatus) (*decisionDraft, error) {
	cause, causeMessage := status.Reason, status.Message
	// The candidate is known when analysis ran; the ReplicaSet lookup below
	// fills it in otherwise.
	d := &decisionDraft{trigger: cause}
	if a := status.Analysis; a != nil {
		d.revision, d.templateHash = a.CandidateRevision, a.CandidateTemplateHash
	}
	at := r.now().UTC().Format(time.RFC3339)
	decide := func(action string) *decisionDraft {
		d.action, d.reason, d.message = action, status.Reason, status.Message
		d.evidence = append(analysisEvidence(status, d.templateHash), deploymentEvidence(deployment, at)...)
		return d
	}
	if !rollbackAuthorized(deployment, guard.Name) {
		// The controller can update any Deployment it watches. Acting for a
		// guard the Deployment never named would let anyone who can create a
		// guard rewrite a Deployment they have no rights on.
		status.Reason = reasonNotAuthorized
		status.Message = notAuthorizedMessage(deployment, guard.Name)
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonNotAuthorized, status.Message)
		rollbackTotal.WithLabelValues("skipped_not_authorized").Inc()
		return decide(attestav1.ActionRollbackSkipped), nil
	}
	if status.HealthyTemplateHash == "" {
		status.Reason = reasonNoHealthyRevision
		status.Message = fmt.Sprintf("rollout of %s is blocked and no healthy revision has been recorded; not rolling back", deployment.Name)
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonNoHealthyRevision, status.Message)
		rollbackTotal.WithLabelValues("skipped_no_healthy_revision").Inc()
		return decide(attestav1.ActionRollbackSkipped), nil
	}

	current, replicaSets, err := r.ownedReplicaSets(ctx, deployment)
	if err != nil {
		return nil, err
	}
	if current != nil {
		d.revision, d.templateHash = current.Annotations[revisionAnnotation], current.Labels[podTemplateHashLabel]
	}
	if current != nil && current.Labels[podTemplateHashLabel] == status.HealthyTemplateHash {
		status.Reason = reasonHealthyRevFailing
		status.Message = fmt.Sprintf("deployment %s is already on the last healthy template (revision %s) and is still blocked; not rolling back again",
			deployment.Name, current.Annotations[revisionAnnotation])
		rollbackTotal.WithLabelValues("skipped_already_healthy").Inc()
		return decide(attestav1.ActionRollbackSkipped), nil
	}

	if current != nil && status.RollbackFromTemplateHash != "" && current.Labels[podTemplateHashLabel] == status.RollbackFromTemplateHash {
		// The template this guard rolled away from is back. Rolling back
		// again would start a loop with whatever re-applied it, typically a
		// GitOps tool with self-heal reconciling the bad version from Git.
		// Only one side can own the pod template; report and stand down.
		status.Reason = reasonRollbackReverted
		status.Message = fmt.Sprintf("deployment %s is back on template %s (revision %s), which this guard rolled back at %s; it was re-applied%s. "+
			"Not rolling back again: fix the source of truth, or deploy a new revision",
			deployment.Name, status.RollbackFromTemplateHash, current.Annotations[revisionAnnotation],
			orUnknown(deployment.Annotations[rollbackAtAnnotation]), gitOpsHint(deployment))
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonRollbackReverted, status.Message)
		rollbackTotal.WithLabelValues("skipped_reverted").Inc()
		return decide(attestav1.ActionRollbackSkipped), nil
	}

	var target *appsv1.ReplicaSet
	for i := range replicaSets {
		if replicaSets[i].Labels[podTemplateHashLabel] == status.HealthyTemplateHash {
			target = &replicaSets[i]
			break
		}
	}
	if target == nil {
		status.Phase = PhaseRollbackFailed
		status.Reason = reasonHealthyRevisionGone
		status.Message = fmt.Sprintf("healthy ReplicaSet with template hash %s no longer exists for deployment %s", status.HealthyTemplateHash, deployment.Name)
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonHealthyRevisionGone, status.Message)
		rollbackTotal.WithLabelValues("skipped_healthy_revision_gone").Inc()
		return decide(attestav1.ActionRollbackFailed), nil
	}

	if err := r.ensureCurrent(ctx, guard); err != nil {
		return nil, err
	}
	fromRevision := deployment.Annotations[revisionAnnotation]
	if current != nil {
		fromRevision = current.Annotations[revisionAnnotation]
	}
	toRevision := target.Annotations[revisionAnnotation]

	updated := deployment.DeepCopy()
	updated.Spec.Template = *target.Spec.Template.DeepCopy()
	// Restoring the template without its hash label lets the Deployment
	// controller adopt the existing ReplicaSet, as `kubectl rollout undo` does.
	// Audit annotations go on the Deployment, not the pod template, so the
	// template stays identical to the healthy one.
	delete(updated.Spec.Template.Labels, podTemplateHashLabel)
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[rollbackFromAnnotation] = fromRevision
	updated.Annotations[rollbackToAnnotation] = toRevision
	updated.Annotations[rollbackAtAnnotation] = r.now().UTC().Format(time.RFC3339)
	updated.Annotations[rollbackGuardAnnotation] = guard.Name
	updated.Annotations[rollbackReasonAnnotation] = cause

	if err := r.Update(ctx, updated); err != nil {
		if apierrors.IsConflict(err) {
			// The Deployment changed after the cached read this decision was
			// made on. Leave the status as evaluated and decide again on the
			// fresh object.
			return nil, fmt.Errorf("roll back deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
		}
		status.Phase = PhaseRollbackFailed
		status.Reason = reasonRollbackFailed
		status.Message = fmt.Sprintf("update deployment %s: %v", deployment.Name, err)
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonRollbackFailed, status.Message)
		rollbackTotal.WithLabelValues("failed").Inc()
		return decide(attestav1.ActionRollbackFailed), fmt.Errorf("roll back deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}

	status.Phase = PhaseRolledBack
	status.Reason = reasonRollbackTriggered
	status.Message = fmt.Sprintf("restored deployment %s from revision %s to healthy revision %s after %s: %s", deployment.Name, fromRevision, toRevision, cause, causeMessage)
	status.RollbackRevision = toRevision
	status.RollbackTemplateHash = status.HealthyTemplateHash
	if current != nil {
		status.RollbackFromTemplateHash = current.Labels[podTemplateHashLabel]
	}
	status.SetCondition(ConditionRolledBack, metav1.ConditionTrue, reasonRollbackTriggered, status.Message)
	rollbackTotal.WithLabelValues("triggered").Inc()
	d.restoredTo = &attestav1.DecisionRevision{Revision: toRevision, TemplateHash: status.HealthyTemplateHash}
	return decide(attestav1.ActionRolledBack), nil
}

// ownedReplicaSets returns the Deployment's ReplicaSets that carry a revision,
// and the one with the highest revision, which is the Deployment's current
// template. It reads the cache, and returns errCacheBehind when the cache has
// not seen the revision the Deployment reports yet.
func (r *RolloutGuardReconciler) ownedReplicaSets(ctx context.Context, deployment *appsv1.Deployment) (*appsv1.ReplicaSet, []appsv1.ReplicaSet, error) {
	var list appsv1.ReplicaSetList
	if err := r.List(ctx, &list, client.InNamespace(deployment.Namespace),
		client.MatchingFields{OwnerIndex: string(deployment.UID)}); err != nil {
		return nil, nil, err
	}
	var owned []appsv1.ReplicaSet
	currentIndex := -1
	var currentRevision int64 = -1
	for i := range list.Items {
		rs := &list.Items[i]
		if !isOwnedByDeployment(rs.OwnerReferences, deployment.UID) {
			continue
		}
		revision, ok := parseRevision(rs.Annotations)
		if !ok {
			continue
		}
		owned = append(owned, *rs)
		if revision > currentRevision {
			currentRevision = revision
			currentIndex = len(owned) - 1
		}
	}
	if want, ok := parseRevision(deployment.Annotations); ok && want > currentRevision {
		return nil, nil, fmt.Errorf("deployment %s/%s is at revision %d, cache has %d: %w",
			deployment.Namespace, deployment.Name, want, currentRevision, errCacheBehind)
	}
	if currentIndex < 0 {
		return nil, owned, nil
	}
	return &owned[currentIndex], owned, nil
}

// rollbackAuthorized reports whether the Deployment names the guard in its
// rollback-guards annotation.
func rollbackAuthorized(deployment *appsv1.Deployment, guardName string) bool {
	for _, name := range strings.Split(deployment.Annotations[rollbackGuardsAnnotation], ",") {
		if strings.TrimSpace(name) == guardName {
			return true
		}
	}
	return false
}

func notAuthorizedMessage(deployment *appsv1.Deployment, guardName string) string {
	return fmt.Sprintf("deployment %s does not list guard %s in its %s annotation; the controller will not roll it back. "+
		"Someone with update rights on the Deployment must add it", deployment.Name, guardName, rollbackGuardsAnnotation)
}

// gitOpsHint names the GitOps tool that manages the Deployment, if its
// markers are present, for messages about reverted rollbacks.
func gitOpsHint(deployment *appsv1.Deployment) string {
	if id := deployment.Annotations[argoTrackingAnnotation]; id != "" {
		app, _, _ := strings.Cut(id, ":")
		return fmt.Sprintf(" by Argo CD application %q, likely with selfHeal on; turn selfHeal off for it or revert the commit in Git", app)
	}
	if name := deployment.Labels[fluxKustomizeLabel]; name != "" {
		return fmt.Sprintf(" by Flux Kustomization %q; revert the commit in Git or suspend it", name)
	}
	if name := deployment.Labels[fluxHelmLabel]; name != "" {
		return fmt.Sprintf(" by Flux HelmRelease %q; revert the change in Git or suspend it", name)
	}
	return ""
}

func orUnknown(s string) string {
	if s == "" {
		return "an unknown time"
	}
	return s
}

func isOwnedByDeployment(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.Kind == "Deployment" && ref.UID == uid {
			return true
		}
	}
	return false
}

func parseRevision(annotations map[string]string) (int64, bool) {
	if annotations == nil {
		return 0, false
	}
	revision, err := strconv.ParseInt(annotations[revisionAnnotation], 10, 64)
	return revision, err == nil
}
