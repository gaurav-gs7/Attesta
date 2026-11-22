package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// Phases written to RolloutGuard.status.phase.
const (
	PhaseProgressing    = "Progressing"
	PhaseHealthy        = "Healthy"
	PhaseVerifying      = "Verifying"
	PhasePaused         = "Paused"
	PhaseBlocked        = "Blocked"
	PhaseRolledBack     = "RolledBack"
	PhaseRollbackFailed = "RollbackFailed"
)

// Condition types written to RolloutGuard.status.conditions.
const (
	ConditionDeploymentFound = "DeploymentFound"
	ConditionHealthy         = "Healthy"
	ConditionBlocked         = "Blocked"
	ConditionRolledBack      = "RolledBack"
	ConditionAnalysis        = "Analysis"
)

type RolloutGuardSpec struct {
	ServiceRef      string
	DeploymentRef   string
	Strategy        string
	RollbackEnabled bool
}

// ServiceLabel is the value of the "service" label that the service's
// metrics carry: spec.serviceRef, or the Deployment's name.
func (s RolloutGuardSpec) ServiceLabel(deploymentName string) string {
	if s.ServiceRef != "" {
		return s.ServiceRef
	}
	return deploymentName
}

func (s RolloutGuardSpec) DeploymentName(guardName string) string {
	if s.DeploymentRef != "" {
		return s.DeploymentRef
	}
	if s.ServiceRef != "" {
		return s.ServiceRef
	}
	return strings.TrimSuffix(guardName, "-rollout")
}

type RolloutEvaluation struct {
	Phase             string
	Reason            string
	Message           string
	DesiredReplicas   int32
	UpdatedReplicas   int32
	AvailableReplicas int32
	Healthy           bool
	Blocked           bool
}

// RolloutGuardStatus is the controller's view of RolloutGuard.status. It is
// loaded from the live object on every reconcile so that conditions, the last
// healthy revision and rollback history survive across passes.
type RolloutGuardStatus struct {
	ObservedGeneration   int64
	DeploymentGeneration int64
	Phase                string
	Reason               string
	Message              string
	ServiceRef           string
	DeploymentRef        string
	Strategy             string
	DesiredReplicas      int32
	UpdatedReplicas      int32
	AvailableReplicas    int32
	RollbackEnabled      bool

	// HealthyRevision, HealthyTemplateHash and HealthyGeneration identify the
	// last ReplicaSet the controller saw fully rolled out. Rollback restores
	// this template and nothing else.
	HealthyRevision     string
	HealthyTemplateHash string
	HealthyGeneration   int64

	// RollbackRevision and RollbackTemplateHash record the last rollback the
	// controller performed. RollbackFromTemplateHash is the template it rolled
	// away from: if that template comes back (a GitOps tool with self-heal, or
	// a manual apply), the controller reports it instead of rolling back again.
	// It is cleared once a different revision rolls out healthy.
	RollbackRevision         string
	RollbackTemplateHash     string
	RollbackFromTemplateHash string

	// Analysis tracks metric checks of the revision currently rolling out.
	// It is nil when analysis is off or the Deployment is on the healthy
	// template.
	Analysis *AnalysisStatus

	Conditions []metav1.Condition
}

// AnalysisStatus is the state of metric analysis for one candidate revision.
type AnalysisStatus struct {
	CandidateRevision     string
	CandidateTemplateHash string
	CandidateGeneration   int64
	ConsecutiveFailures   int
	ConsecutivePasses     int
	LastCheckTime         string
	LastResult            string
	LastMessage           string
	Requests              *float64
	ErrorRatePct          *float64
	LatencyP95Ms          *float64
}

func (a *AnalysisStatus) asMap() map[string]any {
	m := map[string]any{
		"candidateRevision":     a.CandidateRevision,
		"candidateTemplateHash": a.CandidateTemplateHash,
		"candidateGeneration":   a.CandidateGeneration,
		"consecutiveFailures":   a.ConsecutiveFailures,
		"consecutivePasses":     a.ConsecutivePasses,
	}
	for key, value := range map[string]string{"lastCheckTime": a.LastCheckTime, "lastResult": a.LastResult, "lastMessage": a.LastMessage} {
		if value != "" {
			m[key] = value
		}
	}
	for key, value := range map[string]*float64{"requests": a.Requests, "errorRatePct": a.ErrorRatePct, "latencyP95Ms": a.LatencyP95Ms} {
		if value != nil {
			m[key] = *value
		}
	}
	return m
}

func analysisFromGuard(guard *unstructured.Unstructured) *AnalysisStatus {
	m, found, _ := unstructured.NestedMap(guard.Object, "status", "analysis")
	if !found {
		return nil
	}
	str := func(key string) string { v, _ := m[key].(string); return v }
	num := func(key string) *float64 {
		switch v := m[key].(type) {
		case float64:
			return &v
		case int64:
			f := float64(v)
			return &f
		}
		return nil
	}
	integer := func(key string) int64 {
		if v := num(key); v != nil {
			return int64(*v)
		}
		return 0
	}
	return &AnalysisStatus{
		CandidateRevision:     str("candidateRevision"),
		CandidateTemplateHash: str("candidateTemplateHash"),
		CandidateGeneration:   integer("candidateGeneration"),
		ConsecutiveFailures:   int(integer("consecutiveFailures")),
		ConsecutivePasses:     int(integer("consecutivePasses")),
		LastCheckTime:         str("lastCheckTime"),
		LastResult:            str("lastResult"),
		LastMessage:           str("lastMessage"),
		Requests:              num("requests"),
		ErrorRatePct:          num("errorRatePct"),
		LatencyP95Ms:          num("latencyP95Ms"),
	}
}

func rolloutGuardSpecFrom(guard *unstructured.Unstructured) RolloutGuardSpec {
	serviceRef, _, _ := unstructured.NestedString(guard.Object, "spec", "serviceRef")
	deploymentRef, _, _ := unstructured.NestedString(guard.Object, "spec", "deploymentRef")
	strategy, _, _ := unstructured.NestedString(guard.Object, "spec", "strategy")
	// Rollback rewrites a Deployment's pod template, so it is opt-in: a guard
	// without spec.rollback.enabled only reports.
	rollbackEnabled, _, _ := unstructured.NestedBool(guard.Object, "spec", "rollback", "enabled")
	return RolloutGuardSpec{
		ServiceRef:      serviceRef,
		DeploymentRef:   deploymentRef,
		Strategy:        strategy,
		RollbackEnabled: rollbackEnabled,
	}
}

// statusFromGuard loads the status the controller wrote on a previous pass.
// Unknown or malformed fields are ignored rather than failing the reconcile.
func statusFromGuard(guard *unstructured.Unstructured) RolloutGuardStatus {
	var s RolloutGuardStatus
	str := func(field string) string {
		v, _, _ := unstructured.NestedString(guard.Object, "status", field)
		return v
	}
	s.Phase = str("phase")
	s.Reason = str("reason")
	s.HealthyRevision = str("healthyRevision")
	s.HealthyTemplateHash = str("healthyTemplateHash")
	s.RollbackRevision = str("rollbackRevision")
	s.RollbackTemplateHash = str("rollbackTemplateHash")
	s.RollbackFromTemplateHash = str("rollbackFromTemplateHash")
	s.HealthyGeneration, _, _ = unstructured.NestedInt64(guard.Object, "status", "healthyGeneration")
	s.Analysis = analysisFromGuard(guard)

	raw, _, _ := unstructured.NestedSlice(guard.Object, "status", "conditions")
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		var cond metav1.Condition
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &cond); err != nil || cond.Type == "" {
			continue
		}
		s.Conditions = append(s.Conditions, cond)
	}
	// "Reconciled" was always True and carried no information; drop it from
	// objects written by older controller versions.
	meta.RemoveStatusCondition(&s.Conditions, "Reconciled")
	return s
}

// EvaluateDeploymentRollout classifies a Deployment's rollout from its status.
//
// Only ProgressDeadlineExceeded at the generation the Deployment controller has
// observed counts as Blocked. A paused Deployment is reported as Paused and is
// never acted on. Transient states that occur in every healthy rollout (new
// ReplicaSet scaled but not yet counted, old pods still terminating) are
// Progressing.
func EvaluateDeploymentRollout(deployment *appsv1.Deployment) RolloutEvaluation {
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	st := deployment.Status
	result := RolloutEvaluation{
		Phase:             PhaseProgressing,
		Reason:            "RolloutProgressing",
		DesiredReplicas:   desired,
		UpdatedReplicas:   st.UpdatedReplicas,
		AvailableReplicas: st.AvailableReplicas,
	}

	// Conditions and counts describe the generation in observedGeneration.
	// Until the Deployment controller catches up they are stale and must not
	// drive a decision.
	if st.ObservedGeneration < deployment.Generation {
		result.Message = fmt.Sprintf("waiting for the deployment controller to observe generation %d (observed %d)", deployment.Generation, st.ObservedGeneration)
		return result
	}
	if deployment.Spec.Paused {
		result.Phase = PhasePaused
		result.Reason = "DeploymentPaused"
		result.Message = fmt.Sprintf("deployment %s is paused; no action taken", deployment.Name)
		return result
	}
	for _, condition := range st.Conditions {
		if condition.Type == appsv1.DeploymentProgressing &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == "ProgressDeadlineExceeded" {
			result.Phase = PhaseBlocked
			result.Reason = condition.Reason
			result.Message = condition.Message
			result.Blocked = true
			return result
		}
	}
	// Same definition as the Deployment controller's DeploymentComplete: every
	// replica is on the new template, old pods are gone and all are available.
	if st.UpdatedReplicas == desired && st.Replicas == desired && st.AvailableReplicas == desired {
		result.Phase = PhaseHealthy
		result.Reason = "RolloutHealthy"
		result.Message = fmt.Sprintf("deployment %s has %d/%d updated replicas available", deployment.Name, st.AvailableReplicas, desired)
		result.Healthy = true
		return result
	}
	result.Message = fmt.Sprintf("deployment %s has %d/%d updated replicas and %d/%d available", deployment.Name, st.UpdatedReplicas, desired, st.AvailableReplicas, desired)
	return result
}

// nextStatus carries persistent fields over from the previous status and
// applies the current evaluation.
func nextStatus(prev RolloutGuardStatus, guard *unstructured.Unstructured, spec RolloutGuardSpec, deployment *appsv1.Deployment, evaluation RolloutEvaluation) RolloutGuardStatus {
	s := prev
	s.ObservedGeneration = guard.GetGeneration()
	s.DeploymentGeneration = deployment.Generation
	s.Phase = evaluation.Phase
	s.Reason = evaluation.Reason
	s.Message = evaluation.Message
	s.ServiceRef = spec.ServiceRef
	s.DeploymentRef = deployment.Name
	s.Strategy = spec.Strategy
	s.DesiredReplicas = evaluation.DesiredReplicas
	s.UpdatedReplicas = evaluation.UpdatedReplicas
	s.AvailableReplicas = evaluation.AvailableReplicas
	s.RollbackEnabled = spec.RollbackEnabled

	s.SetCondition(ConditionDeploymentFound, metav1.ConditionTrue, "DeploymentFound", fmt.Sprintf("deployment %s found", deployment.Name))
	switch {
	case evaluation.Healthy:
		s.SetCondition(ConditionHealthy, metav1.ConditionTrue, evaluation.Reason, evaluation.Message)
		s.SetCondition(ConditionBlocked, metav1.ConditionFalse, evaluation.Reason, "rollout is not blocked")
	case evaluation.Blocked:
		s.SetCondition(ConditionHealthy, metav1.ConditionFalse, evaluation.Reason, evaluation.Message)
		s.SetCondition(ConditionBlocked, metav1.ConditionTrue, evaluation.Reason, evaluation.Message)
	default:
		s.SetCondition(ConditionHealthy, metav1.ConditionFalse, evaluation.Reason, evaluation.Message)
		s.SetCondition(ConditionBlocked, metav1.ConditionFalse, evaluation.Reason, "rollout is not blocked")
	}
	return s
}

func statusForMissingDeployment(prev RolloutGuardStatus, guard *unstructured.Unstructured, spec RolloutGuardSpec, deploymentName string) RolloutGuardStatus {
	s := prev
	s.ObservedGeneration = guard.GetGeneration()
	s.DeploymentGeneration = 0
	s.Phase = PhaseBlocked
	s.Reason = "DeploymentMissing"
	s.Message = fmt.Sprintf("deployment %s was not found", deploymentName)
	s.ServiceRef = spec.ServiceRef
	s.DeploymentRef = deploymentName
	s.Strategy = spec.Strategy
	s.DesiredReplicas, s.UpdatedReplicas, s.AvailableReplicas = 0, 0, 0
	s.RollbackEnabled = spec.RollbackEnabled
	s.SetCondition(ConditionDeploymentFound, metav1.ConditionFalse, "DeploymentMissing", s.Message)
	s.SetCondition(ConditionHealthy, metav1.ConditionFalse, "DeploymentMissing", s.Message)
	s.SetCondition(ConditionBlocked, metav1.ConditionTrue, "DeploymentMissing", s.Message)
	return s
}

// SetCondition sets a condition, keeping lastTransitionTime when the status
// value did not change.
func (s *RolloutGuardStatus) SetCondition(conditionType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&s.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: s.ObservedGeneration,
		Reason:             reason,
		Message:            message,
	})
}

func (s RolloutGuardStatus) AsMap() map[string]any {
	status := map[string]any{
		"observedGeneration":   s.ObservedGeneration,
		"deploymentGeneration": s.DeploymentGeneration,
		"phase":                s.Phase,
		"reason":               s.Reason,
		"message":              s.Message,
		"deploymentRef":        s.DeploymentRef,
		"desiredReplicas":      s.DesiredReplicas,
		"updatedReplicas":      s.UpdatedReplicas,
		"availableReplicas":    s.AvailableReplicas,
		"rollbackEnabled":      s.RollbackEnabled,
		"conditions":           s.Conditions,
	}
	optional := map[string]string{
		"serviceRef":               s.ServiceRef,
		"strategy":                 s.Strategy,
		"healthyRevision":          s.HealthyRevision,
		"healthyTemplateHash":      s.HealthyTemplateHash,
		"rollbackRevision":         s.RollbackRevision,
		"rollbackTemplateHash":     s.RollbackTemplateHash,
		"rollbackFromTemplateHash": s.RollbackFromTemplateHash,
	}
	for key, value := range optional {
		if value != "" {
			status[key] = value
		}
	}
	if s.HealthyGeneration != 0 {
		status["healthyGeneration"] = s.HealthyGeneration
	}
	if s.Analysis != nil {
		status["analysis"] = s.Analysis.asMap()
	}
	return status
}

// statusChanged reports whether writing next would change the stored status.
// Both sides are normalised through JSON so typed and unstructured values
// compare equal.
func statusChanged(guard *unstructured.Unstructured, next RolloutGuardStatus) bool {
	current, _, _ := unstructured.NestedMap(guard.Object, "status")
	return !reflect.DeepEqual(normalizeJSON(current), normalizeJSON(next.AsMap()))
}

func normalizeJSON(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return value
	}
	if m, ok := out.(map[string]any); ok && len(m) == 0 {
		return nil
	}
	return out
}
