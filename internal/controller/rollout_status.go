package controller

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
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
	// ConditionRollbackAuthorized reports, while rollback is enabled, whether
	// the target Deployment has consented to this guard rolling it back.
	ConditionRollbackAuthorized = "RollbackAuthorized"
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

// RolloutGuardStatus and AnalysisStatus are the API types: the controller
// decodes and writes RolloutGuard.status with the same structs the CRD's
// schema is generated from, so it cannot write a field the API server
// prunes.
type (
	RolloutGuardStatus = attestav1.RolloutGuardStatus
	AnalysisStatus     = attestav1.AnalysisStatus
)

func rolloutGuardSpecFrom(guard *attestav1.RolloutGuard) RolloutGuardSpec {
	return RolloutGuardSpec{
		ServiceRef:    guard.Spec.ServiceRef,
		DeploymentRef: guard.Spec.DeploymentRef,
		Strategy:      guard.Spec.Strategy,
		// Rollback rewrites a Deployment's pod template, so it is opt-in: a
		// guard without spec.rollback.enabled only reports.
		RollbackEnabled: guard.Spec.Rollback != nil && guard.Spec.Rollback.Enabled,
	}
}

// statusFromGuard is the status the controller wrote on a previous pass.
func statusFromGuard(guard *attestav1.RolloutGuard) RolloutGuardStatus {
	s := *guard.Status.DeepCopy()
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
func nextStatus(prev RolloutGuardStatus, guard *attestav1.RolloutGuard, spec RolloutGuardSpec, deployment *appsv1.Deployment, evaluation RolloutEvaluation) RolloutGuardStatus {
	s := prev
	s.ObservedGeneration = guard.Generation
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

func statusForMissingDeployment(prev RolloutGuardStatus, guard *attestav1.RolloutGuard, spec RolloutGuardSpec, deploymentName string) RolloutGuardStatus {
	s := prev
	s.ObservedGeneration = guard.Generation
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

// statusChanged reports whether writing next would change the stored
// status.
func statusChanged(guard *attestav1.RolloutGuard, next RolloutGuardStatus) bool {
	return !equality.Semantic.DeepEqual(guard.Status, next)
}
