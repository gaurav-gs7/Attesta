package v1

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RolloutGuard watches one Deployment's rollouts. It reports their state
// and, when rollback is enabled and the Deployment consents, restores the
// last healthy pod template on ProgressDeadlineExceeded or failed metric
// checks.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rguard
// +kubebuilder:printcolumn:name=Service,type=string,JSONPath=`.spec.serviceRef`
// +kubebuilder:printcolumn:name=Phase,type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name=Reason,type=string,JSONPath=`.status.reason`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`
type RolloutGuard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RolloutGuardSpec   `json:"spec,omitempty"`
	Status RolloutGuardStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type RolloutGuardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RolloutGuard `json:"items"`
}

type RolloutGuardSpec struct {
	// ServiceRef is the value of the "service" label the service's metrics
	// carry, and the Deployment's name when DeploymentRef is empty.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	ServiceRef string `json:"serviceRef,omitempty"`
	// DeploymentRef names the Deployment in the guard's namespace. Default:
	// ServiceRef, else the guard's name without a "-rollout" suffix.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	DeploymentRef string `json:"deploymentRef,omitempty"`
	// Strategy is recorded and reported; the controller acts the same for
	// every strategy.
	// +optional
	// +kubebuilder:validation:Enum=rolling;canary;blue-green
	Strategy string `json:"strategy,omitempty"`
	// Steps are recorded for planned traffic shifting. The controller does
	// not use them.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	Steps []RolloutStep `json:"steps,omitempty"`
	// Checks are metric checks of each new revision. Analysis runs when at
	// least one threshold is set and the controller has --prometheus-url.
	// +optional
	Checks *RolloutChecks `json:"checks,omitempty"`
	// +optional
	Rollback *RollbackPolicy `json:"rollback,omitempty"`
	// SLOGate checks the service's SLOs when a new revision starts rolling
	// out, with the same evaluator as the API's SLO status, and pauses the
	// rollout when they are breached.
	// +optional
	SLOGate *SLOGate `json:"sloGate,omitempty"`
}

// SLOGate pauses a new revision while the service is out of its error
// budget or burning it fast. The service is measured service-wide from
// Prometheus (all revisions) over 5m for error rate and p95, the 5m, 30m,
// 1h and 6h burn-rate windows and the 30-day budget. The gate blocks when
// less than 10% of the budget remains, when p95 or the error rate exceeds
// its target, or when the 1h and 5m burn rates are both at least 14.4x or
// the 6h and 30m ones both at least 6x.
type SLOGate struct {
	// AvailabilityTarget is the SLO, as a percentage of requests that
	// succeed over 30 days. It sets the error budget.
	// +kubebuilder:validation:ExclusiveMinimum=true
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:ExclusiveMaximum=true
	// +kubebuilder:validation:Maximum=100
	AvailabilityTarget float64 `json:"availabilityTarget"`
	// LatencyP95TargetMs is the p95 latency objective (default 300).
	// +optional
	// +kubebuilder:validation:Minimum=1
	LatencyP95TargetMs *int32 `json:"latencyP95TargetMs,omitempty"`
	// ErrorRateThreshold is the 5-minute error rate, in percent, above
	// which a rollout is not started (default 1).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	ErrorRateThreshold *float64 `json:"errorRateThreshold,omitempty"`
	// OnUnknown is what happens when the SLOs cannot be measured (too
	// little traffic, no data, Prometheus unreachable): Proceed (default) or
	// Pause.
	// +optional
	// +kubebuilder:validation:Enum=Proceed;Pause
	OnUnknown string `json:"onUnknown,omitempty"`
}

type RolloutStep struct {
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Weight int32 `json:"weight"`
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	Duration string `json:"duration,omitempty"`
}

type RolloutChecks struct {
	// ErrorRateThreshold is the highest acceptable 5xx percentage.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	ErrorRateThreshold *float64 `json:"errorRateThreshold,omitempty"`
	// LatencyP95ThresholdMs is the highest acceptable p95 latency.
	// +optional
	// +kubebuilder:validation:ExclusiveMinimum=true
	// +kubebuilder:validation:Minimum=0
	LatencyP95ThresholdMs *float64 `json:"latencyP95ThresholdMs,omitempty"`
	// MinAvailability is the lowest acceptable success percentage.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MinAvailability *float64 `json:"minAvailability,omitempty"`
	// Window is the range each check queries (default 2m, at least 10s).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	Window string `json:"window,omitempty"`
	// Interval is the time between checks (default 30s, at least 5s).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	Interval string `json:"interval,omitempty"`
	// MinRequests is the traffic a check needs before it judges (default 20).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinRequests *float64 `json:"minRequests,omitempty"`
	// SuccessfulChecks is how many consecutive passes mark the revision
	// healthy (default 3).
	// +optional
	// +kubebuilder:validation:Minimum=1
	SuccessfulChecks *int32 `json:"successfulChecks,omitempty"`
	// RevisionLabel is the metric label holding the pod-template-hash
	// (default pod_template_hash); empty measures the whole service.
	// +optional
	// +kubebuilder:validation:Pattern=`^([a-zA-Z_][a-zA-Z0-9_]*)?$`
	RevisionLabel *string `json:"revisionLabel,omitempty"`
}

type RollbackPolicy struct {
	// Enabled lets the controller roll the Deployment back. The Deployment
	// must also name this guard in its attesta.io/rollback-guards annotation.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// ConsecutiveFailures is how many failed metric checks in a row trigger
	// a rollback (default 2).
	// +optional
	// +kubebuilder:validation:Minimum=1
	ConsecutiveFailures *int32 `json:"consecutiveFailures,omitempty"`
}

// RolloutGuardStatus is written by the controller only.
type RolloutGuardStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	DeploymentGeneration int64 `json:"deploymentGeneration,omitempty"`
	// Phase is Progressing, Healthy, Verifying, Paused, Blocked, RolledBack
	// or RollbackFailed.
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	ServiceRef string `json:"serviceRef,omitempty"`
	// +optional
	DeploymentRef string `json:"deploymentRef,omitempty"`
	// +optional
	Strategy string `json:"strategy,omitempty"`
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`
	// +optional
	RollbackEnabled bool `json:"rollbackEnabled,omitempty"`

	// HealthyRevision, HealthyTemplateHash and HealthyGeneration identify the
	// last ReplicaSet the controller saw fully rolled out. Rollback restores
	// this template and nothing else.
	// +optional
	HealthyRevision string `json:"healthyRevision,omitempty"`
	// +optional
	HealthyTemplateHash string `json:"healthyTemplateHash,omitempty"`
	// +optional
	HealthyGeneration int64 `json:"healthyGeneration,omitempty"`

	// RollbackRevision and RollbackTemplateHash record the last rollback.
	// RollbackFromTemplateHash is the template it rolled away from: if that
	// template comes back (GitOps self-heal, a manual apply), the controller
	// reports it instead of rolling back again. Cleared once a different
	// revision rolls out healthy.
	// +optional
	RollbackRevision string `json:"rollbackRevision,omitempty"`
	// +optional
	RollbackTemplateHash string `json:"rollbackTemplateHash,omitempty"`
	// +optional
	RollbackFromTemplateHash string `json:"rollbackFromTemplateHash,omitempty"`

	// Analysis tracks metric checks of the revision rolling out. Absent when
	// analysis is off or the Deployment is on the healthy template.
	// +optional
	Analysis *AnalysisStatus `json:"analysis,omitempty"`

	// SLOGate is the gate's verdict on the latest revision it checked.
	// +optional
	SLOGate *SLOGateStatus `json:"sloGate,omitempty"`

	// LastDecision is the head of the guard's chain of RolloutDecisions:
	// the next decision links to it.
	// +optional
	LastDecision *DecisionRef `json:"lastDecision,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SLOGateStatus is the SLO gate's verdict on one revision. The gate checks
// each new revision once: a rollout the gate paused and someone resumed is
// not paused again.
type SLOGateStatus struct {
	TemplateHash string `json:"templateHash"`
	// +optional
	Revision string `json:"revision,omitempty"`
	// Gate is allowed, blocked or unknown.
	Gate string `json:"gate"`
	// +optional
	Reason      string `json:"reason,omitempty"`
	EvaluatedAt string `json:"evaluatedAt"`
	// Paused reports whether the controller paused the rollout.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// AnalysisStatus is the state of metric analysis for one candidate revision.
type AnalysisStatus struct {
	// +optional
	CandidateRevision string `json:"candidateRevision,omitempty"`
	// +optional
	CandidateTemplateHash string `json:"candidateTemplateHash,omitempty"`
	// +optional
	CandidateGeneration int64 `json:"candidateGeneration,omitempty"`
	// +optional
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`
	// +optional
	ConsecutivePasses int32 `json:"consecutivePasses,omitempty"`
	// +optional
	LastCheckTime string `json:"lastCheckTime,omitempty"`
	// LastResult is pass, fail or inconclusive.
	// +optional
	LastResult string `json:"lastResult,omitempty"`
	// +optional
	LastMessage string `json:"lastMessage,omitempty"`
	// +optional
	Requests *float64 `json:"requests,omitempty"`
	// +optional
	ErrorRatePct *float64 `json:"errorRatePct,omitempty"`
	// +optional
	LatencyP95Ms *float64 `json:"latencyP95Ms,omitempty"`
	// LastEvidence is the queries and values of the last check, copied into
	// the decision record when the check decides.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	LastEvidence []DecisionEvidence `json:"lastEvidence,omitempty"`
}

// SetCondition sets a condition at the status's observed generation,
// keeping lastTransitionTime when the condition's status is unchanged.
func (s *RolloutGuardStatus) SetCondition(conditionType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&s.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: s.ObservedGeneration,
		Reason:             reason,
		Message:            message,
	})
}
