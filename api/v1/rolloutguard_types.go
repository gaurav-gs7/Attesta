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
	// Strategy is a label, recorded and reported, nothing more. Attesta
	// does not shift traffic: whatever the strategy, the Deployment does a
	// standard rolling update, and the controller judges the new revision's
	// pods as they take their share of traffic.
	// +optional
	// +kubebuilder:validation:Enum=rolling;canary;blue-green
	Strategy string `json:"strategy,omitempty"`
	// Steps are ignored. Deprecated: use canary.steps, which shift traffic.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	Steps []RolloutStep `json:"steps,omitempty"`
	// Checks are metric checks of each new revision. Analysis runs when at
	// least one threshold is set and the controller has --prometheus-url.
	// +optional
	Checks *RolloutChecks `json:"checks,omitempty"`
	// Canary shifts traffic to a new revision in steps, checking it at
	// each, and promotes or aborts it. Without it, the Deployment does a
	// standard rolling update and the guard rolls it back if it fails.
	// +optional
	Canary *CanarySpec `json:"canary,omitempty"`
	// +optional
	Rollback *RollbackPolicy `json:"rollback,omitempty"`
	// SLOGate checks the service's SLOs when a new revision starts rolling
	// out, with the same evaluator as the API's SLO status, and pauses the
	// rollout when they are breached.
	// +optional
	SLOGate *SLOGate `json:"sloGate,omitempty"`
}

// CanarySpec is a progressive rollout. The controller runs the last
// promoted revision in a Deployment it manages, <deployment>-primary, and
// uses the guarded Deployment itself as the canary: scaled to zero between
// rollouts, scaled up and given a share of traffic when its template
// changes. A revision that passes every step is copied to the primary; one
// that fails is scaled back to zero and gets no traffic. The guarded
// Deployment's replica count is then the controller's to set; tell a GitOps
// tool to ignore it.
type CanarySpec struct {
	// Steps are the canary's share of traffic, in percent, in increasing
	// order. Each is held for its pause and until a metric check passes.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	Steps []CanaryStep `json:"steps"`
	// TrafficRouting is how the share is set. Without gatewayAPI, the share
	// is the canary's fraction of the pods behind the Service that selects
	// both: approximate, and no finer than one pod.
	// +optional
	TrafficRouting *TrafficRouting `json:"trafficRouting,omitempty"`
}

// CanaryStep is one share of traffic.
type CanaryStep struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=99
	Weight int32 `json:"weight"`
	// Pause is how long the step is held at least (default 1m).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	Pause string `json:"pause,omitempty"`
}

// TrafficRouting names what splits traffic.
type TrafficRouting struct {
	// GatewayAPI splits traffic with an HTTPRoute's backendRef weights.
	// +optional
	GatewayAPI *GatewayAPIRouting `json:"gatewayAPI,omitempty"`
}

// GatewayAPIRouting is an HTTPRoute, in the guard's namespace, whose rules
// send traffic to the Services <deployment>-primary and <deployment>-canary.
// The controller creates those Services and sets only the weights of the
// backendRefs that name them; the route is yours.
type GatewayAPIRouting struct {
	// +kubebuilder:validation:MinLength=1
	HTTPRoute string `json:"httpRoute"`
	// Port is the Services' port (default 80); it forwards to the pods'
	// first container port.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port *int32 `json:"port,omitempty"`
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
	// MinErrorBudgetPct blocks a revision while less of the 30-day error
	// budget remains (default 10).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MinErrorBudgetPct *float64 `json:"minErrorBudgetPct,omitempty"`
	// FastBurnRate blocks a revision while the 1h and 5m burn rates are
	// both at or above it (default 14.4: 2% of a 30-day budget an hour).
	// +optional
	// +kubebuilder:validation:ExclusiveMinimum=true
	// +kubebuilder:validation:Minimum=0
	FastBurnRate *float64 `json:"fastBurnRate,omitempty"`
	// SlowBurnRate blocks a revision while the 6h and 30m burn rates are
	// both at or above it (default 6: 5% of the budget in six hours).
	// +optional
	// +kubebuilder:validation:ExclusiveMinimum=true
	// +kubebuilder:validation:Minimum=0
	SlowBurnRate *float64 `json:"slowBurnRate,omitempty"`
	// Resume is what ends a pause: Automatic (default) resumes the rollout
	// once the gate allows it again, checked every RecheckInterval; Manual
	// leaves it to a person (kubectl rollout resume).
	// +optional
	// +kubebuilder:validation:Enum=Automatic;Manual
	Resume string `json:"resume,omitempty"`
	// RecheckInterval is how often a paused revision is measured again
	// (default 1m, at least 10s).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	RecheckInterval string `json:"recheckInterval,omitempty"`
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

	// Canary is the state of a progressive rollout, with spec.canary.
	// +optional
	Canary *CanaryStatus `json:"canary,omitempty"`

	// SLOGate is the gate's verdict on the latest revision it checked.
	// +optional
	SLOGate *SLOGateStatus `json:"sloGate,omitempty"`

	// ArchivedSequence is the last decision the archive outside the cluster
	// has accepted; decisions after it are still to be sent.
	// +optional
	ArchivedSequence int64 `json:"archivedSequence,omitempty"`

	// LastDecision is the head of the guard's chain of RolloutDecisions:
	// the next decision links to it.
	// +optional
	LastDecision *DecisionRef `json:"lastDecision,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CanaryStatus is where a progressive rollout stands.
type CanaryStatus struct {
	// Phase is Initializing, Idle, Waiting (for the SLO gate), Progressing,
	// Promoting or Aborted.
	Phase string `json:"phase"`
	// PrimaryTemplateHash is the guarded Deployment's template hash the
	// primary runs.
	// +optional
	PrimaryTemplateHash string `json:"primaryTemplateHash,omitempty"`
	// +optional
	PrimaryRevision string `json:"primaryRevision,omitempty"`
	// CanaryTemplateHash is the revision being stepped through.
	// +optional
	CanaryTemplateHash string `json:"canaryTemplateHash,omitempty"`
	// +optional
	CanaryRevision string `json:"canaryRevision,omitempty"`
	// Step is the index in spec.canary.steps, -1 before the first.
	// +optional
	Step int32 `json:"step"`
	// Weight is the canary's share of traffic now, in percent; with
	// replica-based routing, the share of pods.
	// +optional
	Weight int32 `json:"weight"`
	// +optional
	StepStartedAt string `json:"stepStartedAt,omitempty"`
	// AbortedTemplateHash is the last revision aborted; it gets no traffic
	// until the template changes again.
	// +optional
	AbortedTemplateHash string `json:"abortedTemplateHash,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
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
	// Paused reports whether the controller has the rollout paused now.
	// +optional
	Paused bool `json:"paused,omitempty"`
	// Resumed reports whether the controller resumed the rollout after the
	// gate allowed it again.
	// +optional
	Resumed bool `json:"resumed,omitempty"`
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
