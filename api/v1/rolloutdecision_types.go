package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// RolloutDecision is the record of one decision a RolloutGuard's controller
// made about a Deployment: what it measured, where, when, and what it did.
// Records are written only by the controller, never change (the API server
// refuses any edit of spec), and form a hash chain per guard: each carries
// the SHA-256 of its own spec and the hash of the record before it, so a
// deleted, inserted or altered record breaks the chain where it happened.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=rdec
// +kubebuilder:printcolumn:name=Guard,type=string,JSONPath=`.spec.guard`
// +kubebuilder:printcolumn:name=Seq,type=integer,JSONPath=`.spec.sequence`
// +kubebuilder:printcolumn:name=Action,type=string,JSONPath=`.spec.action`
// +kubebuilder:printcolumn:name=Trigger,type=string,JSONPath=`.spec.trigger`
// +kubebuilder:printcolumn:name=Revision,type=string,JSONPath=`.spec.deployment.revision`
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`
type RolloutDecision struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="decision records are immutable"
	Spec RolloutDecisionSpec `json:"spec"`
}

// +kubebuilder:object:root=true
type RolloutDecisionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RolloutDecision `json:"items"`
}

// Labels on every RolloutDecision, for listing a guard's chain.
const (
	DecisionGuardLabel    = "attesta.io/guard"
	DecisionGuardUIDLabel = "attesta.io/guard-uid"
)

// Decision actions.
const (
	// ActionPromoted: the revision was recorded as the guard's healthy
	// template, the one a later rollback restores.
	ActionPromoted = "Promoted"
	// ActionRolledBack: the Deployment's pod template was restored to the
	// healthy template.
	ActionRolledBack = "RolledBack"
	// ActionRollbackFailed: a rollback was attempted and the update failed.
	ActionRollbackFailed = "RollbackFailed"
	// ActionRollbackSkipped: a rollback was due and deliberately not done;
	// Reason says why (not authorized, no healthy template, reverted, ...).
	ActionRollbackSkipped = "RollbackSkipped"
	// ActionPaused: the SLO gate paused the Deployment's rollout.
	ActionPaused = "Paused"
	// ActionPauseSkipped: the SLO gate would have paused the rollout and did
	// not, because the Deployment does not name the guard.
	ActionPauseSkipped = "PauseSkipped"
	// ActionResumed: the SLO gate resumed a rollout it had paused, because
	// the service is within its SLOs again.
	ActionResumed = "Resumed"
	// ActionCanaryStarted: a new revision got its first share of traffic.
	ActionCanaryStarted = "CanaryStarted"
	// ActionStepAdvanced: the canary passed a step and got a larger share.
	ActionStepAdvanced = "StepAdvanced"
	// ActionCanaryAborted: the canary failed and was taken out of traffic;
	// the primary serves everything.
	ActionCanaryAborted = "CanaryAborted"
)

// RolloutDecisionSpec is the decision. Every field is covered by Hash.
type RolloutDecisionSpec struct {
	// Guard is the RolloutGuard, in the record's namespace, that decided.
	// +kubebuilder:validation:MinLength=1
	Guard string `json:"guard"`
	// GuardUID is the guard's UID. A guard deleted and created again under
	// the same name starts a new chain rather than continuing the old one.
	// +kubebuilder:validation:MinLength=1
	GuardUID string `json:"guardUID"`
	// Sequence is the decision's position in the guard's chain, from 1.
	// +kubebuilder:validation:Minimum=1
	Sequence int64 `json:"sequence"`
	// PreviousHash is the Hash of decision Sequence-1, empty for the first.
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9a-f]{64})?$`
	PreviousHash string `json:"previousHash,omitempty"`
	// Hash is the hex SHA-256 of this spec without the hash field,
	// serialized as compact JSON with object keys sorted and no HTML
	// escaping, without hash and signature:
	// jq -cjS '.spec | del(.hash, .signature)' reproduces the input.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	Hash string `json:"hash"`
	// Signature is the controller's Ed25519 signature of Hash, when it runs
	// with a signing key. Without the key, nobody else can produce a record
	// that verifies, even with write access to the cluster.
	// +optional
	Signature *DecisionSignature `json:"signature,omitempty"`
	// DecidedAt is when the controller decided, RFC 3339 with nanoseconds.
	// +kubebuilder:validation:MinLength=1
	DecidedAt string `json:"decidedAt"`
	// Actor is the controller instance that decided.
	Actor DecisionActor `json:"actor"`
	// Deployment is the Deployment and revision the decision is about.
	Deployment DecisionTarget `json:"deployment"`
	// Trigger is what the decision responded to: ProgressDeadlineExceeded,
	// AnalysisFailed, AnalysisPassed, RolloutComplete or
	// ErrorBudgetExhausted.
	// +kubebuilder:validation:MinLength=1
	Trigger string `json:"trigger"`
	// +kubebuilder:validation:Enum=Promoted;RolledBack;RollbackFailed;RollbackSkipped;Paused;PauseSkipped;Resumed;CanaryStarted;StepAdvanced;CanaryAborted
	Action string `json:"action"`
	// Reason is the machine-readable outcome, as on the guard's status.
	// +kubebuilder:validation:MinLength=1
	Reason string `json:"reason"`
	// Message explains the decision in a sentence.
	// +optional
	Message string `json:"message,omitempty"`
	// RestoredTo is the revision a rollback restored.
	// +optional
	RestoredTo *DecisionRevision `json:"restoredTo,omitempty"`
	// Evidence is what the controller measured for this decision.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	Evidence []DecisionEvidence `json:"evidence,omitempty"`
}

// DecisionActor identifies the controller instance that decided.
type DecisionActor struct {
	// Controller is the controller's name and version, such as
	// attesta-controller/v0.2.0.
	Controller string `json:"controller"`
	// Instance is the replica that held the leader lease: its pod name.
	// +optional
	Instance string `json:"instance,omitempty"`
}

// DecisionTarget is a Deployment at one generation.
type DecisionTarget struct {
	Name string `json:"name"`
	// UID distinguishes a Deployment from a later one of the same name.
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
	// Revision and TemplateHash identify the ReplicaSet the Deployment was
	// rolling out when the decision was made.
	// +optional
	Revision string `json:"revision,omitempty"`
	// +optional
	TemplateHash string `json:"templateHash,omitempty"`
	// Images are the pod template's container images, in order.
	// +optional
	Images []string `json:"images,omitempty"`
}

// DecisionSignature is an Ed25519 signature over SignedMessage(hash).
type DecisionSignature struct {
	// KeyID is the first 16 hex characters of the SHA-256 of the raw public
	// key.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{16}$`
	KeyID string `json:"keyID"`
	// Value is the signature, base64 (standard encoding).
	// +kubebuilder:validation:MaxLength=128
	Value string `json:"value"`
}

// DecisionRevision is a Deployment revision and its pod template hash.
type DecisionRevision struct {
	Revision     string `json:"revision"`
	TemplateHash string `json:"templateHash"`
}

// DecisionEvidence is one measurement behind a decision.
type DecisionEvidence struct {
	// Name is the check: requests, error-rate, latency-p95, availability,
	// error-budget-remaining, or deployment-condition.
	Name string `json:"name"`
	// Source is prometheus or kubernetes.
	// +kubebuilder:validation:Enum=prometheus;kubernetes
	Source string `json:"source"`
	// Query is the PromQL sent, or for Kubernetes the object and field read.
	Query string `json:"query"`
	// Value is what came back, as text, so the record is the same in every
	// language that verifies it; "absent" when the query returned no data.
	Value string `json:"value"`
	// Threshold is the limit the value was judged against, if any.
	// +optional
	Threshold string `json:"threshold,omitempty"`
	// Passed reports whether the value was within the threshold.
	// +optional
	Passed *bool `json:"passed,omitempty"`
	// EvaluatedAt is when the value was read, RFC 3339.
	EvaluatedAt string `json:"evaluatedAt"`
}

// DecisionRef points a guard at the head of its decision chain.
type DecisionRef struct {
	Name     string `json:"name"`
	Sequence int64  `json:"sequence"`
	Hash     string `json:"hash"`
}
