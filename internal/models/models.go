package models

import "time"

type Service struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	Team               string    `json:"team"`
	Owner              string    `json:"owner"`
	Tier               string    `json:"tier"`
	Language           string    `json:"language"`
	RepoURL            string    `json:"repoUrl,omitempty"`
	Repository         string    `json:"repository,omitempty"`
	Pager              string    `json:"pager,omitempty"`
	RunbookURL         string    `json:"runbookUrl,omitempty"`
	DashboardURL       string    `json:"dashboardUrl,omitempty"`
	Dependencies       []string  `json:"dependencies,omitempty"`
	Namespace          string    `json:"namespace"`
	Environment        string    `json:"environment"`
	SLOTarget          float64   `json:"sloTarget"`
	SLOLatencyP95Ms    int       `json:"sloLatencyP95Ms"`
	SLOErrorRate       float64   `json:"sloErrorRate"`
	DeploymentStrategy string    `json:"deploymentStrategy"`
	RollbackOnFailure  bool      `json:"rollbackOnFailure"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// CreateServiceRequest is the body of POST /api/v1/services. The CLI sends
// it too, so the two cannot drift.
type CreateServiceRequest struct {
	Name               string   `json:"name"`
	Team               string   `json:"team"`
	Owner              string   `json:"owner"`
	Tier               string   `json:"tier"`
	Language           string   `json:"language"`
	RepoURL            string   `json:"repoUrl,omitempty"`
	Repository         string   `json:"repository,omitempty"`
	Pager              string   `json:"pager,omitempty"`
	RunbookURL         string   `json:"runbookUrl,omitempty"`
	DashboardURL       string   `json:"dashboardUrl,omitempty"`
	Dependencies       []string `json:"dependencies,omitempty"`
	Namespace          string   `json:"namespace,omitempty"`
	Environment        string   `json:"environment"`
	SLO                string   `json:"slo,omitempty"`
	SLOTarget          float64  `json:"sloTarget,omitempty"`
	SLOAvailability    float64  `json:"sloAvailability,omitempty"`
	SLOLatencyP95      string   `json:"sloLatencyP95,omitempty"`
	SLOLatencyP95Ms    int      `json:"sloLatencyP95Ms,omitempty"`
	SLOErrorRate       float64  `json:"sloErrorRate,omitempty"`
	DeploymentStrategy string   `json:"deploymentStrategy"`
	Deployment         string   `json:"deployment,omitempty"`
	// RollbackOnFailure defaults to true for canary and blue-green and false
	// for rolling when omitted; an explicit false is honoured.
	RollbackOnFailure *bool `json:"rollbackOnFailure,omitempty"`
}

// Deployment is one entry of a service's deployment history: the initial
// scaffold, or one per rollout. Pauses and rollback requests are rollout
// events, not deployments; a rollout's deployment row carries its current
// status.
type Deployment struct {
	ID          string     `json:"id"`
	ServiceID   string     `json:"serviceId"`
	RolloutID   string     `json:"rolloutId,omitempty"`
	Version     string     `json:"version"`
	Image       string     `json:"image,omitempty"`
	Environment string     `json:"environment"`
	Status      string     `json:"status"`
	Strategy    string     `json:"strategy"`
	StartedAt   time.Time  `json:"startedAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// Rollout is a release of one version of a service, recorded by Attesta. It
// moves through a state machine (see package rollouts); Attesta records the
// transitions and leaves acting on them to deployment tooling.
type Rollout struct {
	ID           string         `json:"id"`
	ServiceID    string         `json:"serviceId"`
	Service      string         `json:"service"`
	DeploymentID string         `json:"deploymentId"`
	Version      string         `json:"version"`
	Image        string         `json:"image"`
	GitSHA       string         `json:"gitSha,omitempty"`
	State        string         `json:"state"`
	CreatedAt    time.Time      `json:"createdAt"`
	UpdatedAt    time.Time      `json:"updatedAt"`
	Events       []RolloutEvent `json:"events"`
}

// RolloutEvent is one state transition of a rollout.
type RolloutEvent struct {
	At     time.Time `json:"at"`
	From   string    `json:"from,omitempty"`
	To     string    `json:"to"`
	Reason string    `json:"reason,omitempty"`
	// Actor is who made the transition.
	Actor string `json:"actor,omitempty"`
}

// ServiceStatus is the current state of a service. Every value is either read
// from a record or evaluated from telemetry; nothing is filled in.
type ServiceStatus struct {
	Service           string      `json:"service"`
	Environment       string      `json:"environment"`
	LatestDeployment  *Deployment `json:"latestDeployment,omitempty"`
	SLO               SLOStatus   `json:"slo"`
	RollbackOnFailure bool        `json:"rollbackOnFailure"`
}

type Incident struct {
	ID        string `json:"id"`
	ServiceID string `json:"serviceId"`
	Service   string `json:"service"`
	Severity  string `json:"severity"`
	Title     string `json:"title"`
	Status    string `json:"status"`
	AlertName string `json:"alertName,omitempty"`
	// Source is how the incident was opened: "api" or "alertmanager".
	Source string `json:"source"`
	// Fingerprint is Alertmanager's alert fingerprint, used to match the
	// resolved notification to the incident it opened.
	Fingerprint string `json:"fingerprint,omitempty"`
	// AlertStartsAt is when the alerting rule started firing, if the sender
	// said so. CreatedAt is when Attesta recorded the incident.
	AlertStartsAt *time.Time `json:"alertStartsAt,omitempty"`
	// AlertReported holds values the alert sender reported. They are the
	// sender's claims, kept apart from Attesta's own SLO evaluation.
	AlertReported  *AlertSignals `json:"alertReported,omitempty"`
	CreatedAt      time.Time     `json:"createdAt"`
	ResolvedAt     *time.Time    `json:"resolvedAt,omitempty"`
	ResolutionNote string        `json:"resolutionNote,omitempty"`
	// OpenedBy and ResolvedBy are the callers who opened and resolved it.
	OpenedBy   string `json:"openedBy,omitempty"`
	ResolvedBy string `json:"resolvedBy,omitempty"`
	// Version counts the incident's stored updates. An update must carry
	// the version it read, so two concurrent updates cannot both apply.
	Version int `json:"version"`
}

type AlertSignals struct {
	ErrorRatePct *float64 `json:"errorRatePct,omitempty"`
	LatencyP95Ms *float64 `json:"latencyP95Ms,omitempty"`
}

type ReadinessCheck struct {
	Name           string `json:"name"`
	Status         string `json:"status"`
	Message        string `json:"message"`
	Recommendation string `json:"recommendation,omitempty"`
	Weight         int    `json:"weight"`
}

type ReadinessReport struct {
	Service string `json:"service"`
	// Scope states what the score covers.
	Scope           string           `json:"scope"`
	Tier            string           `json:"tier"`
	Score           int              `json:"score"`
	Passed          []ReadinessCheck `json:"passed"`
	Failed          []ReadinessCheck `json:"failed"`
	Recommendations []string         `json:"recommendations"`
	EvaluatedAt     time.Time        `json:"evaluatedAt"`
}

// Deployment gate values.
const (
	GateAllowed = "allowed"
	GateBlocked = "blocked"
	GateUnknown = "unknown"
)

// SLO data sources.
const (
	SLOSourcePrometheus = "prometheus"
	SLOSourceNone       = "none"
)

// SLOStatus is an SLO evaluation. Measured values are pointers: a nil value
// means it could not be measured, never zero. When any value the gate needs
// is missing, DeploymentGate is "unknown" and rollouts are refused.
type SLOStatus struct {
	Service    string `json:"service"`
	DataSource string `json:"dataSource"`

	AvailabilityTarget    float64 `json:"availabilityTarget"`
	LatencyP95TargetMs    int     `json:"latencyP95TargetMs"`
	ErrorRateThresholdPct float64 `json:"errorRateThresholdPct"`

	// SignalWindow applies to the error rate and p95 latency, BurnRateWindow
	// to the burn rate, and BudgetWindow to availability and the remaining
	// error budget.
	SignalWindow   string `json:"signalWindow,omitempty"`
	BurnRateWindow string `json:"burnRateWindow,omitempty"`
	BudgetWindow   string `json:"budgetWindow,omitempty"`

	RequestRatePerSec   *float64 `json:"requestRatePerSec,omitempty"`
	ErrorRateCurrentPct *float64 `json:"errorRateCurrentPct,omitempty"`
	LatencyP95CurrentMs *float64 `json:"latencyP95CurrentMs,omitempty"`
	BurnRate            *float64 `json:"burnRate,omitempty"`
	// BurnRates holds the burn rate over each window of the multi-window
	// checks, keyed by window ("5m", "30m", "1h", "6h"). BurnRate is the 1h
	// value.
	BurnRates               map[string]*float64 `json:"burnRates,omitempty"`
	AvailabilityCurrent     *float64            `json:"availabilityCurrent,omitempty"`
	ErrorBudgetRemainingPct *float64            `json:"errorBudgetRemainingPct,omitempty"`

	DeploymentGate string    `json:"deploymentGate"`
	Reason         string    `json:"reason"`
	EvaluatedAt    time.Time `json:"evaluatedAt"`
}

type IncidentEvent struct {
	Timestamp   time.Time `json:"timestamp"`
	EventType   string    `json:"eventType"`
	Description string    `json:"description"`
}

type IncidentRecord struct {
	Incident Incident        `json:"incident"`
	Service  Service         `json:"service"`
	Signals  SLOStatus       `json:"signals"`
	Events   []IncidentEvent `json:"events"`
}

type WorkflowRun struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Kind           string          `json:"kind"`
	State          string          `json:"state"`
	Service        string          `json:"service,omitempty"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"maxAttempts"`
	Steps          []WorkflowStep  `json:"steps"`
	Events         []WorkflowEvent `json:"events"`
	StartedAt      time.Time       `json:"startedAt"`
	CompletedAt    *time.Time      `json:"completedAt,omitempty"`
	Error          string          `json:"error,omitempty"`
	// ErrorCode classifies a failed run (see workflows.Code*); callers
	// branch on it, never on Error's wording.
	ErrorCode string `json:"errorCode,omitempty"`
	// Actor is who started the run: an API caller's identity, or "system".
	Actor string `json:"actor,omitempty"`
}

type WorkflowStep struct {
	Name        string     `json:"name"`
	State       string     `json:"state"`
	Attempt     int        `json:"attempt"`
	MaxAttempts int        `json:"maxAttempts"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	Error       string     `json:"error,omitempty"`
}

type WorkflowEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
}
