// Package rollouts holds the rollout state machine and input validation.
//
//	started ──pause──▶ paused ──resume──▶ started
//	started ──complete──▶ completed
//	started, paused, completed ──rollback──▶ rollback-requested
//
// completed may still be rolled back: a release can turn out bad after it
// finished. rollback-requested is final. At most one rollout per service is
// active (started or paused) at a time.
package rollouts

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

const (
	StateStarted           = "started"
	StatePaused            = "paused"
	StateCompleted         = "completed"
	StateRollbackRequested = "rollback-requested"
)

// Actions that move a rollout between states.
const (
	ActionPause    = "pause"
	ActionResume   = "resume"
	ActionComplete = "complete"
	ActionRollback = "rollback"
)

var transitions = map[string]map[string]string{
	ActionPause:    {StateStarted: StatePaused},
	ActionResume:   {StatePaused: StateStarted},
	ActionComplete: {StateStarted: StateCompleted},
	ActionRollback: {StateStarted: StateRollbackRequested, StatePaused: StateRollbackRequested, StateCompleted: StateRollbackRequested},
}

var (
	// ErrInvalidTransition: the action is not allowed from the rollout's
	// current state.
	ErrInvalidTransition = errors.New("invalid rollout transition")
	// ErrInvalidInput: the rollout request is malformed.
	ErrInvalidInput = errors.New("invalid rollout request")
)

// Active reports whether a rollout in state blocks another from starting.
func Active(state string) bool { return state == StateStarted || state == StatePaused }

// Apply moves r by action, appending the transition to its events. It fails
// with ErrInvalidTransition and leaves r unchanged if the action is not
// allowed from r's state.
func Apply(r *models.Rollout, action, reason, actor string, at time.Time) error {
	to, ok := transitions[action][r.State]
	if !ok {
		return fmt.Errorf("%w: cannot %s a rollout that is %s", ErrInvalidTransition, action, r.State)
	}
	r.Events = append(r.Events, models.RolloutEvent{At: at, From: r.State, To: to, Reason: reason, Actor: actor})
	r.State = to
	r.UpdatedAt = at
	return nil
}

// DeploymentStatus is the status a rollout's deployment record carries in
// each rollout state.
func DeploymentStatus(state string) string {
	switch state {
	case StateStarted:
		return "rolling-out"
	case StatePaused:
		return "paused"
	case StateCompleted:
		return "succeeded"
	case StateRollbackRequested:
		return "rollback-requested"
	}
	return state
}

// StartRequest is the body of POST /services/{name}/rollouts.
type StartRequest struct {
	Version string `json:"version"`
	Image   string `json:"image"`
	GitSHA  string `json:"gitSha"`
}

var (
	versionRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
	// A container image reference: optional registry and path, a name, and a
	// tag and/or digest. Deliberately strict: no spaces, no schemes.
	imageRE = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)*(:[A-Za-z0-9_][A-Za-z0-9._-]{0,127})?(@sha256:[a-f0-9]{64})?$`)
	shaRE   = regexp.MustCompile(`^[0-9a-f]{7,40}$`)
)

// Validate checks a start request.
func (r StartRequest) Validate() error {
	switch {
	case !versionRE.MatchString(r.Version):
		return fmt.Errorf("%w: version is required: 1-128 characters of letters, digits, '.', '_', '+' or '-'", ErrInvalidInput)
	case len(r.Image) > 255 || !imageRE.MatchString(r.Image):
		return fmt.Errorf("%w: image is required and must be an image reference such as registry.example.com/team/app:v2", ErrInvalidInput)
	case r.GitSHA != "" && !shaRE.MatchString(r.GitSHA):
		return fmt.Errorf("%w: gitSha must be 7-40 lowercase hex characters", ErrInvalidInput)
	}
	return nil
}
