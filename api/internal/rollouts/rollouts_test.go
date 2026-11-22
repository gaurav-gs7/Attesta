package rollouts

import (
	"errors"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

func TestStateMachine(t *testing.T) {
	cases := []struct {
		from, action, to string
	}{
		{StateStarted, ActionPause, StatePaused},
		{StatePaused, ActionResume, StateStarted},
		{StateStarted, ActionComplete, StateCompleted},
		{StateStarted, ActionRollback, StateRollbackRequested},
		{StatePaused, ActionRollback, StateRollbackRequested},
		{StateCompleted, ActionRollback, StateRollbackRequested},
		{StatePaused, ActionPause, ""},
		{StatePaused, ActionComplete, ""},
		{StateStarted, ActionResume, ""},
		{StateCompleted, ActionPause, ""},
		{StateRollbackRequested, ActionRollback, ""},
		{StateRollbackRequested, ActionResume, ""},
	}
	for _, tc := range cases {
		r := models.Rollout{State: tc.from}
		err := Apply(&r, tc.action, "test", time.Now())
		if tc.to == "" {
			if !errors.Is(err, ErrInvalidTransition) || r.State != tc.from || len(r.Events) != 0 {
				t.Errorf("%s from %s: err %v state %s; want rejected and unchanged", tc.action, tc.from, err, r.State)
			}
			continue
		}
		if err != nil || r.State != tc.to || len(r.Events) != 1 || r.Events[0].From != tc.from || r.Events[0].To != tc.to {
			t.Errorf("%s from %s: err %v state %s events %+v; want %s", tc.action, tc.from, err, r.State, r.Events, tc.to)
		}
	}
}

func TestValidate(t *testing.T) {
	ok := StartRequest{Version: "v2.1.0", Image: "ghcr.io/acme/payments-api:v2.1.0", GitSHA: "abc1234"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	digest := StartRequest{Version: "v2", Image: "registry.local:5000/payments@sha256:" + string(make64())}
	if err := digest.Validate(); err != nil {
		t.Fatalf("digest image rejected: %v", err)
	}
	for _, bad := range []StartRequest{
		{Image: "app:v1"},
		{Version: "v 2", Image: "app:v1"},
		{Version: "../../etc", Image: "app:v1"},
		{Version: "v2"},
		{Version: "v2", Image: "https://evil/app"},
		{Version: "v2", Image: "app:v1; rm -rf /"},
		{Version: "v2", Image: "App:v1"},
		{Version: "v2", Image: "app:v1", GitSHA: "not-hex"},
		{Version: "v2", Image: "app:v1", GitSHA: "ABCDEF1"},
	} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("Validate(%+v) = %v, want ErrInvalidInput", bad, err)
		}
	}
}

func make64() []byte {
	b := make([]byte, 64)
	for i := range b {
		b[i] = "0123456789abcdef"[i%16]
	}
	return b
}
