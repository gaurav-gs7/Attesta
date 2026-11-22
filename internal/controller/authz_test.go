package controller

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
)

// Audit C-13: a guard the Deployment does not name cannot make the controller
// rewrite it, whatever the guard's own settings.
func TestRollbackRequiresDeploymentConsent(t *testing.T) {
	for _, tc := range []struct {
		name       string
		annotation *string
	}{
		{"annotation missing", nil},
		{"other guard named", ptr("billing-rollout")},
		{"empty", ptr("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, map[string]any{"enabled": true}, nil)
			h.setDeployment(func(d *appsv1.Deployment) {
				if tc.annotation == nil {
					delete(d.Annotations, rollbackGuardsAnnotation)
				} else {
					d.Annotations[rollbackGuardsAnnotation] = *tc.annotation
				}
			})
			h.mustReconcile()
			if c := h.condition(ConditionRollbackAuthorized); c == nil || c["status"] != "False" {
				t.Fatalf("RollbackAuthorized = %v, want False before any incident", c)
			}

			h.startBadRollout()
			updatesBefore := h.deploymentUpdates()
			h.mustReconcile()
			if h.deploymentUpdates() != updatesBefore || h.image() != "payments-api:v2" {
				t.Fatalf("controller rolled back a Deployment that did not consent: image=%s", h.image())
			}
			if got := h.field("reason"); got != reasonNotAuthorized {
				t.Fatalf("reason = %s, want %s", got, reasonNotAuthorized)
			}
			if msg := h.field("message"); !strings.Contains(msg, rollbackGuardsAnnotation) {
				t.Fatalf("message does not say how to authorize: %s", msg)
			}
		})
	}
}

func TestRollbackAuthorizedByListedGuard(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Annotations[rollbackGuardsAnnotation] = "billing-rollout, payments-api-rollout"
	})
	h.mustReconcile()
	if c := h.condition(ConditionRollbackAuthorized); c == nil || c["status"] != "True" {
		t.Fatalf("RollbackAuthorized = %v, want True", c)
	}
	h.startBadRollout()
	h.mustReconcile()
	if h.image() != "payments-api:v1" {
		t.Fatalf("listed guard did not roll back: image = %s", h.image())
	}
}

func TestRollbackAuthorizedConditionOnlyWhenRollbackEnabled(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mustReconcile()
	if c := h.condition(ConditionRollbackAuthorized); c != nil {
		t.Fatalf("report-only guard carries RollbackAuthorized: %v", c)
	}
}

func ptr(s string) *string { return &s }
