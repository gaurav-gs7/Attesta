package controller

import (
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// revertToV2 stands in for Argo CD self-heal: it re-applies the bad v2
// template after the controller rolled it back. The Deployment controller
// reuses ReplicaSet h2 under a new revision, and the rollout hits its
// progress deadline again.
func (h *harness) revertToV2(revision int, argoApp string) {
	h.t.Helper()
	rs2, err := h.kube.AppsV1().ReplicaSets(testNamespace).Get(h.ctx, "payments-api-h2", metav1.GetOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	rs2.Annotations[revisionAnnotation] = itoa(revision)
	if _, err := h.kube.AppsV1().ReplicaSets(testNamespace).Update(h.ctx, rs2, metav1.UpdateOptions{}); err != nil {
		h.t.Fatal(err)
	}
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = int64(revision)
		d.Annotations[revisionAnnotation] = itoa(revision)
		if argoApp != "" {
			d.Annotations[argoTrackingAnnotation] = argoApp + ":apps/Deployment:platform/payments-api"
		}
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: int64(revision), Replicas: 3, UpdatedReplicas: 1, AvailableReplicas: 2, UnavailableReplicas: 1, Conditions: progressDeadlineExceeded()}
	})
}

func itoa(i int) string { return strconv.Itoa(i) }

// Audit C-12: when something re-applies the template the guard rolled away
// from, the controller reports it once and does not start a rollback loop.
func TestReconcileDoesNotFightARevertedRollback(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.mustReconcile()
	if h.image() != "payments-api:v1" {
		t.Fatalf("first rollback did not happen: image = %s", h.image())
	}
	if got := h.field("rollbackFromTemplateHash"); got != "h2" {
		t.Fatalf("rollbackFromTemplateHash = %q, want h2", got)
	}
	h.drainEvents()

	h.revertToV2(3, "payments-api-prod")
	updatesBefore := h.deploymentUpdates()
	h.mustReconcile()
	h.mustReconcile()

	if h.deploymentUpdates() != updatesBefore {
		t.Fatalf("controller rolled back a reverted rollback: %d more deployment updates", h.deploymentUpdates()-updatesBefore)
	}
	if got := h.image(); got != "payments-api:v2" {
		t.Fatalf("image = %s, want the re-applied payments-api:v2 left alone", got)
	}
	if got := h.field("phase"); got != PhaseBlocked {
		t.Fatalf("phase = %s, want %s", got, PhaseBlocked)
	}
	if got := h.field("reason"); got != reasonRollbackReverted {
		t.Fatalf("reason = %s, want %s", got, reasonRollbackReverted)
	}
	c := h.condition(ConditionRolledBack)
	if c == nil || c["status"] != "False" || c["reason"] != reasonRollbackReverted {
		t.Fatalf("RolledBack condition = %v", c)
	}
	if msg, _ := c["message"].(string); !strings.Contains(msg, `Argo CD application "payments-api-prod"`) {
		t.Fatalf("message does not name the GitOps owner: %s", msg)
	}
	events := h.drainEvents()
	if len(events) != 1 || !strings.Contains(events[0], "Warning "+reasonRollbackReverted) {
		t.Fatalf("want one RollbackReverted warning, got %v", events)
	}
}

// The protection is for one bad template, not forever: once a different
// revision rolls out healthy, a later bad rollout is rolled back again.
func TestRevertProtectionClearsAfterNewHealthyRevision(t *testing.T) {
	h := newHarness(t, map[string]any{"enabled": true}, nil)
	h.mustReconcile()
	h.startBadRollout()
	h.mustReconcile()

	h.addReplicaSet(replicaSet("payments-api-h3", 3, "h3", "payments-api:v3"))
	h.setDeployment(func(d *appsv1.Deployment) {
		d.Generation = 3
		d.Annotations[revisionAnnotation] = "3"
		d.Spec.Template.Spec.Containers[0].Image = "payments-api:v3"
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}
	})
	h.mustReconcile()
	if got := h.field("healthyTemplateHash"); got != "h3" {
		t.Fatalf("healthyTemplateHash = %q, want h3", got)
	}
	if got := h.field("rollbackFromTemplateHash"); got != "" {
		t.Fatalf("rollbackFromTemplateHash = %q after a new healthy revision, want cleared", got)
	}

	h.revertToV2(4, "")
	h.mustReconcile()
	if got := h.image(); got != "payments-api:v3" {
		t.Fatalf("image = %s, want a fresh rollback to the new healthy payments-api:v3", got)
	}
}
