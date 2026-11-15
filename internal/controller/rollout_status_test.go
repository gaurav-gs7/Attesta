package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func evalDeployment(generation int64, paused bool, status appsv1.DeploymentStatus) *appsv1.Deployment {
	replicas := int32(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-api", Generation: generation},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas, Paused: paused},
		Status:     status,
	}
}

func progressDeadlineExceeded() []appsv1.DeploymentCondition {
	return []appsv1.DeploymentCondition{{
		Type:    appsv1.DeploymentProgressing,
		Status:  corev1.ConditionFalse,
		Reason:  "ProgressDeadlineExceeded",
		Message: "ReplicaSet has timed out progressing",
	}}
}

func TestEvaluateDeploymentRollout(t *testing.T) {
	cases := []struct {
		name        string
		deployment  *appsv1.Deployment
		wantPhase   string
		wantHealthy bool
		wantBlocked bool
	}{
		{
			name:        "complete rollout is healthy",
			deployment:  evalDeployment(3, false, appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, AvailableReplicas: 2}),
			wantPhase:   PhaseHealthy,
			wantHealthy: true,
		},
		{
			name:       "old pods still terminating is progressing",
			deployment: evalDeployment(3, false, appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 3, UpdatedReplicas: 2, AvailableReplicas: 2}),
			wantPhase:  PhaseProgressing,
		},
		{
			name: "first sync of a normal rollout is progressing, not blocked",
			deployment: evalDeployment(3, false, appsv1.DeploymentStatus{
				ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 0, AvailableReplicas: 2, UnavailableReplicas: 1,
			}),
			wantPhase: PhaseProgressing,
		},
		{
			name:        "progress deadline at the observed generation is blocked",
			deployment:  evalDeployment(3, false, appsv1.DeploymentStatus{ObservedGeneration: 3, Conditions: progressDeadlineExceeded()}),
			wantPhase:   PhaseBlocked,
			wantBlocked: true,
		},
		{
			name:       "progress deadline from an older generation is ignored",
			deployment: evalDeployment(4, false, appsv1.DeploymentStatus{ObservedGeneration: 3, Conditions: progressDeadlineExceeded()}),
			wantPhase:  PhaseProgressing,
		},
		{
			name:       "paused deployment is paused, not blocked",
			deployment: evalDeployment(3, true, appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, AvailableReplicas: 2}),
			wantPhase:  PhasePaused,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EvaluateDeploymentRollout(tc.deployment)
			if got.Phase != tc.wantPhase || got.Healthy != tc.wantHealthy || got.Blocked != tc.wantBlocked {
				t.Fatalf("got phase=%s healthy=%v blocked=%v (%s), want phase=%s healthy=%v blocked=%v",
					got.Phase, got.Healthy, got.Blocked, got.Message, tc.wantPhase, tc.wantHealthy, tc.wantBlocked)
			}
		})
	}
}
