package controller

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

func guardEvent(eventType, reason, message string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("e-%s-%s", reason, message), Namespace: testNamespace},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: "attesta.io/v1", Kind: "RolloutGuard", Namespace: testNamespace, Name: "payments-api-rollout", UID: "guard-uid",
		},
		Source:  corev1.EventSource{Component: "attesta-controller"},
		Type:    eventType,
		Reason:  reason,
		Message: message,
		Count:   1,
	}
}

// Audit C-9: after a burst of other warnings on the same guard, the
// RollbackTriggered event still gets through the spam filter.
func TestRollbackEventSurvivesWarningBurst(t *testing.T) {
	burst := func(spamKey record.EventSpamKeyFunc) bool {
		correlator := record.NewEventCorrelatorWithOptions(record.CorrelatorOptions{SpamKeyFunc: spamKey})
		for i := 0; i < 30; i++ {
			if _, err := correlator.EventCorrelate(guardEvent(corev1.EventTypeWarning, "ProgressDeadlineExceeded", fmt.Sprintf("rollout %d", i))); err != nil {
				t.Fatal(err)
			}
		}
		result, err := correlator.EventCorrelate(guardEvent(corev1.EventTypeWarning, reasonRollbackTriggered, "restored deployment"))
		if err != nil {
			t.Fatal(err)
		}
		return result.Skip
	}
	// Control: client-go's default key drops it, so the test exercises the
	// filter rather than passing trivially.
	if !burst(nil) {
		t.Fatal("default spam key did not drop the event; the test no longer reproduces the problem")
	}
	if burst(eventSpamKey) {
		t.Fatal("RollbackTriggered was dropped after a burst of other warnings")
	}
}
