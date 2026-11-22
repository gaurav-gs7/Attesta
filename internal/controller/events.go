package controller

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

// eventSpamKey is the key client-go's spam filter rate-limits events by. The
// default key is the source, the object and the event type, so every Warning
// on a guard shares one bucket: 25 events, then one per five minutes. A guard
// that reported ProgressDeadlineExceeded or RollbackFailed many times could
// exhaust it and have its RollbackTriggered event dropped. Adding the reason
// gives each reason its own bucket, so routine events can never crowd out a
// rollback.
func eventSpamKey(event *corev1.Event) string {
	return strings.Join([]string{
		event.Source.Component,
		event.Source.Host,
		event.InvolvedObject.APIVersion,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		string(event.InvolvedObject.UID),
		event.Type,
		event.Reason,
	}, "/")
}

func newEventRecorder(kube kubernetes.Interface) record.EventRecorder {
	broadcaster := record.NewBroadcaster(record.WithCorrelatorOptions(record.CorrelatorOptions{SpamKeyFunc: eventSpamKey}))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kube.CoreV1().Events("")})
	return broadcaster.NewRecorder(runtime.NewScheme(), corev1.EventSource{Component: "attesta-controller"})
}
