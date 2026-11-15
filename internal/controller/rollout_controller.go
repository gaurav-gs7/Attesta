package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	dynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

var rolloutGuardGVR = schema.GroupVersionResource{
	Group:    "attesta.io",
	Version:  "v1",
	Resource: "rolloutguards",
}

const (
	revisionAnnotation        = "deployment.kubernetes.io/revision"
	podTemplateHashLabel      = "pod-template-hash"
	rollbackFromAnnotation    = "attesta.io/rollback-from-revision"
	rollbackToAnnotation      = "attesta.io/rollback-to-revision"
	rollbackAtAnnotation      = "attesta.io/rollback-at"
	rollbackGuardAnnotation   = "attesta.io/rollback-by-guard"
	reasonNoHealthyRevision   = "NoHealthyRevision"
	reasonHealthyRevisionGone = "HealthyRevisionGone"
	reasonHealthyRevFailing   = "HealthyRevisionFailing"
	reasonRollbackTriggered   = "RollbackTriggered"
	reasonRollbackFailed      = "RollbackFailed"
)

type Options struct {
	Namespace string
	Resync    time.Duration
	Logger    *slog.Logger
	// Recorder overrides the Kubernetes event recorder. Tests use it.
	Recorder record.EventRecorder
}

type RolloutGuardController struct {
	kube        kubernetes.Interface
	dynamic     dynamic.Interface
	informer    cache.SharedIndexInformer
	deployments cache.SharedIndexInformer
	queue       workqueue.TypedRateLimitingInterface[string]
	recorder    record.EventRecorder
	namespace   string
	logger      *slog.Logger
}

func NewRolloutGuardController(cfg *rest.Config, opts Options) (*RolloutGuardController, error) {
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return NewRolloutGuardControllerWithClients(kube, dyn, opts), nil
}

func NewRolloutGuardControllerWithClients(kube kubernetes.Interface, dyn dynamic.Interface, opts Options) *RolloutGuardController {
	resync := opts.Resync
	if resync <= 0 {
		resync = 30 * time.Second
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	dynamicFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, resync, opts.Namespace, nil)
	informer := dynamicFactory.ForResource(rolloutGuardGVR).Informer()
	typedFactory := informers.NewSharedInformerFactoryWithOptions(kube, resync, informers.WithNamespace(opts.Namespace))
	deployments := typedFactory.Apps().V1().Deployments().Informer()

	recorder := opts.Recorder
	if recorder == nil {
		broadcaster := record.NewBroadcaster()
		broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kube.CoreV1().Events("")})
		recorder = broadcaster.NewRecorder(runtime.NewScheme(), corev1.EventSource{Component: "attesta-controller"})
	}

	c := &RolloutGuardController{
		kube:        kube,
		dynamic:     dyn,
		informer:    informer,
		deployments: deployments,
		queue:       workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
		recorder:    recorder,
		namespace:   opts.Namespace,
		logger:      logger,
	}
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueObject,
		UpdateFunc: func(prev, next any) {
			// The controller's own status writes do not change the guard's
			// generation; skip them so a status patch does not trigger another
			// pass. Periodic resyncs (same resourceVersion) still enqueue.
			if guardSpecOrResyncChanged(prev, next) {
				c.enqueueObject(next)
			}
		},
		DeleteFunc: c.enqueueObject,
	})
	_, _ = deployments.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueDeployment,
		UpdateFunc: func(_, next any) { c.enqueueDeployment(next) },
		DeleteFunc: c.enqueueDeployment,
	})
	return c
}

func (c *RolloutGuardController) Run(ctx context.Context, workers int) error {
	if workers <= 0 {
		workers = 1
	}
	defer c.queue.ShutDown()

	go c.informer.Run(ctx.Done())
	go c.deployments.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), c.informer.HasSynced, c.deployments.HasSynced) {
		return errors.New("rollout guard controller cache sync failed")
	}
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.worker, time.Second)
	}
	<-ctx.Done()
	return nil
}

func (c *RolloutGuardController) worker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *RolloutGuardController) processNext(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	if err := c.Reconcile(ctx, key); err != nil {
		c.logger.Error("reconcile rollout guard", "key", key, "error", err)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *RolloutGuardController) enqueueObject(obj any) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err == nil {
		c.queue.Add(key)
	}
}

func guardSpecOrResyncChanged(prev, next any) bool {
	p, ok1 := prev.(*unstructured.Unstructured)
	n, ok2 := next.(*unstructured.Unstructured)
	if !ok1 || !ok2 {
		return true
	}
	return p.GetResourceVersion() == n.GetResourceVersion() || p.GetGeneration() != n.GetGeneration()
}

func (c *RolloutGuardController) enqueueDeployment(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	deployment, ok := obj.(*appsv1.Deployment)
	if !ok {
		return
	}
	guards, err := c.dynamic.Resource(rolloutGuardGVR).Namespace(deployment.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		c.logger.Warn("list rollout guards for deployment event", "namespace", deployment.Namespace, "deployment", deployment.Name, "error", err)
		return
	}
	for i := range guards.Items {
		guard := &guards.Items[i]
		spec := rolloutGuardSpecFrom(guard)
		if spec.DeploymentName(guard.GetName()) == deployment.Name {
			c.queue.Add(deployment.Namespace + "/" + guard.GetName())
		}
	}
}

// Reconcile evaluates one RolloutGuard against its Deployment.
//
// Rollback is deliberately narrow. It happens only when rollback is enabled,
// the Deployment is Blocked at its observed generation, a healthy template has
// been recorded, and the current template is not that healthy template. The
// last rule is what stops the controller from rolling "back" to a bad revision
// after its own rollback created a newer revision number.
func (c *RolloutGuardController) Reconcile(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	guard, err := c.dynamic.Resource(rolloutGuardGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	spec := rolloutGuardSpecFrom(guard)
	prev := statusFromGuard(guard)
	deploymentName := spec.DeploymentName(guard.GetName())

	deployment, err := c.kube.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		status := statusForMissingDeployment(prev, guard, spec, deploymentName)
		return c.finish(ctx, guard, prev, status, nil)
	}
	if err != nil {
		return err
	}

	evaluation := EvaluateDeploymentRollout(deployment)
	status := nextStatus(prev, guard, spec, deployment, evaluation)

	var actionErr error
	switch {
	case evaluation.Healthy:
		if err := c.recordHealthyRevision(ctx, deployment, &status); err != nil {
			return err
		}
	case evaluation.Blocked && spec.RollbackEnabled:
		actionErr = c.rollback(ctx, guard, deployment, &status)
	}
	return c.finish(ctx, guard, prev, status, actionErr)
}

// finish writes the status if it changed, emits an event on phase or reason
// transitions, and returns actionErr so failed actions are retried.
func (c *RolloutGuardController) finish(ctx context.Context, guard *unstructured.Unstructured, prev, status RolloutGuardStatus, actionErr error) error {
	if statusChanged(guard, status) {
		if err := c.patchStatus(ctx, guard, status); err != nil {
			return errors.Join(actionErr, err)
		}
	}
	if status.Phase != prev.Phase || status.Reason != prev.Reason {
		eventType := corev1.EventTypeNormal
		switch status.Phase {
		case PhaseBlocked, PhaseRolledBack, PhaseRollbackFailed:
			eventType = corev1.EventTypeWarning
		}
		c.recorder.Event(guard, eventType, status.Reason, status.Message)
		c.logger.Info("rollout guard transition",
			"namespace", guard.GetNamespace(),
			"name", guard.GetName(),
			"deployment", status.DeploymentRef,
			"phase", status.Phase,
			"reason", status.Reason,
		)
	}
	return actionErr
}

// recordHealthyRevision remembers the template of a fully rolled out
// Deployment. It lists ReplicaSets only when the Deployment's generation has
// moved since the last recorded healthy state.
func (c *RolloutGuardController) recordHealthyRevision(ctx context.Context, deployment *appsv1.Deployment, status *RolloutGuardStatus) error {
	if status.HealthyGeneration == deployment.Generation && status.HealthyTemplateHash != "" {
		return nil
	}
	current, _, err := c.ownedReplicaSets(ctx, deployment)
	if err != nil {
		return err
	}
	if current == nil {
		return nil
	}
	status.HealthyRevision = current.Annotations[revisionAnnotation]
	status.HealthyTemplateHash = current.Labels[podTemplateHashLabel]
	status.HealthyGeneration = deployment.Generation
	if status.RollbackTemplateHash != "" && status.HealthyTemplateHash != status.RollbackTemplateHash {
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, "NewRevisionHealthy",
			fmt.Sprintf("revision %s rolled out healthy after the last rollback", status.HealthyRevision))
	}
	return nil
}

// rollback restores the last recorded healthy template. It returns an error
// only for failures worth retrying; decisions not to act are recorded in the
// status and return nil.
func (c *RolloutGuardController) rollback(ctx context.Context, guard *unstructured.Unstructured, deployment *appsv1.Deployment, status *RolloutGuardStatus) error {
	if status.HealthyTemplateHash == "" {
		status.Reason = reasonNoHealthyRevision
		status.Message = fmt.Sprintf("rollout of %s is blocked and no healthy revision has been recorded; not rolling back", deployment.Name)
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonNoHealthyRevision, status.Message)
		return nil
	}

	current, replicaSets, err := c.ownedReplicaSets(ctx, deployment)
	if err != nil {
		return err
	}
	if current != nil && current.Labels[podTemplateHashLabel] == status.HealthyTemplateHash {
		status.Reason = reasonHealthyRevFailing
		status.Message = fmt.Sprintf("deployment %s is already on the last healthy template (revision %s) and is still blocked; not rolling back again",
			deployment.Name, current.Annotations[revisionAnnotation])
		return nil
	}

	var target *appsv1.ReplicaSet
	for i := range replicaSets {
		if replicaSets[i].Labels[podTemplateHashLabel] == status.HealthyTemplateHash {
			target = &replicaSets[i]
			break
		}
	}
	if target == nil {
		status.Phase = PhaseRollbackFailed
		status.Reason = reasonHealthyRevisionGone
		status.Message = fmt.Sprintf("healthy ReplicaSet with template hash %s no longer exists for deployment %s", status.HealthyTemplateHash, deployment.Name)
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonHealthyRevisionGone, status.Message)
		return nil
	}

	fromRevision := deployment.Annotations[revisionAnnotation]
	if current != nil {
		fromRevision = current.Annotations[revisionAnnotation]
	}
	toRevision := target.Annotations[revisionAnnotation]

	updated := deployment.DeepCopy()
	updated.Spec.Template = *target.Spec.Template.DeepCopy()
	// Restoring the template without its hash label lets the Deployment
	// controller adopt the existing ReplicaSet, as `kubectl rollout undo` does.
	// Audit annotations go on the Deployment, not the pod template, so the
	// template stays identical to the healthy one.
	delete(updated.Spec.Template.Labels, podTemplateHashLabel)
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[rollbackFromAnnotation] = fromRevision
	updated.Annotations[rollbackToAnnotation] = toRevision
	updated.Annotations[rollbackAtAnnotation] = time.Now().UTC().Format(time.RFC3339)
	updated.Annotations[rollbackGuardAnnotation] = guard.GetName()

	if _, err := c.kube.AppsV1().Deployments(deployment.Namespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		status.Phase = PhaseRollbackFailed
		status.Reason = reasonRollbackFailed
		status.Message = fmt.Sprintf("update deployment %s: %v", deployment.Name, err)
		status.SetCondition(ConditionRolledBack, metav1.ConditionFalse, reasonRollbackFailed, status.Message)
		return fmt.Errorf("roll back deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}

	status.Phase = PhaseRolledBack
	status.Reason = reasonRollbackTriggered
	status.Message = fmt.Sprintf("restored deployment %s from revision %s to healthy revision %s", deployment.Name, fromRevision, toRevision)
	status.RollbackRevision = toRevision
	status.RollbackTemplateHash = status.HealthyTemplateHash
	status.SetCondition(ConditionRolledBack, metav1.ConditionTrue, reasonRollbackTriggered, status.Message)
	return nil
}

// ownedReplicaSets returns the Deployment's ReplicaSets that carry a revision,
// and the one with the highest revision, which is the Deployment's current
// template.
func (c *RolloutGuardController) ownedReplicaSets(ctx context.Context, deployment *appsv1.Deployment) (*appsv1.ReplicaSet, []appsv1.ReplicaSet, error) {
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, nil, fmt.Errorf("deployment %s selector: %w", deployment.Name, err)
	}
	list, err := c.kube.AppsV1().ReplicaSets(deployment.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, nil, err
	}
	var owned []appsv1.ReplicaSet
	currentIndex := -1
	var currentRevision int64 = -1
	for _, rs := range list.Items {
		if !isOwnedByDeployment(rs.OwnerReferences, deployment.UID) {
			continue
		}
		revision, ok := parseRevision(rs.Annotations)
		if !ok {
			continue
		}
		owned = append(owned, rs)
		if revision > currentRevision {
			currentRevision = revision
			currentIndex = len(owned) - 1
		}
	}
	if currentIndex < 0 {
		return nil, owned, nil
	}
	return &owned[currentIndex], owned, nil
}

func (c *RolloutGuardController) patchStatus(ctx context.Context, guard *unstructured.Unstructured, status RolloutGuardStatus) error {
	next := status.AsMap()
	// A merge patch keeps keys it does not mention. Null out keys the new
	// status no longer has, such as fields written by older versions.
	current, _, _ := unstructured.NestedMap(guard.Object, "status")
	for key := range current {
		if _, ok := next[key]; !ok {
			next[key] = nil
		}
	}
	payload, err := json.Marshal(map[string]any{"status": next})
	if err != nil {
		return err
	}
	_, err = c.dynamic.Resource(rolloutGuardGVR).Namespace(guard.GetNamespace()).Patch(
		ctx,
		guard.GetName(),
		types.MergePatchType,
		payload,
		metav1.PatchOptions{},
		"status",
	)
	return err
}

func isOwnedByDeployment(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.Kind == "Deployment" && ref.UID == uid {
			return true
		}
	}
	return false
}

func parseRevision(annotations map[string]string) (int64, bool) {
	if annotations == nil {
		return 0, false
	}
	revision, err := strconv.ParseInt(annotations[revisionAnnotation], 10, 64)
	return revision, err == nil
}
