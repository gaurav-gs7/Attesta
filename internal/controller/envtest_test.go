package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kptr "k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// TestEnvtest runs the controller against a real API server and etcd
// (envtest), with the CRDs from deploy/crds. Unlike the fake client, the API
// server enforces the generated schema, prunes status fields the schema does
// not know, and assigns generations and resourceVersions itself. There is
// no kube-controller-manager, so the test plays the Deployment controller:
// it creates ReplicaSets and writes Deployment status.
//
// It needs KUBEBUILDER_ASSETS (setup-envtest use -p path); CI sets
// ATTESTA_REQUIRE_ENVTEST so it fails rather than skips without them.
func TestEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("ATTESTA_REQUIRE_ENVTEST") != "" {
			t.Fatal("KUBEBUILDER_ASSETS is not set")
		}
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "deploy", "crds")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 testScheme(),
		Client:                 client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&attestav1.RolloutDecision{}}}},
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctrl.LoggerInto(context.Background(), ctrl.Log))
	r := &RolloutGuardReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Recorder: mgr.GetEventRecorder("attesta-controller")}
	if err := r.SetupWithManager(ctx, mgr, 1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	// A direct client, not the manager's cache, so assertions see the API
	// server's state.
	c, err := client.New(cfg, client.Options{Scheme: testScheme()})
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}

	t.Run("the schema rejects malformed guards", func(t *testing.T) {
		for name, mutate := range map[string]func(*attestav1.RolloutGuard){
			"unknown strategy": func(g *attestav1.RolloutGuard) { g.Spec.Strategy = "yolo" },
			"threshold above 100%": func(g *attestav1.RolloutGuard) {
				g.Spec.Checks = &attestav1.RolloutChecks{ErrorRateThreshold: kptr.To(150.0)}
			},
			"malformed window": func(g *attestav1.RolloutGuard) { g.Spec.Checks = &attestav1.RolloutChecks{Window: "five minutes"} },
			"zero failures": func(g *attestav1.RolloutGuard) {
				g.Spec.Rollback = &attestav1.RollbackPolicy{ConsecutiveFailures: kptr.To[int32](0)}
			},
		} {
			g := &attestav1.RolloutGuard{ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: testNamespace}}
			mutate(g)
			if err := c.Create(ctx, g); !apierrors.IsInvalid(err) {
				t.Errorf("%s: %v, want Invalid", name, err)
			}
		}
	})

	t.Run("a failed rollout is rolled back to the recorded healthy template", func(t *testing.T) {
		d := healthyDeployment(0, "payments-api:v1")
		d.UID, d.Generation, d.Status = "", 0, appsv1.DeploymentStatus{}
		if err := c.Create(ctx, d); err != nil {
			t.Fatal(err)
		}
		rs1 := replicaSet("payments-api-h1", 1, "h1", "payments-api:v1")
		rs1.UID = ""
		rs1.OwnerReferences[0].UID = d.UID
		rs1.OwnerReferences[0].Controller = kptr.To(true)
		if err := c.Create(ctx, rs1); err != nil {
			t.Fatal(err)
		}
		writeDeploymentStatus(ctx, t, c, func(d *appsv1.Deployment) appsv1.DeploymentStatus {
			return appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}
		})

		guard := &attestav1.RolloutGuard{
			ObjectMeta: metav1.ObjectMeta{Name: testGuardName, Namespace: testNamespace},
			Spec: attestav1.RolloutGuardSpec{ServiceRef: "payments-api", Strategy: "rolling",
				Rollback: &attestav1.RollbackPolicy{Enabled: true}},
		}
		if err := c.Create(ctx, guard); err != nil {
			t.Fatal(err)
		}
		g := waitForGuard(ctx, t, c, "the healthy template is recorded", func(g *attestav1.RolloutGuard) bool {
			return g.Status.Phase == PhaseHealthy && g.Status.HealthyTemplateHash == "h1"
		})
		if g.Status.ObservedGeneration != g.Generation || len(g.Status.Conditions) == 0 {
			t.Fatalf("status lost fields to schema pruning: %+v", g.Status)
		}

		// Roll out v2, which never becomes available.
		var current appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKeyFromObject(d), &current); err != nil {
			t.Fatal(err)
		}
		current.Annotations[revisionAnnotation] = "2"
		current.Spec.Template.Spec.Containers[0].Image = "payments-api:v2"
		if err := c.Update(ctx, &current); err != nil {
			t.Fatal(err)
		}
		rs2 := replicaSet("payments-api-h2", 2, "h2", "payments-api:v2")
		rs2.UID = ""
		rs2.OwnerReferences[0].UID = current.UID
		rs2.OwnerReferences[0].Controller = kptr.To(true)
		if err := c.Create(ctx, rs2); err != nil {
			t.Fatal(err)
		}
		writeDeploymentStatus(ctx, t, c, func(d *appsv1.Deployment) appsv1.DeploymentStatus {
			return appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 3, UpdatedReplicas: 1,
				ReadyReplicas: 2, AvailableReplicas: 2, UnavailableReplicas: 1, Conditions: progressDeadlineExceeded()}
		})

		// The phase moves on as soon as the rollback bumps the Deployment's
		// generation; the RolledBack condition keeps the outcome.
		g = waitForGuard(ctx, t, c, "the rollback is recorded", func(g *attestav1.RolloutGuard) bool {
			return meta.IsStatusConditionTrue(g.Status.Conditions, ConditionRolledBack)
		})
		var after appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKeyFromObject(d), &after); err != nil {
			t.Fatal(err)
		}
		if img := after.Spec.Template.Spec.Containers[0].Image; img != "payments-api:v1" {
			t.Fatalf("image %s after the rollback, want payments-api:v1", img)
		}
		rolledBack := meta.FindStatusCondition(g.Status.Conditions, ConditionRolledBack)
		if after.Annotations[rollbackGuardAnnotation] != testGuardName || !strings.Contains(rolledBack.Message, "ProgressDeadlineExceeded") ||
			g.Status.RollbackFromTemplateHash != "h2" {
			t.Fatalf("rollback not attributed: annotations %v, status %+v", after.Annotations, g.Status)
		}
	})

	t.Run("decisions are recorded, verifiable after a round trip, and immutable", func(t *testing.T) {
		g := waitForGuard(ctx, t, c, "the rollback decision is the chain head", func(g *attestav1.RolloutGuard) bool {
			return g.Status.LastDecision != nil && g.Status.LastDecision.Sequence >= 2
		})
		var list attestav1.RolloutDecisionList
		if err := c.List(ctx, &list, client.InNamespace(testNamespace),
			client.MatchingLabels{attestav1.DecisionGuardUIDLabel: string(g.UID)}); err != nil {
			t.Fatal(err)
		}
		// Records read back from the API server, after its defaulting and
		// pruning, must still hash to what the controller sealed.
		if err := attestav1.VerifyChain(g.Name, string(g.UID), list.Items, g.Status.LastDecision); err != nil {
			t.Fatal(err)
		}
		var rolledBack *attestav1.RolloutDecision
		for i := range list.Items {
			if list.Items[i].Spec.Action == attestav1.ActionRolledBack {
				rolledBack = &list.Items[i]
			}
		}
		if rolledBack == nil || rolledBack.Spec.RestoredTo == nil || rolledBack.Spec.RestoredTo.TemplateHash != "h1" {
			t.Fatalf("no RolledBack record restoring h1 among %d records", len(list.Items))
		}

		edited := rolledBack.DeepCopy()
		edited.Spec.Reason = "NothingToSeeHere"
		if err := c.Update(ctx, edited); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "immutable") {
			t.Fatalf("editing a decision: %v, want Invalid (immutable)", err)
		}
	})
}

func writeDeploymentStatus(ctx context.Context, t *testing.T, c client.Client, status func(*appsv1.Deployment) appsv1.DeploymentStatus) {
	t.Helper()
	var d appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "payments-api"}, &d); err != nil {
		t.Fatal(err)
	}
	d.Status = status(&d)
	if err := c.Status().Update(ctx, &d); err != nil {
		t.Fatal(err)
	}
}

func waitForGuard(ctx context.Context, t *testing.T, c client.Client, what string, ok func(*attestav1.RolloutGuard) bool) *attestav1.RolloutGuard {
	t.Helper()
	var g attestav1.RolloutGuard
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := c.Get(ctx, testGuardKey, &g); err == nil && ok(&g) {
			return &g
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s; guard status %+v", what, g.Status)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
