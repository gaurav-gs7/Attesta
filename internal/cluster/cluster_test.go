package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

func TestServiceName(t *testing.T) {
	for want, g := range map[string]attestav1.RolloutGuard{
		"payments":     {Spec: attestav1.RolloutGuardSpec{ServiceRef: "payments", DeploymentRef: "payments-v2"}},
		"payments-v2":  {Spec: attestav1.RolloutGuardSpec{DeploymentRef: "payments-v2"}},
		"payments-api": {ObjectMeta: metav1.ObjectMeta{Name: "payments-api-rollout"}},
	} {
		if got := ServiceName(g); got != want {
			t.Errorf("ServiceName = %q, want %q", got, want)
		}
	}
}

// The reader against a real API server: what it returns, after the
// server's defaulting and pruning, still verifies.
func TestReaderAgainstAnAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("ATTESTA_REQUIRE_ENVTEST") != "" {
			t.Fatal("KUBEBUILDER_ASSETS is not set")
		}
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "deploy", "crds")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	ctx := context.Background()
	c, err := client.New(cfg, client.Options{Scheme: Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "payments"}}); err != nil {
		t.Fatal(err)
	}
	guard := &attestav1.RolloutGuard{ObjectMeta: metav1.ObjectMeta{Name: "payments-api-rollout", Namespace: "payments"},
		Spec: attestav1.RolloutGuardSpec{ServiceRef: "payments-api"}}
	if err := c.Create(ctx, guard); err != nil {
		t.Fatal(err)
	}
	var head *attestav1.DecisionRef
	for i := 0; i < 3; i++ {
		spec := attestav1.RolloutDecisionSpec{
			Guard: guard.Name, GuardUID: string(guard.UID), DecidedAt: "2026-11-24T10:00:00Z",
			Actor:      attestav1.DecisionActor{Controller: "attesta-controller/test"},
			Deployment: attestav1.DecisionTarget{Name: "payments-api", UID: "d", Generation: int64(i + 1)},
			Trigger:    "RolloutComplete", Action: attestav1.ActionPromoted, Reason: "RolloutHealthy",
		}
		if err := spec.Seal(head); err != nil {
			t.Fatal(err)
		}
		d := &attestav1.RolloutDecision{ObjectMeta: metav1.ObjectMeta{
			Name: attestav1.DecisionName(guard.Name, string(guard.UID), spec.Sequence), Namespace: guard.Namespace,
			Labels: map[string]string{attestav1.DecisionGuardUIDLabel: string(guard.UID)}}, Spec: spec}
		if err := c.Create(ctx, d); err != nil {
			t.Fatal(err)
		}
		head = &attestav1.DecisionRef{Name: d.Name, Sequence: spec.Sequence, Hash: spec.Hash}
	}
	guard.Status.LastDecision = head
	if err := c.Status().Update(ctx, guard); err != nil {
		t.Fatal(err)
	}

	reader, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	guards, err := GuardsForService(ctx, reader, "payments-api")
	if err != nil || len(guards) != 1 {
		t.Fatalf("guards for payments-api: %d, %v", len(guards), err)
	}
	records, err := reader.Decisions(ctx, guards[0])
	if err != nil {
		t.Fatal(err)
	}
	if v := Verify(guards[0], records); !v.Verified || v.Decisions != 3 {
		t.Fatalf("verification %+v", v)
	}
	if _, err := reader.Guard(ctx, "payments", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing guard: %v", err)
	}
}
