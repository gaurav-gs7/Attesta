package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
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
	var records []attestav1.RolloutDecision
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
		records = append(records, *d)
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
	read, err := reader.Decisions(ctx, guards[0])
	if err != nil {
		t.Fatal(err)
	}
	if v := Verify(guards[0], read, nil); !v.Verified || v.Decisions != 3 {
		t.Fatalf("verification %+v", v)
	}
	if _, err := reader.Guard(ctx, "payments", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing guard: %v", err)
	}

	t.Run("the admission policy keeps records from being deleted or relabelled", func(t *testing.T) {
		applyManifest(ctx, t, c, filepath.Join("..", "..", "deploy", "controller", "decision-protection.yaml"))
		victim := records[0].DeepCopy()

		// The policy takes effect a few seconds after it is created; probe
		// with throwaway records until it does.
		deadline := time.Now().Add(30 * time.Second)
		for i := 0; ; i++ {
			probe := records[0].DeepCopy()
			probe.ResourceVersion, probe.UID = "", ""
			probe.Name = fmt.Sprintf("probe-%d", i)
			if err := c.Create(ctx, probe); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(ctx, probe); err != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the admission policy never took effect")
			}
			time.Sleep(250 * time.Millisecond)
		}
		err := c.Delete(ctx, victim)
		if !apierrors.IsForbidden(err) && !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "not deleted") {
			t.Fatalf("a cluster admin deleting a decision record: %v, want refused", err)
		}
		relabelled := records[1].DeepCopy()
		relabelled.Labels[attestav1.DecisionGuardUIDLabel] = "someone-else"
		if err := c.Update(ctx, relabelled); err == nil || !strings.Contains(err.Error(), "labels") {
			t.Fatalf("relabel: %v, want refused", err)
		}

		// A member of attesta:decision-admins may.
		adminCfg := rest.CopyConfig(cfg)
		adminCfg.Impersonate = rest.ImpersonationConfig{UserName: "auditor", Groups: []string{"attesta:decision-admins", "system:masters"}}
		admin, err := client.New(adminCfg, client.Options{Scheme: Scheme()})
		if err != nil {
			t.Fatal(err)
		}
		if err := admin.Delete(ctx, victim); err != nil {
			t.Fatalf("decision admin delete: %v", err)
		}
	})
}

func applyManifest(ctx context.Context, t *testing.T, c client.Client, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	for {
		var obj unstructured.Unstructured
		if err := dec.Decode(&obj.Object); errors.Is(err, io.EOF) {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		if len(obj.Object) == 0 {
			continue
		}
		if err := c.Create(ctx, &obj); err != nil {
			t.Fatalf("apply %s %s: %v", obj.GetKind(), obj.GetName(), err)
		}
	}
}
