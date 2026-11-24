package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// kubectlList builds what `kubectl get rolloutguards,rolloutdecisions -o
// json` prints for a guard with n sealed decisions.
func kubectlList(t *testing.T, n int, mutate func(records []attestav1.RolloutDecision)) []byte {
	t.Helper()
	guard := attestav1.RolloutGuard{
		TypeMeta:   metav1.TypeMeta{APIVersion: "attesta.io/v1", Kind: "RolloutGuard"},
		ObjectMeta: metav1.ObjectMeta{Name: "payments-api-rollout", Namespace: "payments", UID: "guard-uid-1"},
	}
	var records []attestav1.RolloutDecision
	for i := 0; i < n; i++ {
		spec := attestav1.RolloutDecisionSpec{
			Guard: guard.Name, GuardUID: string(guard.UID), DecidedAt: "2026-11-24T10:00:00Z",
			Actor:      attestav1.DecisionActor{Controller: "attesta-controller/test"},
			Deployment: attestav1.DecisionTarget{Name: "payments-api", UID: "d", Generation: int64(i + 1), Revision: "1"},
			Trigger:    "RolloutComplete", Action: attestav1.ActionPromoted, Reason: "RolloutHealthy",
		}
		if err := spec.Seal(guard.Status.LastDecision); err != nil {
			t.Fatal(err)
		}
		r := attestav1.RolloutDecision{
			TypeMeta:   metav1.TypeMeta{APIVersion: "attesta.io/v1", Kind: "RolloutDecision"},
			ObjectMeta: metav1.ObjectMeta{Name: attestav1.DecisionName(guard.Name, string(guard.UID), spec.Sequence), Namespace: "payments"},
			Spec:       spec,
		}
		records = append(records, r)
		guard.Status.LastDecision = &attestav1.DecisionRef{Name: r.Name, Sequence: spec.Sequence, Hash: spec.Hash}
	}
	if mutate != nil {
		mutate(records)
	}
	items := []any{guard}
	for _, r := range records {
		items = append(items, r)
	}
	raw, err := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "List", "items": items})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestVerifyAcceptsAnIntactChain(t *testing.T) {
	var out bytes.Buffer
	if err := verifyChains(kubectlList(t, 3, nil), &out); err != nil {
		t.Fatalf("verify: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "OK      payments/payments-api-rollout: 3 decisions") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestVerifyReportsATruncatedChain(t *testing.T) {
	raw := kubectlList(t, 3, nil)
	var list struct {
		Kind  string            `json:"kind"`
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatal(err)
	}
	list.Items = list.Items[:len(list.Items)-1] // the last record is gone
	raw, _ = json.Marshal(list)

	var out bytes.Buffer
	err := verifyChains(raw, &out)
	if !errors.Is(err, errChainBroken) || !strings.Contains(out.String(), "BROKEN") {
		t.Fatalf("verify = %v, output:\n%s", err, out.String())
	}
}

func TestVerifyReportsAnEditedRecord(t *testing.T) {
	raw := kubectlList(t, 2, func(r []attestav1.RolloutDecision) { r[0].Spec.Action = attestav1.ActionRolledBack })
	var out bytes.Buffer
	if err := verifyChains(raw, &out); !errors.Is(err, errChainBroken) || !strings.Contains(out.String(), "altered") {
		t.Fatalf("verify = %v, output:\n%s", err, out.String())
	}
}
