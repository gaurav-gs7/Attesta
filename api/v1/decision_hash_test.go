package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func sampleSpec() RolloutDecisionSpec {
	passed := false
	return RolloutDecisionSpec{
		Guard:     "payments-api-rollout",
		GuardUID:  "0f3c9a2e-1111-2222-3333-444455556666",
		DecidedAt: "2026-11-24T10:00:00Z",
		Actor:     DecisionActor{Controller: "attesta-controller/v0.2.0", Instance: "attesta-controller-7d9f-abcde"},
		Deployment: DecisionTarget{
			Name: "payments-api", UID: "d-uid", Generation: 7, Revision: "4", TemplateHash: "6b8f7c9d4", Images: []string{"ghcr.io/acme/payments-api:v2"},
		},
		Trigger:    "AnalysisFailed",
		Action:     ActionRolledBack,
		Reason:     "RollbackTriggered",
		Message:    "error rate 7.10% > 1% <&> must not be HTML-escaped",
		RestoredTo: &DecisionRevision{Revision: "3", TemplateHash: "5c7d6b8a3"},
		Evidence: []DecisionEvidence{{
			Name: "error-rate", Source: "prometheus", Query: "100 * errors / requests", Value: "7.1%",
			Threshold: "<= 1%", Passed: &passed, EvaluatedAt: "2026-11-24T09:59:30Z",
		}},
	}
}

func chain(t *testing.T, n int) []RolloutDecision {
	t.Helper()
	var out []RolloutDecision
	var head *DecisionRef
	for i := 0; i < n; i++ {
		spec := sampleSpec()
		spec.Deployment.Generation = int64(10 + i)
		if err := spec.Seal(head); err != nil {
			t.Fatal(err)
		}
		r := RolloutDecision{Spec: spec}
		r.Name = DecisionName(spec.Guard, spec.GuardUID, spec.Sequence)
		out = append(out, r)
		head = &DecisionRef{Name: r.Name, Sequence: spec.Sequence, Hash: spec.Hash}
	}
	return out
}

func headOf(records []RolloutDecision) *DecisionRef {
	last := records[len(records)-1]
	return &DecisionRef{Name: last.Name, Sequence: last.Spec.Sequence, Hash: last.Spec.Hash}
}

// The canonical form is a contract with every verifier outside this
// repository: sorted keys, compact, no HTML escaping, no hash field.
func TestCanonicalJSONIsSortedCompactAndUnescaped(t *testing.T) {
	spec := sampleSpec()
	spec.Hash = strings.Repeat("a", 64)
	got, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(got, []byte(`"hash"`)) {
		t.Fatal("canonical JSON includes the hash field")
	}
	if !bytes.Contains(got, []byte(`<&>`)) {
		t.Fatalf("canonical JSON escapes HTML characters: %s", got)
	}
	if bytes.ContainsAny(got, " \n") && !bytes.Contains(got, []byte("must not")) {
		t.Fatalf("canonical JSON is not compact: %s", got)
	}
	if !bytes.HasPrefix(got, []byte(`{"action":"RolledBack","actor":{"controller":`)) {
		t.Fatalf("keys are not sorted: %s", got[:60])
	}
}

// Pins the hash of a fixed decision, so a change to the canonical form, or
// to a field's JSON name, cannot slip in unnoticed: it would orphan every
// record already written.
func TestHashIsStable(t *testing.T) {
	const want = "237406215960131801a372a3bd6c14d938b963307b0d612d20a180de86ed7f6f"
	got, err := sampleSpec().ComputeHash()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("hash = %s, want %s", got, want)
	}
}

// The documented way to check a record without Attesta's code.
func TestHashMatchesJq(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq not installed")
	}
	records := chain(t, 1)
	raw, err := json.Marshal(records[0])
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(jq, "-cjS", ".spec | del(.hash)")
	cmd.Stdin = bytes.NewReader(raw)
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	want, err := records[0].Spec.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, want) {
		t.Fatalf("jq canonical form differs\njq:   %s\ngo:   %s", out, want)
	}
}

func TestVerifyChain(t *testing.T) {
	const guard, uid = "payments-api-rollout", "0f3c9a2e-1111-2222-3333-444455556666"
	tests := []struct {
		name    string
		mutate  func([]RolloutDecision) ([]RolloutDecision, *DecisionRef)
		wantSeq int64
		want    string
	}{
		{"intact", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) { return r, headOf(r) }, 0, ""},
		{"intact without a head", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) { return r, nil }, 0, ""},
		{"altered evidence", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) {
			r[1].Spec.Evidence[0].Value = "0.2%"
			return r, headOf(r)
		}, 2, "altered"},
		{"altered and rehashed", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) {
			r[1].Spec.Action = ActionPromoted
			r[1].Spec.Hash, _ = r[1].Spec.ComputeHash()
			return r, headOf(r)
		}, 3, "previousHash"},
		{"middle record deleted", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) {
			return append(r[:1:1], r[2:]...), headOf(r)
		}, 2, "missing"},
		{"last record deleted", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) {
			head := headOf(r)
			return r[:2], head
		}, 3, "last decision is 3"},
		{"duplicate sequence", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) {
			return append(r, r[2]), headOf(r)
		}, 4, "twice"},
		{"record from another guard", func(r []RolloutDecision) ([]RolloutDecision, *DecisionRef) {
			r[0].Spec.GuardUID = "someone-else"
			return r, headOf(r)
		}, 1, "belongs to"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records, head := tt.mutate(chain(t, 3))
			err := VerifyChain(guard, uid, records, head)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("VerifyChain = %v, want nil", err)
				}
				return
			}
			var chainErr *ChainError
			if !errors.As(err, &chainErr) {
				t.Fatalf("VerifyChain = %v, want a ChainError", err)
			}
			if chainErr.Sequence != tt.wantSeq || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("VerifyChain = %v, want a break at %d mentioning %q", err, tt.wantSeq, tt.want)
			}
		})
	}
}

func TestDecisionNameFitsAnObjectName(t *testing.T) {
	name := DecisionName(strings.Repeat("g", 260), "0f3c9a2e-1111", 12)
	if len(name) > 253 || !strings.HasSuffix(name, "-0f3c9a2e-000012") {
		t.Fatalf("name %q (%d chars)", name[len(name)-20:], len(name))
	}
}
