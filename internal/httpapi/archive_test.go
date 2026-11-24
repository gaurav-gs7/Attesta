package httpapi

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/archive"
)

// sealedChain returns n sealed (and, with key, signed) records of one guard.
func sealedChain(t *testing.T, n int, key ed25519.PrivateKey) []attestav1.RolloutDecision {
	t.Helper()
	var out []attestav1.RolloutDecision
	var head *attestav1.DecisionRef
	for i := 0; i < n; i++ {
		spec := attestav1.RolloutDecisionSpec{
			Guard: "payments-api-rollout", GuardUID: "0f3c9a2e-guard", DecidedAt: time.Now().UTC().Format(time.RFC3339),
			Actor:      attestav1.DecisionActor{Controller: "attesta-controller/test"},
			Deployment: attestav1.DecisionTarget{Name: "payments-api", UID: "d", Generation: int64(i + 1), Revision: "1"},
			Trigger:    "RolloutComplete", Action: attestav1.ActionPromoted, Reason: "RolloutHealthy",
		}
		if err := spec.Seal(head); err != nil {
			t.Fatal(err)
		}
		if key != nil {
			_ = spec.Sign(key)
		}
		d := attestav1.RolloutDecision{ObjectMeta: metav1.ObjectMeta{
			Name: attestav1.DecisionName(spec.Guard, spec.GuardUID, spec.Sequence), Namespace: "payments"}, Spec: spec}
		out = append(out, d)
		head = &attestav1.DecisionRef{Name: d.Name, Sequence: spec.Sequence, Hash: spec.Hash}
	}
	return out
}

func postRecord(t *testing.T, server *Server, d attestav1.RolloutDecision) (int, string) {
	t.Helper()
	body, _ := json.Marshal(archive.RecordOf(d))
	rec := authorizedRequest(server, http.MethodPost, "/api/v1/decision-archive", body)
	return rec.Code, rec.Body.String()
}

func TestArchiveIsAppendOnlyAndRefusesForksAndGaps(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			server := newTestServer(t, open(t))
			chain := sealedChain(t, 3, nil)

			if code, body := postRecord(t, server, chain[1]); code != http.StatusConflict || !strings.Contains(body, "before 1") {
				t.Fatalf("out of order: %d %s", code, body)
			}
			for _, d := range chain {
				if code, body := postRecord(t, server, d); code != http.StatusCreated {
					t.Fatalf("archive %d: %d %s", d.Spec.Sequence, code, body)
				}
			}
			if code, body := postRecord(t, server, chain[2]); code != http.StatusOK || !strings.Contains(body, `"duplicate":true`) {
				t.Fatalf("resend: %d %s", code, body)
			}

			// A different, correctly hashed record at an archived position:
			// what rewriting history in the cluster looks like from here.
			fork := chain[1]
			fork.Spec.Action = attestav1.ActionRolledBack
			fork.Spec.Hash, _ = fork.Spec.ComputeHash()
			if code, body := postRecord(t, server, fork); code != http.StatusConflict || !strings.Contains(body, "forks the chain") {
				t.Fatalf("fork: %d %s", code, body)
			}
			if !strings.Contains(scrape(t, server), "attesta_decision_archive_refused_total 2") {
				t.Fatal("refusals are not counted")
			}

			bad := sealedChain(t, 4, nil)[3]
			bad.Spec.Reason = "edited after sealing"
			if code, _ := postRecord(t, server, bad); code != http.StatusUnprocessableEntity {
				t.Fatalf("bad hash: %d", code)
			}

			list := authorizedRequest(server, http.MethodGet, "/api/v1/decision-archive", nil).Body.String()
			if !strings.Contains(list, `"headSequence":3`) || !strings.Contains(list, `"guardUID":"0f3c9a2e-guard"`) {
				t.Fatalf("archive list: %s", list)
			}
			one := authorizedRequest(server, http.MethodGet, "/api/v1/decision-archive/0f3c9a2e-guard", nil).Body.String()
			if !strings.Contains(one, `"verified":true`) || !strings.Contains(one, `"decisions":3`) {
				t.Fatalf("archived chain: %s", one)
			}
		})
	}
}

func TestArchiveRequiresSignaturesWhenKeysAreConfigured(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	keys := attestav1.Keys{}
	keys.Add(pub)
	server := testServer(t, WithAPIToken("secret"), WithDecisionKeys(keys))
	if code, body := postRecord(t, server, sealedChain(t, 1, nil)[0]); code != http.StatusUnprocessableEntity || !strings.Contains(body, "unsigned") {
		t.Fatalf("unsigned: %d %s", code, body)
	}
	_, otherKey, _ := ed25519.GenerateKey(nil)
	if code, body := postRecord(t, server, sealedChain(t, 1, otherKey)[0]); code != http.StatusUnprocessableEntity || !strings.Contains(body, "unknown key") {
		t.Fatalf("another key: %d %s", code, body)
	}
	if code, body := postRecord(t, server, sealedChain(t, 1, priv)[0]); code != http.StatusCreated {
		t.Fatalf("signed: %d %s", code, body)
	}
	one := authorizedRequest(server, http.MethodGet, "/api/v1/decision-archive/0f3c9a2e-guard", nil).Body.String()
	if !strings.Contains(one, `"signaturesChecked":true`) || !strings.Contains(one, `"verified":true`) {
		t.Fatalf("archived chain: %s", one)
	}
}

func TestArchiveNeedsOperator(t *testing.T) {
	server, _ := tokenServer(t, &logBuffer{})
	body, _ := json.Marshal(archive.RecordOf(sealedChain(t, 1, nil)[0]))
	if rec := as(server, "tok-view", http.MethodPost, "/api/v1/decision-archive", string(body)); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer archiving: %d", rec.Code)
	}
	if rec := as(server, "tok-ops", http.MethodPost, "/api/v1/decision-archive", string(body)); rec.Code != http.StatusCreated {
		t.Fatalf("operator archiving: %d %s", rec.Code, rec.Body.String())
	}
}

// The guard view compares the cluster's records with the archive: a record
// deleted from the cluster, or changed there, shows.
func TestGuardIsComparedWithTheArchive(t *testing.T) {
	reader, c := clusterWith(t, decided("Promoted", "3", time.Hour), decided("Promoted", "4", 20*time.Minute), decided("RolledBack", "4", 5*time.Minute))
	server := testServer(t, WithAPIToken("secret"), WithCluster(reader))
	var list attestav1.RolloutDecisionList
	if err := c.List(t.Context(), &list); err != nil {
		t.Fatal(err)
	}
	records := map[int64]attestav1.RolloutDecision{}
	for _, d := range list.Items {
		records[d.Spec.Sequence] = d
	}
	for seq := int64(1); seq <= 3; seq++ {
		if code, body := postRecord(t, server, records[seq]); code != http.StatusCreated {
			t.Fatalf("archive %d: %d %s", seq, code, body)
		}
	}
	guardPath := "/api/v1/guards/payments/payments-api-rollout"
	if body := authorizedRequest(server, http.MethodGet, guardPath, nil).Body.String(); !strings.Contains(body, `"matches":true`) {
		t.Fatalf("an untouched chain does not match its archive: %s", body)
	}

	// Rewrite record 2 in place (a direct etcd edit), delete record 3.
	r2 := records[2]
	r2.Spec.Deployment.Revision = "9"
	if err := c.Update(t.Context(), &r2); err != nil {
		t.Fatal(err)
	}
	r3 := records[3]
	if err := c.Delete(t.Context(), &r3); err != nil {
		t.Fatal(err)
	}
	body := authorizedRequest(server, http.MethodGet, guardPath, nil).Body.String()
	if !strings.Contains(body, `"missingFromCluster":[3]`) || !strings.Contains(body, `"differ":[2]`) || !strings.Contains(body, `"matches":false`) {
		t.Fatalf("tampering not caught against the archive: %s", body)
	}
}
