package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/cluster"
	"github.com/gaurav-gs7/attesta/internal/models"
)

// clusterWith is a cluster holding one guard for payments-api with a sealed
// chain of decisions, made the given durations ago.
func clusterWith(t *testing.T, decisions ...func(*attestav1.RolloutDecisionSpec)) (cluster.Reader, client.Client) {
	t.Helper()
	guard := &attestav1.RolloutGuard{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-api-rollout", Namespace: "payments", UID: types.UID("0f3c9a2e-guard")},
		Spec:       attestav1.RolloutGuardSpec{ServiceRef: "payments-api", Rollback: &attestav1.RollbackPolicy{Enabled: true}},
	}
	objs := []client.Object{guard,
		// A guard for another service, which must not show up.
		&attestav1.RolloutGuard{ObjectMeta: metav1.ObjectMeta{Name: "orders-rollout", Namespace: "orders", UID: "other"}}}
	for _, mutate := range decisions {
		spec := attestav1.RolloutDecisionSpec{
			Guard: guard.Name, GuardUID: string(guard.UID), Actor: attestav1.DecisionActor{Controller: "attesta-controller/test"},
			Deployment: attestav1.DecisionTarget{Name: "payments-api", UID: "d"},
		}
		mutate(&spec)
		if err := spec.Seal(guard.Status.LastDecision); err != nil {
			t.Fatal(err)
		}
		name := attestav1.DecisionName(guard.Name, string(guard.UID), spec.Sequence)
		objs = append(objs, &attestav1.RolloutDecision{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: guard.Namespace,
				Labels: map[string]string{attestav1.DecisionGuardUIDLabel: string(guard.UID)}},
			Spec: spec,
		})
		guard.Status.LastDecision = &attestav1.DecisionRef{Name: name, Sequence: spec.Sequence, Hash: spec.Hash}
	}
	c := fake.NewClientBuilder().WithScheme(cluster.Scheme()).WithObjects(objs...).Build()
	return cluster.NewForClient(c), c
}

func decided(action, revision string, ago time.Duration) func(*attestav1.RolloutDecisionSpec) {
	return func(s *attestav1.RolloutDecisionSpec) {
		s.Action, s.Trigger, s.Reason = action, "AnalysisPassed", "RolloutHealthy"
		s.Deployment.Revision, s.DecidedAt = revision, time.Now().UTC().Add(-ago).Format(time.RFC3339)
		if action == attestav1.ActionRolledBack {
			s.Trigger, s.RestoredTo = "AnalysisFailed", &attestav1.DecisionRevision{Revision: "3", TemplateHash: "h3"}
		}
	}
}

func TestGuardEndpointsWithoutClusterAccessSaySo(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	for _, path := range []string{"/api/v1/guards", "/api/v1/guards/payments/payments-api-rollout", "/api/v1/services/payments-api/guards"} {
		rec := authorizedRequest(server, http.MethodGet, path, nil)
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "ATTESTA_CLUSTER_READ") {
			t.Fatalf("%s: %d %s, want 503 naming the setting", path, rec.Code, rec.Body.String())
		}
	}
}

func TestGuardShowsTheControllersVerifiedDecisions(t *testing.T) {
	reader, _ := clusterWith(t, decided("Promoted", "3", time.Hour), decided("Promoted", "4", 20*time.Minute), decided("RolledBack", "4", 5*time.Minute))
	server := testServer(t, WithAPIToken("secret"), WithCluster(reader))
	onboard(t, server)

	rec := authorizedRequest(server, http.MethodGet, "/api/v1/guards/payments/payments-api-rollout", nil)
	var out struct {
		Guard     guardView            `json:"guard"`
		Decisions []decisionView       `json:"decisions"`
		Chain     cluster.Verification `json:"chain"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if out.Guard.Service != "payments-api" || len(out.Decisions) != 3 || !out.Chain.Verified || out.Chain.Decisions != 3 {
		t.Fatalf("guard %+v, %d decisions, chain %+v", out.Guard, len(out.Decisions), out.Chain)
	}
	if d := out.Decisions[2]; d.Action != "RolledBack" || d.RestoredTo.Revision != "3" || d.Sequence != 3 || d.Name == "" {
		t.Fatalf("last decision %+v", d)
	}

	// The service's guards, and only its guards.
	rec = authorizedRequest(server, http.MethodGet, "/api/v1/services/payments-api/guards", nil)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "orders-rollout") || !strings.Contains(rec.Body.String(), `"verified":true`) {
		t.Fatalf("service guards: %d %s", rec.Code, rec.Body.String())
	}
	status := authorizedRequest(server, http.MethodGet, "/api/v1/services/payments-api/status", nil).Body.String()
	if !strings.Contains(status, `"guards":[`) || strings.Contains(status, "guardsUnavailable") {
		t.Fatalf("status does not carry the guards: %s", status)
	}

	// Every guard, or one namespace's.
	all := authorizedRequest(server, http.MethodGet, "/api/v1/guards", nil).Body.String()
	one := authorizedRequest(server, http.MethodGet, "/api/v1/guards?namespace=orders", nil).Body.String()
	if !strings.Contains(all, "payments-api-rollout") || !strings.Contains(all, "orders-rollout") ||
		strings.Contains(one, "payments-api-rollout") || !strings.Contains(one, "orders-rollout") {
		t.Fatalf("lists:\n%s\n%s", all, one)
	}
	if rec := authorizedRequest(server, http.MethodGet, "/api/v1/guards?namespace=Not_A_Label", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad namespace: %d", rec.Code)
	}
	if rec := authorizedRequest(server, http.MethodGet, "/api/v1/guards/payments/missing", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("missing guard: %d", rec.Code)
	}
}

func TestGuardReportsATamperedChain(t *testing.T) {
	reader, c := clusterWith(t, decided("Promoted", "3", time.Hour), decided("Promoted", "4", 20*time.Minute))
	var list attestav1.RolloutDecisionList
	if err := c.List(t.Context(), &list); err != nil {
		t.Fatal(err)
	}
	for i := range list.Items {
		if list.Items[i].Spec.Sequence == 1 {
			list.Items[i].Spec.Deployment.Revision = "9" // an edit made directly in etcd
			if err := c.Update(t.Context(), &list.Items[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	server := testServer(t, WithAPIToken("secret"), WithCluster(reader))
	rec := authorizedRequest(server, http.MethodGet, "/api/v1/guards/payments/payments-api-rollout", nil)
	if !strings.Contains(rec.Body.String(), `"verified":false`) || !strings.Contains(rec.Body.String(), `"brokenAt":1`) {
		t.Fatalf("tampered chain not reported: %s", rec.Body.String())
	}
}

// The incident timeline cites the controller's records: the promotion in
// the window is the correlated revision, the rollback is listed.
func TestIncidentTimelineCitesTheControllersDecisions(t *testing.T) {
	reader, _ := clusterWith(t, decided("Promoted", "3", 3*time.Hour), decided("Promoted", "4", 20*time.Minute), decided("RolledBack", "4", 5*time.Minute))
	server := testServer(t, WithAPIToken("secret"), WithPrometheusURL(prometheusStub(t, healthyPrometheus())), WithCluster(reader))
	onboard(t, server)
	record := openIncident(t, server, `{"service":"payments-api","alertName":"HighErrorRate"}`)
	var decisions int
	var correlation *models.IncidentEvent
	for i, e := range record.Events {
		switch e.EventType {
		case "rollout-decision":
			decisions++
			if !strings.Contains(e.Description, "[record payments-api-rollout-0f3c9a2e-") {
				t.Fatalf("decision event does not cite its record: %s", e.Description)
			}
		case "correlation":
			correlation = &record.Events[i]
		}
	}
	if decisions != 2 {
		t.Fatalf("%d decisions in the 2h window, want 2: %+v", decisions, record.Events)
	}
	if correlation == nil || !strings.Contains(correlation.Description, "revision 4 of payments/payments-api was promoted") ||
		!strings.Contains(correlation.Description, "not a confirmed cause") {
		t.Fatalf("correlation: %+v", correlation)
	}
	if record.Signals.DeploymentGate != models.GateAllowed || record.Signals.ErrorRateCurrentPct == nil {
		t.Fatalf("incident must carry the measured SLO state, got %+v", record.Signals)
	}
}

func TestIncidentWithoutClusterAccessSaysTheDecisionsWereNotRead(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	record := openIncident(t, server, `{"service":"payments-api","alertName":"HighErrorRate"}`)
	for _, e := range record.Events {
		if e.EventType == "rollout-decisions-unavailable" && strings.Contains(e.Description, "not configured") {
			return
		}
	}
	t.Fatalf("timeline does not say the decisions were not read: %+v", record.Events)
}
