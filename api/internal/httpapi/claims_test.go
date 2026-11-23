package httpapi

// Tests that the API reports only measured or recorded data (audit T-1 to
// T-4, T-11, S-1, S-2).

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

const onboardBody = `{"name":"payments-api","team":"platform","owner":"gaurav","language":"go","environment":"staging","slo":"99.9","deploymentStrategy":"canary"}`

// prometheusStub serves instant-query results keyed by the query's window and
// kind. A key absent from values yields an empty result.
func prometheusStub(t *testing.T, values map[string]string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		errs := strings.Contains(q, `status=~"5.."`)
		key := "unknown"
		switch {
		case strings.Contains(q, "histogram_quantile"):
			key = "p95"
		case strings.Contains(q, "[5m]"):
			key = map[bool]string{true: "err5m", false: "total5m"}[errs]
		case strings.Contains(q, "[1h]"):
			key = map[bool]string{true: "err1h", false: "total1h"}[errs]
		case strings.Contains(q, "[30d]"):
			key = map[bool]string{true: "err30d", false: "total30d"}[errs]
		}
		w.Header().Set("Content-Type", "application/json")
		v, ok := values[key]
		if !ok {
			_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,%q]}]}}`, v)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func healthyPrometheus() map[string]string {
	return map[string]string{
		"total5m": "50", "err5m": "0.05", "p95": "120",
		"total1h": "50", "err1h": "0.01",
		"total30d": "1000000", "err30d": "200",
	}
}

type sloResponse struct {
	SLO models.SLOStatus `json:"slo"`
}

func getSLO(t *testing.T, server *Server) (int, sloResponse, string) {
	t.Helper()
	rec := authorizedRequest(server, http.MethodGet, "/api/v1/services/payments-api/slos", nil)
	var out sloResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not valid JSON (%d bytes, status %d): %v", rec.Body.Len(), rec.Code, err)
	}
	return rec.Code, out, rec.Body.String()
}

func onboard(t *testing.T, server *Server) {
	t.Helper()
	if rec := authorizedRequest(server, http.MethodPost, "/api/v1/services", []byte(onboardBody)); rec.Code != http.StatusCreated {
		t.Fatalf("onboard: %d %s", rec.Code, rec.Body.String())
	}
}

func startRollout(server *Server) *httptest.ResponseRecorder {
	return authorizedRequest(server, http.MethodPost, "/api/v1/services/payments-api/rollouts", []byte(`{"version":"v2","image":"ghcr.io/acme/payments-api:v2"}`))
}

// Audit T-4: without Prometheus there is no data, so nothing is reported and
// the gate fails closed.
func TestSLOWithoutPrometheusIsUnknownAndBlocksRollouts(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	code, out, body := getSLO(t, server)
	if code != http.StatusOK || out.SLO.DeploymentGate != models.GateUnknown || out.SLO.DataSource != models.SLOSourceNone {
		t.Fatalf("expected unknown gate from source none, got %d %s", code, body)
	}
	for _, field := range []string{"errorRateCurrentPct", "latencyP95CurrentMs", "availabilityCurrent", "burnRate", "errorBudgetRemainingPct"} {
		if strings.Contains(body, field) {
			t.Fatalf("unknown SLO status must not report %s: %s", field, body)
		}
	}
	if rec := startRollout(server); rec.Code != http.StatusConflict {
		t.Fatalf("rollout with unknown SLO state: got %d, want 409", rec.Code)
	}
}

// Audit S-1: a NaN p95 used to produce a 200 with an empty body and approve
// the rollout.
func TestSLOWithNaNLatencyIsUnknownAndValidJSON(t *testing.T) {
	values := healthyPrometheus()
	values["p95"] = "NaN"
	server := testServer(t, WithAPIToken("secret"), WithPrometheusURL(prometheusStub(t, values)))
	onboard(t, server)
	_, out, body := getSLO(t, server)
	if out.SLO.DeploymentGate != models.GateUnknown || !strings.Contains(out.SLO.Reason, "p95 latency") {
		t.Fatalf("expected unknown gate naming p95 latency, got %s", body)
	}
	rec := startRollout(server)
	if rec.Code != http.StatusConflict || rec.Body.Len() == 0 {
		t.Fatalf("rollout with NaN latency: got %d with %d-byte body, want 409 with a body", rec.Code, rec.Body.Len())
	}
}

// Audit S-2: no series for the service used to read as 100% availability.
func TestSLOWithNoSeriesIsUnknownAndBlocksRollouts(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"), WithPrometheusURL(prometheusStub(t, map[string]string{})))
	onboard(t, server)
	_, out, body := getSLO(t, server)
	if out.SLO.DeploymentGate != models.GateUnknown || out.SLO.AvailabilityCurrent != nil {
		t.Fatalf("expected unknown gate and no availability, got %s", body)
	}
	if rec := startRollout(server); rec.Code != http.StatusConflict {
		t.Fatalf("rollout with no telemetry: got %d, want 409", rec.Code)
	}
}

// Audit S-2: a trickle of traffic is not enough to judge a rollout.
func TestSLOWithTooLittleTrafficBlocksRollouts(t *testing.T) {
	values := healthyPrometheus()
	values["total5m"], values["err5m"] = "0.03", "0" // about 9 requests in 5m
	server := testServer(t, WithAPIToken("secret"), WithPrometheusURL(prometheusStub(t, values)))
	onboard(t, server)
	_, out, body := getSLO(t, server)
	if out.SLO.DeploymentGate != models.GateUnknown || out.SLO.ErrorRateCurrentPct != nil || !strings.Contains(out.SLO.Reason, "fewer than the 20 needed") {
		t.Fatalf("expected an unknown gate that names the volume floor, got %s", body)
	}
	if rec := startRollout(server); rec.Code != http.StatusConflict {
		t.Fatalf("rollout on 9 requests: got %d, want 409", rec.Code)
	}
}

func TestSLOWithHealthyMeasurementsAllowsRollout(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"), WithPrometheusURL(prometheusStub(t, healthyPrometheus())))
	onboard(t, server)
	_, out, body := getSLO(t, server)
	if out.SLO.DeploymentGate != models.GateAllowed || out.SLO.ErrorBudgetRemainingPct == nil || out.SLO.BudgetWindow != "30d" {
		t.Fatalf("expected allowed gate with a 30d budget, got %s", body)
	}
	if rec := startRollout(server); rec.Code != http.StatusAccepted {
		t.Fatalf("rollout with healthy SLOs: got %d %s, want 202", rec.Code, rec.Body.String())
	}
}

// Audit T-3: the status endpoint used to return fixed latency and error values.
func TestStatusReportsEvaluatedSLOAndRecordedDeployment(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	rec := authorizedRequest(server, http.MethodGet, "/api/v1/services/payments-api/status", nil)
	var status models.ServiceStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.SLO.DeploymentGate != models.GateUnknown || status.SLO.ErrorRateCurrentPct != nil {
		t.Fatalf("status must carry the real (unknown) SLO evaluation, got %s", rec.Body.String())
	}
	if status.LatestDeployment == nil || status.LatestDeployment.Status != "generated" {
		t.Fatalf("status must carry the recorded deployment, got %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "p99LatencyMs") || strings.Contains(rec.Body.String(), "healthGate") {
		t.Fatalf("status still carries the old fixed fields: %s", rec.Body.String())
	}
}

type incidentResponse struct {
	Incident models.IncidentRecord `json:"incident"`
}

func openIncident(t *testing.T, server *Server, body string) models.IncidentRecord {
	t.Helper()
	rec := authorizedRequest(server, http.MethodPost, "/api/v1/incidents", []byte(body))
	if rec.Code != http.StatusCreated {
		t.Fatalf("open incident: %d %s", rec.Code, rec.Body.String())
	}
	var out incidentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Incident
}

// Audit T-1 and T-2: the timeline used to invent six events, two of them in
// the future, and the SLO state was set to constants.
func TestIncidentTimelineHoldsOnlyRecordedEvents(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	record := openIncident(t, server, `{"service":"payments-api","alertName":"HighErrorRate","errorRate":8.2,"latencyP95Ms":850}`)

	now := time.Now().UTC().Add(time.Second)
	types := map[string]int{}
	for _, e := range record.Events {
		if e.Timestamp.After(now) {
			t.Fatalf("timeline event in the future: %+v", e)
		}
		types[e.EventType]++
	}
	if types["deployment"] != 1 || types["incident-opened"] != 1 || types["slo-evaluation"] != 1 || types["correlation"] != 0 || len(record.Events) != 3 {
		t.Fatalf("expected exactly the onboarding deployment record, the open event and the SLO evaluation, got %+v", record.Events)
	}
	if record.Signals.DeploymentGate != models.GateUnknown || record.Signals.BurnRate != nil {
		t.Fatalf("incident SLO state must be the real (unknown) evaluation, got %+v", record.Signals)
	}
	if r := record.Incident.AlertReported; r == nil || r.ErrorRatePct == nil || *r.ErrorRatePct != 8.2 {
		t.Fatalf("alert-reported values must be kept as the sender's claims, got %+v", record.Incident.AlertReported)
	}

	pm := authorizedRequest(server, http.MethodGet, "/api/v1/incidents/"+record.Incident.ID+"/postmortem", nil).Body.String()
	for _, want := range []string{"Not measured", "Undetermined", "did not verify", "TODO"} {
		if !strings.Contains(pm, want) {
			t.Fatalf("postmortem missing %q:\n%s", want, pm)
		}
	}
	if strings.Contains(pm, "Recent deployment or configuration change correlated") {
		t.Fatalf("postmortem still asserts a cause:\n%s", pm)
	}
}

func TestIncidentCorrelatesARecordedRollout(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"), WithPrometheusURL(prometheusStub(t, healthyPrometheus())))
	onboard(t, server)
	if rec := startRollout(server); rec.Code != http.StatusAccepted {
		t.Fatalf("rollout: %d %s", rec.Code, rec.Body.String())
	}
	record := openIncident(t, server, `{"service":"payments-api","alertName":"HighErrorRate"}`)
	var correlation *models.IncidentEvent
	for i := range record.Events {
		if record.Events[i].EventType == "correlation" {
			correlation = &record.Events[i]
		}
	}
	if correlation == nil || !strings.Contains(correlation.Description, "v2") || !strings.Contains(correlation.Description, "not a confirmed cause") {
		t.Fatalf("expected a labelled correlation with rollout v2, got %+v", record.Events)
	}
	if record.Signals.DeploymentGate != models.GateAllowed || record.Signals.ErrorRateCurrentPct == nil {
		t.Fatalf("incident must carry the measured SLO state, got %+v", record.Signals)
	}
}

func TestResolveIsHonestAndIdempotent(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	record := openIncident(t, server, `{"service":"payments-api","alertName":"HighErrorRate"}`)
	path := "/api/v1/incidents/" + record.Incident.ID + "/resolve"

	first := authorizedRequest(server, http.MethodPost, path, []byte(`{"note":"bad config reverted"}`))
	var out incidentResponse
	if err := json.Unmarshal(first.Body.Bytes(), &out); err != nil || first.Code != http.StatusOK {
		t.Fatalf("resolve: %d %s", first.Code, first.Body.String())
	}
	note := out.Incident.Incident.ResolutionNote
	if !strings.Contains(note, "via the Attesta API") || !strings.Contains(note, "bad config reverted") || strings.Contains(note, "SLO recovery checks") {
		t.Fatalf("resolution note = %q", note)
	}
	events := len(out.Incident.Events)
	second := authorizedRequest(server, http.MethodPost, path, nil)
	if err := json.Unmarshal(second.Body.Bytes(), &out); err != nil || len(out.Incident.Events) != events {
		t.Fatalf("second resolve changed the record: %s", second.Body.String())
	}
}

type webhookResponse struct {
	Results []alertResult `json:"results"`
}

func postAlertmanager(t *testing.T, server *Server, status string, alerts string) webhookResponse {
	t.Helper()
	body := fmt.Sprintf(`{"version":"4","status":%q,"receiver":"attesta","alerts":[%s]}`, status, alerts)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts/alertmanager", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret") // Alertmanager's http_config sends this header
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("webhook: %d %s", rec.Code, rec.Body.String())
	}
	var out webhookResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// Audit T-11: Alertmanager's own webhook format is now accepted.
func TestAlertmanagerWebhookOpensDedupesAndResolves(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	firing := `{"status":"firing","labels":{"alertname":"PaymentsHighErrorRate","service":"payments-api","severity":"page"},
		"annotations":{"summary":"5xx above 1%"},"startsAt":"2026-11-22T04:00:00Z","fingerprint":"abc123"}`

	opened := postAlertmanager(t, server, "firing", firing)
	if len(opened.Results) != 1 || opened.Results[0].Action != "opened" {
		t.Fatalf("first firing: %+v", opened.Results)
	}
	id := opened.Results[0].IncidentID
	if dup := postAlertmanager(t, server, "firing", firing); dup.Results[0].Action != "duplicate" || dup.Results[0].IncidentID != id {
		t.Fatalf("repeat firing: %+v", dup.Results)
	}

	resolved := `{"status":"resolved","labels":{"alertname":"PaymentsHighErrorRate","service":"payments-api"},
		"startsAt":"2026-11-22T04:00:00Z","endsAt":"2026-11-22T04:20:00Z","fingerprint":"abc123"}`
	if res := postAlertmanager(t, server, "resolved", resolved); res.Results[0].Action != "resolved" || res.Results[0].IncidentID != id {
		t.Fatalf("resolved: %+v", res.Results)
	}

	rec := authorizedRequest(server, http.MethodGet, "/api/v1/incidents/"+id, nil)
	var record models.IncidentRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	inc := record.Incident
	if inc.Source != "alertmanager" || inc.Severity != "page" || inc.Title != "5xx above 1%" || inc.Status != "resolved" {
		t.Fatalf("unexpected incident %+v", inc)
	}
	if inc.ResolvedAt == nil || !inc.ResolvedAt.Equal(time.Date(2026, 11, 22, 4, 20, 0, 0, time.UTC)) {
		t.Fatalf("resolvedAt = %v, want the alert's endsAt", inc.ResolvedAt)
	}
	if inc.AlertStartsAt == nil || !inc.AlertStartsAt.Equal(time.Date(2026, 11, 22, 4, 0, 0, 0, time.UTC)) {
		t.Fatalf("alertStartsAt = %v, want the alert's startsAt", inc.AlertStartsAt)
	}
}

func TestAlertmanagerWebhookSkipsAlertsItCannotPlace(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	out := postAlertmanager(t, server, "firing",
		`{"status":"firing","labels":{"alertname":"NoService"},"fingerprint":"f1"},
		 {"status":"firing","labels":{"alertname":"Other","service":"not-registered"},"fingerprint":"f2"},
		 {"status":"resolved","labels":{"alertname":"Never","service":"payments-api"},"fingerprint":"f3"}`)
	if len(out.Results) != 3 {
		t.Fatalf("expected three results, got %+v", out.Results)
	}
	for i, want := range []string{`no "service" label`, "not registered", "no active incident"} {
		if out.Results[i].Action != "skipped" || !strings.Contains(out.Results[i].Reason, want) {
			t.Fatalf("result %d = %+v, want skipped with %q", i, out.Results[i], want)
		}
	}
}

func TestAlertmanagerWebhookRejectsOtherVersions(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	rec := authorizedRequest(server, http.MethodPost, "/api/v1/alerts/alertmanager", []byte(`{"version":"3","alerts":[]}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("version 3: got %d, want 400", rec.Code)
	}
}

// Audit T-8: a 5-field payload used to score 100 because the API filled in
// pager, runbook, dashboard and repository itself.
func TestReadinessScoresOnlyRegisteredMetadata(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	if rec := authorizedRequest(server, http.MethodPost, "/api/v1/services",
		[]byte(`{"name":"bare-api","team":"t","owner":"o","language":"go","environment":"prod"}`)); rec.Code != http.StatusCreated {
		t.Fatalf("onboard: %d %s", rec.Code, rec.Body.String())
	}
	rec := authorizedRequest(server, http.MethodGet, "/api/v1/services/bare-api/readiness", nil)
	var out struct {
		Report models.ReadinessReport `json:"report"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Report.Score >= 100 {
		t.Fatalf("minimal payload scored %d", out.Report.Score)
	}
	failed := map[string]bool{}
	for _, c := range out.Report.Failed {
		failed[c.Name] = true
	}
	for _, name := range []string{"pager", "runbook", "dashboard", "repository"} {
		if !failed[name] {
			t.Fatalf("check %q passed without the field being registered: %+v", name, out.Report.Failed)
		}
	}
	if !strings.HasPrefix(out.Report.Scope, "metadata-only") {
		t.Fatalf("scope = %q", out.Report.Scope)
	}
	svc := authorizedRequest(server, http.MethodGet, "/api/v1/services/bare-api", nil).Body.String()
	if strings.Contains(svc, "github.com/example") || strings.Contains(svc, "-oncall") {
		t.Fatalf("service record carries invented metadata: %s", svc)
	}
}

func TestExplicitRollbackOnFailureFalseIsHonoured(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	body := `{"name":"hold-api","team":"t","owner":"o","language":"go","environment":"staging","deploymentStrategy":"canary","rollbackOnFailure":false}`
	if rec := authorizedRequest(server, http.MethodPost, "/api/v1/services", []byte(body)); rec.Code != http.StatusCreated {
		t.Fatalf("onboard: %d %s", rec.Code, rec.Body.String())
	}
	svc := authorizedRequest(server, http.MethodGet, "/api/v1/services/hold-api", nil).Body.String()
	if !strings.Contains(svc, `"rollbackOnFailure":false`) {
		t.Fatalf("explicit false was overridden for a canary service: %s", svc)
	}

	// Audit S-5: the health gate must not record a rollback request for it.
	gate := authorizedRequest(server, http.MethodPost, "/api/v1/services/hold-api/health-gate", []byte(`{"p95LatencyMs":900,"errorRate":5,"successCount":500}`))
	if gate.Code != http.StatusOK || !strings.Contains(gate.Body.String(), `"action":"hold"`) || !strings.Contains(gate.Body.String(), `"inputSource":"caller-supplied"`) {
		t.Fatalf("health gate: %d %s", gate.Code, gate.Body.String())
	}
	deps := authorizedRequest(server, http.MethodGet, "/api/v1/services/hold-api/deployments", nil).Body.String()
	if strings.Contains(deps, "rollback-requested") {
		t.Fatalf("rollback recorded although rollbackOnFailure is false: %s", deps)
	}
}

func TestCanaryDefaultsToRollbackOnFailure(t *testing.T) {
	server := testServer(t, WithAPIToken("secret"))
	onboard(t, server)
	svc := authorizedRequest(server, http.MethodGet, "/api/v1/services/payments-api", nil).Body.String()
	if !strings.Contains(svc, `"rollbackOnFailure":true`) {
		t.Fatalf("canary service without the field should default to true: %s", svc)
	}
}
