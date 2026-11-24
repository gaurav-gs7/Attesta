package incidents

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/internal/models"
)

var update = flag.Bool("update", false, "rewrite the golden files")

var t0 = time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)

func f(v float64) *float64 { return &v }

func TestAlertFingerprint(t *testing.T) {
	if got := AlertFingerprint(AlertmanagerAlert{Fingerprint: "abc"}); got != "abc" {
		t.Fatalf("Alertmanager's fingerprint ignored: %s", got)
	}
	a := AlertFingerprint(AlertmanagerAlert{Labels: map[string]string{"alertname": "X", "service": "a"}})
	b := AlertFingerprint(AlertmanagerAlert{Labels: map[string]string{"service": "a", "alertname": "X"}})
	c := AlertFingerprint(AlertmanagerAlert{Labels: map[string]string{"alertname": "X", "service": "b"}})
	if a != b || a == c || !strings.HasPrefix(a, "labels-") {
		t.Fatalf("label fingerprints: %s %s %s", a, b, c)
	}
}

func TestTimeline(t *testing.T) {
	measured := models.SLOStatus{DataSource: "prometheus", SignalWindow: "5m", DeploymentGate: models.GateBlocked,
		ErrorRateCurrentPct: f(8.2), ErrorRateThresholdPct: 1, LatencyP95CurrentMs: f(850), LatencyP95TargetMs: 300, EvaluatedAt: t0}
	decision := func(action, revision string, ago time.Duration) Decision {
		return Decision{At: t0.Add(-ago), Namespace: "payments", Guard: "payments-api-rollout", Deployment: "payments-api",
			Record: "payments-api-rollout-0f3c9a2e-00000" + revision, Hash: "ab12cd34ef56aaaa", Action: action, Trigger: "AnalysisPassed", Revision: revision}
	}
	rolledBack := decision("RolledBack", "4", 5*time.Minute)
	rolledBack.Trigger, rolledBack.RestoredTo = "AnalysisFailed", "3"
	cases := []struct {
		name       string
		incident   models.Incident
		decisions  Decisions
		types      []string
		correlated string
	}{
		{
			name:     "no decisions",
			incident: models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			types:    []string{EventOpened, EventSLOEvaluation},
		},
		{
			name:       "the newest promotion in the window is correlated",
			incident:   models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			decisions:  Decisions{Records: []Decision{decision("Promoted", "3", 90*time.Minute), decision("Promoted", "4", 10*time.Minute), rolledBack}},
			types:      []string{EventDecision, EventDecision, EventDecision, EventOpened, EventSLOEvaluation, EventCorrelation},
			correlated: "revision 4 of payments/payments-api was promoted 10m0s before the alert (decision payments-api-rollout-0f3c9a2e-000004, hash ab12cd34ef56)",
		},
		{
			name:      "outside the window, or after the alert, is left out",
			incident:  models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			decisions: Decisions{Records: []Decision{decision("Promoted", "1", 3*time.Hour), decision("Promoted", "9", -time.Minute)}},
			types:     []string{EventOpened, EventSLOEvaluation},
		},
		{
			name:      "a rollback is listed but is not a cause",
			incident:  models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			decisions: Decisions{Records: []Decision{rolledBack}},
			types:     []string{EventDecision, EventOpened, EventSLOEvaluation},
		},
		{
			name:      "unreadable decisions are stated, not implied absent",
			incident:  models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			decisions: Decisions{Unavailable: "no cluster access is configured"},
			types:     []string{EventNoDecisions, EventOpened, EventSLOEvaluation},
		},
		{
			name: "the window is anchored on when the alert started firing",
			incident: models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAlertmanager,
				CreatedAt: t0, AlertStartsAt: func() *time.Time { at := t0.Add(-3 * time.Hour); return &at }()},
			decisions:  Decisions{Records: []Decision{decision("Promoted", "2", 4*time.Hour), decision("Promoted", "3", 10*time.Minute)}},
			types:      []string{EventDecision, EventAlertFiring, EventOpened, EventSLOEvaluation, EventCorrelation},
			correlated: "revision 2 of payments/payments-api was promoted 1h0m0s before the alert",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := Timeline(tc.incident, tc.decisions, measured)
			var types []string
			for i, e := range events {
				types = append(types, e.EventType)
				if i > 0 && e.Timestamp.Before(events[i-1].Timestamp) {
					t.Errorf("events out of order at %d", i)
				}
			}
			if strings.Join(types, ",") != strings.Join(tc.types, ",") {
				t.Fatalf("event types %v, want %v", types, tc.types)
			}
			if c := findEvent(events, EventCorrelation); tc.correlated != "" {
				if c == nil || !strings.Contains(c.Description, tc.correlated) || !strings.Contains(c.Description, "not a confirmed cause") {
					t.Fatalf("correlation: %+v", c)
				}
			}
		})
	}
}

func TestDescribeSLO(t *testing.T) {
	if got := DescribeSLO(models.SLOStatus{DeploymentGate: models.GateUnknown, Reason: "no Prometheus"}); got != "SLO state unknown: no Prometheus" {
		t.Fatalf("unknown: %q", got)
	}
	got := DescribeSLO(models.SLOStatus{DataSource: "prometheus", SignalWindow: "5m", DeploymentGate: models.GateAllowed,
		ErrorRateThresholdPct: 1, LatencyP95TargetMs: 300, LatencyP95CurrentMs: f(120)})
	if !strings.Contains(got, "error rate not measured") || !strings.Contains(got, "p95 latency 120ms (target 300ms)") {
		t.Fatalf("measured: %q", got)
	}
}

// The postmortem draft is pinned by a golden file. Run go test
// ./internal/incidents -update after a deliberate change to its wording.
func TestPostmortemDraft(t *testing.T) {
	resolvedAt := t0.Add(45 * time.Minute)
	incident := models.Incident{ID: "inc-1", Service: "payments-api", Title: "5xx above 1%", AlertName: "HighErrorRate",
		Severity: "page", Source: SourceAlertmanager, Status: "resolved", CreatedAt: t0, ResolvedAt: &resolvedAt,
		ResolutionNote: "Rolled back to v2.", AlertReported: &models.AlertSignals{ErrorRatePct: f(8.2)}}
	signals := models.SLOStatus{DataSource: "prometheus", SignalWindow: "5m", DeploymentGate: models.GateBlocked,
		ErrorRateCurrentPct: f(8.2), ErrorRateThresholdPct: 1, LatencyP95CurrentMs: f(850), LatencyP95TargetMs: 300, EvaluatedAt: t0}
	decisions := Decisions{Records: []Decision{{At: t0.Add(-10 * time.Minute), Namespace: "payments", Guard: "payments-api-rollout",
		Deployment: "payments-api", Record: "payments-api-rollout-0f3c9a2e-000004", Hash: "ab12cd34ef56aaaa", Action: "Promoted",
		Trigger: "AnalysisPassed", Revision: "4", Message: "revision 4 (template 6b8f7c9d4) is the healthy revision a rollback restores"}}}
	got := Postmortem(models.IncidentRecord{Incident: incident, Signals: signals, Events: Timeline(incident, decisions, signals)})

	path := filepath.Join("testdata", "postmortem.golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if got != string(want) {
		t.Fatalf("postmortem differs from the golden file\n--- got\n%s\n--- want\n%s", got, want)
	}
}
