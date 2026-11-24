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
	rollout := func(version string, ago time.Duration) models.Deployment {
		return models.Deployment{Version: version, Status: "started", Strategy: "canary", StartedAt: t0.Add(-ago), RolloutID: "r-" + version}
	}
	scaffold := models.Deployment{Version: "v0.1.0", Status: "generated", Strategy: "canary", StartedAt: t0.Add(-30 * time.Minute)}
	cases := []struct {
		name        string
		incident    models.Incident
		deployments []models.Deployment
		types       []string
		correlated  string
	}{
		{
			name:     "no deployments",
			incident: models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			types:    []string{EventOpened, EventSLOEvaluation},
		},
		{
			name:        "the newest rollout in the window is correlated",
			incident:    models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			deployments: []models.Deployment{rollout("v2", 90*time.Minute), rollout("v3", 10*time.Minute), scaffold},
			types:       []string{EventDeployment, EventDeployment, EventDeployment, EventOpened, EventSLOEvaluation, EventCorrelation},
			correlated:  "rollout v3 was recorded 10m0s before the alert",
		},
		{
			name:        "outside the window, or after the alert, is left out",
			incident:    models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			deployments: []models.Deployment{rollout("old", 3*time.Hour), rollout("later", -time.Minute)},
			types:       []string{EventOpened, EventSLOEvaluation},
		},
		{
			name:        "a deployment record without a rollout is not correlated",
			incident:    models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAPI, CreatedAt: t0},
			deployments: []models.Deployment{scaffold},
			types:       []string{EventDeployment, EventOpened, EventSLOEvaluation},
		},
		{
			name: "the window is anchored on when the alert started firing",
			incident: models.Incident{ID: "i", AlertName: "HighErrorRate", Source: SourceAlertmanager,
				CreatedAt: t0, AlertStartsAt: func() *time.Time { at := t0.Add(-3 * time.Hour); return &at }()},
			deployments: []models.Deployment{rollout("v2", 4*time.Hour), rollout("v3", 10*time.Minute)},
			types:       []string{EventDeployment, EventAlertFiring, EventOpened, EventSLOEvaluation, EventCorrelation},
			correlated:  "rollout v2 was recorded 1h0m0s before the alert",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := Timeline(tc.incident, tc.deployments, measured)
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
	deployments := []models.Deployment{{Version: "v3", Status: "started", Strategy: "canary", StartedAt: t0.Add(-10 * time.Minute), RolloutID: "r3"}}
	got := Postmortem(models.IncidentRecord{Incident: incident, Signals: signals, Events: Timeline(incident, deployments, signals)})

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
