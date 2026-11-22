// Package incidents builds incident timelines and postmortem drafts from
// records Attesta holds: deployment records, the SLO evaluation taken when
// the incident opened, and the alert itself. It does not invent events,
// offsets or causes; anything it cannot know is marked as unknown or TODO.
package incidents

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

// CorrelationWindow is how far before an alert deployment records are pulled
// into the timeline.
const CorrelationWindow = 2 * time.Hour

// Incident sources.
const (
	SourceAPI          = "api"
	SourceAlertmanager = "alertmanager"
)

// Event types written to the timeline.
const (
	EventDeployment    = "deployment"
	EventAlertFiring   = "alert-firing"
	EventOpened        = "incident-opened"
	EventSLOEvaluation = "slo-evaluation"
	EventCorrelation   = "correlation"
	EventResolved      = "resolved"
)

// AlertRequest is the JSON body of POST /api/v1/incidents. ErrorRate and
// Latency are optional values reported by the sender.
type AlertRequest struct {
	Service   string   `json:"service"`
	Severity  string   `json:"severity"`
	AlertName string   `json:"alertName"`
	Title     string   `json:"title"`
	ErrorRate *float64 `json:"errorRate"`
	Latency   *float64 `json:"latencyP95Ms"`
}

// AlertmanagerWebhook is Alertmanager's webhook payload (version 4).
type AlertmanagerWebhook struct {
	Version  string              `json:"version"`
	Status   string              `json:"status"`
	Receiver string              `json:"receiver"`
	Alerts   []AlertmanagerAlert `json:"alerts"`
}

type AlertmanagerAlert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    time.Time         `json:"startsAt"`
	EndsAt      time.Time         `json:"endsAt"`
	Fingerprint string            `json:"fingerprint"`
}

// AlertFingerprint returns Alertmanager's fingerprint, or a stable hash of
// the labels when the sender did not include one.
func AlertFingerprint(alert AlertmanagerAlert) string {
	if alert.Fingerprint != "" {
		return alert.Fingerprint
	}
	keys := make([]string, 0, len(alert.Labels))
	for k := range alert.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		_, _ = fmt.Fprintf(h, "%s=%s\n", k, alert.Labels[k])
	}
	return "labels-" + hex.EncodeToString(h.Sum(nil))[:16]
}

// Timeline orders the recorded evidence around an incident. Deployment
// records are included when they started within CorrelationWindow before the
// alert. A correlation event is added only for a recorded rollout start in
// that window, and it says plainly that it is a correlation.
func Timeline(incident models.Incident, deployments []models.Deployment, sloStatus models.SLOStatus) []models.IncidentEvent {
	anchor := incident.CreatedAt
	if incident.AlertStartsAt != nil && !incident.AlertStartsAt.IsZero() {
		anchor = *incident.AlertStartsAt
	}

	var events []models.IncidentEvent
	var lastRollout *models.Deployment
	for i := range deployments {
		d := deployments[i]
		if d.StartedAt.Before(anchor.Add(-CorrelationWindow)) || d.StartedAt.After(anchor) {
			continue
		}
		events = append(events, models.IncidentEvent{
			Timestamp:   d.StartedAt,
			EventType:   EventDeployment,
			Description: fmt.Sprintf("deployment record %s: status %s, strategy %s", d.Version, d.Status, d.Strategy),
		})
		if d.Status == "rollout-started" && (lastRollout == nil || d.StartedAt.After(lastRollout.StartedAt)) {
			lastRollout = &deployments[i]
		}
	}
	if incident.AlertStartsAt != nil && !incident.AlertStartsAt.IsZero() {
		events = append(events, models.IncidentEvent{
			Timestamp:   *incident.AlertStartsAt,
			EventType:   EventAlertFiring,
			Description: fmt.Sprintf("%s started firing (reported by %s)", incident.AlertName, incident.Source),
		})
	}
	opened := fmt.Sprintf("Attesta opened %s from %s alert %s, severity %s", incident.ID, incident.Source, incident.AlertName, incident.Severity)
	if reported := describeReported(incident.AlertReported); reported != "" {
		opened += "; " + reported
	}
	events = append(events, models.IncidentEvent{Timestamp: incident.CreatedAt, EventType: EventOpened, Description: opened})
	events = append(events, models.IncidentEvent{
		Timestamp:   sloStatus.EvaluatedAt,
		EventType:   EventSLOEvaluation,
		Description: DescribeSLO(sloStatus),
	})
	if lastRollout != nil {
		events = append(events, models.IncidentEvent{
			Timestamp: incident.CreatedAt,
			EventType: EventCorrelation,
			Description: fmt.Sprintf("rollout %s was recorded %s before the alert; it is a rollback candidate. This is a time correlation, not a confirmed cause, and Attesta performed no rollback.",
				lastRollout.Version, anchor.Sub(lastRollout.StartedAt).Round(time.Second)),
		})
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Timestamp.Before(events[j].Timestamp) })
	return events
}

// DescribeSLO renders an SLO evaluation in one sentence, naming its source
// and windows, or stating that it is unknown and why.
func DescribeSLO(status models.SLOStatus) string {
	if status.DeploymentGate == models.GateUnknown {
		return "SLO state unknown: " + status.Reason
	}
	parts := []string{
		fmt.Sprintf("error rate %s (threshold %.2f%%)", pct(status.ErrorRateCurrentPct), status.ErrorRateThresholdPct),
		fmt.Sprintf("p95 latency %s (target %dms)", ms(status.LatencyP95CurrentMs), status.LatencyP95TargetMs),
	}
	if status.BurnRate != nil {
		parts = append(parts, fmt.Sprintf("burn rate %.2fx over %s", *status.BurnRate, status.BurnRateWindow))
	}
	if status.ErrorBudgetRemainingPct != nil {
		parts = append(parts, fmt.Sprintf("%.1f%% of the %s error budget remaining", *status.ErrorBudgetRemainingPct, status.BudgetWindow))
	}
	return fmt.Sprintf("measured from %s over %s: %s; deployment gate %s", status.DataSource, status.SignalWindow, strings.Join(parts, ", "), status.DeploymentGate)
}

// Postmortem renders a postmortem draft. Sections Attesta cannot fill from
// its records are marked TODO.
func Postmortem(record models.IncidentRecord) string {
	inc := record.Incident
	var b strings.Builder
	fmt.Fprintf(&b, "# Postmortem draft: %s, %s\n\n", inc.Service, inc.Title)
	b.WriteString("Generated from Attesta records. Sections marked TODO need a person.\n\n")

	b.WriteString("## Summary\n\n")
	fmt.Fprintf(&b, "%s (severity %s) opened incident %s for %s at %s. Source: %s. Status: %s.\n\n",
		inc.AlertName, inc.Severity, inc.ID, inc.Service, inc.CreatedAt.Format(time.RFC3339), inc.Source, inc.Status)

	b.WriteString("## Impact\n\n")
	if record.Signals.DeploymentGate == models.GateUnknown {
		fmt.Fprintf(&b, "Not measured. %s\n", record.Signals.Reason)
	} else {
		fmt.Fprintf(&b, "At incident open, %s.\n", DescribeSLO(record.Signals))
	}
	if reported := describeReported(inc.AlertReported); reported != "" {
		fmt.Fprintf(&b, "\nThe alert sender reported: %s. Attesta did not verify these values.\n", strings.TrimPrefix(reported, "alert reported "))
	}
	b.WriteString("\n## Timeline\n\n")
	for _, event := range record.Events {
		fmt.Fprintf(&b, "- %s [%s] %s\n", event.Timestamp.Format(time.RFC3339), event.EventType, event.Description)
	}

	b.WriteString("\n## Contributing factors\n\n")
	if correlation := findEvent(record.Events, EventCorrelation); correlation != nil {
		fmt.Fprintf(&b, "%s\n\n", correlation.Description)
	} else {
		fmt.Fprintf(&b, "Undetermined. No rollout was recorded in the %s before the alert.\n\n", CorrelationWindow)
	}
	b.WriteString("TODO: confirm the cause.\n")

	b.WriteString("\n## Resolution\n\n")
	if inc.ResolvedAt != nil {
		fmt.Fprintf(&b, "Resolved at %s.", inc.ResolvedAt.Format(time.RFC3339))
		if inc.ResolutionNote != "" {
			fmt.Fprintf(&b, " %s", inc.ResolutionNote)
		}
		b.WriteString("\n")
	} else {
		b.WriteString("Open.\n")
	}

	b.WriteString("\n## Follow-ups\n\nTODO: list the action items agreed in review.\n")
	return b.String()
}

func describeReported(reported *models.AlertSignals) string {
	if reported == nil {
		return ""
	}
	var parts []string
	if reported.ErrorRatePct != nil {
		parts = append(parts, fmt.Sprintf("error rate %.2f%%", *reported.ErrorRatePct))
	}
	if reported.LatencyP95Ms != nil {
		parts = append(parts, fmt.Sprintf("p95 latency %.0fms", *reported.LatencyP95Ms))
	}
	if len(parts) == 0 {
		return ""
	}
	return "alert reported " + strings.Join(parts, ", ")
}

func findEvent(events []models.IncidentEvent, eventType string) *models.IncidentEvent {
	for i := range events {
		if events[i].EventType == eventType {
			return &events[i]
		}
	}
	return nil
}

func pct(v *float64) string {
	if v == nil {
		return "not measured"
	}
	return fmt.Sprintf("%.2f%%", *v)
}

func ms(v *float64) string {
	if v == nil {
		return "not measured"
	}
	return fmt.Sprintf("%.0fms", *v)
}
