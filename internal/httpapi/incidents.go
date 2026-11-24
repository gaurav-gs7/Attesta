package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/internal/auth"
	"github.com/gaurav-gs7/attesta/internal/catalog"
	"github.com/gaurav-gs7/attesta/internal/incidents"
	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/page"
	"github.com/gaurav-gs7/attesta/internal/workflows"
)

const defaultSeverity = "unspecified"

// incidentInput is what both ingestion paths know about a new incident.
type incidentInput struct {
	severity      string
	alertName     string
	title         string
	source        string
	fingerprint   string
	alertStartsAt *time.Time
	reported      *models.AlertSignals
}

// createIncident handles POST /api/v1/incidents, a plain JSON alert.
func (s *Server) createIncident(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var req incidents.AlertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Service == "" {
		writeError(w, http.StatusBadRequest, "service is required")
		return
	}
	service, err := s.store.GetServiceByName(r.Context(), strings.ToLower(strings.TrimSpace(req.Service)))
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "service not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	in := incidentInput{
		severity:  strings.ToLower(strings.TrimSpace(req.Severity)),
		alertName: strings.TrimSpace(req.AlertName),
		title:     strings.TrimSpace(req.Title),
		source:    incidents.SourceAPI,
	}
	if req.ErrorRate != nil || req.Latency != nil {
		in.reported = &models.AlertSignals{ErrorRatePct: req.ErrorRate, LatencyP95Ms: req.Latency}
	}
	record, run := s.openIncident(r.Context(), service, in)
	if run.State != workflows.StateSucceeded {
		writeRunFailure(w, r, run)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"incident": record, "workflow": run})
}

// openIncident evaluates the service's SLOs, loads recent deployment
// records, builds the timeline from them and persists the incident.
func (s *Server) openIncident(ctx context.Context, service models.Service, in incidentInput) (models.IncidentRecord, models.WorkflowRun) {
	if in.severity == "" {
		in.severity = defaultSeverity
	}
	if in.alertName == "" {
		in.alertName = "UnnamedAlert"
	}
	if in.title == "" {
		in.title = in.alertName
	}
	var sloStatus models.SLOStatus
	var deployments []models.Deployment
	var record models.IncidentRecord
	incident := models.Incident{
		ID:            "inc-" + catalog.NewID(),
		ServiceID:     service.ID,
		Service:       service.Name,
		Severity:      in.severity,
		Title:         in.title,
		Status:        "active",
		AlertName:     in.alertName,
		Source:        in.source,
		Fingerprint:   in.fingerprint,
		AlertStartsAt: in.alertStartsAt,
		AlertReported: in.reported,
		OpenedBy:      auth.Actor(ctx),
		CreatedAt:     time.Now().UTC(),
	}
	run := s.workflows.Run(ctx, workflows.Spec{
		Name:    "incident-open",
		Kind:    "attesta.incident.open",
		Service: service.Name,
		Steps: []workflows.Step{
			{
				Name: "evaluate-slo-at-open",
				Run: func(ctx context.Context) error {
					sloStatus = s.evaluateSLO(ctx, service)
					return nil
				},
			},
			{
				Name: "load-recent-deployment-records",
				Run: func(ctx context.Context) error {
					// Every record in the correlation window, however many.
					anchor := incident.CreatedAt
					if incident.AlertStartsAt != nil && !incident.AlertStartsAt.IsZero() {
						anchor = *incident.AlertStartsAt
					}
					var err error
					deployments, err = s.allDeployments(ctx, service.ID, anchor.Add(-incidents.CorrelationWindow))
					return err
				},
			},
			{
				Name: "build-timeline-from-records",
				Run: func(context.Context) error {
					record = models.IncidentRecord{
						Incident: incident,
						Service:  service,
						Signals:  sloStatus,
						Events:   incidents.Timeline(incident, deployments, sloStatus),
					}
					return nil
				},
			},
			{
				Name: "persist-incident-record",
				Run: func(ctx context.Context) error {
					stored, err := s.store.CreateIncident(ctx, record)
					if errors.Is(err, catalog.ErrConflict) {
						return workflows.WithCode(err, workflows.CodeConflict)
					}
					record = stored
					return err
				},
			},
		},
	})
	return record, run
}

// resolveIncident handles POST /api/v1/incidents/{id}/resolve with an
// optional {"note": "..."} body. Resolving a resolved incident is a no-op.
func (s *Server) resolveIncident(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var req struct {
		Note string `json:"note"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
	}
	record, err := s.store.GetIncidentRecord(r.Context(), r.PathValue("id"))
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "incident not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if record.Incident.Status == "resolved" {
		writeJSON(w, http.StatusOK, map[string]any{"incident": record, "alreadyResolved": true})
		return
	}
	note := "Resolved via the Attesta API."
	if n := strings.TrimSpace(req.Note); n != "" {
		note += " Note: " + n
	}
	record, run, already := s.resolve(r.Context(), record, time.Now().UTC(), note)
	if run.State != workflows.StateSucceeded {
		writeRunFailure(w, r, run)
		return
	}
	if already {
		writeJSON(w, http.StatusOK, map[string]any{"incident": record, "alreadyResolved": true, "workflow": run})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"incident": record, "workflow": run})
}

// resolve marks the incident resolved if it is still at the version read.
// If another request changed it in between and it is now resolved, the
// stored record is returned with already set; any other change is a
// conflict.
func (s *Server) resolve(ctx context.Context, record models.IncidentRecord, at time.Time, note string) (models.IncidentRecord, models.WorkflowRun, bool) {
	already := false
	run := s.workflows.Run(ctx, workflows.Spec{
		Name:    "incident-resolve",
		Kind:    "attesta.incident.resolve",
		Service: record.Incident.Service,
		Steps: []workflows.Step{{
			Name: "mark-incident-resolved",
			Run: func(ctx context.Context) error {
				next := record
				next.Events = append(append([]models.IncidentEvent(nil), record.Events...), models.IncidentEvent{
					Timestamp:   at,
					EventType:   incidents.EventResolved,
					Description: note,
				})
				next.Incident.Status = "resolved"
				next.Incident.ResolvedAt = &at
				next.Incident.ResolutionNote = note
				next.Incident.ResolvedBy = auth.Actor(ctx)
				stored, err := s.store.UpdateIncident(ctx, next)
				if errors.Is(err, catalog.ErrConflict) {
					current, getErr := s.store.GetIncidentRecord(ctx, record.Incident.ID)
					if getErr == nil && current.Incident.Status == "resolved" {
						record, already = current, true
						return nil
					}
					return workflows.WithCode(fmt.Errorf("%w; reload the incident and retry", err), workflows.CodeConflict)
				}
				if err != nil {
					return err
				}
				record = stored
				return nil
			},
		}},
	})
	return record, run, already
}

type alertResult struct {
	Fingerprint string `json:"fingerprint"`
	Service     string `json:"service,omitempty"`
	Action      string `json:"action"` // opened, duplicate, resolved, skipped, failed
	IncidentID  string `json:"incidentId,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

// alertmanagerWebhook handles POST /api/v1/alerts/alertmanager, Alertmanager's
// webhook payload (version 4). A firing alert with a "service" label opens an
// incident unless one is already active for its fingerprint; a resolved alert
// resolves that incident at the alert's endsAt.
func (s *Server) alertmanagerWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var payload incidents.AlertmanagerWebhook
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid Alertmanager webhook payload")
		return
	}
	if payload.Version != "" && payload.Version != "4" {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unsupported Alertmanager webhook version %q; expected 4", payload.Version))
		return
	}
	results := make([]alertResult, 0, len(payload.Alerts))
	failed := false
	for _, alert := range payload.Alerts {
		result := s.handleAlert(r.Context(), alert)
		failed = failed || result.Action == "failed"
		results = append(results, result)
	}
	status := http.StatusOK
	if failed {
		// Alertmanager retries non-2xx responses; duplicates are absorbed by
		// the fingerprint check.
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, map[string]any{"results": results})
}

func (s *Server) handleAlert(ctx context.Context, alert incidents.AlertmanagerAlert) alertResult {
	result := alertResult{Fingerprint: incidents.AlertFingerprint(alert)}
	name := strings.ToLower(strings.TrimSpace(alert.Labels["service"]))
	if name == "" {
		result.Action, result.Reason = "skipped", `alert has no "service" label`
		return result
	}
	result.Service = name
	service, err := s.store.GetServiceByName(ctx, name)
	if errors.Is(err, catalog.ErrNotFound) {
		result.Action, result.Reason = "skipped", "service is not registered in Attesta"
		return result
	}
	if err != nil {
		result.Action, result.Reason = "failed", s.failedAlert(ctx, result.Fingerprint, err)
		return result
	}
	active, err := s.findActiveIncident(ctx, service.Name, result.Fingerprint)
	if err != nil {
		result.Action, result.Reason = "failed", s.failedAlert(ctx, result.Fingerprint, err)
		return result
	}

	switch alert.Status {
	case "firing":
		if active != nil {
			result.Action, result.IncidentID = "duplicate", active.Incident.ID
			return result
		}
		in := incidentInput{
			severity:    strings.ToLower(alert.Labels["severity"]),
			alertName:   alert.Labels["alertname"],
			title:       alert.Annotations["summary"],
			source:      incidents.SourceAlertmanager,
			fingerprint: result.Fingerprint,
		}
		if !alert.StartsAt.IsZero() {
			startsAt := alert.StartsAt.UTC()
			in.alertStartsAt = &startsAt
		}
		record, run := s.openIncident(ctx, service, in)
		if run.ErrorCode == workflows.CodeConflict {
			// A concurrent notification for the same alert opened it first.
			if active, err := s.findActiveIncident(ctx, service.Name, result.Fingerprint); err == nil && active != nil {
				result.Action, result.IncidentID = "duplicate", active.Incident.ID
				return result
			}
		}
		if run.State != workflows.StateSucceeded {
			result.Action, result.Reason = "failed", run.Error
			return result
		}
		result.Action, result.IncidentID = "opened", record.Incident.ID
	case "resolved":
		if active == nil {
			result.Action, result.Reason = "skipped", "no active incident for this alert fingerprint"
			return result
		}
		at := time.Now().UTC()
		if !alert.EndsAt.IsZero() {
			at = alert.EndsAt.UTC()
		}
		_, run, _ := s.resolve(ctx, *active, at, fmt.Sprintf("Alertmanager reported %s as resolved.", active.Incident.AlertName))
		if run.State != workflows.StateSucceeded {
			result.Action, result.Reason = "failed", run.Error
			return result
		}
		result.Action, result.IncidentID = "resolved", active.Incident.ID
	default:
		result.Action, result.Reason = "skipped", fmt.Sprintf("unknown alert status %q", alert.Status)
	}
	return result
}

// findActiveIncident returns the active incident for this fingerprint, or
// nil. It used to scan only the 100 most recent records.
func (s *Server) findActiveIncident(ctx context.Context, service, fingerprint string) (*models.IncidentRecord, error) {
	record, err := s.store.FindActiveIncident(ctx, service, fingerprint)
	if errors.Is(err, catalog.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// allDeployments returns every deployment record of a service since a time,
// following the store's pages.
func (s *Server) allDeployments(ctx context.Context, serviceID string, since time.Time) ([]models.Deployment, error) {
	var all []models.Deployment
	req := page.Request{Limit: page.MaxLimit, Since: since}
	for {
		batch, next, err := s.store.ListDeployments(ctx, serviceID, req)
		if err != nil {
			return nil, err
		}
		all = append(all, batch...)
		if next == "" {
			return all, nil
		}
		if req, err = page.Parse("", next); err != nil {
			return nil, err
		}
		req.Limit, req.Since = page.MaxLimit, since
	}
}

// failedAlert logs why an alert could not be processed and returns the
// reason reported back to Alertmanager, which carries no store detail.
func (s *Server) failedAlert(ctx context.Context, fingerprint string, err error) string {
	s.logger.ErrorContext(ctx, "alert processing failed", "fingerprint", fingerprint, "error", err)
	return "internal error; details are in the server log under request " + RequestID(ctx)
}
