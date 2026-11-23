package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/incidents"
	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/page"
	promclient "github.com/gaurav-gs7/attesta/api/internal/prometheus"
	"github.com/gaurav-gs7/attesta/api/internal/readiness"
	"github.com/gaurav-gs7/attesta/api/internal/reliability"
	"github.com/gaurav-gs7/attesta/api/internal/rollouts"
	"github.com/gaurav-gs7/attesta/api/internal/security"
	sloengine "github.com/gaurav-gs7/attesta/api/internal/slo"
	"github.com/gaurav-gs7/attesta/api/internal/templates"
	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

type Server struct {
	store         catalog.Store
	generator     *templates.Generator
	apiToken      string
	maxBodyBytes  int64
	prometheusURL string
	startedAt     time.Time
	metrics       *serverMetrics
	workflows     *workflows.Engine
	workflowOpts  []workflows.Option
	logger        *slog.Logger
}

// WithLogger sets the logger for access logs, recovered panics and workflow
// runs. Records written with a request's context carry its request_id.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		s.logger = l
	}
}

type Option func(*Server)

func WithAPIToken(token string) Option {
	return func(s *Server) {
		s.apiToken = token
	}
}

func WithMaxBodyBytes(bytes int64) Option {
	return func(s *Server) {
		if bytes > 0 {
			s.maxBodyBytes = bytes
		}
	}
}

func WithPrometheusURL(url string) Option {
	return func(s *Server) {
		s.prometheusURL = strings.TrimRight(strings.TrimSpace(url), "/")
	}
}

// WithWorkflowOptions configures the workflow engine, for example how long an
// identical request waits for an in-flight onboarding run.
func WithWorkflowOptions(opts ...workflows.Option) Option {
	return func(s *Server) {
		s.workflowOpts = append(s.workflowOpts, opts...)
	}
}

func NewServer(store catalog.Store, generator *templates.Generator, opts ...Option) *Server {
	server := &Server{
		store:        store,
		generator:    generator,
		maxBodyBytes: 1 << 20,
		startedAt:    time.Now().UTC(),
		metrics:      newServerMetrics(),
	}
	for _, opt := range opts {
		opt(server)
	}
	if server.logger == nil {
		server.logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	server.logger = slog.New(requestIDHandler{server.logger.Handler()})
	server.workflows = workflows.NewEngine(store, append([]workflows.Option{workflows.WithLogger(server.logger)}, server.workflowOpts...)...)
	return server
}

// StartWorkflowReaper periodically marks failed the workflow runs left
// "running" by a crashed process, until ctx is done.
func (s *Server) StartWorkflowReaper(ctx context.Context) {
	s.workflows.StartReaper(ctx)
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", s.live)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.prometheusMetrics)
	mux.HandleFunc("POST /api/v1/services", s.createService)
	mux.HandleFunc("GET /api/v1/services", s.listServices)
	mux.HandleFunc("GET /api/v1/services/{name}", s.getService)
	mux.HandleFunc("GET /api/v1/services/{name}/status", s.serviceStatus)
	mux.HandleFunc("POST /api/v1/services/{name}/check", s.readiness)
	mux.HandleFunc("GET /api/v1/services/{name}/readiness", s.readiness)
	mux.HandleFunc("GET /api/v1/services/{name}/score", s.score)
	mux.HandleFunc("GET /api/v1/services/{name}/slos", s.slos)
	mux.HandleFunc("GET /api/v1/services/{name}/error-budget", s.slos)
	mux.HandleFunc("GET /api/v1/services/{name}/deployments", s.deployments)
	mux.HandleFunc("POST /api/v1/services/{name}/rollouts", s.startRollout)
	mux.HandleFunc("GET /api/v1/services/{name}/rollouts/{id}", s.getRollout)
	mux.HandleFunc("POST /api/v1/services/{name}/rollouts/{id}/pause", s.rolloutAction(rollouts.ActionPause))
	mux.HandleFunc("POST /api/v1/services/{name}/rollouts/{id}/resume", s.rolloutAction(rollouts.ActionResume))
	mux.HandleFunc("POST /api/v1/services/{name}/rollouts/{id}/complete", s.rolloutAction(rollouts.ActionComplete))
	mux.HandleFunc("POST /api/v1/services/{name}/rollouts/{id}/rollback", s.rolloutAction(rollouts.ActionRollback))
	mux.HandleFunc("POST /api/v1/services/{name}/health-gate", s.healthGate)
	mux.HandleFunc("POST /api/v1/services/{name}/rollback", s.serviceRollback)
	mux.HandleFunc("GET /api/v1/services/{name}/runbook", s.runbook)
	mux.HandleFunc("POST /api/v1/incidents", s.createIncident)
	mux.HandleFunc("GET /api/v1/incidents", s.listIncidents)
	mux.HandleFunc("GET /api/v1/incidents/{id}", s.getIncident)
	mux.HandleFunc("POST /api/v1/incidents/{id}/resolve", s.resolveIncident)
	mux.HandleFunc("POST /api/v1/alerts/alertmanager", s.alertmanagerWebhook)
	mux.HandleFunc("GET /api/v1/incidents/{id}/timeline", s.incidentTimeline)
	mux.HandleFunc("GET /api/v1/incidents/{id}/postmortem", s.incidentPostmortem)
	mux.HandleFunc("GET /api/v1/workflows", s.listWorkflows)
	mux.HandleFunc("GET /api/v1/workflows/{id}", s.getWorkflow)
	// The recoverer sits outside auth and inside logging and metrics, so a
	// panic anywhere in a handler is logged and counted as the 500 it became.
	return withRequestID(s.logging(s.metricsMiddleware(s.recoverer(s.auth(mux)))))
}

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// readyTimeout bounds the store check in /readyz, so a hung database makes
// the probe fail rather than hang past the kubelet's timeout.
const readyTimeout = 2 * time.Second

// ready reports ready only when the store answers. Liveness (/livez) does not
// depend on the store, so a database outage takes the API out of the Service
// endpoints without restarting it.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	body := map[string]any{
		"authEnabled": s.apiToken != "",
		"uptimeSec":   int(time.Since(s.startedAt).Seconds()),
	}
	if err := s.store.Ping(ctx); err != nil {
		body["status"], body["store"] = "not ready", "unreachable: "+err.Error()
		writeJSON(w, http.StatusServiceUnavailable, body)
		return
	}
	body["status"], body["store"] = "ready", "ok"
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) createService(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var req models.CreateServiceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	service, err := requestToService(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if findings := security.ValidateService(service); len(findings) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": "policy validation failed", "findings": findings})
		return
	}

	var created models.Service
	var generated templates.Result
	idempotent := false
	run, outcome := s.workflows.RunIdempotent(r.Context(), workflows.Spec{
		Name:           "service-onboarding",
		Kind:           "attesta.service.onboarding",
		Service:        service.Name,
		IdempotencyKey: serviceOnboardingKey(service),
		Steps: []workflows.Step{
			{
				Name:        "persist-service-metadata",
				MaxAttempts: 1,
				Run: func(ctx context.Context) error {
					var err error
					created, err = s.store.CreateService(ctx, service)
					if err == nil {
						return nil
					}
					if !errors.Is(err, catalog.ErrConflict) {
						return err
					}
					existing, getErr := s.store.GetServiceByName(ctx, service.Name)
					if getErr != nil {
						return getErr
					}
					if !sameServiceSpec(existing, service) {
						return workflows.WithCode(fmt.Errorf("%w: service %q already exists with different metadata", catalog.ErrConflict, service.Name), workflows.CodeConflict)
					}
					created = existing
					idempotent = true
					return nil
				},
			},
			{
				Name:        "record-initial-deployment",
				MaxAttempts: 1,
				Run: func(ctx context.Context) error {
					if idempotent {
						// The service already existed, for example because an
						// earlier run failed after creating it. Record the
						// initial deployment only if that run did not.
						existing, _, err := s.store.ListDeployments(ctx, created.ID, page.Request{Limit: 1})
						if err != nil || len(existing) > 0 {
							return err
						}
					}
					_, err := s.store.RecordDeployment(ctx, models.Deployment{
						ServiceID:   created.ID,
						Version:     "v0.1.0",
						Environment: created.Environment,
						Status:      "generated",
						Strategy:    created.DeploymentStrategy,
						StartedAt:   time.Now().UTC(),
					})
					return err
				},
			},
			{
				Name:        "render-golden-path-scaffold",
				MaxAttempts: 2,
				Run: func(context.Context) error {
					var err error
					generated, err = s.generator.Generate(created)
					return err
				},
			},
		},
	})
	if outcome == workflows.InProgress {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":    "an identical onboarding request is still in progress; retry shortly",
			"workflow": run,
		})
		return
	}
	if run.State != workflows.StateSucceeded {
		writeRunFailure(w, run)
		return
	}
	if outcome == workflows.Replayed {
		created, err = s.store.GetServiceByName(r.Context(), service.Name)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		generated, err = s.generator.Generate(created)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		idempotent = true
	}

	status := http.StatusCreated
	if idempotent {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"service":    created,
		"generated":  generated,
		"workflow":   run,
		"idempotent": idempotent,
	})
}

func (s *Server) listServices(w http.ResponseWriter, r *http.Request) {
	services, err := s.store.ListServices(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": services})
}

func (s *Server) getService(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"service": service})
}

func (s *Server) serviceStatus(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	status := models.ServiceStatus{
		Service:           service.Name,
		Environment:       service.Environment,
		SLO:               s.evaluateSLO(r.Context(), service),
		RollbackOnFailure: service.RollbackOnFailure,
	}
	deployment, err := s.store.LatestDeployment(r.Context(), service.ID)
	switch {
	case err == nil:
		status.LatestDeployment = &deployment
	case !errors.Is(err, catalog.ErrNotFound):
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, status)
}

// readiness, score and slos are reads: they compute from the stored service
// and live metrics and record nothing. They used to create a workflow run per
// request, so 500 GETs wrote 500 runs.
func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"report": readiness.Evaluate(service)})
}

func (s *Server) score(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	report := readiness.Evaluate(service)
	writeJSON(w, http.StatusOK, map[string]any{
		"service": service.Name,
		"tier":    service.Tier,
		"score":   report.Score,
		"failed":  report.Failed,
	})
}

func (s *Server) slos(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"slo": s.evaluateSLO(r.Context(), service)})
}

// evaluateSLO measures the service's SLOs. Without Prometheus, or when a
// query fails, the result is an unknown gate with the reason; Attesta never
// substitutes estimated values.
func (s *Server) evaluateSLO(ctx context.Context, service models.Service) models.SLOStatus {
	if s.prometheusURL == "" {
		return sloengine.Unknown(service, models.SLOSourceNone,
			"no Prometheus configured (ATTESTA_PROMETHEUS_URL is unset); SLO state unknown, deployment gate fails closed")
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	signals, err := promclient.NewClient(s.prometheusURL).Signals(checkCtx, service)
	if err != nil {
		return sloengine.Unknown(service, models.SLOSourcePrometheus,
			fmt.Sprintf("Prometheus query failed: %v; SLO state unknown, deployment gate fails closed", err))
	}
	return sloengine.Evaluate(service, signals)
}

func (s *Server) deployments(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	req, ok := pageRequest(w, r)
	if !ok {
		return
	}
	deployments, next, err := s.store.ListDeployments(r.Context(), service.ID, req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, pageBody(map[string]any{
		"service":     service.Name,
		"deployments": nonNil(deployments),
	}, next))
}

func (s *Server) startRollout(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	var req rollouts.StartRequest
	if !s.decodeStrict(w, r, &req) {
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var gate models.SLOStatus
	var rollout models.Rollout
	run := s.workflows.Run(r.Context(), workflows.Spec{
		Name:    "rollout-start",
		Kind:    "attesta.rollout.start",
		Service: service.Name,
		Steps: []workflows.Step{
			{
				Name: "evaluate-slo-deployment-gate",
				Run: func(ctx context.Context) error {
					gate = s.evaluateSLO(ctx, service)
					if gate.DeploymentGate != models.GateAllowed {
						return workflows.WithCode(fmt.Errorf("deployment gate is %s: %s", gate.DeploymentGate, gate.Reason), workflows.CodeGateClosed)
					}
					return nil
				},
			},
			{
				Name: "record-rollout",
				Run: func(ctx context.Context) error {
					now := time.Now().UTC()
					var err error
					rollout, err = s.store.CreateRollout(ctx, models.Rollout{
						ServiceID: service.ID, Service: service.Name,
						Version: req.Version, Image: req.Image, GitSHA: req.GitSHA,
						State: rollouts.StateStarted, CreatedAt: now, UpdatedAt: now,
						Events: []models.RolloutEvent{{At: now, To: rollouts.StateStarted, Reason: "deployment gate allowed"}},
					}, models.Deployment{
						ServiceID: service.ID, Version: req.Version, Image: req.Image,
						Environment: service.Environment, Status: rollouts.DeploymentStatus(rollouts.StateStarted),
						Strategy: service.DeploymentStrategy, StartedAt: now,
					})
					if errors.Is(err, catalog.ErrConflict) {
						return workflows.WithCode(fmt.Errorf("%w; pause, complete or roll it back first", err), workflows.CodeConflict)
					}
					return err
				},
			},
		},
	})
	if run.State != workflows.StateSucceeded {
		if run.ErrorCode == workflows.CodeGateClosed {
			writeJSON(w, http.StatusConflict, map[string]any{"error": run.Error, "errorCode": run.ErrorCode, "service": service.Name, "gate": gate, "workflow": run})
			return
		}
		writeRunFailure(w, run)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"service": service.Name, "rollout": rollout, "gate": gate, "workflow": run})
}

// writeRunFailure answers for a failed workflow run by its error code:
// conflicts and closed gates are 409, anything else 500.
func writeRunFailure(w http.ResponseWriter, run models.WorkflowRun) {
	status := http.StatusInternalServerError
	switch run.ErrorCode {
	case workflows.CodeConflict, workflows.CodeGateClosed:
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]any{"error": run.Error, "errorCode": run.ErrorCode, "workflow": run})
}

// decodeStrict reads a bounded JSON body that must contain only known
// fields. It answers 400 itself on failure.
func (s *Server) decodeStrict(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

var rolloutIDRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// loadRollout finds the rollout named by the {id} path value ("latest" when
// allowLatest). It answers 404 or 500 itself.
func (s *Server) loadRollout(w http.ResponseWriter, r *http.Request, service models.Service, allowLatest bool) (models.Rollout, bool) {
	id := r.PathValue("id")
	var rollout models.Rollout
	var err error
	switch {
	case allowLatest && id == "latest":
		rollout, err = s.store.LatestRollout(r.Context(), service.ID)
	case rolloutIDRE.MatchString(id):
		rollout, err = s.store.GetRollout(r.Context(), service.ID, id)
	default:
		err = catalog.ErrNotFound
	}
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "rollout not found")
		return models.Rollout{}, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return models.Rollout{}, false
	}
	return rollout, true
}

func (s *Server) getRollout(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	rollout, ok := s.loadRollout(w, r, service, true)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"service": service.Name, "rollout": rollout})
}

// rolloutAction handles POST .../rollouts/{id}/{pause,resume,complete,rollback}.
func (s *Server) rolloutAction(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		service, ok := s.loadService(w, r)
		if !ok {
			return
		}
		reason, ok := s.actionReason(w, r)
		if !ok {
			return
		}
		rollout, ok := s.loadRollout(w, r, service, false)
		if !ok {
			return
		}
		s.transition(w, r, service, rollout, action, reason)
	}
}

// serviceRollback handles POST /services/{name}/rollback: it requests a
// rollback of the service's most recent rollout.
func (s *Server) serviceRollback(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	reason, ok := s.actionReason(w, r)
	if !ok {
		return
	}
	rollout, err := s.store.LatestRollout(r.Context(), service.ID)
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusConflict, "service has no recorded rollout to roll back")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.transition(w, r, service, rollout, rollouts.ActionRollback, reason)
}

// actionReason reads an optional {"reason": "..."} body.
func (s *Server) actionReason(w http.ResponseWriter, r *http.Request) (string, bool) {
	var body struct {
		Reason string `json:"reason"`
	}
	if r.ContentLength != 0 {
		if !s.decodeStrict(w, r, &body) {
			return "", false
		}
	}
	if len(body.Reason) > 500 {
		writeError(w, http.StatusBadRequest, "reason must be at most 500 characters")
		return "", false
	}
	return strings.TrimSpace(body.Reason), true
}

func (s *Server) transition(w http.ResponseWriter, r *http.Request, service models.Service, rollout models.Rollout, action, reason string) {
	from := rollout.State
	if err := rollouts.Apply(&rollout, action, reason, time.Now().UTC()); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "rollout": rollout})
		return
	}
	if err := s.store.UpdateRollout(r.Context(), rollout, from); err != nil {
		if errors.Is(err, catalog.ErrConflict) {
			writeError(w, http.StatusConflict, err.Error()+"; reload it and retry")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	body := map[string]any{"service": service.Name, "rollout": rollout}
	if action == rollouts.ActionRollback {
		body["note"] = "Attesta recorded a rollback request. It did not change the cluster; your deployment tooling must act on it."
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) healthGate(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	var metrics reliability.Metrics
	if !s.decodeStrict(w, r, &metrics) {
		return
	}
	if err := metrics.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	policy := reliability.PolicyFor(service)
	var decision reliability.Decision
	status := http.StatusOK
	var rolledBack *models.Rollout
	run := s.workflows.Run(r.Context(), workflows.Spec{
		Name:    "rollout-health-gate",
		Kind:    "attesta.rollout.health-gate",
		Service: service.Name,
		Steps: []workflows.Step{
			{
				Name:        "evaluate-canary-sli-window",
				MaxAttempts: 1,
				Run: func(context.Context) error {
					decision = reliability.EvaluateCanary(policy, metrics)
					if decision.Action == reliability.ActionRollback && !service.RollbackOnFailure {
						decision.Action = reliability.ActionHold
						decision.Reason += "; rollbackOnFailure is false for this service, so no rollback was requested"
					}
					return nil
				},
			},
			{
				Name: "request-rollback-of-active-rollout",
				Run: func(ctx context.Context) error {
					if decision.Action != reliability.ActionRollback {
						return nil
					}
					active, err := s.store.LatestRollout(ctx, service.ID)
					if errors.Is(err, catalog.ErrNotFound) || (err == nil && !rollouts.Active(active.State)) {
						return nil // nothing in flight to roll back
					}
					if err != nil {
						return err
					}
					from := active.State
					if err := rollouts.Apply(&active, rollouts.ActionRollback, "health gate: "+decision.Reason, time.Now().UTC()); err != nil {
						return err
					}
					if err := s.store.UpdateRollout(ctx, active, from); err != nil {
						return err
					}
					rolledBack = &active
					return nil
				},
			},
		},
	})
	if run.State != workflows.StateSucceeded {
		writeRunFailure(w, run)
		return
	}
	if rolledBack != nil {
		status = http.StatusAccepted
	}
	writeJSON(w, status, map[string]any{
		"service": service.Name,
		// The gate judges the numbers in the request body. Attesta does not
		// measure the canary itself on this endpoint.
		"inputSource": "caller-supplied",
		"metrics":     metrics,
		// The thresholds applied, taken from the service record.
		"policy":   policy,
		"decision": decision,
		// The active rollout marked rollback-requested, if the gate breached
		// and one was in flight.
		"rollout":  rolledBack,
		"workflow": run,
	})
}

func (s *Server) listIncidents(w http.ResponseWriter, r *http.Request) {
	req, ok := pageRequest(w, r)
	if !ok {
		return
	}
	records, next, err := s.store.ListIncidentRecords(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, pageBody(map[string]any{"incidents": nonNil(records)}, next))
}

func (s *Server) getIncident(w http.ResponseWriter, r *http.Request) {
	record, ok := s.loadIncident(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) listWorkflows(w http.ResponseWriter, r *http.Request) {
	req, ok := pageRequest(w, r)
	if !ok {
		return
	}
	runs, next, err := s.workflows.List(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, pageBody(map[string]any{"workflows": runs}, next))
}

// pageRequest reads ?limit= and ?cursor=; it answers 400 itself if they are
// malformed.
func pageRequest(w http.ResponseWriter, r *http.Request) (page.Request, bool) {
	req, err := page.Parse(r.URL.Query().Get("limit"), r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return page.Request{}, false
	}
	return req, true
}

// pageBody adds nextCursor to a list response when there is another page.
func pageBody(body map[string]any, next string) map[string]any {
	if next != "" {
		body["nextCursor"] = next
	}
	return body
}

func nonNil[T any](items []T) []T {
	if items == nil {
		return []T{}
	}
	return items
}

func (s *Server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	run, ok := s.workflows.Get(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) incidentTimeline(w http.ResponseWriter, r *http.Request) {
	record, ok := s.loadIncident(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"incident": record.Incident.ID,
		"timeline": record.Events,
	})
}

func (s *Server) incidentPostmortem(w http.ResponseWriter, r *http.Request) {
	record, ok := s.loadIncident(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	_, _ = w.Write([]byte(incidents.Postmortem(record)))
}

func (s *Server) runbook(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	_, _ = fmt.Fprintf(w, `# Incident Runbook: %s

## Ownership

- Team: %s
- Owner: %s
- Tier: %s
- Pager: %s
- Environment: %s
- Namespace: %s
- Repository: %s

## SLO

- Availability target: %.2f%%
- p95 latency target: %dms
- Error rate threshold: %.2f%%
- Deployment strategy: %s
- Rollback on failure: %t

## First checks

1. Check current rollout status.
2. Compare p95 latency, availability, burn rate, and error rate against the SLO.
3. Inspect pod restarts, CPU, memory, PDB status, and HPA saturation.
4. Review the latest deployment, Git SHA, and image scan result.
5. Roll back if the health gate fails during canary.
6. Create an incident and attach the Attesta timeline/postmortem draft.

## Rollback

POST /api/v1/services/%s/rollback
`, service.Name, service.Team, service.Owner, service.Tier, service.Pager, service.Environment, service.Namespace,
		service.Repository, service.SLOTarget, service.SLOLatencyP95Ms, service.SLOErrorRate,
		service.DeploymentStrategy, service.RollbackOnFailure, service.Name)
}

func (s *Server) loadService(w http.ResponseWriter, r *http.Request) (models.Service, bool) {
	service, err := s.store.GetServiceByName(r.Context(), r.PathValue("name"))
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "service not found")
		return models.Service{}, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return models.Service{}, false
	}
	return service, true
}

func (s *Server) loadIncident(w http.ResponseWriter, r *http.Request) (models.IncidentRecord, bool) {
	record, err := s.store.GetIncidentRecord(r.Context(), r.PathValue("id"))
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "incident not found")
		return models.IncidentRecord{}, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return models.IncidentRecord{}, false
	}
	return record, true
}

func requestToService(req models.CreateServiceRequest) (models.Service, error) {
	slo := req.SLOTarget
	if slo == 0 {
		slo = req.SLOAvailability
	}
	if slo == 0 && req.SLO != "" {
		parsed, err := strconv.ParseFloat(req.SLO, 64)
		if err != nil {
			return models.Service{}, fmt.Errorf("slo must be a number")
		}
		slo = parsed
	}
	if slo == 0 {
		slo = 99.9
	}
	latencyP95 := req.SLOLatencyP95Ms
	if latencyP95 == 0 && req.SLOLatencyP95 != "" {
		parsed, err := parseMillis(req.SLOLatencyP95)
		if err != nil {
			return models.Service{}, err
		}
		latencyP95 = parsed
	}
	if latencyP95 == 0 {
		latencyP95 = 300
	}
	errorRate := req.SLOErrorRate
	if errorRate == 0 {
		errorRate = 1
	}
	namespace := req.Namespace
	if namespace == "" {
		namespace = req.Team
	}
	strategy := strings.ToLower(strings.TrimSpace(req.DeploymentStrategy))
	if strategy == "" {
		strategy = strings.ToLower(strings.TrimSpace(req.Deployment))
	}
	if strategy == "" {
		strategy = "canary"
	}
	tier := strings.ToLower(strings.TrimSpace(req.Tier))
	if tier == "" {
		tier = "medium"
	}
	// Ownership and link metadata is stored only as given. The readiness score
	// checks these fields, so filling them in here would make it meaningless.
	repository := strings.TrimSpace(req.Repository)
	if repository == "" {
		repository = strings.TrimSpace(req.RepoURL)
	}
	runbookURL := strings.TrimSpace(req.RunbookURL)
	dashboardURL := strings.TrimSpace(req.DashboardURL)
	pager := strings.TrimSpace(req.Pager)
	rollbackOnFailure := strategy == "canary" || strategy == "blue-green"
	if req.RollbackOnFailure != nil {
		rollbackOnFailure = *req.RollbackOnFailure
	}
	return models.Service{
		Name:               strings.ToLower(strings.TrimSpace(req.Name)),
		Team:               strings.ToLower(strings.TrimSpace(req.Team)),
		Owner:              strings.TrimSpace(req.Owner),
		Tier:               tier,
		Language:           strings.ToLower(strings.TrimSpace(req.Language)),
		RepoURL:            strings.TrimSpace(req.RepoURL),
		Repository:         repository,
		Pager:              pager,
		RunbookURL:         runbookURL,
		DashboardURL:       dashboardURL,
		Dependencies:       trimList(req.Dependencies),
		Namespace:          strings.ToLower(strings.TrimSpace(namespace)),
		Environment:        strings.ToLower(strings.TrimSpace(req.Environment)),
		SLOTarget:          slo,
		SLOLatencyP95Ms:    latencyP95,
		SLOErrorRate:       errorRate,
		DeploymentStrategy: strategy,
		RollbackOnFailure:  rollbackOnFailure,
	}, nil
}

func sameServiceSpec(a, b models.Service) bool {
	return a.Name == b.Name &&
		a.Team == b.Team &&
		a.Owner == b.Owner &&
		a.Tier == b.Tier &&
		a.Language == b.Language &&
		a.RepoURL == b.RepoURL &&
		a.Repository == b.Repository &&
		a.Pager == b.Pager &&
		a.RunbookURL == b.RunbookURL &&
		a.DashboardURL == b.DashboardURL &&
		strings.Join(a.Dependencies, ",") == strings.Join(b.Dependencies, ",") &&
		a.Namespace == b.Namespace &&
		a.Environment == b.Environment &&
		a.SLOTarget == b.SLOTarget &&
		a.SLOLatencyP95Ms == b.SLOLatencyP95Ms &&
		a.SLOErrorRate == b.SLOErrorRate &&
		a.DeploymentStrategy == b.DeploymentStrategy &&
		a.RollbackOnFailure == b.RollbackOnFailure
}

func serviceOnboardingKey(service models.Service) string {
	payload, _ := json.Marshal(struct {
		Name               string
		Team               string
		Owner              string
		Tier               string
		Language           string
		RepoURL            string
		Repository         string
		Pager              string
		RunbookURL         string
		DashboardURL       string
		Dependencies       []string
		Namespace          string
		Environment        string
		SLOTarget          float64
		SLOLatencyP95Ms    int
		SLOErrorRate       float64
		DeploymentStrategy string
		RollbackOnFailure  bool
	}{
		Name:               service.Name,
		Team:               service.Team,
		Owner:              service.Owner,
		Tier:               service.Tier,
		Language:           service.Language,
		RepoURL:            service.RepoURL,
		Repository:         service.Repository,
		Pager:              service.Pager,
		RunbookURL:         service.RunbookURL,
		DashboardURL:       service.DashboardURL,
		Dependencies:       service.Dependencies,
		Namespace:          service.Namespace,
		Environment:        service.Environment,
		SLOTarget:          service.SLOTarget,
		SLOLatencyP95Ms:    service.SLOLatencyP95Ms,
		SLOErrorRate:       service.SLOErrorRate,
		DeploymentStrategy: service.DeploymentStrategy,
		RollbackOnFailure:  service.RollbackOnFailure,
	})
	sum := sha256.Sum256(payload)
	return "service-onboarding:" + service.Name + ":" + hex.EncodeToString(sum[:8])
}

func parseMillis(value string) (int, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimSuffix(value, "ms")
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("sloLatencyP95 must be a positive millisecond value")
	}
	return parsed, nil
}

func trimList(values []string) []string {
	var trimmed []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			trimmed = append(trimmed, value)
		}
	}
	return trimmed
}

// writeJSON encodes before writing the status line, so a value that cannot
// be encoded becomes a 500 instead of a success status with an empty body.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error: response could not be encoded"}` + "\n"))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiToken == "" || !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		// Alertmanager's webhook http_config can only send an Authorization
		// header, so a Bearer token is accepted as well as X-Attesta-Token.
		presented := r.Header.Get("X-Attesta-Token")
		if presented == "" {
			if bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
				presented = bearer
			}
		}
		// Hash both sides so the comparison runs in constant time regardless of
		// the presented token's length.
		got := sha256.Sum256([]byte(presented))
		want := sha256.Sum256([]byte(s.apiToken))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or invalid Attesta API token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		s.logger.InfoContext(r.Context(), "request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote_addr", r.RemoteAddr,
		)
	})
}

// recoverer turns a handler panic into a 500 and logs the panic value and
// stack with the request ID, which the response also carries so a caller can
// quote it. http.ErrAbortHandler keeps its meaning: abort without logging.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			p := recover()
			if p == nil {
				return
			}
			if p == http.ErrAbortHandler {
				panic(p)
			}
			s.logger.ErrorContext(r.Context(), "handler panicked",
				"method", r.Method,
				"path", r.URL.Path,
				"panic", fmt.Sprint(p),
				"stack", string(debug.Stack()),
			)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error":     "internal server error",
				"requestId": RequestID(r.Context()),
			})
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		s.metrics.observe(r.Method, r.URL.Path, recorder.status)
	})
}

func (s *Server) prometheusMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(s.metrics.render(time.Since(s.startedAt))))
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

type serverMetrics struct {
	mu       sync.Mutex
	requests map[string]int64
}

func newServerMetrics() *serverMetrics {
	return &serverMetrics{requests: make(map[string]int64)}
}

func (m *serverMetrics) observe(method, path string, status int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s|%s|%d", method, routeLabel(path), status)
	m.requests[key]++
}

func (m *serverMetrics) render(uptime time.Duration) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	b.WriteString("# HELP attesta_uptime_seconds Attesta API uptime in seconds.\n")
	b.WriteString("# TYPE attesta_uptime_seconds gauge\n")
	fmt.Fprintf(&b, "attesta_uptime_seconds %d\n", int(uptime.Seconds()))
	b.WriteString("# HELP attesta_http_requests_total Total Attesta HTTP requests.\n")
	b.WriteString("# TYPE attesta_http_requests_total counter\n")
	for key, count := range m.requests {
		parts := strings.Split(key, "|")
		fmt.Fprintf(&b, "attesta_http_requests_total{method=%q,route=%q,status=%q} %d\n", parts[0], parts[1], parts[2], count)
	}
	return b.String()
}

func routeLabel(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 4 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "services" {
		if len(parts) == 4 {
			return "/api/v1/services/{name}"
		}
		return "/api/v1/services/{name}/" + strings.Join(parts[4:], "/")
	}
	return path
}
