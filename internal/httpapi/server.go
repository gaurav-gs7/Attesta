package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/auth"
	"github.com/gaurav-gs7/attesta/internal/catalog"
	"github.com/gaurav-gs7/attesta/internal/cluster"
	"github.com/gaurav-gs7/attesta/internal/incidents"
	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/page"
	promclient "github.com/gaurav-gs7/attesta/internal/prometheus"
	"github.com/gaurav-gs7/attesta/internal/readiness"
	scaffoldpkg "github.com/gaurav-gs7/attesta/internal/scaffold"
	"github.com/gaurav-gs7/attesta/internal/security"
	sloengine "github.com/gaurav-gs7/attesta/internal/slo"
	"github.com/gaurav-gs7/attesta/internal/templates"
	"github.com/gaurav-gs7/attesta/internal/workflows"
)

type Server struct {
	store         catalog.Store
	generator     *templates.Generator
	authenticator auth.Authenticator
	insecure      bool
	maxBodyBytes  int64
	promConfig    promclient.Config
	prom          *promclient.Client
	promErr       error
	startedAt     time.Time
	metrics       *serverMetrics
	workflows     *workflows.Engine
	cluster       cluster.Reader
	decisionKeys  attestav1.Keys
	sloThresholds *sloengine.Thresholds
	workflowOpts  []workflows.Option
	logger        *slog.Logger

	rateLimits       RateLimits
	ipLimiter        *limiter
	principalLimiter *limiter
	requestTimeout   time.Duration
}

// WithLogger sets the logger for access logs, recovered panics and workflow
// runs. Records written with a request's context carry its request_id.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		s.logger = l
	}
}

type Option func(*Server)

// WithAPIToken accepts a single shared token as an admin identity named
// "shared-api-token". Prefer WithAuthenticator with a tokens file or
// Kubernetes TokenReview, which give every caller its own identity.
func WithAPIToken(token string) Option {
	return func(s *Server) {
		if token != "" {
			s.authenticator = auth.NewStaticToken(token, auth.RoleAdmin)
		}
	}
}

// WithAuthenticator sets how callers are identified.
func WithAuthenticator(a auth.Authenticator) Option {
	return func(s *Server) {
		s.authenticator = a
	}
}

// WithInsecureNoAuth lets requests through without credentials, as an
// anonymous admin. For local development only.
func WithInsecureNoAuth() Option {
	return func(s *Server) {
		s.insecure = true
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
		s.promConfig.URL = strings.TrimRight(strings.TrimSpace(url), "/")
	}
}

// WithPrometheus configures the Prometheus client: URL, authentication, TLS,
// per-query timeout and metric names.
func WithPrometheus(cfg promclient.Config) Option {
	return func(s *Server) {
		cfg.URL = strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
		s.promConfig = cfg
	}
}

// WithSLOThresholds replaces the gate's default thresholds.
func WithSLOThresholds(t sloengine.Thresholds) Option {
	return func(s *Server) {
		s.sloThresholds = &t
	}
}

// WithCluster lets the API read RolloutGuards and the controller's
// RolloutDecisions. Without it, the guard endpoints answer 503 and incident
// timelines say the decisions were not read.
func WithCluster(r cluster.Reader) Option {
	return func(s *Server) {
		s.cluster = r
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
		metrics:      newServerMetrics(time.Now()),

		requestTimeout: DefaultRequestTimeout,
	}
	for _, opt := range opts {
		opt(server)
	}
	server.ipLimiter = newLimiter(server.rateLimits.PerIP)
	server.principalLimiter = newLimiter(server.rateLimits.PerPrincipal)
	if server.logger == nil {
		server.logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	server.logger = slog.New(requestIDHandler{server.logger.Handler()})
	if server.promConfig.URL != "" {
		// One client for the life of the server, so connections are reused.
		server.prom, server.promErr = promclient.New(server.promConfig)
	}
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
	mux.Handle("GET /metrics", s.metrics.handler())
	mux.HandleFunc("POST /api/v1/services", s.createService)
	mux.HandleFunc("GET /api/v1/services", s.listServices)
	mux.HandleFunc("GET /api/v1/services/{name}", s.getService)
	mux.HandleFunc("GET /api/v1/services/{name}/status", s.serviceStatus)
	mux.HandleFunc("POST /api/v1/services/{name}/check", s.readiness)
	mux.HandleFunc("GET /api/v1/services/{name}/readiness", s.readiness)
	mux.HandleFunc("GET /api/v1/services/{name}/score", s.score)
	mux.HandleFunc("GET /api/v1/services/{name}/slos", s.slos)
	mux.HandleFunc("GET /api/v1/services/{name}/error-budget", s.slos)
	mux.HandleFunc("GET /api/v1/services/{name}/guards", s.serviceGuards)
	mux.HandleFunc("GET /api/v1/guards", s.listGuards)
	mux.HandleFunc("GET /api/v1/guards/{namespace}/{name}", s.getGuard)
	mux.HandleFunc("POST /api/v1/decision-archive", s.archiveDecision)
	mux.HandleFunc("GET /api/v1/decision-archive", s.listArchive)
	mux.HandleFunc("GET /api/v1/decision-archive/{guardUID}", s.getArchive)
	mux.HandleFunc("GET /api/v1/services/{name}/runbook", s.runbook)
	mux.HandleFunc("GET /api/v1/services/{name}/scaffold", s.scaffold)
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
	return withRequestID(s.logging(s.metricsMiddleware(s.recoverer(s.withTimeout(s.limitIP(s.auth(s.limitPrincipal(recordPattern(mux)))))))))
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
		"authEnabled": s.authenticator != nil,
		"uptimeSec":   int(time.Since(s.startedAt).Seconds()),
	}
	if err := s.store.Ping(ctx); err != nil {
		s.logger.ErrorContext(r.Context(), "readiness check: store unreachable", "error", err)
		body["status"], body["store"] = "not ready", "unreachable"
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
	var generated templates.Scaffold
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
				Name:        "render-golden-path-scaffold",
				MaxAttempts: 2,
				Run: func(context.Context) error {
					// Rendered in memory, to check the service renders; the
					// files are served by GET .../scaffold, never written
					// to this server's disk.
					var err error
					generated, err = s.generator.Render(created)
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
		writeRunFailure(w, r, run)
		return
	}
	if outcome == workflows.Replayed {
		created, err = s.store.GetServiceByName(r.Context(), service.Name)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		generated, err = s.generator.Render(created)
		if err != nil {
			s.internalError(w, r, err)
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
		"generated":  scaffoldSummary(generated),
		"workflow":   run,
		"idempotent": idempotent,
	})
}

func (s *Server) listServices(w http.ResponseWriter, r *http.Request) {
	services, err := s.store.ListServices(r.Context())
	if err != nil {
		s.internalError(w, r, err)
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
	status := map[string]any{
		"service":           service.Name,
		"environment":       service.Environment,
		"slo":               s.evaluateSLO(r.Context(), service),
		"rollbackOnFailure": service.RollbackOnFailure,
	}
	// What the controller is doing about the service, from the cluster.
	guards, err := s.guardsForService(r.Context(), service.Name)
	switch {
	case errors.Is(err, errNoCluster):
		status["guardsUnavailable"] = err.Error()
	case err != nil:
		s.logger.ErrorContext(r.Context(), "read rollout guards", "service", service.Name, "error", err)
		status["guardsUnavailable"] = "the cluster could not be read; details are in the server log under request " + RequestID(r.Context())
	default:
		status["guards"] = guards
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
	if s.promConfig.URL == "" {
		return sloengine.Unknown(service, models.SLOSourceNone,
			"no Prometheus configured (ATTESTA_PROMETHEUS_URL is unset); SLO state unknown, deployment gate fails closed")
	}
	if s.promErr != nil {
		s.logger.ErrorContext(ctx, "Prometheus client misconfigured", "service", service.Name, "error", s.promErr)
		return sloengine.Unknown(service, models.SLOSourcePrometheus,
			"Prometheus client misconfigured; SLO state unknown, deployment gate fails closed")
	}
	// Each query has its own timeout; this bounds the whole evaluation.
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	signals, err := s.prom.Signals(checkCtx, service)
	if err != nil {
		s.logger.ErrorContext(ctx, "Prometheus query failed", "service", service.Name, "error", err)
		return sloengine.Unknown(service, models.SLOSourcePrometheus,
			"Prometheus query failed; SLO state unknown, deployment gate fails closed")
	}
	if s.sloThresholds != nil {
		return sloengine.EvaluateWith(service, signals, *s.sloThresholds)
	}
	return sloengine.Evaluate(service, signals)
}

// writeRunFailure answers for a failed workflow run by its error code:
// conflicts and closed gates are 409, anything else 500.
// The run's error text is already safe to return (workflows.Engine logs
// the detail of internal failures and records a generic message).
func writeRunFailure(w http.ResponseWriter, r *http.Request, run models.WorkflowRun) {
	status := http.StatusInternalServerError
	switch run.ErrorCode {
	case workflows.CodeConflict, workflows.CodeGateClosed:
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]any{"error": run.Error, "errorCode": run.ErrorCode, "requestId": RequestID(r.Context()), "workflow": run})
}

// internalError answers 500 for a failure the client cannot act on. Store
// and Prometheus errors carry hosts, ports, database users and SQL, so the
// detail goes to the log under the request ID and the client gets only
// the ID to quote.
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{
		"error":     "internal error",
		"requestId": RequestID(r.Context()),
	})
}

func (s *Server) listIncidents(w http.ResponseWriter, r *http.Request) {
	req, ok := pageRequest(w, r)
	if !ok {
		return
	}
	records, next, err := s.store.ListIncidentRecords(r.Context(), req)
	if err != nil {
		s.internalError(w, r, err)
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
		s.internalError(w, r, err)
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

// scaffoldSummary is what onboarding reports about the scaffold: the files
// it contains and where to download it. No server paths.
func scaffoldSummary(sc templates.Scaffold) map[string]any {
	return map[string]any{
		"serviceName": sc.ServiceName,
		"files":       sc.Paths(),
		"archive":     "/api/v1/services/" + sc.ServiceName + "/scaffold",
	}
}

// scaffold renders the service's scaffold on demand and returns it as a
// gzipped tar with every file under <name>/. It is rendered into memory
// first, so a failure is a JSON error rather than a truncated archive.
func (s *Server) scaffold(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	rendered, err := s.generator.Render(service)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	var buf bytes.Buffer
	if err := scaffoldpkg.WriteTarGz(&buf, service.Name, rendered.Files); err != nil {
		s.internalError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-scaffold.tar.gz"`, service.Name))
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	_, _ = w.Write(buf.Bytes())
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

1. Check the service's RolloutGuards and their latest decisions: GET /api/v1/services/%s/guards.
2. Compare p95 latency, availability, burn rate, and error rate against the SLO.
3. Inspect pod restarts, CPU, memory, PDB status, and HPA saturation.
4. Review the latest revision, Git SHA, and image scan result.
5. Create an incident and attach the Attesta timeline/postmortem draft.

## Rollback

With rollback enabled on its RolloutGuard, the controller restores the last healthy revision when the new one fails its checks, and records the decision. To roll back by hand: kubectl rollout undo deployment/<name> -n %s.
`, service.Name, service.Team, service.Owner, service.Tier, service.Pager, service.Environment, service.Namespace,
		service.Repository, service.SLOTarget, service.SLOLatencyP95Ms, service.SLOErrorRate,
		service.DeploymentStrategy, service.RollbackOnFailure, service.Name, service.Namespace)
}

func (s *Server) loadService(w http.ResponseWriter, r *http.Request) (models.Service, bool) {
	service, err := s.store.GetServiceByName(r.Context(), r.PathValue("name"))
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(w, http.StatusNotFound, "service not found")
		return models.Service{}, false
	}
	if err != nil {
		s.internalError(w, r, err)
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
		s.internalError(w, r, err)
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

// auth identifies the caller of every /api/ request and checks its role
// against the route (auth.Required). The caller's identity goes into the
// request context, the access log, and every workflow run, rollout
// transition and incident the request records.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		var principal auth.Principal
		switch {
		case s.authenticator != nil:
			p, err := s.authenticator.Authenticate(r.Context(), auth.Token(r))
			switch {
			case errors.Is(err, auth.ErrNoCredentials):
				w.Header().Set("WWW-Authenticate", `Bearer realm="attesta"`)
				writeError(w, http.StatusUnauthorized, "missing credentials: send Authorization: Bearer <token>")
				return
			case errors.Is(err, auth.ErrInvalidCredentials):
				w.Header().Set("WWW-Authenticate", `Bearer realm="attesta", error="invalid_token"`)
				writeError(w, http.StatusUnauthorized, "invalid or unrecognised credentials")
				return
			case err != nil:
				s.logger.ErrorContext(r.Context(), "authentication check failed", "error", err)
				writeError(w, http.StatusServiceUnavailable, "authentication is temporarily unavailable")
				return
			}
			principal = p
		case s.insecure:
			principal = auth.Principal{Name: "anonymous", Role: auth.RoleAdmin, Via: "insecure"}
		default:
			writeError(w, http.StatusUnauthorized, "authentication is not configured on this server")
			return
		}
		if need := auth.Required(r.Method, r.URL.Path); !principal.Allows(need) {
			writeError(w, http.StatusForbidden, fmt.Sprintf("%s has role %s; this request needs %s", principal.Name, principal.Role, need))
			return
		}
		if info := infoFrom(r.Context()); info != nil {
			info.actor = principal.Name
		}
		ctx := workflows.WithActor(auth.WithPrincipal(r.Context(), principal), principal.Name)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		actor := ""
		if info := infoFrom(r.Context()); info != nil {
			actor = info.actor
		}
		s.logger.InfoContext(r.Context(), "request",
			"actor", actor,
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

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}
