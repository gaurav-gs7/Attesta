package httpapi

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"time"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/cluster"
	"github.com/gaurav-gs7/attesta/internal/incidents"
)

// The API reports what the controller does; it never writes to the
// cluster. Guards and decisions are read from the API server on request.

var errNoCluster = errors.New("cluster access is not configured on this server (set ATTESTA_CLUSTER_READ=true)")

// dnsLabel is a Kubernetes namespace; a guard's name is a DNS subdomain.
var (
	dnsLabel     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)
)

// guardView is a RolloutGuard as the API reports it.
type guardView struct {
	Namespace string                       `json:"namespace"`
	Name      string                       `json:"name"`
	Service   string                       `json:"service"`
	Spec      attestav1.RolloutGuardSpec   `json:"spec"`
	Status    attestav1.RolloutGuardStatus `json:"status"`
}

// decisionView is a RolloutDecision with its object name.
type decisionView struct {
	Name string `json:"name"`
	attestav1.RolloutDecisionSpec
}

func viewOf(g attestav1.RolloutGuard) guardView {
	return guardView{Namespace: g.Namespace, Name: g.Name, Service: cluster.ServiceName(g), Spec: g.Spec, Status: g.Status}
}

// guardWithChain is a guard with its decision chain checked.
type guardWithChain struct {
	guardView
	Chain cluster.Verification `json:"chain"`
}

func (s *Server) guardsForService(ctx context.Context, service string) ([]guardWithChain, error) {
	if s.cluster == nil {
		return nil, errNoCluster
	}
	guards, err := cluster.GuardsForService(ctx, s.cluster, service)
	if err != nil {
		return nil, err
	}
	out := make([]guardWithChain, 0, len(guards))
	for _, g := range guards {
		records, err := s.cluster.Decisions(ctx, g)
		if err != nil {
			return nil, err
		}
		out = append(out, guardWithChain{guardView: viewOf(g), Chain: cluster.Verify(g, records, s.decisionKeys)})
	}
	return out, nil
}

func (s *Server) clusterUnavailable(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errNoCluster) {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.internalError(w, r, err)
}

// serviceGuards answers GET /services/{name}/guards: the guards protecting
// the service, wherever they are, each with its chain verified.
func (s *Server) serviceGuards(w http.ResponseWriter, r *http.Request) {
	service, ok := s.loadService(w, r)
	if !ok {
		return
	}
	guards, err := s.guardsForService(r.Context(), service.Name)
	if err != nil {
		s.clusterUnavailable(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"service": service.Name, "guards": guards})
}

// listGuards answers GET /guards[?namespace=].
func (s *Server) listGuards(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		s.clusterUnavailable(w, r, errNoCluster)
		return
	}
	namespace := r.URL.Query().Get("namespace")
	if namespace != "" && !dnsLabel.MatchString(namespace) {
		writeError(w, http.StatusBadRequest, "namespace must be a DNS label")
		return
	}
	guards, err := s.cluster.Guards(r.Context(), namespace)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	views := make([]guardView, 0, len(guards))
	for _, g := range guards {
		views = append(views, viewOf(g))
	}
	writeJSON(w, http.StatusOK, map[string]any{"guards": views})
}

// getGuard answers GET /guards/{namespace}/{name}: the guard, every
// decision it recorded, and whether the chain verifies.
func (s *Server) getGuard(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		s.clusterUnavailable(w, r, errNoCluster)
		return
	}
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if !dnsLabel.MatchString(namespace) || !dnsSubdomain.MatchString(name) {
		writeError(w, http.StatusNotFound, "rollout guard not found")
		return
	}
	g, err := s.cluster.Guard(r.Context(), namespace, name)
	if errors.Is(err, cluster.ErrNotFound) {
		writeError(w, http.StatusNotFound, "rollout guard not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	records, err := s.cluster.Decisions(r.Context(), g)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	decisions := make([]decisionView, 0, len(records))
	for _, d := range records {
		decisions = append(decisions, decisionView{Name: d.Name, RolloutDecisionSpec: d.Spec})
	}
	body := map[string]any{
		"guard":     viewOf(g),
		"decisions": decisions,
		"chain":     cluster.Verify(g, records, s.decisionKeys),
	}
	// Against the copy outside the cluster: catches what the cluster's own
	// chain cannot, such as every record deleted.
	comparison, err := s.compareWithArchive(r, g, records)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if comparison != nil {
		body["archive"] = comparison
	}
	writeJSON(w, http.StatusOK, body)
}

// decisionsSince collects the decisions of every guard of the service made
// after since, for an incident timeline. A failure to read is reported in
// the result, not returned: the incident still opens.
func (s *Server) decisionsSince(ctx context.Context, service string, since time.Time) incidents.Decisions {
	if s.cluster == nil {
		return incidents.Decisions{Unavailable: "cluster access is not configured on this server"}
	}
	guards, err := cluster.GuardsForService(ctx, s.cluster, service)
	if err != nil {
		s.logger.ErrorContext(ctx, "read rollout guards for an incident", "service", service, "error", err)
		return incidents.Decisions{Unavailable: "the cluster could not be read"}
	}
	var out incidents.Decisions
	for _, g := range guards {
		records, err := s.cluster.Decisions(ctx, g)
		if err != nil {
			s.logger.ErrorContext(ctx, "read rollout decisions for an incident", "guard", g.Namespace+"/"+g.Name, "error", err)
			return incidents.Decisions{Unavailable: "the cluster could not be read"}
		}
		for _, d := range records {
			at, err := time.Parse(time.RFC3339, d.Spec.DecidedAt)
			if err != nil || at.Before(since) {
				continue
			}
			decision := incidents.Decision{
				At: at, Namespace: d.Namespace, Guard: d.Spec.Guard, Record: d.Name, Hash: d.Spec.Hash,
				Action: d.Spec.Action, Trigger: d.Spec.Trigger, Deployment: d.Spec.Deployment.Name,
				Revision: d.Spec.Deployment.Revision, Message: d.Spec.Message,
			}
			if d.Spec.RestoredTo != nil {
				decision.RestoredTo = d.Spec.RestoredTo.Revision
			}
			out.Records = append(out.Records, decision)
		}
	}
	return out
}
