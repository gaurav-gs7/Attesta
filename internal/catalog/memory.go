package catalog

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/page"
	"github.com/gaurav-gs7/attesta/internal/rollouts"
	"github.com/gaurav-gs7/attesta/internal/workflows"
)

type MemoryStore struct {
	mu          sync.RWMutex
	services    map[string]models.Service
	deployments map[string][]models.Deployment
	incidents   map[string]models.IncidentRecord
	rollouts    map[string]models.Rollout
	// Workflow runs, with the engine's idempotency contract.
	*workflows.MemoryStore
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		services:    make(map[string]models.Service),
		deployments: make(map[string][]models.Deployment),
		incidents:   make(map[string]models.IncidentRecord),
		rollouts:    make(map[string]models.Rollout),
		MemoryStore: workflows.NewMemoryStore(),
	}
}

// Ping always succeeds: the in-memory store has nothing to reach.
func (s *MemoryStore) Ping(context.Context) error { return nil }

func (s *MemoryStore) CreateService(_ context.Context, service models.Service) (models.Service, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.services[service.Name]; exists {
		return models.Service{}, fmt.Errorf("%w: service %q already exists", ErrConflict, service.Name)
	}
	if service.ID == "" {
		service.ID = NewID()
	}
	now := nowUTC()
	service.CreatedAt = now
	service.UpdatedAt = now
	s.services[service.Name] = service
	return service, nil
}

func (s *MemoryStore) ListServices(_ context.Context) ([]models.Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	services := make([]models.Service, 0, len(s.services))
	for _, service := range s.services {
		services = append(services, service)
	}
	sort.Slice(services, func(i, j int) bool {
		return services[i].Name < services[j].Name
	})
	return services, nil
}

func (s *MemoryStore) GetServiceByName(_ context.Context, name string) (models.Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	service, ok := s.services[name]
	if !ok {
		return models.Service{}, ErrNotFound
	}
	return service, nil
}

func (s *MemoryStore) RecordDeployment(_ context.Context, deployment models.Deployment) (models.Deployment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if deployment.ID == "" {
		deployment.ID = NewID()
	}
	if deployment.StartedAt.IsZero() {
		deployment.StartedAt = nowUTC()
	}
	s.deployments[deployment.ServiceID] = append(s.deployments[deployment.ServiceID], deployment)
	return deployment, nil
}

func (s *MemoryStore) LatestDeployment(_ context.Context, serviceID string) (models.Deployment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	deployments := s.deployments[serviceID]
	if len(deployments) == 0 {
		return models.Deployment{}, ErrNotFound
	}
	latest := deployments[0]
	for _, d := range deployments[1:] {
		if page.Less(d.StartedAt, d.ID, latest.StartedAt, latest.ID) {
			latest = d
		}
	}
	return latest, nil
}

func (s *MemoryStore) CreateRollout(_ context.Context, rollout models.Rollout, deployment models.Deployment) (models.Rollout, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.rollouts {
		if existing.ServiceID == rollout.ServiceID && rollouts.Active(existing.State) {
			return models.Rollout{}, fmt.Errorf("%w: rollout %s (%s) is %s", ErrConflict, existing.ID, existing.Version, existing.State)
		}
	}
	if rollout.ID == "" {
		rollout.ID = NewID()
	}
	if deployment.ID == "" {
		deployment.ID = NewID()
	}
	rollout.DeploymentID, deployment.RolloutID = deployment.ID, rollout.ID
	s.rollouts[rollout.ID] = cloneRollout(rollout)
	s.deployments[deployment.ServiceID] = append(s.deployments[deployment.ServiceID], deployment)
	return rollout, nil
}

func (s *MemoryStore) GetRollout(_ context.Context, serviceID, id string) (models.Rollout, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rollout, ok := s.rollouts[id]
	if !ok || rollout.ServiceID != serviceID {
		return models.Rollout{}, ErrNotFound
	}
	return cloneRollout(rollout), nil
}

func (s *MemoryStore) LatestRollout(_ context.Context, serviceID string) (models.Rollout, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest *models.Rollout
	for _, r := range s.rollouts {
		if r.ServiceID == serviceID && (latest == nil || page.Less(r.CreatedAt, r.ID, latest.CreatedAt, latest.ID)) {
			rr := r
			latest = &rr
		}
	}
	if latest == nil {
		return models.Rollout{}, ErrNotFound
	}
	return cloneRollout(*latest), nil
}

func (s *MemoryStore) UpdateRollout(_ context.Context, rollout models.Rollout, fromState string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.rollouts[rollout.ID]
	if !ok {
		return ErrNotFound
	}
	if stored.State != fromState {
		return fmt.Errorf("%w: rollout %s is now %s", ErrConflict, rollout.ID, stored.State)
	}
	s.rollouts[rollout.ID] = cloneRollout(rollout)
	for i, d := range s.deployments[rollout.ServiceID] {
		if d.ID == rollout.DeploymentID {
			d.Status = rollouts.DeploymentStatus(rollout.State)
			if !rollouts.Active(rollout.State) && d.CompletedAt == nil {
				at := rollout.UpdatedAt
				d.CompletedAt = &at
			}
			s.deployments[rollout.ServiceID][i] = d
		}
	}
	return nil
}

func cloneRollout(r models.Rollout) models.Rollout {
	r.Events = append([]models.RolloutEvent(nil), r.Events...)
	return r
}

func (s *MemoryStore) ListDeployments(_ context.Context, serviceID string, req page.Request) ([]models.Deployment, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	deployments := append([]models.Deployment(nil), s.deployments[serviceID]...)
	sort.Slice(deployments, func(i, j int) bool {
		return page.Less(deployments[i].StartedAt, deployments[i].ID, deployments[j].StartedAt, deployments[j].ID)
	})
	out, next := page.Slice(deployments, req, func(d models.Deployment) (time.Time, string) { return d.StartedAt, d.ID })
	return out, next, nil
}

func (s *MemoryStore) activeDuplicate(record models.IncidentRecord) bool {
	in := record.Incident
	if in.Status != "active" || in.Fingerprint == "" {
		return false
	}
	for id, other := range s.incidents {
		o := other.Incident
		if id != in.ID && o.ServiceID == in.ServiceID && o.Fingerprint == in.Fingerprint && o.Status == "active" {
			return true
		}
	}
	return false
}

// PruneResolvedIncidents deletes incidents resolved before cutoff.
func (s *MemoryStore) PruneResolvedIncidents(_ context.Context, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, r := range s.incidents {
		if r.Incident.Status == "resolved" && r.Incident.ResolvedAt != nil && r.Incident.ResolvedAt.Before(cutoff) {
			delete(s.incidents, id)
			n++
		}
	}
	return n, nil
}

func cloneIncident(r models.IncidentRecord) models.IncidentRecord {
	r.Events = append([]models.IncidentEvent(nil), r.Events...)
	return r
}

func (s *MemoryStore) CreateIncident(_ context.Context, record models.IncidentRecord) (models.IncidentRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.incidents[record.Incident.ID]; exists || s.activeDuplicate(record) {
		return models.IncidentRecord{}, fmt.Errorf("%w: %s already has an active incident for this alert", ErrConflict, record.Service.Name)
	}
	record.Incident.Version = 1
	s.incidents[record.Incident.ID] = cloneIncident(record)
	return record, nil
}

func (s *MemoryStore) UpdateIncident(_ context.Context, record models.IncidentRecord) (models.IncidentRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.incidents[record.Incident.ID]
	if !ok {
		return models.IncidentRecord{}, ErrNotFound
	}
	if stored.Incident.Version != record.Incident.Version {
		return models.IncidentRecord{}, fmt.Errorf("%w: incident %s is at version %d, not %d", ErrConflict, record.Incident.ID, stored.Incident.Version, record.Incident.Version)
	}
	if s.activeDuplicate(record) {
		return models.IncidentRecord{}, fmt.Errorf("%w: %s already has an active incident for this alert", ErrConflict, record.Service.Name)
	}
	// Events are append-only, as in PostgreSQL.
	events := append([]models.IncidentEvent(nil), stored.Events...)
	if len(record.Events) > len(events) {
		events = append(events, record.Events[len(events):]...)
	}
	record.Events = events
	record.Incident.Version++
	s.incidents[record.Incident.ID] = cloneIncident(record)
	return record, nil
}

func (s *MemoryStore) GetIncidentRecord(_ context.Context, id string) (models.IncidentRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.incidents[id]
	if !ok {
		return models.IncidentRecord{}, ErrNotFound
	}
	return cloneIncident(record), nil
}

func (s *MemoryStore) ListIncidentRecords(_ context.Context, req page.Request) ([]models.IncidentRecord, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	records := make([]models.IncidentRecord, 0, len(s.incidents))
	for _, record := range s.incidents {
		records = append(records, cloneIncident(record))
	}
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i].Incident, records[j].Incident
		return page.Less(a.CreatedAt, a.ID, b.CreatedAt, b.ID)
	})
	out, next := page.Slice(records, req, func(r models.IncidentRecord) (time.Time, string) {
		return r.Incident.CreatedAt, r.Incident.ID
	})
	return out, next, nil
}

func (s *MemoryStore) FindActiveIncident(_ context.Context, service, fingerprint string) (models.IncidentRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var found *models.IncidentRecord
	for _, record := range s.incidents {
		inc := record.Incident
		if inc.Service == service && inc.Fingerprint == fingerprint && inc.Status == "active" &&
			(found == nil || inc.CreatedAt.After(found.Incident.CreatedAt)) {
			r := cloneIncident(record)
			found = &r
		}
	}
	if found == nil {
		return models.IncidentRecord{}, ErrNotFound
	}
	return *found, nil
}
