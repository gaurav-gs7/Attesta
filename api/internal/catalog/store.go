package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/page"
	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")

type Store interface {
	// Ping reports whether the store can serve requests.
	Ping(context.Context) error
	CreateService(context.Context, models.Service) (models.Service, error)
	ListServices(context.Context) ([]models.Service, error)
	GetServiceByName(context.Context, string) (models.Service, error)
	RecordDeployment(context.Context, models.Deployment) (models.Deployment, error)
	LatestDeployment(context.Context, string) (models.Deployment, error)
	// CreateRollout stores a new rollout and its deployment record together.
	// It returns ErrConflict if the service already has an active rollout.
	CreateRollout(ctx context.Context, rollout models.Rollout, deployment models.Deployment) (models.Rollout, error)
	// GetRollout returns a service's rollout by ID, or ErrNotFound.
	GetRollout(ctx context.Context, serviceID, id string) (models.Rollout, error)
	// LatestRollout returns a service's most recent rollout, or ErrNotFound.
	LatestRollout(ctx context.Context, serviceID string) (models.Rollout, error)
	// UpdateRollout writes a transition of rollout, made from fromState, and
	// the matching status on its deployment record. It returns ErrConflict if
	// the stored rollout is no longer in fromState.
	UpdateRollout(ctx context.Context, rollout models.Rollout, fromState string) error
	// ListDeployments returns one page of a service's deployments, newest
	// first, and the cursor of the next page ("" at the end).
	ListDeployments(context.Context, string, page.Request) ([]models.Deployment, string, error)
	workflows.Store
	Pruner
	// CreateIncident stores a new incident at version 1. A second active
	// incident for the same service and non-empty alert fingerprint is
	// ErrConflict.
	CreateIncident(context.Context, models.IncidentRecord) (models.IncidentRecord, error)
	// UpdateIncident stores record if the stored incident is still at
	// record.Incident.Version, appends events beyond those stored, and
	// returns the record at the next version. ErrConflict if the incident
	// changed since it was read, ErrNotFound if it does not exist.
	UpdateIncident(context.Context, models.IncidentRecord) (models.IncidentRecord, error)
	GetIncidentRecord(context.Context, string) (models.IncidentRecord, error)
	ListIncidentRecords(context.Context, page.Request) ([]models.IncidentRecord, string, error)
	// FindActiveIncident returns the active incident of a service with the
	// given alert fingerprint, or ErrNotFound.
	FindActiveIncident(ctx context.Context, service, fingerprint string) (models.IncidentRecord, error)
}

func Open(ctx context.Context, databaseURL string) (Store, func(), error) {
	if databaseURL == "" {
		return NewMemoryStore(), func() {}, nil
	}

	store, err := NewPostgresStore(ctx, databaseURL)
	if err != nil {
		return nil, nil, err
	}
	return store, func() { _ = store.Close() }, nil
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("read random bytes: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

func nowUTC() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

func nullableTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *t, Valid: true}
}
