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
	CreateService(context.Context, models.Service) (models.Service, error)
	ListServices(context.Context) ([]models.Service, error)
	GetServiceByName(context.Context, string) (models.Service, error)
	RecordDeployment(context.Context, models.Deployment) (models.Deployment, error)
	LatestDeployment(context.Context, string) (models.Deployment, error)
	// ListDeployments returns one page of a service's deployments, newest
	// first, and the cursor of the next page ("" at the end).
	ListDeployments(context.Context, string, page.Request) ([]models.Deployment, string, error)
	workflows.Store
	SaveIncidentRecord(context.Context, models.IncidentRecord) error
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
