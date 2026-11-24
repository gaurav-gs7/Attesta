package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/page"
	"github.com/gaurav-gs7/attesta/internal/workflows"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")

type Store interface {
	// Ping reports whether the store can serve requests.
	Ping(context.Context) error
	CreateService(context.Context, models.Service) (models.Service, error)
	ListServices(context.Context) ([]models.Service, error)
	GetServiceByName(context.Context, string) (models.Service, error)
	// ArchiveDecision appends a decision to its guard's archived chain. It
	// returns duplicate=true for the record already held at that position,
	// and ErrConflict when a different record is held there (a fork) or the
	// record does not link to the one before it (a gap or a mismatch).
	ArchiveDecision(context.Context, models.ArchivedDecision) (duplicate bool, err error)
	// ArchivedDecisions returns a guard's archived chain, oldest first.
	ArchivedDecisions(ctx context.Context, guardUID string) ([]models.ArchivedDecision, error)
	// ArchivedGuards summarises every archived chain.
	ArchivedGuards(context.Context) ([]models.ArchivedGuard, error)
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
