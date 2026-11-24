package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gaurav-gs7/attesta/internal/models"
)

// linkCheck applies the archive's rules for a record at sequence n, given
// what is held at n and n-1.
func linkCheck(d models.ArchivedDecision, atSeq, before *models.ArchivedDecision) (duplicate bool, err error) {
	switch {
	case atSeq != nil && atSeq.Hash == d.Hash:
		return true, nil
	case atSeq != nil:
		return false, fmt.Errorf("%w: a different record (hash %.12s) is archived at sequence %d of guard %s; this one (hash %.12s) forks the chain",
			ErrConflict, atSeq.Hash, d.Sequence, d.GuardUID, d.Hash)
	case d.Sequence == 1 && d.PreviousHash != "":
		return false, fmt.Errorf("%w: the first record links to %.12s", ErrConflict, d.PreviousHash)
	case d.Sequence > 1 && before == nil:
		return false, fmt.Errorf("%w: sequence %d arrived before %d; records are archived in order", ErrConflict, d.Sequence, d.Sequence-1)
	case d.Sequence > 1 && before.Hash != d.PreviousHash:
		return false, fmt.Errorf("%w: sequence %d links to %.12s, the archive holds %.12s at %d",
			ErrConflict, d.Sequence, d.PreviousHash, before.Hash, d.Sequence-1)
	}
	return false, nil
}

func (s *MemoryStore) ArchiveDecision(_ context.Context, d models.ArchivedDecision) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.archive == nil {
		s.archive = map[string][]models.ArchivedDecision{}
	}
	chain := s.archive[d.GuardUID]
	at := func(seq int64) *models.ArchivedDecision {
		if seq >= 1 && seq <= int64(len(chain)) {
			return &chain[seq-1]
		}
		return nil
	}
	duplicate, err := linkCheck(d, at(d.Sequence), at(d.Sequence-1))
	if err != nil || duplicate {
		return duplicate, err
	}
	d.ArchivedAt = nowUTC()
	s.archive[d.GuardUID] = append(chain, d)
	return false, nil
}

func (s *MemoryStore) ArchivedDecisions(_ context.Context, guardUID string) ([]models.ArchivedDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]models.ArchivedDecision(nil), s.archive[guardUID]...), nil
}

func (s *MemoryStore) ArchivedGuards(context.Context) ([]models.ArchivedGuard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []models.ArchivedGuard
	for uid, chain := range s.archive {
		head := chain[len(chain)-1]
		out = append(out, models.ArchivedGuard{GuardUID: uid, Namespace: head.Namespace, Guard: head.Guard,
			Decisions: int64(len(chain)), HeadSequence: head.Sequence, HeadHash: head.Hash, LastArchivedAt: head.ArchivedAt})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].Guard != out[j].Guard {
			return out[i].Guard < out[j].Guard
		}
		return out[i].GuardUID < out[j].GuardUID
	})
	return out, nil
}

const archivedColumns = `guard_uid, namespace, guard, name, sequence, hash, previous_hash, key_id, record, archived_at`

func scanArchived(row interface{ Scan(...any) error }) (models.ArchivedDecision, error) {
	var d models.ArchivedDecision
	var record []byte
	err := row.Scan(&d.GuardUID, &d.Namespace, &d.Guard, &d.Name, &d.Sequence, &d.Hash, &d.PreviousHash, &d.KeyID, &record, &d.ArchivedAt)
	d.Record = record
	return d, err
}

// ArchiveDecision checks the link and inserts in one transaction. The
// table refuses UPDATE and DELETE (migration 7), and its primary key on
// (guard_uid, sequence) settles two inserts racing for one position.
func (s *PostgresStore) ArchiveDecision(ctx context.Context, d models.ArchivedDecision) (_ bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	get := func(seq int64) (*models.ArchivedDecision, error) {
		if seq < 1 {
			return nil, nil
		}
		held, err := scanArchived(tx.QueryRowContext(ctx,
			`SELECT `+archivedColumns+` FROM decision_archive WHERE guard_uid = $1 AND sequence = $2`, d.GuardUID, seq))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return &held, err
	}
	atSeq, err := get(d.Sequence)
	if err != nil {
		return false, err
	}
	before, err := get(d.Sequence - 1)
	if err != nil {
		return false, err
	}
	duplicate, err := linkCheck(d, atSeq, before)
	if err != nil || duplicate {
		_ = tx.Rollback()
		return duplicate, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO decision_archive (`+archivedColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())`,
		d.GuardUID, d.Namespace, d.Guard, d.Name, d.Sequence, d.Hash, d.PreviousHash, d.KeyID, []byte(d.Record))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Another request archived this position first; judge again.
			_ = tx.Rollback()
			return s.ArchiveDecision(ctx, d)
		}
		return false, err
	}
	return false, tx.Commit()
}

func (s *PostgresStore) ArchivedDecisions(ctx context.Context, guardUID string) ([]models.ArchivedDecision, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+archivedColumns+` FROM decision_archive WHERE guard_uid = $1 ORDER BY sequence`, guardUID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []models.ArchivedDecision
	for rows.Next() {
		d, err := scanArchived(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *PostgresStore) ArchivedGuards(ctx context.Context) ([]models.ArchivedGuard, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT ON (guard_uid) guard_uid, namespace, guard, sequence, hash, archived_at
		FROM decision_archive
		ORDER BY guard_uid, sequence DESC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []models.ArchivedGuard
	for rows.Next() {
		var g models.ArchivedGuard
		if err := rows.Scan(&g.GuardUID, &g.Namespace, &g.Guard, &g.HeadSequence, &g.HeadHash, &g.LastArchivedAt); err != nil {
			return nil, err
		}
		g.Decisions = g.HeadSequence // the archive holds 1..head without gaps
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].Guard != out[j].Guard {
			return out[i].Guard < out[j].Guard
		}
		return out[i].GuardUID < out[j].GuardUID
	})
	return out, nil
}
