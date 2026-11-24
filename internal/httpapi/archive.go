package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/archive"
	"github.com/gaurav-gs7/attesta/internal/catalog"
	"github.com/gaurav-gs7/attesta/internal/cluster"
	"github.com/gaurav-gs7/attesta/internal/models"
)

// The API is the archive outside the cluster: the controller sends it every
// decision (--archive-url), and it keeps them append-only. A guard and all
// of its records deleted from the cluster are still here, and a record
// changed in the cluster no longer matches the archived one.

// WithDecisionKeys makes the archive accept only records signed by one of
// keys, and adds signature checks to every chain the API verifies.
func WithDecisionKeys(keys attestav1.Keys) Option {
	return func(s *Server) {
		s.decisionKeys = keys
	}
}

var guardUIDPattern = regexp.MustCompile(`^[0-9a-zA-Z-]{1,64}$`)

// archiveDecision answers POST /api/v1/decision-archive.
func (s *Server) archiveDecision(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var rec archive.Record
	if err := dec.Decode(&rec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	spec := rec.Spec
	switch {
	case rec.Kind != "RolloutDecision" || rec.APIVersion != attestav1.GroupVersion.String():
		writeError(w, http.StatusBadRequest, "body must be an attesta.io/v1 RolloutDecision")
		return
	case !guardUIDPattern.MatchString(spec.GuardUID) || spec.Sequence < 1 || !dnsLabel.MatchString(rec.Metadata.Namespace) ||
		!dnsSubdomain.MatchString(spec.Guard) || rec.Metadata.Name != attestav1.DecisionName(spec.Guard, spec.GuardUID, spec.Sequence):
		writeError(w, http.StatusBadRequest, "the record's guard, guard UID, sequence, namespace or name is malformed")
		return
	}
	if hash, err := spec.ComputeHash(); err != nil || hash != spec.Hash {
		writeError(w, http.StatusUnprocessableEntity, "the record does not hash to the hash it carries")
		return
	}
	keyID := ""
	if s.decisionKeys != nil {
		if err := spec.VerifySignature(s.decisionKeys); err != nil {
			writeError(w, http.StatusUnprocessableEntity, "signature: "+err.Error())
			return
		}
	}
	if spec.Signature != nil {
		keyID = spec.Signature.KeyID
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	duplicate, err := s.store.ArchiveDecision(r.Context(), models.ArchivedDecision{
		GuardUID: spec.GuardUID, Namespace: rec.Metadata.Namespace, Guard: spec.Guard, Name: rec.Metadata.Name,
		Sequence: spec.Sequence, Hash: spec.Hash, PreviousHash: spec.PreviousHash, KeyID: keyID, Record: raw,
	})
	switch {
	case errors.Is(err, catalog.ErrConflict):
		s.logger.ErrorContext(r.Context(), "decision archive refused a record", "guard", rec.Metadata.Namespace+"/"+spec.Guard,
			"sequence", spec.Sequence, "error", err)
		s.metrics.archiveRefused.Inc()
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		s.internalError(w, r, err)
	case duplicate:
		writeJSON(w, http.StatusOK, map[string]any{"archived": false, "duplicate": true, "sequence": spec.Sequence})
	default:
		writeJSON(w, http.StatusCreated, map[string]any{"archived": true, "sequence": spec.Sequence})
	}
}

// listArchive answers GET /api/v1/decision-archive: every archived chain,
// including those of guards no longer in the cluster.
func (s *Server) listArchive(w http.ResponseWriter, r *http.Request) {
	guards, err := s.store.ArchivedGuards(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"guards": nonNil(guards)})
}

// getArchive answers GET /api/v1/decision-archive/{guardUID}: the archived
// chain and its verification.
func (s *Server) getArchive(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("guardUID")
	if !guardUIDPattern.MatchString(uid) {
		writeError(w, http.StatusNotFound, "no archived decisions for that guard")
		return
	}
	archived, err := s.store.ArchivedDecisions(r.Context(), uid)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if len(archived) == 0 {
		writeError(w, http.StatusNotFound, "no archived decisions for that guard")
		return
	}
	records, err := archivedRecords(archived)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	decisions := make([]decisionView, 0, len(records))
	for _, d := range records {
		decisions = append(decisions, decisionView{Name: d.Name, RolloutDecisionSpec: d.Spec})
	}
	head := archived[len(archived)-1]
	writeJSON(w, http.StatusOK, map[string]any{
		"guardUID": uid, "namespace": head.Namespace, "guard": head.Guard,
		"decisions": decisions,
		"chain":     cluster.VerifyRecords(head.Guard, uid, records, nil, s.decisionKeys),
	})
}

func archivedRecords(archived []models.ArchivedDecision) ([]attestav1.RolloutDecision, error) {
	out := make([]attestav1.RolloutDecision, 0, len(archived))
	for _, a := range archived {
		var rec archive.Record
		if err := json.Unmarshal(a.Record, &rec); err != nil {
			return nil, err
		}
		d := attestav1.RolloutDecision{Spec: rec.Spec}
		d.Name, d.Namespace = rec.Metadata.Name, rec.Metadata.Namespace
		out = append(out, d)
	}
	return out, nil
}

// archiveComparison is how the cluster's records of a guard compare with
// the archive's.
type archiveComparison struct {
	Archived int64 `json:"archived"`
	// MissingFromCluster are sequences the archive holds and the cluster no
	// longer does: deleted records.
	MissingFromCluster []int64 `json:"missingFromCluster,omitempty"`
	// Differ are sequences whose cluster record is not the archived one.
	Differ []int64 `json:"differ,omitempty"`
	// NotYetArchived are sequences in the cluster the archive lacks.
	NotYetArchived []int64 `json:"notYetArchived,omitempty"`
	Matches        bool    `json:"matches"`
}

func (s *Server) compareWithArchive(r *http.Request, g attestav1.RolloutGuard, records []attestav1.RolloutDecision) (*archiveComparison, error) {
	archived, err := s.store.ArchivedDecisions(r.Context(), string(g.UID))
	if err != nil || len(archived) == 0 {
		return nil, err
	}
	// What each cluster record hashes to now, not the hash it claims: an
	// edit in place keeps the claim and changes the contents.
	inCluster := map[int64]string{}
	for _, d := range records {
		hash, err := d.Spec.ComputeHash()
		if err != nil {
			hash = "unhashable"
		}
		if hash != d.Spec.Hash {
			hash = "altered:" + hash
		}
		inCluster[d.Spec.Sequence] = hash
	}
	c := &archiveComparison{Archived: int64(len(archived))}
	held := map[int64]bool{}
	for _, a := range archived {
		held[a.Sequence] = true
		switch hash, ok := inCluster[a.Sequence]; {
		case !ok:
			c.MissingFromCluster = append(c.MissingFromCluster, a.Sequence)
		case hash != a.Hash:
			c.Differ = append(c.Differ, a.Sequence)
		}
	}
	for _, d := range records {
		if !held[d.Spec.Sequence] {
			c.NotYetArchived = append(c.NotYetArchived, d.Spec.Sequence)
		}
	}
	c.Matches = len(c.MissingFromCluster) == 0 && len(c.Differ) == 0
	return c, nil
}
