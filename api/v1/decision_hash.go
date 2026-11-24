package v1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// CanonicalJSON is the byte string a decision's hash is computed over: the
// spec without its hash field, as compact JSON with object keys sorted and
// no HTML escaping. It is what `jq -cjS '.spec | del(.hash)'` prints for the
// stored object, so a record can be checked without this code.
func (s RolloutDecisionSpec) CanonicalJSON() ([]byte, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	// A round trip through generic values sorts every object's keys.
	var generic any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}
	if m, ok := generic.(map[string]any); ok {
		delete(m, "hash")
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(generic); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// ComputeHash returns the hex SHA-256 of the spec's canonical JSON.
func (s RolloutDecisionSpec) ComputeHash() (string, error) {
	canonical, err := s.CanonicalJSON()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// Seal links the spec to the chain head prev (nil for the first decision)
// and sets its hash.
func (s *RolloutDecisionSpec) Seal(prev *DecisionRef) error {
	s.Sequence, s.PreviousHash = 1, ""
	if prev != nil {
		s.Sequence, s.PreviousHash = prev.Sequence+1, prev.Hash
	}
	hash, err := s.ComputeHash()
	if err != nil {
		return err
	}
	s.Hash = hash
	return nil
}

// DecisionName is the object name of a guard's decision at a sequence: the
// guard's name, the first 8 characters of its UID and the zero-padded
// sequence. A fixed name per position means two writers racing for the same
// position cannot both succeed.
func DecisionName(guard, guardUID string, sequence int64) string {
	uid := guardUID
	if len(uid) > 8 {
		uid = uid[:8]
	}
	suffix := fmt.Sprintf("-%s-%06d", uid, sequence)
	// Object names are at most 253 characters.
	if max := 253 - len(suffix); len(guard) > max {
		guard = guard[:max]
	}
	return guard + suffix
}

// ChainError reports where a guard's decision chain breaks.
type ChainError struct {
	Guard    string
	Sequence int64
	Problem  string
}

func (e *ChainError) Error() string {
	return fmt.Sprintf("decision chain of %s breaks at sequence %d: %s", e.Guard, e.Sequence, e.Problem)
}

// VerifyChain checks a guard's decisions: all from the guard with this UID,
// sequences 1..n with no gaps or repeats, each record's hash matching its contents, and each record linked
// to the hash of the one before. head, if set, is the guard's
// status.lastDecision; the chain must end there, which catches deleted
// records at the end. It returns nil or a *ChainError.
func VerifyChain(guard, guardUID string, records []RolloutDecision, head *DecisionRef) error {
	sorted := append([]RolloutDecision(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Spec.Sequence < sorted[j].Spec.Sequence })
	prevHash := ""
	for i, r := range sorted {
		want := int64(i + 1)
		fail := func(problem string, args ...any) error {
			return &ChainError{Guard: guard, Sequence: want, Problem: fmt.Sprintf(problem, args...)}
		}
		switch {
		case r.Spec.Guard != guard || r.Spec.GuardUID != guardUID:
			return fail("record %s belongs to guard %q (uid %s)", r.Name, r.Spec.Guard, r.Spec.GuardUID)
		case r.Spec.Sequence < want:
			return fail("sequence %d appears twice", r.Spec.Sequence)
		case r.Spec.Sequence > want:
			return fail("record missing (next is %d)", r.Spec.Sequence)
		case r.Spec.PreviousHash != prevHash:
			return fail("previousHash %s does not match the previous record's hash %s", short(r.Spec.PreviousHash), short(prevHash))
		}
		hash, err := r.Spec.ComputeHash()
		if err != nil {
			return fail("cannot hash: %v", err)
		}
		if hash != r.Spec.Hash {
			return fail("record %s was altered: its contents hash to %s, it claims %s", r.Name, short(hash), short(r.Spec.Hash))
		}
		prevHash = r.Spec.Hash
	}
	if head != nil {
		n := int64(len(sorted))
		if head.Sequence != n || head.Hash != prevHash {
			return &ChainError{Guard: guard, Sequence: n + 1,
				Problem: fmt.Sprintf("the guard's last decision is %d (%s) but the records end at %d (%s)", head.Sequence, short(head.Hash), n, short(prevHash))}
		}
	}
	return nil
}

func short(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	if hash == "" {
		return "(none)"
	}
	return hash
}
