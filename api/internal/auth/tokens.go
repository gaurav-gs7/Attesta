package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// TokenFile authenticates API tokens listed, by SHA-256 hash, in a JSON
// file:
//
//	{"tokens": [
//	  {"name": "ci-deployer", "role": "operator", "sha256": "<hex>"},
//	  {"name": "alertmanager", "role": "operator", "sha256": "<hex>"},
//	  {"name": "gaurav", "role": "admin", "sha256": "<hex>"}
//	]}
//
// Each token has its own identity, recorded as the actor of what it does.
// The file is re-read when it changes, so a token is rotated by adding the
// new hash, moving callers over and removing the old one, without a
// restart. Only hashes are stored.
type TokenFile struct {
	path string

	mu      sync.Mutex
	modTime time.Time
	size    int64
	entries []tokenEntry
	checked time.Time
}

type tokenEntry struct {
	name string
	role Role
	hash [sha256.Size]byte
}

var tokenName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`)

// NewTokenFile loads path and fails if it is missing or malformed.
func NewTokenFile(path string) (*TokenFile, error) {
	f := &TokenFile{path: path}
	if err := f.reload(true); err != nil {
		return nil, err
	}
	return f, nil
}

// HashToken returns the hex SHA-256 of a token, the form the file stores.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (f *TokenFile) Authenticate(_ context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, ErrNoCredentials
	}
	if err := f.reload(false); err != nil {
		return Principal{}, err
	}
	presented := sha256.Sum256([]byte(token))
	f.mu.Lock()
	entries := f.entries
	f.mu.Unlock()
	var match *tokenEntry
	// Compare against every entry, in constant time each, so timing does
	// not reveal which entry, if any, matched.
	for i := range entries {
		if subtle.ConstantTimeCompare(presented[:], entries[i].hash[:]) == 1 {
			match = &entries[i]
		}
	}
	if match == nil {
		return Principal{}, ErrInvalidCredentials
	}
	return Principal{Name: match.name, Role: match.role, Via: "token"}, nil
}

// reload re-reads the file if it changed, at most once a second. A file
// that becomes malformed keeps the last good entries and reports the error
// at startup only.
func (f *TokenFile) reload(initial bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !initial && time.Since(f.checked) < time.Second {
		return nil
	}
	f.checked = time.Now()
	info, err := os.Stat(f.path)
	if err != nil {
		if initial {
			return fmt.Errorf("API tokens file: %w", err)
		}
		return nil
	}
	if !initial && info.ModTime().Equal(f.modTime) && info.Size() == f.size {
		return nil
	}
	entries, err := parseTokenFile(f.path)
	if err != nil {
		if initial {
			return err
		}
		return nil
	}
	f.entries, f.modTime, f.size = entries, info.ModTime(), info.Size()
	return nil
}

func parseTokenFile(path string) ([]tokenEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("API tokens file: %w", err)
	}
	var doc struct {
		Tokens []struct {
			Name   string `json:"name"`
			Role   string `json:"role"`
			SHA256 string `json:"sha256"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("API tokens file %s: %w", path, err)
	}
	var entries []tokenEntry
	seen := map[string]bool{}
	for i, t := range doc.Tokens {
		if !tokenName.MatchString(t.Name) {
			return nil, fmt.Errorf("API tokens file %s: entry %d: invalid name %q", path, i, t.Name)
		}
		role, err := ParseRole(t.Role)
		if err != nil {
			return nil, fmt.Errorf("API tokens file %s: %s: %w", path, t.Name, err)
		}
		hash, err := hex.DecodeString(strings.ToLower(t.SHA256))
		if err != nil || len(hash) != sha256.Size {
			return nil, fmt.Errorf("API tokens file %s: %s: sha256 must be 64 hex characters", path, t.Name)
		}
		if seen[t.SHA256] {
			return nil, fmt.Errorf("API tokens file %s: %s: duplicate token hash", path, t.Name)
		}
		seen[t.SHA256] = true
		e := tokenEntry{name: t.Name, role: role}
		copy(e.hash[:], hash)
		entries = append(entries, e)
	}
	return entries, nil
}

// StaticToken is the single shared token from ATTESTA_API_TOKEN. It is kept
// for compatibility: every caller using it is the same identity, so prefer
// a tokens file.
type StaticToken struct {
	hash [sha256.Size]byte
	name string
	role Role
}

func NewStaticToken(token string, role Role) *StaticToken {
	return &StaticToken{hash: sha256.Sum256([]byte(token)), name: "shared-api-token", role: role}
}

func (s *StaticToken) Authenticate(_ context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, ErrNoCredentials
	}
	got := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(got[:], s.hash[:]) != 1 {
		return Principal{}, ErrInvalidCredentials
	}
	return Principal{Name: s.name, Role: s.role, Via: "token"}, nil
}
