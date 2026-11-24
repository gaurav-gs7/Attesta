// Package page implements keyset pagination for lists ordered newest first.
//
// A cursor names the last item of the previous page by its sort time and ID.
// The next page holds the items strictly older than it in (time, ID) order,
// so pages stay stable while new items are added at the front.
package page

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

// ErrInvalid is returned for a malformed limit or cursor.
var ErrInvalid = errors.New("invalid pagination")

// Request asks for one page.
type Request struct {
	// Limit is the page size; 0 means DefaultLimit. It is capped at MaxLimit.
	Limit int
	// After is the decoded cursor; zero means the first page.
	After Cursor
	// Since, if set, drops items older than it.
	Since time.Time
}

// Cursor is the position after which the next page starts.
type Cursor struct {
	Time time.Time
	ID   string
}

func (c Cursor) IsZero() bool { return c.ID == "" && c.Time.IsZero() }

// Size returns the effective page size.
func (r Request) Size() int {
	switch {
	case r.Limit <= 0:
		return DefaultLimit
	case r.Limit > MaxLimit:
		return MaxLimit
	}
	return r.Limit
}

// Includes reports whether an item sorted at (t, id) comes after the cursor in
// newest-first order, that is, belongs to the requested page.
func (r Request) Includes(t time.Time, id string) bool {
	if !r.Since.IsZero() && t.Before(r.Since) {
		return false
	}
	if r.After.IsZero() {
		return true
	}
	return t.Before(r.After.Time) || (t.Equal(r.After.Time) && id < r.After.ID)
}

// Less orders items newest first, then by ID descending, the order every
// paginated list uses.
func Less(ti time.Time, idi string, tj time.Time, idj string) bool {
	if !ti.Equal(tj) {
		return ti.After(tj)
	}
	return idi > idj
}

// Encode returns the opaque cursor string for c.
func (c Cursor) Encode() string {
	if c.IsZero() {
		return ""
	}
	raw := strconv.FormatInt(c.Time.UnixNano(), 10) + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// Parse builds a Request from query parameters.
func Parse(limit, cursor string) (Request, error) {
	var req Request
	if limit != "" {
		n, err := strconv.Atoi(limit)
		if err != nil || n < 1 || n > MaxLimit {
			return Request{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalid, MaxLimit)
		}
		req.Limit = n
	}
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return Request{}, fmt.Errorf("%w: malformed cursor", ErrInvalid)
		}
		nanos, id, ok := strings.Cut(string(raw), "|")
		n, err := strconv.ParseInt(nanos, 10, 64)
		if !ok || err != nil || id == "" {
			return Request{}, fmt.Errorf("%w: malformed cursor", ErrInvalid)
		}
		req.After = Cursor{Time: time.Unix(0, n).UTC(), ID: id}
	}
	return req, nil
}

// Slice applies req to items already sorted newest first and returns the
// page and the cursor of the next one ("" when this is the last page).
func Slice[T any](items []T, req Request, key func(T) (time.Time, string)) ([]T, string) {
	out := make([]T, 0, req.Size())
	for _, item := range items {
		t, id := key(item)
		if !req.Includes(t, id) {
			continue
		}
		if len(out) == req.Size() {
			lt, lid := key(out[len(out)-1])
			return out, Cursor{Time: lt, ID: lid}.Encode()
		}
		out = append(out, item)
	}
	return out, ""
}
