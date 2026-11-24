package page

import (
	"errors"
	"testing"
	"time"
)

type item struct {
	t  time.Time
	id string
}

func TestSliceWalksEveryItemOnce(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	var items []item
	for i := 0; i < 23; i++ {
		// Pairs share a timestamp to exercise the ID tiebreak.
		items = append(items, item{base.Add(-time.Duration(i/2) * time.Second), string(rune('z' - i))})
	}
	key := func(it item) (time.Time, string) { return it.t, it.id }
	seen := map[string]bool{}
	req := Request{Limit: 5}
	pages := 0
	for {
		got, next := Slice(items, req, key)
		pages++
		for _, it := range got {
			if seen[it.id] {
				t.Fatalf("item %s returned twice", it.id)
			}
			seen[it.id] = true
		}
		if next == "" {
			break
		}
		var err error
		if req, err = Parse("5", next); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != len(items) || pages != 5 {
		t.Fatalf("saw %d items in %d pages, want %d in 5", len(seen), pages, len(items))
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	for _, tc := range []struct{ limit, cursor string }{
		{"0", ""}, {"201", ""}, {"x", ""}, {"", "!!!"}, {"", "bm9waXBl"},
	} {
		if _, err := Parse(tc.limit, tc.cursor); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Parse(%q, %q) = %v, want ErrInvalid", tc.limit, tc.cursor, err)
		}
	}
	if r, err := Parse("", ""); err != nil || r.Size() != DefaultLimit {
		t.Fatalf("defaults: %+v %v", r, err)
	}
}
