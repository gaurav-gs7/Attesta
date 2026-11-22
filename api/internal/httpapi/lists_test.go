package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/page"
)

// walk follows nextCursor through a list endpoint and returns the IDs of
// every item, failing on a repeat.
func walk(t *testing.T, server *Server, path, field, idField string, limit int) []string {
	t.Helper()
	var ids []string
	seen := map[string]bool{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatal("pagination does not terminate")
		}
		q := url.Values{"limit": {fmt.Sprint(limit)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		rec := authorizedRequest(server, http.MethodGet, path+"?"+q.Encode(), nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: %d %s", path, rec.Code, rec.Body.String())
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		var items []map[string]any
		if err := json.Unmarshal(body[field], &items); err != nil {
			t.Fatalf("%s: %v in %s", field, err, rec.Body.String())
		}
		if len(items) > limit {
			t.Fatalf("page of %d items, limit %d", len(items), limit)
		}
		for _, item := range items {
			id := fmt.Sprint(item[idField])
			if inc, ok := item["incident"].(map[string]any); ok {
				id = fmt.Sprint(inc["id"])
			}
			if seen[id] {
				t.Fatalf("%s returned %s twice", path, id)
			}
			seen[id] = true
			ids = append(ids, id)
		}
		next, ok := body["nextCursor"]
		if !ok {
			return ids
		}
		if err := json.Unmarshal(next, &cursor); err != nil {
			t.Fatal(err)
		}
	}
}

// Audit W-6: lists were capped at 100 runs, 100 incidents and 50
// deployments, with no way to see the rest.
func TestListsPaginateBeyondTheOldCaps(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			server := newTestServer(t, store)
			if code := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody).Code; code != http.StatusCreated {
				t.Fatalf("onboard: %d", code)
			}
			service, err := store.GetServiceByName(context.Background(), "payments-api")
			if err != nil {
				t.Fatal(err)
			}
			base := time.Now().UTC().Add(-time.Hour)
			for i := 0; i < 120; i++ {
				// Pairs share a start time to exercise the ID tiebreak.
				if _, err := store.RecordDeployment(context.Background(), models.Deployment{
					ServiceID: service.ID, Version: fmt.Sprintf("v%d", i), Environment: "staging",
					Status: "succeeded", Strategy: "canary", StartedAt: base.Add(time.Duration(i/2) * time.Second),
				}); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 130; i++ {
				rec := authorizedRequest(server, http.MethodPost, "/api/v1/incidents", []byte(
					fmt.Sprintf(`{"service":"payments-api","severity":"sev3","title":"t%d"}`, i)))
				if rec.Code != http.StatusCreated {
					t.Fatalf("incident %d: %d %s", i, rec.Code, rec.Body.String())
				}
			}

			if got := walk(t, server, "/api/v1/services/payments-api/deployments", "deployments", "id", 7); len(got) != 121 {
				t.Fatalf("walked %d deployments, want 121", len(got))
			}
			if got := walk(t, server, "/api/v1/incidents", "incidents", "id", 9); len(got) != 130 {
				t.Fatalf("walked %d incidents, want 130", len(got))
			}
			if got := walk(t, server, "/api/v1/workflows", "workflows", "id", 11); len(got) != 131 {
				t.Fatalf("walked %d workflow runs, want 131", len(got))
			}
		})
	}
}

func TestListsRejectBadPagination(t *testing.T) {
	server := newTestServer(t, catalog.NewMemoryStore())
	for _, q := range []string{"limit=0", "limit=500", "limit=abc", "cursor=%21%21"} {
		if rec := authorizedRequest(server, http.MethodGet, "/api/v1/workflows?"+q, nil); rec.Code != http.StatusBadRequest {
			t.Fatalf("?%s: %d, want 400", q, rec.Code)
		}
	}
}

type listFailingStore struct{ catalog.Store }

func (listFailingStore) ListWorkflowRuns(context.Context, page.Request) ([]models.WorkflowRun, string, error) {
	return nil, "", errors.New("connection refused")
}

// Audit W-6: with the database stopped the endpoint answered 200 from an
// in-memory cache.
func TestListWorkflowsSurfacesStoreErrors(t *testing.T) {
	server := newTestServer(t, listFailingStore{catalog.NewMemoryStore()})
	if rec := authorizedRequest(server, http.MethodGet, "/api/v1/workflows", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("GET /workflows with the store down: %d %s, want 500", rec.Code, rec.Body.String())
	}
}
