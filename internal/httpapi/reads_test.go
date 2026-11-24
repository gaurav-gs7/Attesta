package httpapi

import (
	"context"
	"net/http"
	"testing"

	"github.com/gaurav-gs7/attesta/internal/page"
)

// Audit W-5: GET readiness, score and slos each created a workflow run, so
// 500 GETs wrote 500 runs. Reads must not write.
func TestReadsDoNotCreateWorkflowRuns(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			server := newTestServer(t, store)
			if code := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody).Code; code != http.StatusCreated {
				t.Fatalf("onboard: %d", code)
			}
			before, _, err := store.ListWorkflowRuns(context.Background(), page.Request{Limit: page.MaxLimit})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 500; i++ {
				for _, path := range []string{"readiness", "score", "slos", "error-budget"} {
					rec := authorizedRequest(server, http.MethodGet, "/api/v1/services/payments-api/"+path, nil)
					if rec.Code != http.StatusOK {
						t.Fatalf("GET %s: %d %s", path, rec.Code, rec.Body.String())
					}
				}
			}
			after, _, err := store.ListWorkflowRuns(context.Background(), page.Request{Limit: page.MaxLimit})
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != len(before) {
				t.Fatalf("2000 GETs changed the workflow run count from %d to %d", len(before), len(after))
			}
		})
	}
}
