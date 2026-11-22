package httpapi

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

// Audit W-2, as reproduced on PostgreSQL: 20 identical onboarding requests
// returned 1x201, 3x200 and 16x500, and four succeeded runs shared one key.
// Requests are spread over two API replicas sharing the store.
func TestConcurrentIdenticalOnboardingRunsOnce(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			replicas := []*Server{newTestServer(t, store), newTestServer(t, store)}

			const requests = 20
			codes := make([]int, requests)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					codes[i] = authorizedRequest(replicas[i%2], http.MethodPost, "/api/v1/services", onboardingBody).Code
				}(i)
			}
			close(start)
			wg.Wait()

			count := map[int]int{}
			for _, c := range codes {
				count[c]++
			}
			if count[http.StatusCreated] != 1 || count[http.StatusOK] != requests-1 {
				t.Fatalf("status codes = %v, want one 201 and %d 200s", count, requests-1)
			}

			runs, err := store.ListWorkflowRuns(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			active := 0
			for _, run := range runs {
				if run.IdempotencyKey == onboardingKeyFor(t) && run.State != workflows.StateFailed {
					active++
					if run.State != workflows.StateSucceeded {
						t.Fatalf("left a %s run behind: %+v", run.State, run)
					}
				}
			}
			if active != 1 {
				t.Fatalf("%d non-failed runs hold the onboarding key, want 1", active)
			}
		})
	}
}
