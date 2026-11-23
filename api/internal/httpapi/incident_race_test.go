package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
)

// barrierStore holds the first n reads of an incident until all n have
// happened, so n concurrent requests act on the same version.
type barrierStore struct {
	catalog.Store
	mu      sync.Mutex
	waiting int
	n       int
	release chan struct{}
}

func newBarrierStore(s catalog.Store, n int) *barrierStore {
	return &barrierStore{Store: s, n: n, release: make(chan struct{})}
}

func (b *barrierStore) wait() {
	b.mu.Lock()
	if b.n == 0 {
		b.mu.Unlock()
		return
	}
	b.waiting++
	if b.waiting == b.n {
		b.n = 0
		close(b.release)
	}
	b.mu.Unlock()
	select {
	case <-b.release:
	case <-time.After(5 * time.Second):
	}
}

func (b *barrierStore) arm(n int) {
	b.mu.Lock()
	b.n, b.waiting, b.release = n, 0, make(chan struct{})
	b.mu.Unlock()
}

func (b *barrierStore) GetIncidentRecord(ctx context.Context, id string) (models.IncidentRecord, error) {
	r, err := b.Store.GetIncidentRecord(ctx, id)
	b.wait()
	return r, err
}

func (b *barrierStore) FindActiveIncident(ctx context.Context, service, fp string) (models.IncidentRecord, error) {
	r, err := b.Store.FindActiveIncident(ctx, service, fp)
	b.wait()
	return r, err
}

// Audit D-5: resolve had no concurrency check, so two concurrent
// resolutions both wrote, each overwriting the other's note and events.
func TestConcurrentResolvesApplyOnce(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			store := newBarrierStore(open(t), 0)
			server := newTestServer(t, store)
			onboard(t, server)
			record := openIncident(t, server, `{"service":"payments-api","alertName":"HighErrorRate"}`)

			store.arm(2)
			codes := make(chan int, 2)
			bodies := make(chan string, 2)
			var wg sync.WaitGroup
			for _, note := range []string{"alice reverted", "bob reverted"} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rec := authorizedRequest(server, http.MethodPost, "/api/v1/incidents/"+record.Incident.ID+"/resolve", []byte(`{"note":"`+note+`"}`))
					codes <- rec.Code
					bodies <- rec.Body.String()
				}()
			}
			wg.Wait()
			close(codes)
			close(bodies)
			for code := range codes {
				if code != http.StatusOK {
					t.Fatalf("concurrent resolve: %d %v", code, <-bodies)
				}
			}
			stored, err := store.Store.GetIncidentRecord(context.Background(), record.Incident.ID)
			if err != nil {
				t.Fatal(err)
			}
			resolvedEvents := 0
			for _, e := range stored.Events {
				if e.EventType == "resolved" {
					resolvedEvents++
				}
			}
			if resolvedEvents != 1 || stored.Incident.Version != 2 {
				t.Fatalf("%d resolution events at version %d; want one resolution", resolvedEvents, stored.Incident.Version)
			}
		})
	}
}

// Two notifications for one firing alert used to open two incidents.
func TestConcurrentFiringAlertsOpenOneIncident(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			store := newBarrierStore(open(t), 0)
			server := newTestServer(t, store)
			onboard(t, server)
			store.arm(2)
			firing := `{"status":"firing","labels":{"alertname":"HighErrorRate","service":"payments-api","severity":"page"},
				"annotations":{"summary":"5xx"},"fingerprint":"fp-race"}`
			actions := make(chan string, 2)
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					actions <- postAlertmanager(t, server, "firing", firing).Results[0].Action
				}()
			}
			wg.Wait()
			close(actions)
			seen := map[string]int{}
			for a := range actions {
				seen[a]++
			}
			if seen["opened"] != 1 || seen["duplicate"] != 1 {
				t.Fatalf("actions %v, want one opened and one duplicate", seen)
			}
			rec := authorizedRequest(server, http.MethodGet, "/api/v1/incidents", nil)
			var list struct {
				Incidents []json.RawMessage `json:"incidents"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || len(list.Incidents) != 1 {
				t.Fatalf("incidents after a race: %s", rec.Body.String())
			}
		})
	}
}
