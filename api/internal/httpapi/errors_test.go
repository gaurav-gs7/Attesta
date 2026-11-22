package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/api/internal/catalog"
	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/workflows"
)

// Audit W-10: the 409 for conflicting onboarding metadata was chosen by
// searching the run's error text.
func TestOnboardingConflictIsTyped(t *testing.T) {
	for name, open := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			server := newTestServer(t, store)
			if code := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody).Code; code != http.StatusCreated {
				t.Fatalf("onboard: %d", code)
			}
			changed := strings.Replace(string(onboardingBody), `"team":"platform"`, `"team":"payments"`, 1)
			rec := authorizedRequest(server, http.MethodPost, "/api/v1/services", []byte(changed))
			var body struct {
				ErrorCode string             `json:"errorCode"`
				Workflow  models.WorkflowRun `json:"workflow"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if rec.Code != http.StatusConflict || body.ErrorCode != workflows.CodeConflict {
				t.Fatalf("conflicting metadata: %d code %q, want 409 %q", rec.Code, body.ErrorCode, workflows.CodeConflict)
			}
			stored, ok := newTestServer(t, store).workflows.Get(body.Workflow.ID)
			if !ok || stored.ErrorCode != workflows.CodeConflict {
				t.Fatalf("stored run lost its error code: %+v", stored)
			}
		})
	}
}

type misleadingStore struct{ catalog.Store }

func (misleadingStore) CreateService(context.Context, models.Service) (models.Service, error) {
	return models.Service{}, errors.New("replica reports different metadata version; connection reset")
}

// A store failure whose message happens to contain "different metadata" is
// a 500, not a conflict: the status no longer depends on wording.
func TestStatusDoesNotDependOnErrorWording(t *testing.T) {
	server := newTestServer(t, misleadingStore{catalog.NewMemoryStore()})
	rec := authorizedRequest(server, http.MethodPost, "/api/v1/services", onboardingBody)
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"errorCode":"internal"`) {
		t.Fatalf("store failure: %d %s, want 500 with errorCode internal", rec.Code, rec.Body.String())
	}
}
