package security

import (
	"strings"
	"testing"

	"github.com/gaurav-gs7/attesta/internal/models"
)

func TestValidateServiceRejectsInvalidName(t *testing.T) {
	findings := ValidateService(models.Service{
		Name:               "Payments_API",
		Team:               "platform",
		Owner:              "gaurav",
		Language:           "go",
		Namespace:          "platform",
		Environment:        "staging",
		SLOTarget:          99.9,
		DeploymentStrategy: "canary",
	})
	if len(findings) == 0 {
		t.Fatal("expected a finding")
	}
}

func TestValidateServiceAcceptsProductionReadyMetadata(t *testing.T) {
	findings := ValidateService(models.Service{
		Name:               "payments-api",
		Team:               "platform",
		Owner:              "gaurav",
		Tier:               "critical",
		Language:           "go",
		Namespace:          "payments",
		Environment:        "staging",
		SLOTarget:          99.9,
		SLOLatencyP95Ms:    300,
		SLOErrorRate:       1,
		DeploymentStrategy: "canary",
		RollbackOnFailure:  true,
	})
	if len(findings) != 0 {
		t.Fatalf("unexpected findings: %+v", findings)
	}
}

func validService() models.Service {
	return models.Service{
		Name: "payments-api", Team: "platform", Owner: "gaurav.gs@example.com", Tier: "critical",
		Language: "go", Namespace: "payments", Environment: "staging", SLOTarget: 99.9,
		SLOLatencyP95Ms: 300, SLOErrorRate: 1, DeploymentStrategy: "canary",
		Pager: "pagerduty:PABC123", Repository: "acme/payments-api",
		RunbookURL: "https://runbooks.example.com/payments-api#rollback", DashboardURL: "https://grafana.example.com/d/abc?var-svc=payments",
		Dependencies: []string{"ledger", "fraud-check"},
	}
}

// Audit X-2: team, owner, pager, the URLs and dependencies were not
// validated, and repository accepted newlines. A team containing a newline
// added a privileged pod-security label to the generated Namespace.
func TestValidateServiceRejectsInjection(t *testing.T) {
	if findings := ValidateService(validService()); len(findings) != 0 {
		t.Fatalf("valid service rejected: %v", findings)
	}
	cases := map[string]func(*models.Service){
		"team with newline":       func(s *models.Service) { s.Team = "platform\n    pod-security.kubernetes.io/enforce: privileged" },
		"team not a DNS label":    func(s *models.Service) { s.Team = "Platform Team" },
		"owner with quote":        func(s *models.Service) { s.Owner = `gaurav" injected: "x` },
		"owner too long":          func(s *models.Service) { s.Owner = strings.Repeat("a", 129) },
		"pager with newline":      func(s *models.Service) { s.Pager = "oncall\nkind: Secret" },
		"repository with newline": func(s *models.Service) { s.Repository = "acme/repo\nmalicious: true" },
		"repository http":         func(s *models.Service) { s.Repository = "http://github.com/acme/repo" },
		"runbook javascript":      func(s *models.Service) { s.RunbookURL = "javascript:alert(1)" },
		"runbook with space":      func(s *models.Service) { s.RunbookURL = "https://x.example/a b" },
		"dashboard with quote":    func(s *models.Service) { s.DashboardURL = `https://x.example/"}` },
		"dashboard with creds":    func(s *models.Service) { s.DashboardURL = "https://user:pw@grafana.example.com" },
		"dependency with colon":   func(s *models.Service) { s.Dependencies = []string{"ledger: {x: y}"} },
		"too many dependencies": func(s *models.Service) {
			s.Dependencies = make([]string, 51)
			for i := range s.Dependencies {
				s.Dependencies[i] = "svc"
			}
		},
	}
	for name, mutate := range cases {
		svc := validService()
		mutate(&svc)
		if findings := ValidateService(svc); len(findings) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
}
