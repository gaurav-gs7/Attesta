package readiness

import (
	"testing"

	"github.com/gaurav-gs7/attesta/internal/models"
)

func complete() models.Service {
	return models.Service{
		Name: "payments-api", Owner: "gaurav", Team: "platform", Tier: "critical", Pager: "payments-oncall",
		RunbookURL: "https://runbooks.example.com/payments", DashboardURL: "https://grafana.example.com/d/payments",
		SLOTarget: 99.9, SLOLatencyP95Ms: 300, SLOErrorRate: 1, DeploymentStrategy: "canary",
		RollbackOnFailure: true, Namespace: "payments", Repository: "github.com/acme/payments-api",
		Dependencies: []string{"ledger"},
	}
}

func TestEvaluate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*models.Service)
		score  int
		failed []string
	}{
		{"complete", func(*models.Service) {}, 100, nil},
		// An unset tier is not critical, so the dependency check passes: 5 of 95.
		{"nothing registered", func(s *models.Service) { *s = models.Service{Name: "x"} }, 5, []string{
			"ownership", "pager", "tier", "runbook", "dashboard", "slo", "latency-slo", "error-rate-policy",
			"deployment-strategy", "rollback-policy", "namespace", "repository"}},
		// Scores truncate: 90 of 95 is 94.
		{"critical without dependencies", func(s *models.Service) { s.Dependencies = nil }, 94, []string{"dependencies"}},
		{"non-critical without dependencies", func(s *models.Service) { s.Tier, s.Dependencies = "low", nil }, 100, nil},
		{"SLO above 99.99", func(s *models.Service) { s.SLOTarget = 99.999 }, 89, []string{"slo"}},
		{"SLO below 95", func(s *models.Service) { s.SLOTarget = 90 }, 89, []string{"slo"}},
		{"owner without team", func(s *models.Service) { s.Team = "" }, 89, []string{"ownership"}},
		{"repository by URL only", func(s *models.Service) { s.Repository, s.RepoURL = "", "https://github.com/acme/x" }, 100, nil},
		{"unknown tier", func(s *models.Service) { s.Tier = "gold" }, 93, []string{"tier"}},
		{"rollback off", func(s *models.Service) { s.RollbackOnFailure = false }, 91, []string{"rollback-policy"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := complete()
			tc.mutate(&svc)
			report := Evaluate(svc)
			if report.Score != tc.score {
				t.Errorf("score %d, want %d", report.Score, tc.score)
			}
			var failed []string
			for _, c := range report.Failed {
				failed = append(failed, c.Name)
				if c.Recommendation == "" {
					t.Errorf("failed check %s has no recommendation", c.Name)
				}
			}
			if len(failed) != len(tc.failed) {
				t.Fatalf("failed %v, want %v", failed, tc.failed)
			}
			for i := range failed {
				if failed[i] != tc.failed[i] {
					t.Fatalf("failed %v, want %v", failed, tc.failed)
				}
			}
			if len(report.Passed)+len(report.Failed) != 13 || len(report.Recommendations) != len(report.Failed) {
				t.Errorf("%d passed, %d failed, %d recommendations", len(report.Passed), len(report.Failed), len(report.Recommendations))
			}
			if report.Scope == "" {
				t.Error("the report does not say it scores metadata only")
			}
		})
	}
}
