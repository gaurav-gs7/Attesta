package templates

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/gaurav-gs7/attesta/api/internal/models"
)

func testService(language string) models.Service {
	return models.Service{
		Name:               "payments-api",
		Team:               "platform",
		Owner:              "gaurav",
		Tier:               "critical",
		Language:           language,
		Namespace:          "payments",
		Environment:        "staging",
		SLOTarget:          99.9,
		SLOLatencyP95Ms:    450,
		SLOErrorRate:       2.5,
		DeploymentStrategy: "canary",
		RollbackOnFailure:  true,
	}
}

func generate(t *testing.T, service models.Service) Result {
	t.Helper()
	root := filepath.Clean("../../..")
	result, err := NewGenerator(filepath.Join(root, "templates"), t.TempDir()).Generate(service)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func read(t *testing.T, result Result, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(result.OutputPath, rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestGeneratorCreatesServiceScaffold(t *testing.T) {
	result := generate(t, testService("go"))
	required := []string{
		"Dockerfile",
		"attesta-service.yaml",
		"slo.yaml",
		"rollout.yaml",
		".github/workflows/ci.yml",
		"k8s/base/deployment.yaml",
		"k8s/base/pdb.yaml",
		"observability/dashboard.json",
		"observability/servicemonitor.yaml",
		"observability/prometheus-rules.yaml",
		"runbooks/incident-runbook.md",
		"infra/argocd/application.yaml",
		"observability/otel-collector.yaml",
		"observability/jaeger.yaml",
	}
	for _, rel := range required {
		if _, err := os.Stat(filepath.Join(result.OutputPath, rel)); err != nil {
			t.Fatalf("expected generated file %s: %v", rel, err)
		}
	}
}

func TestGeneratedManifestsParse(t *testing.T) {
	result := generate(t, testService("go"))
	for _, file := range result.Files {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		switch filepath.Ext(file) {
		case ".yaml", ".yml":
			for i, doc := range strings.Split(string(b), "\n---") {
				var out any
				if err := yaml.Unmarshal([]byte(doc), &out); err != nil {
					t.Fatalf("%s document %d does not parse: %v", file, i, err)
				}
			}
		case ".json":
			var out any
			if err := json.Unmarshal(b, &out); err != nil {
				t.Fatalf("%s does not parse: %v", file, err)
			}
		}
	}
}

// Audit G-2: alert thresholds and the runbook used fixed 1%, 300ms and 200ms.
func TestGeneratedThresholdsComeFromTheSLO(t *testing.T) {
	result := generate(t, testService("go"))
	alerts := read(t, result, "observability/alerts.yaml")
	for _, want := range []string{"* 100 > 2.5", "* 1000 > 450"} {
		if !strings.Contains(alerts, want) {
			t.Fatalf("alerts.yaml missing threshold %q:\n%s", want, alerts)
		}
	}
	for _, unwanted := range []string{"> 300", "> 200", "db_query_duration", "runbook_url"} {
		if strings.Contains(alerts, unwanted) {
			t.Fatalf("alerts.yaml still contains %q:\n%s", unwanted, alerts)
		}
	}
	runbook := read(t, result, "runbooks/incident-runbook.md")
	if !strings.Contains(runbook, "450ms") || !strings.Contains(runbook, "2.5%") || strings.Contains(runbook, "300ms") {
		t.Fatalf("runbook does not use the service's SLO:\n%s", runbook)
	}
	if !strings.Contains(runbook, "TODO: register a pager rotation") {
		t.Fatalf("runbook should mark the missing pager:\n%s", runbook)
	}
	if strings.Contains(read(t, result, "observability/dashboard.json"), "db_") {
		t.Fatal("dashboard still charts database metrics the service does not emit")
	}
}

func TestGeneratedRunbookLinksWhenRegistered(t *testing.T) {
	service := testService("go")
	service.RunbookURL = "https://runbooks.example.internal/payments-api"
	alerts := read(t, generate(t, service), "observability/alerts.yaml")
	if strings.Count(alerts, `runbook_url: "https://runbooks.example.internal/payments-api"`) != 2 {
		t.Fatalf("expected the registered runbook on both alerts:\n%s", alerts)
	}
}

func TestGeneratedScaffoldPassesValidator(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	result := generate(t, testService("go"))
	out, err := exec.Command("bash", "../../../scripts/validate-service-scaffold.sh", result.OutputPath).CombinedOutput()
	if err != nil {
		t.Fatalf("validator failed: %v\n%s", err, out)
	}
}

// Audit G-1: the generated service used to serve a constant /metrics body,
// so every SLO query against it returned zero or NaN.
func TestGeneratedGoServiceCountsRealRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the generated service")
	}
	result := generate(t, testService("go"))
	bin := filepath.Join(t.TempDir(), "service")
	build := exec.Command("go", "build", "-o", bin, "./cmd/server")
	build.Dir = result.OutputPath
	build.Env = append(os.Environ(), "GOFLAGS=", "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("generated Go service does not build: %v\n%s", err, out)
	}
	assertCountsRequests(t, exec.Command(bin))
}

func TestGeneratedPythonServiceCountsRealRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the generated service")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	result := generate(t, testService("python"))
	cmd := exec.Command(python, "-m", "app")
	cmd.Dir = result.OutputPath
	assertCountsRequests(t, cmd)
}

func assertCountsRequests(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	port := freePort(t)
	cmd.Env = append(os.Environ(), "PORT="+port)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	base := "http://127.0.0.1:" + port
	waitFor(t, base+"/readyz")

	before := get(t, base+"/metrics")
	if strings.Contains(before, `status="200"`) {
		t.Fatalf("probes must not be counted:\n%s", before)
	}
	for i := 0; i < 3; i++ {
		get(t, base+"/")
	}
	after := get(t, base+"/metrics")
	for _, want := range []string{
		`http_requests_total{service="payments-api",status="200"} 3`,
		`http_request_duration_seconds_count{service="payments-api"} 3`,
		`http_request_duration_seconds_bucket{service="payments-api",le="+Inf"} 3`,
	} {
		if !strings.Contains(after, want) {
			t.Fatalf("metrics missing %q after 3 requests:\n%s", want, after)
		}
	}
	if strings.Contains(after, "db_") {
		t.Fatalf("metrics still expose made-up database series:\n%s", after)
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
}

func waitFor(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(url); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready", url)
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Audit C-12: with controller rollback on, Argo CD self-heal would re-apply
// the bad template from Git and undo the rollback.
func TestArgoSelfHealIsOffWhenTheControllerOwnsRollback(t *testing.T) {
	for _, tc := range []struct {
		rollback     bool
		wantSelfHeal bool
	}{{true, false}, {false, true}} {
		service := testService("go")
		service.RollbackOnFailure = tc.rollback
		raw := read(t, generate(t, service), "infra/argocd/application.yaml")
		var app struct {
			Spec struct {
				SyncPolicy struct {
					Automated struct {
						Prune    bool `json:"prune"`
						SelfHeal bool `json:"selfHeal"`
					} `json:"automated"`
				} `json:"syncPolicy"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(raw), &app); err != nil {
			t.Fatalf("application.yaml does not parse: %v\n%s", err, raw)
		}
		if got := app.Spec.SyncPolicy.Automated.SelfHeal; got != tc.wantSelfHeal {
			t.Fatalf("rollbackOnFailure=%v: selfHeal=%v, want %v\n%s", tc.rollback, got, tc.wantSelfHeal, raw)
		}
		if !app.Spec.SyncPolicy.Automated.Prune {
			t.Fatalf("rollbackOnFailure=%v: automated sync lost prune", tc.rollback)
		}
	}
}
