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
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/scaffold"
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

// Result is a scaffold rendered and written to a temporary directory, so
// tests can run the validator and build the generated service.
type Result struct {
	OutputPath string
	Files      []string
}

func generate(t *testing.T, service models.Service) Result {
	t.Helper()
	root := filepath.Clean("../..")
	rendered, err := NewGenerator(filepath.Join(root, "templates")).Render(service)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), service.Name)
	if err := scaffold.WriteDir(dir, rendered.Files); err != nil {
		t.Fatal(err)
	}
	result := Result{OutputPath: dir}
	for _, p := range rendered.Paths() {
		result.Files = append(result.Files, filepath.Join(dir, filepath.FromSlash(p)))
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
	out, err := exec.Command("bash", "../../scripts/validate-service-scaffold.sh", result.OutputPath).CombinedOutput()
	if err != nil {
		t.Fatalf("validator failed: %v\n%s", err, out)
	}

	// A tag reference must fail validation.
	ci := filepath.Join(result.OutputPath, ".github/workflows/ci.yml")
	raw, err := os.ReadFile(ci)
	if err != nil {
		t.Fatal(err)
	}
	unpinned := regexp.MustCompile(`actions/checkout@[0-9a-f]{40}`).ReplaceAllString(string(raw), "actions/checkout@v4")
	if err := os.WriteFile(ci, []byte(unpinned), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err = exec.Command("bash", "../../scripts/validate-service-scaffold.sh", result.OutputPath).CombinedOutput()
	if err == nil || !strings.Contains(string(out), "pinned to a full commit SHA") {
		t.Fatalf("validator accepted an unpinned action: %v\n%s", err, out)
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
	// Install the scaffold's pinned, hashed requirements where only this
	// test sees them.
	site := t.TempDir()
	install := exec.Command(python, "-m", "pip", "install", "--quiet", "--disable-pip-version-check",
		"--require-hashes", "--target", site, "-r", "requirements.txt")
	install.Dir = result.OutputPath
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("install the generated service's requirements: %v\n%s", err, out)
	}
	cmd := exec.Command(python, "-m", "app")
	cmd.Dir = result.OutputPath
	cmd.Env = append(os.Environ(), "PYTHONPATH="+site)
	assertCountsRequests(t, cmd)
}

func assertCountsRequests(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	port := freePort(t)
	cmd.Env = append(cmd.Environ(), "PORT="+port)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	base := "http://127.0.0.1:" + port
	waitFor(t, base+"/readyz")

	before := get(t, base+"/metrics")
	if strings.Contains(before, `status="200"`) {
		t.Fatalf("probes must not be counted:\n%s", before)
	}
	for i := 0; i < 3; i++ {
		get(t, base+"/")
	}
	// A request is counted after its response is written (that is when its
	// duration is known), so a scrape right after the response can precede
	// the count by a moment. Poll briefly instead of racing it.
	// Go's client prints 3, Python's 3.0.
	want := []*regexp.Regexp{
		regexp.MustCompile(`(?m)^http_requests_total\{service="payments-api",status="200"\} 3(\.0)?$`),
		regexp.MustCompile(`(?m)^http_request_duration_seconds_count\{service="payments-api"\} 3(\.0)?$`),
		// Label order differs between the clients and does not matter.
		regexp.MustCompile(`(?m)^http_request_duration_seconds_bucket\{(service="payments-api",le="\+Inf"|le="\+Inf",service="payments-api")\} 3(\.0)?$`),
	}
	var after string
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		after = get(t, base+"/metrics")
		missing := ""
		for _, w := range want {
			if !w.MatchString(after) {
				missing = w.String()
				break
			}
		}
		if missing == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics missing %q after 3 requests:\n%s", missing, after)
		}
	}
	if strings.Contains(after, "db_") {
		t.Fatalf("metrics still expose made-up database series:\n%s", after)
	}

	// Audit G-6: SIGTERM drains and exits cleanly.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("service did not exit cleanly on SIGTERM: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("service still running 5s after SIGTERM")
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

// Audit C-13: the controller rolls back only Deployments that name the guard.
func TestGeneratedDeploymentConsentsOnlyWhenRollbackIsOn(t *testing.T) {
	for _, rollback := range []bool{true, false} {
		service := testService("go")
		service.RollbackOnFailure = rollback
		raw := read(t, generate(t, service), "k8s/base/deployment.yaml")
		var d struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if err := yaml.Unmarshal([]byte(raw), &d); err != nil {
			t.Fatalf("deployment.yaml does not parse: %v\n%s", err, raw)
		}
		got, ok := d.Metadata.Annotations["attesta.io/rollback-guards"]
		if rollback && got != "payments-api-rollout" {
			t.Fatalf("rollback on: attesta.io/rollback-guards = %q, want payments-api-rollout", got)
		}
		if !rollback && ok {
			t.Fatalf("rollback off: deployment still consents to rollback (%q)", got)
		}
	}
}

// Audit S-6: burn-rate alerting used one 5-minute window. The generated
// rules record the error ratio over each window and alert on the two
// multi-window pairs.
func TestGeneratedBurnRateRulesAreMultiWindow(t *testing.T) {
	raw := read(t, generate(t, testService("go")), "observability/prometheus-rules.yaml")
	var rule struct {
		Spec struct {
			Groups []struct {
				Rules []struct {
					Record string `json:"record"`
					Alert  string `json:"alert"`
					Expr   string `json:"expr"`
				} `json:"rules"`
			} `json:"groups"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal([]byte(raw), &rule); err != nil {
		t.Fatalf("prometheus-rules.yaml does not parse: %v\n%s", err, raw)
	}
	records, alerts := map[string]bool{}, map[string]string{}
	for _, g := range rule.Spec.Groups {
		for _, r := range g.Rules {
			if r.Record != "" {
				records[r.Record] = true
			}
			if r.Alert != "" {
				alerts[r.Alert] = r.Expr
			}
		}
	}
	for _, w := range []string{"5m", "30m", "1h", "6h"} {
		if !records["attesta:http_error_ratio:rate"+w] {
			t.Errorf("no recording rule for the %s error ratio", w)
		}
	}
	fast, slow := alerts["payments_api_fast_burn"], alerts["payments_api_slow_burn"]
	if !strings.Contains(fast, "rate1h") || !strings.Contains(fast, "rate5m") || !strings.Contains(fast, "14.4 * (1 - 99.9 / 100)") {
		t.Errorf("fast burn alert is not the 1h+5m pair at 14.4x: %q", fast)
	}
	if !strings.Contains(slow, "rate6h") || !strings.Contains(slow, "rate30m") || !strings.Contains(slow, "6 * (1 - 99.9 / 100)") {
		t.Errorf("slow burn alert is not the 6h+30m pair at 6x: %q", slow)
	}
}

// Audit X-2, second layer: even a value that reached a template unvalidated
// stays one YAML scalar, because user values are rendered with quote.
func TestTemplatesQuoteUserValues(t *testing.T) {
	root := filepath.Clean("../..")
	evil := testService("go")
	evil.Team = "platform\n    pod-security.kubernetes.io/enforce: privileged"
	source := filepath.Join(root, "templates/argocd/application.yaml.tmpl")
	tmpl, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := renderTemplate(source, tmpl, data{Service: evil})
	if err != nil {
		t.Fatal(err)
	}
	var app struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			SyncPolicy struct {
				ManagedNamespaceMetadata struct {
					Labels map[string]string `json:"labels"`
				} `json:"managedNamespaceMetadata"`
			} `json:"syncPolicy"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(raw, &app); err != nil {
		t.Fatalf("application.yaml does not parse: %v\n%s", err, raw)
	}
	for name, labels := range map[string]map[string]string{
		"application": app.Metadata.Labels,
		"namespace":   app.Spec.SyncPolicy.ManagedNamespaceMetadata.Labels,
	} {
		if _, injected := labels["pod-security.kubernetes.io/enforce"]; injected || labels["attesta.dev/team"] != evil.Team {
			t.Fatalf("team value escaped its scalar in the %s labels: %v\n%s", name, labels, raw)
		}
	}

	// And the generator refuses such input outright.
	if _, err := NewGenerator(filepath.Join(root, "templates")).Render(evil); err == nil {
		t.Fatal("generator rendered a service with an injected team")
	}
}

// Audit X-8: the generated CI ran aquasecurity/trivy-action@master with
// packages: write granted to the whole workflow.
func TestGeneratedCIPinsActionsAndScopesWrites(t *testing.T) {
	raw := read(t, generate(t, testService("go")), ".github/workflows/ci.yml")
	var wf struct {
		Permissions map[string]string `json:"permissions"`
		Jobs        map[string]struct {
			If          string            `json:"if"`
			Needs       any               `json:"needs"`
			Permissions map[string]string `json:"permissions"`
			Steps       []struct {
				Uses string `json:"uses"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(raw), &wf); err != nil {
		t.Fatalf("ci.yml does not parse: %v\n%s", err, raw)
	}
	if len(wf.Permissions) != 1 || wf.Permissions["contents"] != "read" {
		t.Fatalf("workflow-level permissions = %v, want contents: read only", wf.Permissions)
	}
	pinned := regexp.MustCompile(`^[\w.-]+/[\w.-]+@[0-9a-f]{40}$`)
	for name, job := range wf.Jobs {
		for _, step := range job.Steps {
			if step.Uses != "" && !pinned.MatchString(step.Uses) {
				t.Errorf("job %s uses %q, not pinned to a commit SHA", name, step.Uses)
			}
		}
		if job.Permissions["packages"] == "write" && (name != "push" || !strings.Contains(job.If, "refs/heads/main") || job.Needs == nil) {
			t.Errorf("job %s can write packages without being the main-only push job after the checks", name)
		}
	}
	if wf.Jobs["push"].Permissions["packages"] != "write" {
		t.Error("the push job cannot push")
	}
}

func TestImageRegistryIsConfigured(t *testing.T) {
	root := filepath.Clean("../..")
	for registry, login := range map[string]string{
		"ghcr.io/acme":                    "secrets.GITHUB_TOKEN",
		"registry.example.com:5000/team/": "secrets.REGISTRY_PASSWORD",
	} {
		rendered, err := NewGenerator(filepath.Join(root, "templates"), WithImageRegistry(registry)).Render(testService("go"))
		if err != nil {
			t.Fatal(err)
		}
		var ci, deploy string
		for _, f := range rendered.Files {
			switch f.Path {
			case ".github/workflows/ci.yml":
				ci = string(f.Content)
			case "k8s/base/deployment.yaml":
				deploy = string(f.Content)
			}
		}
		host := strings.SplitN(registry, "/", 2)[0]
		image := strings.TrimSuffix(registry, "/") + "/payments-api"
		if !strings.Contains(ci, "IMAGE: "+image) || !strings.Contains(ci, "docker login "+host+" ") || !strings.Contains(ci, login) {
			t.Errorf("%s: CI does not push to the registry with %s:\n%s", registry, login, ci)
		}
		if strings.Contains(ci, ":latest") || strings.Contains(deploy, ":latest") || !strings.Contains(deploy, "image: "+image+"\n") {
			t.Errorf("%s: deployment image line wrong:\n%s", registry, deploy)
		}
	}
	for _, bad := range []string{"https://ghcr.io/acme", "GHCR.io/acme", "ghcr.io/acme/../x", ""} {
		if err := ValidateImageRegistry(bad); err == nil {
			t.Errorf("registry %q accepted", bad)
		}
	}
}
