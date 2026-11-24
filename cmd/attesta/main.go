package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gaurav-gs7/attesta/internal/incidents"
	"github.com/gaurav-gs7/attesta/internal/models"
	"github.com/gaurav-gs7/attesta/internal/scaffold"
)

// serviceName is a DNS label, as the API requires; checked before the name
// becomes a directory.
var serviceName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

func main() {
	err := run(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0) // the flag set printed its usage
	}
	if errors.Is(err, errUsage) {
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "attesta: %v\n", err)
		os.Exit(1)
	}
}

var errUsage = errors.New("usage")

// stdout is where responses are printed; tests replace it.
var stdout io.Writer = os.Stdout

// run executes one CLI command. It returns errUsage for an unknown or
// missing command.
func run(args []string) error {
	if len(args) == 0 {
		return errUsage
	}
	rest := args[1:]
	switch args[0] {
	case "service":
		return service(rest)
	case "onboard":
		return onboard(rest)
	case "list":
		return get("/api/v1/services")
	case "status":
		return serviceGet(rest, "status", "/api/v1/services/%s/status")
	case "deployments":
		return serviceGet(rest, "deployments", "/api/v1/services/%s/deployments")
	case "rollback":
		return servicePost(rest, "rollback", "/api/v1/services/%s/rollback", nil)
	case "check":
		return serviceGetByArg(rest, "check", "/api/v1/services/%s/readiness")
	case "score":
		return serviceGetByArg(rest, "score", "/api/v1/services/%s/score")
	case "slo":
		return slo(rest)
	case "rollout":
		return rollout(rest)
	case "incident":
		return incident(rest)
	case "workflow", "workflows":
		return workflow(rest)
	case "verify":
		return verify(rest)
	default:
		return errUsage
	}
}

func service(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("service subcommand is required")
	}
	switch args[0] {
	case "init":
		return onboardInit(args[1:])
	case "register":
		return fmt.Errorf("service register is intentionally file-driven in GitOps; use service init for this local MVP")
	case "list":
		return get("/api/v1/services")
	case "describe":
		return serviceGetByArg(args[1:], "service describe", "/api/v1/services/%s")
	case "scaffold":
		return serviceScaffold(args[1:])
	default:
		return fmt.Errorf("unknown service subcommand %q", args[0])
	}
}

func onboard(args []string) error {
	fs := flag.NewFlagSet("onboard", flag.ContinueOnError)
	req := models.CreateServiceRequest{}
	fs.StringVar(&req.Name, "name", "", "service name")
	fs.StringVar(&req.Language, "language", "go", "service language: go or python")
	fs.StringVar(&req.Team, "team", "", "owning team")
	fs.StringVar(&req.Owner, "owner", "", "service owner")
	fs.StringVar(&req.Tier, "tier", "medium", "service tier: critical, high, medium, low")
	fs.StringVar(&req.Namespace, "namespace", "", "kubernetes namespace")
	fs.StringVar(&req.Environment, "env", "staging", "environment")
	fs.StringVar(&req.SLO, "slo", "99.9", "availability SLO target")
	fs.StringVar(&req.SLOLatencyP95, "slo-latency-p95", "300ms", "p95 latency SLO")
	fs.Float64Var(&req.SLOErrorRate, "slo-error-rate", 1, "max error-rate percentage")
	fs.StringVar(&req.DeploymentStrategy, "strategy", "canary", "deployment strategy")
	fs.StringVar(&req.RepoURL, "repo-url", "", "service repository URL")
	fs.StringVar(&req.Repository, "repository", "", "service repository URL or owner/repo")
	fs.StringVar(&req.Pager, "pager", "", "pager/on-call rotation")
	fs.StringVar(&req.RunbookURL, "runbook", "", "runbook URL")
	fs.StringVar(&req.DashboardURL, "dashboard", "", "dashboard URL")
	output := fs.String("output", "generated/services", "directory to write the scaffold into, as <output>/<name>; empty to skip the download")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if req.Name == "" || req.Owner == "" {
		return fmt.Errorf("--name and --owner are required")
	}
	if req.Team == "" {
		req.Team = req.Owner
	}

	if err := post("/api/v1/services", req); err != nil {
		return err
	}
	if *output == "" {
		return nil
	}
	return fetchScaffold(req.Name, *output)
}

func serviceScaffold(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("service name is required")
	}
	fs := flag.NewFlagSet("service scaffold", flag.ContinueOnError)
	output := fs.String("output", "generated/services", "directory to write the scaffold into, as <output>/<name>")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	return fetchScaffold(args[0], *output)
}

// fetchScaffold downloads the service's scaffold archive and writes it to
// output/name. The archive is checked (regular files only, all under
// name/, size limits) and written to a temporary directory renamed into
// place, so a failure leaves any earlier scaffold untouched.
func fetchScaffold(name, output string) error {
	if !serviceName.MatchString(name) {
		return fmt.Errorf("service name %q is not a DNS label", name)
	}
	raw, err := download("/api/v1/services/" + url.PathEscape(name) + "/scaffold")
	if err != nil {
		return err
	}
	files, err := scaffold.ReadTarGz(bytes.NewReader(raw), name)
	if err != nil {
		return err
	}
	dir := filepath.Join(output, name)
	if err := scaffold.WriteDir(dir, files); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "scaffold: %d files written to %s\n", len(files), dir)
	return nil
}

func download(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL(), "/")+path, nil)
	if err != nil {
		return nil, err
	}
	if token := os.Getenv("ATTESTA_API_TOKEN"); token != "" {
		req.Header.Set("X-Attesta-Token", token)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, scaffold.MaxBytes+1))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("api returned %s: %s", resp.Status, string(body))
	}
	if len(body) > scaffold.MaxBytes {
		return nil, fmt.Errorf("scaffold archive is over %d bytes", scaffold.MaxBytes)
	}
	return body, nil
}

func onboardInit(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("service name is required")
	}
	name := args[0]
	reqArgs := append([]string{"--name", name}, args[1:]...)
	normalized := make([]string, 0, len(reqArgs))
	for i := 0; i < len(reqArgs); i++ {
		switch reqArgs[i] {
		case "--slo-availability":
			normalized = append(normalized, "--slo")
		case "--deployment":
			normalized = append(normalized, "--strategy")
		default:
			normalized = append(normalized, reqArgs[i])
		}
	}
	return onboard(normalized)
}

func serviceGet(args []string, command, path string) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	name := fs.String("name", "", "service name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	return get(fmt.Sprintf(path, *name))
}

func serviceGetByArg(args []string, command, path string) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	nameFlag := fs.String("name", "", "service name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	name := *nameFlag
	if name == "" && fs.NArg() > 0 {
		name = fs.Arg(0)
	}
	if name == "" {
		return fmt.Errorf("service name is required")
	}
	return get(fmt.Sprintf(path, name))
}

func servicePost(args []string, command, path string, payload any) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	name := fs.String("name", "", "service name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	return post(fmt.Sprintf(path, *name), payload)
}

func slo(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("slo subcommand is required")
	}
	switch args[0] {
	case "status":
		return serviceGetByArg(args[1:], "slo status", "/api/v1/services/%s/slos")
	default:
		return fmt.Errorf("unknown slo subcommand %q", args[0])
	}
}

func rollout(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("rollout subcommand is required")
	}
	switch args[0] {
	case "status":
		return serviceGetByArg(args[1:], "rollout status", "/api/v1/services/%s/status")
	case "rollback":
		return servicePostByArg(args[1:], "rollout rollback", "/api/v1/services/%s/rollback", nil)
	case "start", "pause":
		return fmt.Errorf("rollout %s is represented by the local CI/CD runner in this MVP", args[0])
	default:
		return fmt.Errorf("unknown rollout subcommand %q", args[0])
	}
}

func incident(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("incident subcommand is required")
	}
	switch args[0] {
	case "create":
		return incidentCreate(args[1:])
	case "list":
		return get("/api/v1/incidents")
	case "show":
		id, err := firstArg(args[1:], "incident id")
		if err != nil {
			return err
		}
		return get("/api/v1/incidents/" + id)
	case "timeline":
		id, err := firstArg(args[1:], "incident id")
		if err != nil {
			return err
		}
		return get("/api/v1/incidents/" + id + "/timeline")
	case "postmortem":
		id, err := firstArg(args[1:], "incident id")
		if err != nil {
			return err
		}
		return get("/api/v1/incidents/" + id + "/postmortem")
	default:
		return fmt.Errorf("unknown incident subcommand %q", args[0])
	}
}

func incidentCreate(args []string) error {
	fs := flag.NewFlagSet("incident create", flag.ContinueOnError)
	req := incidents.AlertRequest{}
	fs.StringVar(&req.Service, "service", "", "service name")
	fs.StringVar(&req.Severity, "severity", "", "incident severity")
	fs.StringVar(&req.AlertName, "alert", "", "alert name")
	fs.StringVar(&req.Title, "title", "", "incident title (defaults to the alert name)")
	errorRate := fs.Float64("error-rate", 0, "error rate percentage reported by the alert (optional)")
	latency := fs.Float64("latency-p95-ms", 0, "p95 latency in milliseconds reported by the alert (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "error-rate":
			req.ErrorRate = errorRate
		case "latency-p95-ms":
			req.Latency = latency
		}
	})
	if req.Service == "" && fs.NArg() > 0 {
		req.Service = fs.Arg(0)
	}
	if req.Service == "" {
		return fmt.Errorf("service is required")
	}
	return post("/api/v1/incidents", req)
}

func workflow(args []string) error {
	if len(args) == 0 {
		return get("/api/v1/workflows")
	}
	switch args[0] {
	case "list":
		return get("/api/v1/workflows")
	case "show":
		id, err := firstArg(args[1:], "workflow id")
		if err != nil {
			return err
		}
		return get("/api/v1/workflows/" + id)
	default:
		return fmt.Errorf("unknown workflow subcommand %q", args[0])
	}
}

func servicePostByArg(args []string, command, path string, payload any) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	nameFlag := fs.String("name", "", "service name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	name := *nameFlag
	if name == "" && fs.NArg() > 0 {
		name = fs.Arg(0)
	}
	if name == "" {
		return fmt.Errorf("service name is required")
	}
	return post(fmt.Sprintf(path, name), payload)
}

func firstArg(args []string, label string) (string, error) {
	if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
		return "", fmt.Errorf("%s is required", label)
	}
	return args[0], nil
}

func get(path string) error {
	return call(http.MethodGet, path, nil)
}

func post(path string, payload any) error {
	return call(http.MethodPost, path, payload)
}

func call(method, path string, payload any) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, strings.TrimRight(baseURL(), "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token := os.Getenv("ATTESTA_API_TOKEN"); token != "" {
		req.Header.Set("X-Attesta-Token", token)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("api returned %s: %s", resp.Status, string(responseBody))
	}
	printJSON(responseBody)
	return nil
}

func printJSON(raw []byte) {
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		_, _ = fmt.Fprintln(stdout, string(raw))
		return
	}
	formatted, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		_, _ = fmt.Fprintln(stdout, string(raw))
		return
	}
	_, _ = fmt.Fprintln(stdout, string(formatted))
}

func baseURL() string {
	value := os.Getenv("ATTESTA_API_URL")
	if value == "" {
		return "http://127.0.0.1:8080"
	}
	return value
}

func usage() {
	fmt.Fprintf(os.Stderr, `Attesta CLI

Usage:
  attesta service init payments-api --owner payments-platform --team platform --tier critical --language go --slo-availability 99.9 --slo-latency-p95 300ms --deployment canary --pager payments-oncall
  attesta onboard --name payments-api --language go --team platform --owner gaurav --env staging --slo 99.9 --strategy canary
  attesta service list
  attesta service describe payments-api
  attesta service scaffold payments-api --output generated/services
  attesta check payments-api
  attesta score payments-api
  attesta slo status payments-api
  attesta rollout status payments-api
  attesta rollout rollback payments-api
  attesta incident create --service payments-api --alert HighErrorRate --error-rate 8.2 --latency-p95-ms 850
  attesta incident list
  attesta incident show inc-20260516103000
  attesta workflows list
  attesta workflows show wf-...
  kubectl get rolloutguards,rolloutdecisions -n payments -o json | attesta verify
  attesta list
  attesta status --name payments-api
  attesta rollback --name payments-api
  attesta deployments --name payments-api
`)
}
