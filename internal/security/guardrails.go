package security

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/gaurav-gs7/attesta/internal/models"
)

var (
	dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	// A person or group: a handle or an email address.
	ownerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}$`)
	// A pager rotation reference such as "platform-oncall" or
	// "pagerduty:PABC123".
	pagerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@:+/-]{0,127}$`)
	// owner/repo, as GitHub and GitLab name repositories.
	repoPath = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$`)
	// Characters allowed in a URL: RFC 3986 unreserved and reserved
	// characters and percent-encoding, which excludes whitespace, quotes,
	// backslashes and angle brackets.
	urlChars = regexp.MustCompile(`^[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+$`)
)

const (
	maxURLLength    = 2048
	maxDependencies = 50
)

type Finding struct {
	Policy  string `json:"policy"`
	Message string `json:"message"`
}

func ValidateService(service models.Service) []Finding {
	var findings []Finding

	if !dnsLabel.MatchString(service.Name) {
		findings = append(findings, Finding{"service-name", "service name must be a valid Kubernetes DNS label"})
	}
	if !dnsLabel.MatchString(service.Namespace) {
		findings = append(findings, Finding{"namespace", "namespace must be a valid Kubernetes DNS label"})
	}
	switch strings.ToLower(service.Language) {
	case "go", "python":
	default:
		findings = append(findings, Finding{"language", "supported languages are go and python"})
	}
	switch strings.ToLower(service.DeploymentStrategy) {
	case "rolling", "canary", "blue-green":
	default:
		findings = append(findings, Finding{"deployment-strategy", "strategy must be rolling, canary, or blue-green"})
	}
	switch strings.ToLower(service.Environment) {
	case "dev", "staging", "prod":
	default:
		findings = append(findings, Finding{"environment", "environment must be dev, staging, or prod"})
	}
	if service.SLOTarget < 95 || service.SLOTarget > 99.99 {
		findings = append(findings, Finding{"slo", "slo target must be between 95 and 99.99"})
	}
	if service.SLOLatencyP95Ms <= 0 {
		findings = append(findings, Finding{"latency-slo", "p95 latency SLO must be configured"})
	}
	if service.SLOErrorRate <= 0 || service.SLOErrorRate > 25 {
		findings = append(findings, Finding{"error-rate", "error-rate threshold must be between 0 and 25 percent"})
	}
	switch strings.ToLower(service.Tier) {
	case "critical", "high", "medium", "low":
	default:
		findings = append(findings, Finding{"tier", "tier must be critical, high, medium, or low"})
	}
	// Every free-text field ends up in generated manifests, labels and
	// source. Each one has a strict pattern and a length cap, so no value
	// can carry a newline, quote or YAML syntax into them.
	if service.Team == "" || service.Owner == "" {
		findings = append(findings, Finding{"ownership", "team and owner are required for operational accountability"})
	}
	if service.Team != "" && !dnsLabel.MatchString(service.Team) {
		findings = append(findings, Finding{"team", "team must be a Kubernetes DNS label (lowercase letters, digits and '-', at most 63 characters)"})
	}
	if service.Owner != "" && !ownerPattern.MatchString(service.Owner) {
		findings = append(findings, Finding{"owner", "owner must be a handle or email address: letters, digits and . _ @ + -, at most 128 characters"})
	}
	if service.Pager != "" && !pagerPattern.MatchString(service.Pager) {
		findings = append(findings, Finding{"pager", "pager must be a rotation reference: letters, digits and . _ @ : + / -, at most 128 characters"})
	}
	if service.RepoURL != "" && !validRepository(service.RepoURL) {
		findings = append(findings, Finding{"repo-url", "repoUrl must be an https URL or an owner/repo path"})
	}
	if service.Repository != "" && !validRepository(service.Repository) {
		findings = append(findings, Finding{"repository", "repository must be an https URL or an owner/repo path"})
	}
	if service.RunbookURL != "" && !validHTTPSURL(service.RunbookURL) {
		findings = append(findings, Finding{"runbook-url", "runbookUrl must be an https URL"})
	}
	if service.DashboardURL != "" && !validHTTPSURL(service.DashboardURL) {
		findings = append(findings, Finding{"dashboard-url", "dashboardUrl must be an https URL"})
	}
	if len(service.Dependencies) > maxDependencies {
		findings = append(findings, Finding{"dependencies", fmt.Sprintf("at most %d dependencies", maxDependencies)})
	}
	for _, dep := range service.Dependencies {
		if !dnsLabel.MatchString(dep) {
			findings = append(findings, Finding{"dependencies", fmt.Sprintf("dependency %q must be a service name (a Kubernetes DNS label)", dep)})
			break
		}
	}
	return findings
}

func validRepository(value string) bool {
	if strings.Contains(value, "://") {
		return validHTTPSURL(value)
	}
	return repoPath.MatchString(value)
}

// validHTTPSURL accepts an absolute https URL with a host, made only of
// characters a URL may contain, of bounded length.
func validHTTPSURL(value string) bool {
	if len(value) > maxURLLength || !urlChars.MatchString(value) {
		return false
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}

func RejectIfFindings(findings []Finding) error {
	if len(findings) == 0 {
		return nil
	}
	return fmt.Errorf("%s: %s", findings[0].Policy, findings[0].Message)
}
