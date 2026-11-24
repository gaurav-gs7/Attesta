package templates

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/security"
	"github.com/gaurav-gs7/attesta/api/internal/slo"
	"github.com/gaurav-gs7/attesta/internal/scaffold"
)

// Generator renders a service's golden-path scaffold from templateDir. It
// only reads templateDir: the scaffold is built in memory and handed to
// the caller, which serves it as an archive or writes it where the
// developer asked.
type Generator struct {
	templateDir string
	registry    string
}

// DefaultImageRegistry is a placeholder: images pushed there go nowhere
// useful. Set the registry with WithImageRegistry (ATTESTA_IMAGE_REGISTRY
// for the API).
const DefaultImageRegistry = "ghcr.io/change-me"

// Option configures a Generator.
type Option func(*Generator)

// WithImageRegistry sets the registry and path prefix of generated
// services' images, such as ghcr.io/acme or registry.example.com:5000/team.
// A service's image is <registry>/<service name>.
func WithImageRegistry(registry string) Option {
	return func(g *Generator) {
		if registry != "" {
			g.registry = strings.TrimSuffix(registry, "/")
		}
	}
}

var imageRegistry = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?(:[0-9]{1,5})?(/[a-z0-9]+([._-][a-z0-9]+)*)*$`)

// ValidateImageRegistry rejects a registry that is not a lowercase
// host[:port][/path], the form an image reference allows.
func ValidateImageRegistry(registry string) error {
	if len(registry) > 200 || !imageRegistry.MatchString(strings.TrimSuffix(registry, "/")) {
		return fmt.Errorf("image registry %q: want a lowercase host[:port][/path], such as ghcr.io/acme", registry)
	}
	return nil
}

// Scaffold is a rendered scaffold. File paths are relative to the
// service's directory.
type Scaffold struct {
	ServiceName string
	Files       []scaffold.File
}

// Paths lists the scaffold's files, sorted.
func (s Scaffold) Paths() []string {
	paths := make([]string, 0, len(s.Files))
	for _, f := range s.Files {
		paths = append(paths, f.Path)
	}
	sort.Strings(paths)
	return paths
}

type data struct {
	Service        models.Service
	ServiceTitle   string
	PrometheusName string
	// BurnWindows are the windows of the multi-window burn-rate rules, the
	// same ones the API's deployment gate uses.
	BurnWindows []string
	// ImageName is <registry>/<service name>, without a tag: each overlay
	// pins the tag or digest it runs.
	ImageName string
	// RegistryHost is the registry's host[:port], for docker login.
	RegistryHost  string
	ContainerPort int
}

func NewGenerator(templateDir string, opts ...Option) *Generator {
	g := &Generator{templateDir: templateDir, registry: DefaultImageRegistry}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Render renders service's scaffold in memory. Nothing is written on
// failure because nothing is written at all.
func (g *Generator) Render(service models.Service) (Scaffold, error) {
	// The API validates services before generating, but the generator is a
	// library too: it refuses input it cannot render safely.
	if err := security.RejectIfFindings(security.ValidateService(service)); err != nil {
		return Scaffold{}, fmt.Errorf("refusing to generate a scaffold: %w", err)
	}
	if err := ValidateImageRegistry(g.registry); err != nil {
		return Scaffold{}, err
	}
	languageTemplates := filepath.Join(g.templateDir, "service-"+strings.ToLower(service.Language))
	if _, err := os.Stat(languageTemplates); err != nil {
		return Scaffold{}, fmt.Errorf("language templates: %w", err)
	}

	d := data{
		Service:        service,
		ServiceTitle:   title(service.Name),
		PrometheusName: prometheusName(service.Name),
		BurnWindows:    burnWindowNames(),
		ImageName:      g.registry + "/" + service.Name,
		RegistryHost:   strings.SplitN(g.registry, "/", 2)[0],
		ContainerPort:  8080,
	}

	out := Scaffold{ServiceName: service.Name}
	seen := map[string]string{}
	for _, section := range []copySection{
		{source: languageTemplates, target: "."},
		{source: filepath.Join(g.templateDir, "attesta"), target: "."},
		{source: filepath.Join(g.templateDir, "github-actions"), target: ".github/workflows"},
		{source: filepath.Join(g.templateDir, "kustomize", "base"), target: "k8s/base"},
		{source: filepath.Join(g.templateDir, "kustomize", "overlays", "dev"), target: "k8s/overlays/dev"},
		{source: filepath.Join(g.templateDir, "kustomize", "overlays", "staging"), target: "k8s/overlays/staging"},
		{source: filepath.Join(g.templateDir, "kustomize", "overlays", "prod"), target: "k8s/overlays/prod"},
		{source: filepath.Join(g.templateDir, "grafana"), target: "observability"},
		{source: filepath.Join(g.templateDir, "alertmanager"), target: "observability"},
		{source: filepath.Join(g.templateDir, "opentelemetry"), target: "observability"},
		{source: filepath.Join(g.templateDir, "runbooks"), target: "runbooks"},
		{source: filepath.Join(g.templateDir, "argocd"), target: "infra/argocd"},
	} {
		files, err := renderSection(section, d)
		if err != nil {
			return Scaffold{}, err
		}
		for _, f := range files {
			if from, dup := seen[f.Path]; dup {
				return Scaffold{}, fmt.Errorf("templates %s and %s both render %s", from, section.source, f.Path)
			}
			seen[f.Path] = section.source
			out.Files = append(out.Files, f)
		}
	}
	return out, nil
}

type copySection struct {
	source string
	// target is slash-separated, relative to the service's directory.
	target string
}

func renderSection(section copySection, d data) ([]scaffold.File, error) {
	if _, err := os.Stat(section.source); err != nil {
		return nil, fmt.Errorf("template section %s: %w", section.source, err)
	}

	var files []scaffold.File
	err := filepath.WalkDir(section.source, func(p string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("template %s is not a regular file", p)
		}
		rel, err := filepath.Rel(section.source, p)
		if err != nil {
			return err
		}
		target := path.Join(section.target, strings.TrimSuffix(filepath.ToSlash(rel), ".tmpl"))
		content, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if strings.HasSuffix(p, ".tmpl") {
			if content, err = renderTemplate(p, content, d); err != nil {
				return err
			}
		}
		files = append(files, scaffold.File{Path: target, Content: content})
		return nil
	})
	return files, err
}

func renderTemplate(source string, content []byte, d data) ([]byte, error) {
	tpl, err := template.New(filepath.Base(source)).Funcs(funcs).Parse(string(content))
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, d); err != nil {
		return nil, fmt.Errorf("render %s: %w", source, err)
	}
	return buf.Bytes(), nil
}

func title(name string) string {
	parts := strings.FieldsFunc(name, func(r rune) bool { return r == '-' || r == '_' })
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, " ")
}

func prometheusName(name string) string {
	return strings.NewReplacer("-", "_", ".", "_").Replace(name)
}

func burnWindowNames() []string {
	names := make([]string, len(slo.BurnWindows))
	for i, w := range slo.BurnWindows {
		names[i] = w.Name
	}
	return names
}

// funcs are available to every template. quote renders a string as a JSON
// string literal, which is also a valid double-quoted YAML scalar, so a value
// can never close its quotes or start a new key in a generated manifest.
var funcs = template.FuncMap{
	"quote": func(s string) (string, error) {
		b, err := json.Marshal(s)
		return string(b), err
	},
}
