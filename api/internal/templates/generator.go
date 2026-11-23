package templates

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/gaurav-gs7/attesta/api/internal/models"
	"github.com/gaurav-gs7/attesta/api/internal/security"
	"github.com/gaurav-gs7/attesta/api/internal/slo"
)

type Generator struct {
	templateDir string
	outputDir   string
}

type Result struct {
	ServiceName string   `json:"serviceName"`
	OutputPath  string   `json:"outputPath"`
	Files       []string `json:"files"`
}

type data struct {
	Service        models.Service
	ServiceTitle   string
	PrometheusName string
	// BurnWindows are the windows of the multi-window burn-rate rules, the
	// same ones the API's deployment gate uses.
	BurnWindows   []string
	ImageName     string
	ContainerPort int
}

func NewGenerator(templateDir, outputDir string) *Generator {
	return &Generator{templateDir: templateDir, outputDir: outputDir}
}

func (g *Generator) Generate(service models.Service) (Result, error) {
	// The API validates services before generating, but the generator is a
	// library too: it refuses input it cannot render safely.
	if err := security.RejectIfFindings(security.ValidateService(service)); err != nil {
		return Result{}, fmt.Errorf("refusing to generate a scaffold: %w", err)
	}
	languageTemplates := filepath.Join(g.templateDir, "service-"+strings.ToLower(service.Language))
	if _, err := os.Stat(languageTemplates); err != nil {
		return Result{}, fmt.Errorf("language templates: %w", err)
	}

	outputPath := filepath.Join(g.outputDir, service.Name)
	if err := os.MkdirAll(outputPath, 0o755); err != nil {
		return Result{}, err
	}

	d := data{
		Service:        service,
		ServiceTitle:   title(service.Name),
		PrometheusName: prometheusName(service.Name),
		BurnWindows:    burnWindowNames(),
		ImageName:      "ghcr.io/example/" + service.Name,
		ContainerPort:  8080,
	}

	var files []string
	for _, section := range []copySection{
		{source: languageTemplates, target: outputPath},
		{source: filepath.Join(g.templateDir, "attesta"), target: outputPath},
		{source: filepath.Join(g.templateDir, "github-actions"), target: filepath.Join(outputPath, ".github", "workflows")},
		{source: filepath.Join(g.templateDir, "kustomize", "base"), target: filepath.Join(outputPath, "k8s", "base")},
		{source: filepath.Join(g.templateDir, "kustomize", "overlays", "dev"), target: filepath.Join(outputPath, "k8s", "overlays", "dev")},
		{source: filepath.Join(g.templateDir, "kustomize", "overlays", "staging"), target: filepath.Join(outputPath, "k8s", "overlays", "staging")},
		{source: filepath.Join(g.templateDir, "kustomize", "overlays", "prod"), target: filepath.Join(outputPath, "k8s", "overlays", "prod")},
		{source: filepath.Join(g.templateDir, "grafana"), target: filepath.Join(outputPath, "observability")},
		{source: filepath.Join(g.templateDir, "alertmanager"), target: filepath.Join(outputPath, "observability")},
		{source: filepath.Join(g.templateDir, "opentelemetry"), target: filepath.Join(outputPath, "observability")},
		{source: filepath.Join(g.templateDir, "runbooks"), target: filepath.Join(outputPath, "runbooks")},
		{source: filepath.Join(g.templateDir, "terraform"), target: filepath.Join(outputPath, "infra", "terraform")},
		{source: filepath.Join(g.templateDir, "argocd"), target: filepath.Join(outputPath, "infra", "argocd")},
	} {
		written, err := g.renderSection(section, d)
		if err != nil {
			return Result{}, err
		}
		files = append(files, written...)
	}

	return Result{ServiceName: service.Name, OutputPath: outputPath, Files: files}, nil
}

type copySection struct {
	source string
	target string
}

func (g *Generator) renderSection(section copySection, d data) ([]string, error) {
	if _, err := os.Stat(section.source); err != nil {
		return nil, fmt.Errorf("template section %s: %w", section.source, err)
	}

	var files []string
	err := filepath.WalkDir(section.source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(section.source, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		targetRel := strings.TrimSuffix(rel, ".tmpl")
		targetPath := filepath.Join(section.target, targetRel)
		if entry.IsDir() {
			return os.MkdirAll(targetPath, 0o755)
		}
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			return err
		}
		if strings.HasSuffix(path, ".tmpl") {
			if err := renderTemplateFile(path, targetPath, d); err != nil {
				return err
			}
		} else if err := copyFile(path, targetPath); err != nil {
			return err
		}
		files = append(files, targetPath)
		return nil
	})
	return files, err
}

func renderTemplateFile(source, target string, d data) error {
	content, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	tpl, err := template.New(filepath.Base(source)).Funcs(funcs).Parse(string(content))
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, d); err != nil {
		return err
	}
	return os.WriteFile(target, buf.Bytes(), 0o644)
}

func copyFile(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	// A failed close can mean the data never reached the disk.
	return out.Close()
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
