package templates

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
	"github.com/gaurav-gs7/attesta/internal/scaffold"
)

// The scaffold's custom resources must be objects of the API types, field
// for field: the CRDs are generated from those types and the API server
// prunes or rejects anything else.
func TestGeneratedCustomResourcesMatchTheAPI(t *testing.T) {
	for _, lang := range []string{"go", "python"} {
		svc := testService(lang)
		svc.RollbackOnFailure = true
		result := generate(t, svc)
		for file, obj := range map[string]any{
			"attesta-service.yaml": &attestav1.AttestaService{},
			"slo.yaml":             &attestav1.SLOPolicy{},
			"rollout.yaml":         &attestav1.RolloutGuard{},
		} {
			if err := yaml.UnmarshalStrict([]byte(read(t, result, file)), obj); err != nil {
				t.Errorf("%s %s: %v", lang, file, err)
			}
		}
	}
}

// TestWriteScaffoldForE2E writes a rendered scaffold to
// $ATTESTA_WRITE_SCAFFOLD, so the kind job can submit its custom resources
// to an API server with the generated CRDs (kubectl apply --dry-run=server),
// which checks enums, patterns and ranges as well as fields.
func TestWriteScaffoldForE2E(t *testing.T) {
	dir := os.Getenv("ATTESTA_WRITE_SCAFFOLD")
	if dir == "" {
		t.Skip("ATTESTA_WRITE_SCAFFOLD is not set")
	}
	root := filepath.Clean("../..")
	rendered, err := NewGenerator(filepath.Join(root, "templates")).Render(testService("go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := scaffold.WriteDir(filepath.Join(dir, "payments-api"), rendered.Files); err != nil {
		t.Fatal(err)
	}
}
