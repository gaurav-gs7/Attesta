package templates

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/gaurav-gs7/attesta/internal/scaffold"
)

// renderOverlay runs kustomize on one generated overlay and returns its
// objects by kind, after kubeconform when it is installed. It skips without
// kustomize unless ATTESTA_REQUIRE_KUSTOMIZE is set, as in CI's manifests job.
func renderOverlay(t *testing.T, dir, env string) map[string][]map[string]any {
	t.Helper()
	kustomize, err := exec.LookPath("kustomize")
	if err != nil {
		if os.Getenv("ATTESTA_REQUIRE_KUSTOMIZE") != "" {
			t.Fatal("kustomize not installed")
		}
		t.Skip("kustomize not installed")
	}
	out, err := exec.Command(kustomize, "build", filepath.Join(dir, "k8s", "overlays", env)).CombinedOutput()
	if err != nil {
		t.Fatalf("kustomize build %s: %v\n%s", env, err, out)
	}
	kubeconform, err := exec.LookPath("kubeconform")
	if err != nil && os.Getenv("ATTESTA_REQUIRE_KUSTOMIZE") != "" {
		t.Fatal("kubeconform not installed")
	}
	if err == nil {
		cmd := exec.Command(kubeconform, "-strict", "-summary", "-kubernetes-version", "1.32.0", "-")
		cmd.Stdin = strings.NewReader(string(out))
		if res, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("kubeconform %s: %v\n%s", env, err, res)
		}
	}
	objects := map[string][]map[string]any{}
	for _, doc := range strings.Split(string(out), "\n---\n") {
		var obj map[string]any
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatal(err)
		}
		if kind, _ := obj["kind"].(string); kind != "" {
			objects[kind] = append(objects[kind], obj)
		}
	}
	return objects
}

func spec(obj map[string]any) map[string]any {
	s, _ := obj["spec"].(map[string]any)
	return s
}

// Audit G-4: dev patched Deployment replicas to 1 while the HPA kept
// minReplicas 2, and the PDB's minAvailable: 1 would block drains at one
// replica.
func TestRenderedOverlaysScaleThroughTheAutoscaler(t *testing.T) {
	dir := generate(t, testService("go")).OutputPath
	bounds := map[string][2]float64{"dev": {1, 2}, "staging": {2, 5}, "prod": {3, 10}}
	for env, want := range bounds {
		objects := renderOverlay(t, dir, env)
		if len(objects["Deployment"]) != 1 || len(objects["HorizontalPodAutoscaler"]) != 1 || len(objects["PodDisruptionBudget"]) != 1 {
			t.Fatalf("%s: rendered kinds %v", env, objects)
		}
		if _, set := spec(objects["Deployment"][0])["replicas"]; set {
			t.Errorf("%s: the Deployment sets replicas, which the HPA owns", env)
		}
		hpa := spec(objects["HorizontalPodAutoscaler"][0])
		if hpa["minReplicas"] != want[0] || hpa["maxReplicas"] != want[1] {
			t.Errorf("%s: HPA %v..%v, want %v..%v", env, hpa["minReplicas"], hpa["maxReplicas"], want[0], want[1])
		}
		pdb := spec(objects["PodDisruptionBudget"][0])
		if _, blocks := pdb["minAvailable"]; blocks || pdb["maxUnavailable"] != float64(1) {
			t.Errorf("%s: PDB %v, want maxUnavailable: 1", env, pdb)
		}
	}
}

// Audit G-5: every service ran ghcr.io/example/<name>:latest with
// IfNotPresent, so nodes could run different builds under one tag.
func TestRenderedOverlaysPinTheImage(t *testing.T) {
	root := filepath.Clean("../../..")
	rendered, err := NewGenerator(filepath.Join(root, "templates"), WithImageRegistry("registry.example.com:5000/payments")).Render(testService("go"))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "payments-api")
	if err := scaffold.WriteDir(dir, rendered.Files); err != nil {
		t.Fatal(err)
	}
	for _, env := range []string{"dev", "staging", "prod"} {
		deploy := renderOverlay(t, dir, env)["Deployment"][0]
		podSpec := spec(spec(deploy)["template"].(map[string]any))
		container := podSpec["containers"].([]any)[0].(map[string]any)
		if container["image"] != "registry.example.com:5000/payments/payments-api:unset" {
			t.Errorf("%s: image %v, want the configured registry and the overlay's pinned tag", env, container["image"])
		}
		if policy, set := container["imagePullPolicy"]; set {
			t.Errorf("%s: imagePullPolicy %v set; the pinned reference decides", env, policy)
		}
	}
}

// Audit G-6: liveness hit the readiness endpoint, and nothing gave the pod
// time to leave the endpoints before it stopped.
func TestRenderedDeploymentProbesAndDrains(t *testing.T) {
	dir := generate(t, testService("go")).OutputPath
	deploy := renderOverlay(t, dir, "staging")["Deployment"][0]
	c := spec(spec(deploy)["template"].(map[string]any))["containers"].([]any)[0].(map[string]any)
	path := func(probe string) any {
		p, _ := c[probe].(map[string]any)
		get, _ := p["httpGet"].(map[string]any)
		return get["path"]
	}
	if path("livenessProbe") != "/livez" || path("readinessProbe") != "/readyz" {
		t.Fatalf("probes: liveness %v, readiness %v", path("livenessProbe"), path("readinessProbe"))
	}
	lifecycle, _ := c["lifecycle"].(map[string]any)
	if _, ok := lifecycle["preStop"]; !ok {
		t.Fatal("no preStop delay before SIGTERM")
	}
}

// Audit G-7: the namespace was declared by the Kustomize base, a Terraform
// module and Argo CD's CreateNamespace at once.
func TestRenderedNamespaceHasOneOwner(t *testing.T) {
	result := generate(t, testService("go"))
	for _, f := range result.Files {
		rel, _ := filepath.Rel(result.OutputPath, f)
		if strings.HasPrefix(rel, "infra/terraform") {
			t.Errorf("%s: Terraform still ships", rel)
		}
	}
	for _, env := range []string{"dev", "staging", "prod"} {
		if ns := renderOverlay(t, result.OutputPath, env)["Namespace"]; len(ns) != 0 {
			t.Errorf("%s overlay renders a Namespace", env)
		}
	}
	app := read(t, result, "infra/argocd/application.yaml")
	if !strings.Contains(app, "CreateNamespace=true") || !strings.Contains(app, "managedNamespaceMetadata:") {
		t.Fatalf("Argo CD does not own the namespace:\n%s", app)
	}
}
