package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func writeTokens(t *testing.T, path string, entries ...[3]string) {
	t.Helper()
	body := `{"tokens":[`
	for i, e := range entries {
		if i > 0 {
			body += ","
		}
		body += fmt.Sprintf(`{"name":%q,"role":%q,"sha256":%q}`, e[0], e[1], HashToken(e[2]))
	}
	body += "]}"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Audit X-4: one shared static token gave no identity, no roles and no
// rotation.
func TestTokenFileGivesEachTokenAnIdentityAndRole(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tokens.json")
	writeTokens(t, path, [3]string{"ci-deployer", "operator", "tok-ci"}, [3]string{"dashboard", "viewer", "tok-view"})
	f, err := NewTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.Authenticate(context.Background(), "tok-ci")
	if err != nil || p.Name != "ci-deployer" || p.Role != RoleOperator || p.Via != "token" {
		t.Fatalf("ci token: %+v %v", p, err)
	}
	if _, err := f.Authenticate(context.Background(), "nope"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("unknown token: %v", err)
	}

	// Rotation: add the new token, retire the old one, no restart.
	time.Sleep(1100 * time.Millisecond)
	writeTokens(t, path, [3]string{"ci-deployer", "operator", "tok-ci-2"}, [3]string{"dashboard", "viewer", "tok-view"})
	if p, err := f.Authenticate(context.Background(), "tok-ci-2"); err != nil || p.Name != "ci-deployer" {
		t.Fatalf("rotated token not accepted: %+v %v", p, err)
	}
	if _, err := f.Authenticate(context.Background(), "tok-ci"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("retired token still accepted: %v", err)
	}
}

func TestTokenFileRejectsBadEntries(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"bad role":  `{"tokens":[{"name":"x","role":"root","sha256":"` + HashToken("a") + `"}]}`,
		"bad hash":  `{"tokens":[{"name":"x","role":"viewer","sha256":"abc"}]}`,
		"bad name":  `{"tokens":[{"name":"x y","role":"viewer","sha256":"` + HashToken("a") + `"}]}`,
		"duplicate": `{"tokens":[{"name":"x","role":"viewer","sha256":"` + HashToken("a") + `"},{"name":"y","role":"admin","sha256":"` + HashToken("a") + `"}]}`,
		"not JSON":  `tokens:`,
	} {
		path := filepath.Join(dir, name)
		_ = os.WriteFile(path, []byte(body), 0o600)
		if _, err := NewTokenFile(path); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRolesAndRequirements(t *testing.T) {
	viewer, operator, admin := Principal{Role: RoleViewer}, Principal{Role: RoleOperator}, Principal{Role: RoleAdmin}
	for _, tc := range []struct {
		method, path string
		need         Role
	}{
		{http.MethodGet, "/api/v1/services", RoleViewer},
		{http.MethodPost, "/api/v1/services", RoleAdmin},
		{http.MethodPost, "/api/v1/services/x/rollouts", RoleOperator},
		{http.MethodPost, "/api/v1/alerts/alertmanager", RoleOperator},
	} {
		if got := Required(tc.method, tc.path); got != tc.need {
			t.Errorf("%s %s needs %s, want %s", tc.method, tc.path, got, tc.need)
		}
	}
	if viewer.Allows(RoleOperator) || !operator.Allows(RoleViewer) || operator.Allows(RoleAdmin) || !admin.Allows(RoleOperator) {
		t.Fatal("role ordering is wrong")
	}
	if (Principal{}).Allows(RoleViewer) {
		t.Fatal("a principal without a role was allowed")
	}
}

func TestTokenReviewMapsKubernetesIdentitiesToRoles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.json")
	_ = os.WriteFile(path, []byte(`{"bindings":[
		{"role":"operator","users":["system:serviceaccount:ci:deployer"]},
		{"role":"viewer","groups":["system:authenticated"]},
		{"role":"admin","groups":["platform-admins"]}]}`), 0o600)
	bindings, err := LoadBindings(path)
	if err != nil {
		t.Fatal(err)
	}
	users := map[string]authenticationv1.UserInfo{
		"sa-token":    {Username: "system:serviceaccount:ci:deployer", Groups: []string{"system:authenticated"}},
		"admin-token": {Username: "gaurav", Groups: []string{"system:authenticated", "platform-admins"}},
		"other-token": {Username: "someone", Groups: []string{"elsewhere"}},
	}
	client := fake.NewSimpleClientset()
	calls := 0
	client.PrependReactor("create", "tokenreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		review := a.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		if len(review.Spec.Audiences) != 1 || review.Spec.Audiences[0] != "attesta" {
			t.Errorf("audiences not sent: %v", review.Spec.Audiences)
		}
		user, ok := users[review.Spec.Token]
		review.Status = authenticationv1.TokenReviewStatus{Authenticated: ok, User: user}
		return true, review, nil
	})
	tr := NewTokenReview(client, bindings, []string{"attesta"})
	for token, want := range map[string]Role{"sa-token": RoleOperator, "admin-token": RoleAdmin} {
		p, err := tr.Authenticate(context.Background(), token)
		if err != nil || p.Role != want || p.Name != users[token].Username || p.Via != "kubernetes" {
			t.Errorf("%s: %+v %v, want %s", token, p, err, want)
		}
	}
	if _, err := tr.Authenticate(context.Background(), "other-token"); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("authenticated user without a binding: %v", err)
	}
	if _, err := tr.Authenticate(context.Background(), "forged"); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("unauthenticated token: %v", err)
	}
	before := calls
	_, _ = tr.Authenticate(context.Background(), "sa-token")
	if calls != before {
		t.Errorf("repeat token review not cached: %d API calls", calls-before)
	}
}
