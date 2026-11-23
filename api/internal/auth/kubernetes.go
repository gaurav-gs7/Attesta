package auth

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Bindings map Kubernetes users and groups to roles:
//
//	{"bindings": [
//	  {"role": "operator", "users": ["system:serviceaccount:ci:deployer"]},
//	  {"role": "viewer", "groups": ["system:authenticated"]},
//	  {"role": "admin", "groups": ["platform-admins"]}
//	]}
//
// A caller gets the highest role any of its bindings grants; a caller with
// no binding is rejected even if Kubernetes authenticated it.
type Bindings struct {
	users  map[string]Role
	groups map[string]Role
}

// LoadBindings reads a bindings file.
func LoadBindings(path string) (*Bindings, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("role bindings file: %w", err)
	}
	var doc struct {
		Bindings []struct {
			Role   string   `json:"role"`
			Users  []string `json:"users"`
			Groups []string `json:"groups"`
		} `json:"bindings"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("role bindings file %s: %w", path, err)
	}
	b := &Bindings{users: map[string]Role{}, groups: map[string]Role{}}
	grant := func(m map[string]Role, key string, r Role) {
		if rank[r] > rank[m[key]] {
			m[key] = r
		}
	}
	for _, binding := range doc.Bindings {
		role, err := ParseRole(binding.Role)
		if err != nil {
			return nil, fmt.Errorf("role bindings file %s: %w", path, err)
		}
		for _, u := range binding.Users {
			grant(b.users, u, role)
		}
		for _, g := range binding.Groups {
			grant(b.groups, g, role)
		}
	}
	return b, nil
}

func (b *Bindings) roleFor(user string, groups []string) Role {
	best := b.users[user]
	for _, g := range groups {
		if rank[b.groups[g]] > rank[best] {
			best = b.groups[g]
		}
	}
	return best
}

// TokenReview authenticates Kubernetes service account and user tokens
// with the TokenReview API and maps the result to a role through Bindings.
// Results are cached briefly by token hash, so a busy client does not cost
// an API call per request.
type TokenReview struct {
	client    kubernetes.Interface
	audiences []string
	bindings  *Bindings
	ttl       time.Duration
	now       func() time.Time

	mu    sync.Mutex
	cache map[[sha256.Size]byte]cached
}

type cached struct {
	principal Principal
	ok        bool
	expires   time.Time
}

// NewTokenReview returns an authenticator. audiences, if set, are required
// in the token (for example "attesta"), so a token issued for another
// service is not accepted here.
func NewTokenReview(client kubernetes.Interface, bindings *Bindings, audiences []string) *TokenReview {
	return &TokenReview{client: client, bindings: bindings, audiences: audiences, ttl: time.Minute, now: time.Now,
		cache: map[[sha256.Size]byte]cached{}}
}

func (t *TokenReview) Authenticate(ctx context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, ErrNoCredentials
	}
	key := sha256.Sum256([]byte(token))
	t.mu.Lock()
	if c, hit := t.cache[key]; hit && t.now().Before(c.expires) {
		t.mu.Unlock()
		if !c.ok {
			return Principal{}, ErrInvalidCredentials
		}
		return c.principal, nil
	}
	t.mu.Unlock()

	review, err := t.client.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: token, Audiences: t.audiences},
	}, metav1.CreateOptions{})
	if err != nil {
		return Principal{}, fmt.Errorf("token review: %w", err)
	}
	var p Principal
	ok := review.Status.Authenticated
	if ok {
		role := t.bindings.roleFor(review.Status.User.Username, review.Status.User.Groups)
		p = Principal{Name: review.Status.User.Username, Role: role, Via: "kubernetes"}
		ok = role != ""
	}
	t.mu.Lock()
	if len(t.cache) > 10000 {
		t.cache = map[[sha256.Size]byte]cached{}
	}
	t.cache[key] = cached{principal: p, ok: ok, expires: t.now().Add(t.ttl)}
	t.mu.Unlock()
	if !ok {
		return Principal{}, ErrInvalidCredentials
	}
	return p, nil
}
