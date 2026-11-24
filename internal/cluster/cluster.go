// Package cluster is the API's read-only view of what the controller does:
// RolloutGuards and the RolloutDecisions they record. The API never writes
// to the cluster; it reports the state the controller acts on and the
// records it leaves, and verifies their hash chains.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// ErrNotFound is returned for a guard that does not exist.
var ErrNotFound = errors.New("not found")

// Reader reads guards and decisions.
type Reader interface {
	// Guards lists guards in a namespace, or in all namespaces for "".
	Guards(ctx context.Context, namespace string) ([]attestav1.RolloutGuard, error)
	Guard(ctx context.Context, namespace, name string) (attestav1.RolloutGuard, error)
	// Decisions returns a guard's decisions, oldest first.
	Decisions(ctx context.Context, guard attestav1.RolloutGuard) ([]attestav1.RolloutDecision, error)
}

// Scheme holds the types the view reads.
func Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	if err := attestav1.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}

// New returns a Reader that reads the API server on every call. Guards
// and decisions are read rarely, by people; a cache would watch every
// record in the cluster to save a few requests.
func New(cfg *rest.Config) (Reader, error) {
	c, err := client.New(cfg, client.Options{Scheme: Scheme()})
	if err != nil {
		return nil, err
	}
	return NewForClient(c), nil
}

// NewForClient wraps a client; tests pass a fake.
func NewForClient(c client.Reader) Reader { return kubeReader{c} }

type kubeReader struct{ c client.Reader }

func (k kubeReader) Guards(ctx context.Context, namespace string) ([]attestav1.RolloutGuard, error) {
	var list attestav1.RolloutGuardList
	if err := k.c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list rollout guards: %w", err)
	}
	sort.Slice(list.Items, func(i, j int) bool {
		a, b := list.Items[i], list.Items[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return list.Items, nil
}

func (k kubeReader) Guard(ctx context.Context, namespace, name string) (attestav1.RolloutGuard, error) {
	var g attestav1.RolloutGuard
	err := k.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &g)
	if apierrors.IsNotFound(err) {
		return g, ErrNotFound
	}
	if err != nil {
		return g, fmt.Errorf("get rollout guard: %w", err)
	}
	return g, nil
}

func (k kubeReader) Decisions(ctx context.Context, guard attestav1.RolloutGuard) ([]attestav1.RolloutDecision, error) {
	var list attestav1.RolloutDecisionList
	if err := k.c.List(ctx, &list, client.InNamespace(guard.Namespace),
		client.MatchingLabels{attestav1.DecisionGuardUIDLabel: string(guard.UID)}); err != nil {
		return nil, fmt.Errorf("list rollout decisions: %w", err)
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Spec.Sequence < list.Items[j].Spec.Sequence })
	return list.Items, nil
}

// ServiceName is the service a guard protects: the "service" label its
// metrics carry, spec.serviceRef or else its Deployment's name. The
// controller resolves it the same way.
func ServiceName(g attestav1.RolloutGuard) string {
	if g.Spec.ServiceRef != "" {
		return g.Spec.ServiceRef
	}
	if g.Spec.DeploymentRef != "" {
		return g.Spec.DeploymentRef
	}
	return strings.TrimSuffix(g.Name, "-rollout")
}

// GuardsForService returns the guards protecting a service, in any
// namespace.
func GuardsForService(ctx context.Context, r Reader, service string) ([]attestav1.RolloutGuard, error) {
	all, err := r.Guards(ctx, "")
	if err != nil {
		return nil, err
	}
	var out []attestav1.RolloutGuard
	for _, g := range all {
		if ServiceName(g) == service {
			out = append(out, g)
		}
	}
	return out, nil
}

// Verification is the result of checking a guard's decision chain.
type Verification struct {
	Verified  bool   `json:"verified"`
	Decisions int    `json:"decisions"`
	Problem   string `json:"problem,omitempty"`
	// BrokenAt is the first sequence that fails, when Verified is false.
	BrokenAt int64 `json:"brokenAt,omitempty"`
}

// Verify checks a guard's records against its status.lastDecision.
func Verify(g attestav1.RolloutGuard, records []attestav1.RolloutDecision) Verification {
	v := Verification{Decisions: len(records), Verified: true}
	if err := attestav1.VerifyChain(g.Name, string(g.UID), records, g.Status.LastDecision); err != nil {
		v.Verified, v.Problem = false, err.Error()
		var chainErr *attestav1.ChainError
		if errors.As(err, &chainErr) {
			v.BrokenAt = chainErr.Sequence
		}
	}
	return v
}
