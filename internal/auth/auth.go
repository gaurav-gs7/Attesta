// Package auth identifies API callers and decides what they may do.
//
// A caller is a Principal: a name and a role. Roles are ordered, each
// including the ones below it:
//
//	viewer    read everything
//	operator  open and resolve incidents, send alerts
//	admin     register services
//
// Principals come from an Authenticator: a file of hashed API tokens, each
// with its own identity and role, or Kubernetes TokenReview for service
// account and user tokens, with roles bound to users and groups.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Role is a caller's permission level.
type Role string

const (
	RoleViewer   Role = "viewer"
	RoleOperator Role = "operator"
	RoleAdmin    Role = "admin"
)

var rank = map[Role]int{RoleViewer: 1, RoleOperator: 2, RoleAdmin: 3}

// ParseRole validates a role name.
func ParseRole(s string) (Role, error) {
	r := Role(strings.ToLower(strings.TrimSpace(s)))
	if _, ok := rank[r]; !ok {
		return "", fmt.Errorf("unknown role %q (want viewer, operator or admin)", s)
	}
	return r, nil
}

// Principal is an authenticated caller.
type Principal struct {
	// Name identifies the caller in audit records: a token's name, or a
	// Kubernetes username such as system:serviceaccount:ci:deployer.
	Name string `json:"name"`
	Role Role   `json:"role"`
	// Via is how the caller authenticated: "token", "kubernetes" or
	// "insecure".
	Via string `json:"via"`
}

// Allows reports whether the principal's role includes need.
func (p Principal) Allows(need Role) bool { return rank[p.Role] >= rank[need] && rank[need] > 0 }

var (
	// ErrNoCredentials: the request carried no token.
	ErrNoCredentials = errors.New("no credentials")
	// ErrInvalidCredentials: the token was not accepted.
	ErrInvalidCredentials = errors.New("invalid credentials")
)

// Authenticator identifies the caller of a request.
type Authenticator interface {
	// Authenticate returns the caller, ErrNoCredentials when the request
	// carries none, or ErrInvalidCredentials (possibly wrapped) when its
	// credentials are not accepted. Other errors mean the check itself
	// failed.
	Authenticate(ctx context.Context, token string) (Principal, error)
}

// Token extracts the bearer token from Authorization, or from the legacy
// X-Attesta-Token header.
func Token(r *http.Request) string {
	if bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(bearer)
	}
	return strings.TrimSpace(r.Header.Get("X-Attesta-Token"))
}

// Chain tries each authenticator in turn. A token one of them does not
// recognise is passed to the next; the first that accepts it wins.
type Chain []Authenticator

func (c Chain) Authenticate(ctx context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, ErrNoCredentials
	}
	lastErr := ErrInvalidCredentials
	for _, a := range c {
		p, err := a.Authenticate(ctx, token)
		if err == nil {
			return p, nil
		}
		if !errors.Is(err, ErrInvalidCredentials) {
			lastErr = err
		}
	}
	return Principal{}, lastErr
}

type principalKey struct{}

// WithPrincipal returns ctx carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// From returns the principal in ctx, if any.
func From(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// Actor names the caller in ctx for audit records, or "system" for work
// not started by a request.
func Actor(ctx context.Context) string {
	if p, ok := From(ctx); ok {
		return p.Name
	}
	return "system"
}

// Required returns the role a request needs. Reads need viewer; registering
// a service needs admin; every other write needs operator.
func Required(method, path string) Role {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return RoleViewer
	}
	if strings.TrimSuffix(path, "/") == "/api/v1/services" {
		return RoleAdmin
	}
	return RoleOperator
}
