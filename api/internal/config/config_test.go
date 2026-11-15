package config

import (
	"errors"
	"testing"
)

func TestValidateRequiresTokenByDefault(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "")
	if err := Load().Validate(); !errors.Is(err, ErrNoAuth) {
		t.Fatalf("expected ErrNoAuth without a token, got %v", err)
	}
}

func TestValidateAcceptsToken(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "secret")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "")
	if err := Load().Validate(); err != nil {
		t.Fatalf("expected a configured token to validate, got %v", err)
	}
}

func TestValidateAllowsExplicitInsecureMode(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "true")
	if err := Load().Validate(); err != nil {
		t.Fatalf("expected explicit insecure mode to validate, got %v", err)
	}
}

func TestInsecureFlagRejectsNonBooleanValues(t *testing.T) {
	t.Setenv("ATTESTA_API_TOKEN", "")
	t.Setenv("ATTESTA_INSECURE_NO_AUTH", "yes please")
	if err := Load().Validate(); !errors.Is(err, ErrNoAuth) {
		t.Fatalf("expected a non-boolean flag to be ignored, got %v", err)
	}
}
