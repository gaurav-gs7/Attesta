package main

import (
	"flag"
	"testing"
)

// The flags go on the default flag set, beside the ones controller-runtime
// and client-go register there in init. A clash panics before main runs
// anything; v0.2's --kubeconfig did exactly that.
func TestFlagsDoNotClashWithLibraryFlags(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("registering flags panicked: %v", r)
		}
	}()
	o := registerFlags(flag.CommandLine)
	if flag.Lookup("kubeconfig") == nil {
		t.Fatal("--kubeconfig is not registered; ctrl.GetConfig would not see it")
	}
	if o.workers != 2 || o.shutdownTimeout.String() != "30s" {
		t.Fatalf("defaults: %+v", o)
	}
}

func TestSchemeKnowsEveryTypeTheControllerUses(t *testing.T) {
	s := Scheme()
	for _, kind := range []string{"RolloutGuard", "Deployment", "ReplicaSet"} {
		found := false
		for gvk := range s.AllKnownTypes() {
			if gvk.Kind == kind {
				found = true
			}
		}
		if !found {
			t.Errorf("scheme has no %s", kind)
		}
	}
}
