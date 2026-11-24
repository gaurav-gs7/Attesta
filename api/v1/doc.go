// Package v1 holds the attesta.io/v1 API types: RolloutGuard, which the
// controller reconciles; RolloutDecision, the hash-chained record of each
// decision it makes; and AttestaService and SLOPolicy, which the generated
// scaffold declares and nothing reconciles yet.
//
// The CRDs in deploy/crds are generated from these types (make generate),
// so the schema the API server enforces is the one the controller decodes.
//
// +kubebuilder:object:generate=true
// +groupName=attesta.io
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is attesta.io/v1.
var GroupVersion = schema.GroupVersion{Group: "attesta.io", Version: "v1"}

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&RolloutGuard{}, &RolloutGuardList{},
		&AttestaService{}, &AttestaServiceList{},
		&SLOPolicy{}, &SLOPolicyList{},
		&RolloutDecision{}, &RolloutDecisionList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
