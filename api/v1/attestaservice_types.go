package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// AttestaService is the declarative record of a service's ownership and
// SLO, written by the generated scaffold. Nothing reconciles it yet.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=asvc
type AttestaService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AttestaServiceSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true
type AttestaServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AttestaService `json:"items"`
}

type AttestaServiceSpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=100
	Owner string `json:"owner"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Team string `json:"team"`
	// +kubebuilder:validation:Enum=critical;high;medium;low
	Tier string `json:"tier"`
	// +kubebuilder:validation:Enum=go;python
	Language string `json:"language"`
	// +optional
	Runtime string `json:"runtime,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=100
	Pager string `json:"pager,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=300
	Repository string `json:"repository,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Runbook string `json:"runbook,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Dashboard string `json:"dashboard,omitempty"`
	// +optional
	Deployment ServiceDeployment `json:"deployment,omitempty"`
	// +optional
	SLO ServiceSLO `json:"slo,omitempty"`
}

type ServiceDeployment struct {
	// +optional
	// +kubebuilder:validation:Enum=rolling;canary;blue-green
	Strategy string `json:"strategy,omitempty"`
	// MaxErrorRate is the error-rate percentage a rollout may reach.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxErrorRate *float64 `json:"maxErrorRate,omitempty"`
	// +optional
	RollbackOnFailure bool `json:"rollbackOnFailure,omitempty"`
}

type ServiceSLO struct {
	// Availability is the target percentage over 30 days.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Availability *float64 `json:"availability,omitempty"`
	// LatencyP95 is a duration, such as 300ms.
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	LatencyP95 string `json:"latencyP95,omitempty"`
	// ErrorRate is the highest acceptable 5xx percentage.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	ErrorRate *float64 `json:"errorRate,omitempty"`
}
