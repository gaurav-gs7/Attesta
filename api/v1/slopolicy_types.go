package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SLOPolicy is the declarative record of a service's objectives and
// burn-rate alerts, written by the generated scaffold. Nothing reconciles
// it yet: the API reads SLO targets from its catalog.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=slo
type SLOPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SLOPolicySpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true
type SLOPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SLOPolicy `json:"items"`
}

type SLOPolicySpec struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	ServiceRef string `json:"serviceRef"`
	// Window is the period availability and the error budget are measured
	// over, such as 30d.
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s|m|h|d|w)$`
	Window string `json:"window"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	Objectives []SLOObjective `json:"objectives"`
	// +optional
	// +kubebuilder:validation:MaxItems=10
	BurnRateAlerts []BurnRateAlert `json:"burnRateAlerts,omitempty"`
}

type SLOObjective struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name   string  `json:"name"`
	Target float64 `json:"target"`
	// +kubebuilder:validation:Enum=percent;ms
	Unit string `json:"unit"`
	// EvaluationWindow judges the objective on recent traffic instead of
	// the policy's window.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s|m|h|d|w)$`
	EvaluationWindow string `json:"evaluationWindow,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	PrometheusQuery string `json:"prometheusQuery"`
}

type BurnRateAlert struct {
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s|m|h|d|w)$`
	LongWindow string `json:"longWindow"`
	// +kubebuilder:validation:Pattern=`^[0-9]+(ms|s|m|h|d|w)$`
	ShortWindow string `json:"shortWindow"`
	// +kubebuilder:validation:ExclusiveMinimum=true
	// +kubebuilder:validation:Minimum=0
	BurnRate float64 `json:"burnRate"`
	// +kubebuilder:validation:Enum=page;ticket
	Severity string `json:"severity"`
}
