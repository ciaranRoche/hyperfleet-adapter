// Package desire defines the core desire types for the desire-based delivery system.
//
// The desire model replaces Maestro/OCM ManifestWork as the transport mechanism
// for delivering Kubernetes resources to target clusters. Three desire types exist:
//
//   - ApplyDesire: make a resource exist with specific content (SSA force=true)
//   - DeleteDesire: make a resource not exist (confirmed gone past finalizers)
//   - ReadDesire: mirror a live object's state back to the control plane
//
// Each desire targets exactly one Kubernetes resource instance. No lists, no label
// selectors, no bulk ops. This is for simplicity in reasoning about status.
//
// Behavioral semantics are aligned with the ARO-HCP kube-applier specification.
package desire

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DesireID uniquely identifies a desire within a store partition.
// The exact shape will be refined as we settle the desire identity model.
// For the POC, the ID is a simple string combining adapter, resource, and cluster info.
type DesireID struct {
	// Partition is the target cluster name. Used as the store partition key.
	Partition string `json:"partition"`

	// Name is the unique name of this desire within the partition.
	// Convention: {adapter}-{resource-name} or derived from the K8s resource identity.
	Name string `json:"name"`
}

// String returns the full desire ID as a string suitable for store keys.
func (id DesireID) String() string {
	return id.Partition + ":" + id.Name
}

// TargetItem identifies exactly one Kubernetes resource instance.
type TargetItem struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

// GVR returns the GroupVersionResource for this target item.
func (t TargetItem) GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    t.Group,
		Version:  t.Version,
		Resource: t.Resource,
	}
}

// DesireStatus is the uniform condition contract across all desire types.
// Two writers exist per desire: the adapter writes .spec, the agent writes .status.
type DesireStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition reasons used across all desire types.
const (
	// ConditionTypeSuccessful indicates whether the desired effect was achieved.
	ConditionTypeSuccessful = "Successful"

	// ConditionTypeDegraded indicates controller-level health.
	ConditionTypeDegraded = "Degraded"

	// ReasonKubeAPIError indicates the kube-apiserver call failed.
	ReasonKubeAPIError = "KubeAPIError"

	// ReasonPreCheckFailed indicates the call could not be executed.
	ReasonPreCheckFailed = "PreCheckFailed"

	// ReasonWaitingForDeletion indicates the object still exists but has a deletion timestamp.
	ReasonWaitingForDeletion = "WaitingForDeletion"

	// ReasonApplied indicates the resource was successfully applied.
	ReasonApplied = "Applied"

	// ReasonDeleted indicates the resource is confirmed gone.
	ReasonDeleted = "Deleted"

	// ReasonSynced indicates the informer/watch is synced and live object is mirrored.
	ReasonSynced = "Synced"
)

// ApplyDesire represents the intent to make a Kubernetes resource exist with specific content.
// The applier agent processes this by issuing server-side apply with force=true.
//
// Successful=True when the apply call succeeds.
// Successful=False with ReasonKubeAPIError when the kube-apiserver call fails.
// Successful=False with ReasonPreCheckFailed when the call could not be executed.
type ApplyDesire struct {
	ID      DesireID         `json:"id"`
	Spec    ApplyDesireSpec  `json:"spec"`
	Status  DesireStatus     `json:"status"`
	Version int64            `json:"version"` // ETag for CAS writes
}

// ApplyDesireSpec holds the specification for an apply desire.
type ApplyDesireSpec struct {
	// KubeContent is the raw Kubernetes manifest as JSON bytes.
	// The store persists this opaquely; the applier parses and applies it.
	KubeContent []byte `json:"kubeContent"`

	// TargetItem identifies the resource for lookup and generation tracking.
	TargetItem TargetItem `json:"targetItem"`
}

// DeleteDesire represents the intent to make a Kubernetes resource not exist.
// The applier agent confirms the resource is gone past finalizers before reporting success.
//
// Successful=True only when the item is no longer present (not just when delete returned 200).
// Successful=False with ReasonWaitingForDeletion while a deletion timestamp is pending.
// The controller resyncs every 60 seconds.
type DeleteDesire struct {
	ID      DesireID          `json:"id"`
	Spec    DeleteDesireSpec  `json:"spec"`
	Status  DesireStatus      `json:"status"`
	Version int64             `json:"version"`
}

// DeleteDesireSpec holds the specification for a delete desire.
type DeleteDesireSpec struct {
	TargetItem TargetItem `json:"targetItem"`
}

// ReadDesire represents the intent to mirror a live Kubernetes object's state.
// The applier agent watches the target resource and writes the full object into status.
//
// Successful=True when the informer/watch is synced and the live object is mirrored.
// Unconditional resync every 60 seconds so absence of the object is reportable.
type ReadDesire struct {
	ID      DesireID         `json:"id"`
	Spec    ReadDesireSpec   `json:"spec"`
	Status  ReadDesireStatus `json:"status"`
	Version int64            `json:"version"`
}

// ReadDesireSpec holds the specification for a read desire.
type ReadDesireSpec struct {
	TargetItem TargetItem `json:"targetItem"`
}

// ReadDesireStatus extends DesireStatus with the mirrored live object.
type ReadDesireStatus struct {
	DesireStatus `json:",inline"`

	// KubeContent is the full live Kubernetes object as JSON bytes.
	// Populated by the applier agent when the resource is found.
	// Nil when the resource does not exist.
	KubeContent []byte `json:"kubeContent,omitempty"`
}
