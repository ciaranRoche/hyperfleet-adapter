// Package desireclient provides a TransportClient implementation that delivers
// Kubernetes resources via the desire-based delivery system.
//
// Adapter authors write plain Kubernetes manifests. This transport wraps them
// as desires (ApplyDesire, DeleteDesire, ReadDesire) and writes them to a
// pluggable desire store. An applier agent on the target cluster reads the
// desires, reconciles them, and writes status back.
package desireclient

import (
	"github.com/openshift-hyperfleet/hyperfleet-adapter/pkg/desire"
)

// TransportContext carries per-request routing information for the desire transport.
// The Partition field identifies which target cluster this request is for,
// and becomes the partition key in the desire store.
type TransportContext struct {
	// Partition is the target cluster name. Maps to DesireID.Partition.
	Partition string
}

// Config holds configuration for creating a DesireClient.
type Config struct {
	// SpecStore is the store for writing desire specs (adapter side).
	SpecStore desire.SpecStore

	// StatusStore is the store for reading desire statuses (written by the agent).
	StatusStore desire.StatusStore
}
