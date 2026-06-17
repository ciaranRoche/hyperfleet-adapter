package desire

import (
	"context"
	"errors"
)

// ErrVersionConflict is returned when a CAS write fails due to version mismatch.
var ErrVersionConflict = errors.New("desire version conflict: object was modified")

// ErrNotFound is returned when a desire does not exist in the store.
var ErrNotFound = errors.New("desire not found")

// SpecStore is the adapter's view of the desire store.
// Adapters write desire specs and read back statuses written by agents.
type SpecStore interface {
	// ApplyDesire operations
	CreateApplyDesire(ctx context.Context, desire *ApplyDesire) error
	GetApplyDesire(ctx context.Context, id DesireID) (*ApplyDesire, error)
	UpdateApplyDesireSpec(ctx context.Context, id DesireID, spec ApplyDesireSpec, version int64) error
	DeleteApplyDesire(ctx context.Context, id DesireID) error
	ListApplyDesires(ctx context.Context, partition string) ([]*ApplyDesire, error)

	// DeleteDesire operations
	CreateDeleteDesire(ctx context.Context, desire *DeleteDesire) error
	GetDeleteDesire(ctx context.Context, id DesireID) (*DeleteDesire, error)
	DeleteDeleteDesire(ctx context.Context, id DesireID) error
	ListDeleteDesires(ctx context.Context, partition string) ([]*DeleteDesire, error)

	// ReadDesire operations
	CreateReadDesire(ctx context.Context, desire *ReadDesire) error
	GetReadDesire(ctx context.Context, id DesireID) (*ReadDesire, error)
	DeleteReadDesire(ctx context.Context, id DesireID) error
	ListReadDesires(ctx context.Context, partition string) ([]*ReadDesire, error)

	// DeleteByPrefix removes all desires in a partition matching the given prefix.
	// Used for cleanup when a cluster is deprovisioned.
	DeleteByPrefix(ctx context.Context, partition string, prefix string) error
}

// StatusStore is the agent's view of the desire store.
// Agents read desire specs and write back statuses after reconciliation.
type StatusStore interface {
	// ApplyDesire
	GetApplyDesire(ctx context.Context, id DesireID) (*ApplyDesire, error)
	UpdateApplyDesireStatus(ctx context.Context, id DesireID, status DesireStatus, version int64) error
	ListApplyDesires(ctx context.Context, partition string) ([]*ApplyDesire, error)

	// DeleteDesire
	GetDeleteDesire(ctx context.Context, id DesireID) (*DeleteDesire, error)
	UpdateDeleteDesireStatus(ctx context.Context, id DesireID, status DesireStatus, version int64) error
	ListDeleteDesires(ctx context.Context, partition string) ([]*DeleteDesire, error)

	// ReadDesire
	GetReadDesire(ctx context.Context, id DesireID) (*ReadDesire, error)
	UpdateReadDesireStatus(ctx context.Context, id DesireID, status ReadDesireStatus, version int64) error
	ListReadDesires(ctx context.Context, partition string) ([]*ReadDesire, error)
}
