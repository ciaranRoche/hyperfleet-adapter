// Package memory provides an in-memory implementation of the desire store interfaces.
// Intended for unit tests and local development. Not safe for multi-process access.
package memory

import (
	"context"
	"strings"
	"sync"

	"github.com/openshift-hyperfleet/hyperfleet-adapter/pkg/desire"
)

// Store implements both desire.SpecStore and desire.StatusStore in memory.
// Thread-safe via a mutex. All data is lost on process restart.
type Store struct {
	mu            sync.RWMutex
	applyDesires  map[string]*desire.ApplyDesire
	deleteDesires map[string]*desire.DeleteDesire
	readDesires   map[string]*desire.ReadDesire
}

// New creates a new in-memory desire store.
func New() *Store {
	return &Store{
		applyDesires:  make(map[string]*desire.ApplyDesire),
		deleteDesires: make(map[string]*desire.DeleteDesire),
		readDesires:   make(map[string]*desire.ReadDesire),
	}
}

// key builds the internal map key from a DesireID.
func key(id desire.DesireID) string {
	return id.String()
}

// partitionPrefix returns the prefix for partition-scoped queries.
func partitionPrefix(partition string) string {
	return partition + ":"
}

// --- ApplyDesire ---

func (s *Store) CreateApplyDesire(_ context.Context, d *desire.ApplyDesire) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(d.ID)
	if _, exists := s.applyDesires[k]; exists {
		return desire.ErrVersionConflict
	}
	d.Version = 1
	cp := *d
	s.applyDesires[k] = &cp
	return nil
}

func (s *Store) GetApplyDesire(_ context.Context, id desire.DesireID) (*desire.ApplyDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.applyDesires[key(id)]
	if !ok {
		return nil, desire.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *Store) UpdateApplyDesireSpec(_ context.Context, id desire.DesireID, spec desire.ApplyDesireSpec, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.applyDesires[key(id)]
	if !ok {
		return desire.ErrNotFound
	}
	if d.Version != version {
		return desire.ErrVersionConflict
	}
	d.Spec = spec
	d.Version++
	return nil
}

func (s *Store) UpdateApplyDesireStatus(_ context.Context, id desire.DesireID, status desire.DesireStatus, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.applyDesires[key(id)]
	if !ok {
		return desire.ErrNotFound
	}
	if d.Version != version {
		return desire.ErrVersionConflict
	}
	d.Status = status
	d.Version++
	return nil
}

func (s *Store) DeleteApplyDesire(_ context.Context, id desire.DesireID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.applyDesires, key(id))
	return nil
}

func (s *Store) ListApplyDesires(_ context.Context, partition string) ([]*desire.ApplyDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := partitionPrefix(partition)
	var result []*desire.ApplyDesire
	for k, d := range s.applyDesires {
		if strings.HasPrefix(k, prefix) {
			cp := *d
			result = append(result, &cp)
		}
	}
	return result, nil
}

// --- DeleteDesire ---

func (s *Store) CreateDeleteDesire(_ context.Context, d *desire.DeleteDesire) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(d.ID)
	if _, exists := s.deleteDesires[k]; exists {
		return desire.ErrVersionConflict
	}
	d.Version = 1
	cp := *d
	s.deleteDesires[k] = &cp
	return nil
}

func (s *Store) GetDeleteDesire(_ context.Context, id desire.DesireID) (*desire.DeleteDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.deleteDesires[key(id)]
	if !ok {
		return nil, desire.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *Store) UpdateDeleteDesireStatus(_ context.Context, id desire.DesireID, status desire.DesireStatus, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deleteDesires[key(id)]
	if !ok {
		return desire.ErrNotFound
	}
	if d.Version != version {
		return desire.ErrVersionConflict
	}
	d.Status = status
	d.Version++
	return nil
}

func (s *Store) DeleteDeleteDesire(_ context.Context, id desire.DesireID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.deleteDesires, key(id))
	return nil
}

func (s *Store) ListDeleteDesires(_ context.Context, partition string) ([]*desire.DeleteDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := partitionPrefix(partition)
	var result []*desire.DeleteDesire
	for k, d := range s.deleteDesires {
		if strings.HasPrefix(k, prefix) {
			cp := *d
			result = append(result, &cp)
		}
	}
	return result, nil
}

// --- ReadDesire ---

func (s *Store) CreateReadDesire(_ context.Context, d *desire.ReadDesire) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(d.ID)
	if _, exists := s.readDesires[k]; exists {
		return desire.ErrVersionConflict
	}
	d.Version = 1
	cp := *d
	s.readDesires[k] = &cp
	return nil
}

func (s *Store) GetReadDesire(_ context.Context, id desire.DesireID) (*desire.ReadDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.readDesires[key(id)]
	if !ok {
		return nil, desire.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *Store) UpdateReadDesireStatus(_ context.Context, id desire.DesireID, status desire.ReadDesireStatus, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.readDesires[key(id)]
	if !ok {
		return desire.ErrNotFound
	}
	if d.Version != version {
		return desire.ErrVersionConflict
	}
	d.Status = status
	d.Version++
	return nil
}

func (s *Store) DeleteReadDesire(_ context.Context, id desire.DesireID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.readDesires, key(id))
	return nil
}

func (s *Store) ListReadDesires(_ context.Context, partition string) ([]*desire.ReadDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := partitionPrefix(partition)
	var result []*desire.ReadDesire
	for k, d := range s.readDesires {
		if strings.HasPrefix(k, prefix) {
			cp := *d
			result = append(result, &cp)
		}
	}
	return result, nil
}

// --- Cross-cutting ---

func (s *Store) DeleteByPrefix(_ context.Context, partition string, prefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fullPrefix := partitionPrefix(partition) + prefix
	for k := range s.applyDesires {
		if strings.HasPrefix(k, fullPrefix) {
			delete(s.applyDesires, k)
		}
	}
	for k := range s.deleteDesires {
		if strings.HasPrefix(k, fullPrefix) {
			delete(s.deleteDesires, k)
		}
	}
	for k := range s.readDesires {
		if strings.HasPrefix(k, fullPrefix) {
			delete(s.readDesires, k)
		}
	}
	return nil
}

// Compile-time interface checks.
var (
	_ desire.SpecStore   = (*Store)(nil)
	_ desire.StatusStore = (*Store)(nil)
)
