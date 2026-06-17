// Package redis provides a Redis-backed implementation of the desire store interfaces.
// Used for integration testing and POC deployments. Key scheme: {partition}:{type}:{name}
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/openshift-hyperfleet/hyperfleet-adapter/pkg/desire"
	goredis "github.com/redis/go-redis/v9"
)

const (
	applyPrefix  = "apply"
	deletePrefix = "delete"
	readPrefix   = "read"
)

// Config holds Redis connection configuration.
type Config struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password,omitempty"`
	DB       int    `yaml:"db"`
}

// Store implements both desire.SpecStore and desire.StatusStore against Redis.
type Store struct {
	client *goredis.Client
}

// New creates a new Redis desire store.
func New(cfg Config) (*Store, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return &Store{client: client}, nil
}

// Close closes the Redis connection.
func (s *Store) Close() error {
	return s.client.Close()
}

// Ping checks the Redis connection.
func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// redisKey builds the Redis key for a desire.
func redisKey(desireType string, id desire.DesireID) string {
	return fmt.Sprintf("%s:%s:%s", id.Partition, desireType, id.Name)
}

// scanPattern builds the Redis SCAN pattern for listing desires by partition and type.
func scanPattern(partition, desireType string) string {
	return fmt.Sprintf("%s:%s:*", partition, desireType)
}

// --- ApplyDesire ---

func (s *Store) CreateApplyDesire(ctx context.Context, d *desire.ApplyDesire) error {
	d.Version = 1
	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("marshal apply desire: %w", err)
	}
	k := redisKey(applyPrefix, d.ID)
	ok, err := s.client.SetNX(ctx, k, data, 0).Result()
	if err != nil {
		return fmt.Errorf("redis setnx: %w", err)
	}
	if !ok {
		return desire.ErrVersionConflict
	}
	return nil
}

func (s *Store) GetApplyDesire(ctx context.Context, id desire.DesireID) (*desire.ApplyDesire, error) {
	data, err := s.client.Get(ctx, redisKey(applyPrefix, id)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, desire.ErrNotFound
		}
		return nil, fmt.Errorf("redis get: %w", err)
	}
	var d desire.ApplyDesire
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("unmarshal apply desire: %w", err)
	}
	return &d, nil
}

func (s *Store) UpdateApplyDesireSpec(ctx context.Context, id desire.DesireID, spec desire.ApplyDesireSpec, version int64) error {
	return s.casUpdate(ctx, applyPrefix, id, version, func(raw []byte) ([]byte, error) {
		var d desire.ApplyDesire
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		d.Spec = spec
		d.Version++
		return json.Marshal(d)
	})
}

func (s *Store) UpdateApplyDesireStatus(ctx context.Context, id desire.DesireID, status desire.DesireStatus, version int64) error {
	return s.casUpdate(ctx, applyPrefix, id, version, func(raw []byte) ([]byte, error) {
		var d desire.ApplyDesire
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		d.Status = status
		d.Version++
		return json.Marshal(d)
	})
}

func (s *Store) DeleteApplyDesire(ctx context.Context, id desire.DesireID) error {
	return s.client.Del(ctx, redisKey(applyPrefix, id)).Err()
}

func (s *Store) ListApplyDesires(ctx context.Context, partition string) ([]*desire.ApplyDesire, error) {
	keys, err := s.scanKeys(ctx, scanPattern(partition, applyPrefix))
	if err != nil {
		return nil, err
	}
	var result []*desire.ApplyDesire
	for _, k := range keys {
		data, err := s.client.Get(ctx, k).Bytes()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				continue // deleted between scan and get
			}
			return nil, fmt.Errorf("redis get %s: %w", k, err)
		}
		var d desire.ApplyDesire
		if err := json.Unmarshal(data, &d); err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", k, err)
		}
		result = append(result, &d)
	}
	return result, nil
}

// --- DeleteDesire ---

func (s *Store) CreateDeleteDesire(ctx context.Context, d *desire.DeleteDesire) error {
	d.Version = 1
	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("marshal delete desire: %w", err)
	}
	k := redisKey(deletePrefix, d.ID)
	ok, err := s.client.SetNX(ctx, k, data, 0).Result()
	if err != nil {
		return fmt.Errorf("redis setnx: %w", err)
	}
	if !ok {
		return desire.ErrVersionConflict
	}
	return nil
}

func (s *Store) GetDeleteDesire(ctx context.Context, id desire.DesireID) (*desire.DeleteDesire, error) {
	data, err := s.client.Get(ctx, redisKey(deletePrefix, id)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, desire.ErrNotFound
		}
		return nil, fmt.Errorf("redis get: %w", err)
	}
	var d desire.DeleteDesire
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("unmarshal delete desire: %w", err)
	}
	return &d, nil
}

func (s *Store) UpdateDeleteDesireStatus(ctx context.Context, id desire.DesireID, status desire.DesireStatus, version int64) error {
	return s.casUpdate(ctx, deletePrefix, id, version, func(raw []byte) ([]byte, error) {
		var d desire.DeleteDesire
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		d.Status = status
		d.Version++
		return json.Marshal(d)
	})
}

func (s *Store) DeleteDeleteDesire(ctx context.Context, id desire.DesireID) error {
	return s.client.Del(ctx, redisKey(deletePrefix, id)).Err()
}

func (s *Store) ListDeleteDesires(ctx context.Context, partition string) ([]*desire.DeleteDesire, error) {
	keys, err := s.scanKeys(ctx, scanPattern(partition, deletePrefix))
	if err != nil {
		return nil, err
	}
	var result []*desire.DeleteDesire
	for _, k := range keys {
		data, err := s.client.Get(ctx, k).Bytes()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				continue
			}
			return nil, fmt.Errorf("redis get %s: %w", k, err)
		}
		var d desire.DeleteDesire
		if err := json.Unmarshal(data, &d); err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", k, err)
		}
		result = append(result, &d)
	}
	return result, nil
}

// --- ReadDesire ---

func (s *Store) CreateReadDesire(ctx context.Context, d *desire.ReadDesire) error {
	d.Version = 1
	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("marshal read desire: %w", err)
	}
	k := redisKey(readPrefix, d.ID)
	ok, err := s.client.SetNX(ctx, k, data, 0).Result()
	if err != nil {
		return fmt.Errorf("redis setnx: %w", err)
	}
	if !ok {
		return desire.ErrVersionConflict
	}
	return nil
}

func (s *Store) GetReadDesire(ctx context.Context, id desire.DesireID) (*desire.ReadDesire, error) {
	data, err := s.client.Get(ctx, redisKey(readPrefix, id)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, desire.ErrNotFound
		}
		return nil, fmt.Errorf("redis get: %w", err)
	}
	var d desire.ReadDesire
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("unmarshal read desire: %w", err)
	}
	return &d, nil
}

func (s *Store) UpdateReadDesireStatus(ctx context.Context, id desire.DesireID, status desire.ReadDesireStatus, version int64) error {
	return s.casUpdate(ctx, readPrefix, id, version, func(raw []byte) ([]byte, error) {
		var d desire.ReadDesire
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		d.Status = status
		d.Version++
		return json.Marshal(d)
	})
}

func (s *Store) DeleteReadDesire(ctx context.Context, id desire.DesireID) error {
	return s.client.Del(ctx, redisKey(readPrefix, id)).Err()
}

func (s *Store) ListReadDesires(ctx context.Context, partition string) ([]*desire.ReadDesire, error) {
	keys, err := s.scanKeys(ctx, scanPattern(partition, readPrefix))
	if err != nil {
		return nil, err
	}
	var result []*desire.ReadDesire
	for _, k := range keys {
		data, err := s.client.Get(ctx, k).Bytes()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				continue
			}
			return nil, fmt.Errorf("redis get %s: %w", k, err)
		}
		var d desire.ReadDesire
		if err := json.Unmarshal(data, &d); err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", k, err)
		}
		result = append(result, &d)
	}
	return result, nil
}

// --- Cross-cutting ---

func (s *Store) DeleteByPrefix(ctx context.Context, partition string, prefix string) error {
	for _, dt := range []string{applyPrefix, deletePrefix, readPrefix} {
		pattern := fmt.Sprintf("%s:%s:%s*", partition, dt, prefix)
		keys, err := s.scanKeys(ctx, pattern)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := s.client.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("redis del by prefix: %w", err)
			}
		}
	}
	return nil
}

// --- Helpers ---

// casUpdate performs a compare-and-swap update using Redis WATCH/MULTI/EXEC.
func (s *Store) casUpdate(
	ctx context.Context,
	desireType string,
	id desire.DesireID,
	expectedVersion int64,
	mutate func(raw []byte) ([]byte, error),
) error {
	k := redisKey(desireType, id)

	// Use a transaction with WATCH for optimistic locking
	err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
		raw, err := tx.Get(ctx, k).Bytes()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				return desire.ErrNotFound
			}
			return fmt.Errorf("redis get in watch: %w", err)
		}

		// Check the version matches what the caller expects
		var versionCheck struct {
			Version int64 `json:"version"`
		}
		if err := json.Unmarshal(raw, &versionCheck); err != nil {
			return fmt.Errorf("unmarshal version: %w", err)
		}
		if versionCheck.Version != expectedVersion {
			return desire.ErrVersionConflict
		}

		updated, err := mutate(raw)
		if err != nil {
			return fmt.Errorf("mutate: %w", err)
		}

		_, err = tx.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
			pipe.Set(ctx, k, updated, 0)
			return nil
		})
		return err
	}, k)

	if err != nil {
		// WATCH detected a concurrent modification
		if errors.Is(err, goredis.TxFailedErr) {
			return desire.ErrVersionConflict
		}
		return err
	}
	return nil
}

// scanKeys collects all keys matching a pattern using SCAN (non-blocking).
func (s *Store) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var allKeys []string
	var cursor uint64
	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("redis scan %q: %w", pattern, err)
		}
		// Filter out keys that don't actually match (SCAN can return false positives)
		for _, k := range keys {
			if matchesPattern(k, pattern) {
				allKeys = append(allKeys, k)
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return allKeys, nil
}

// matchesPattern does a simple glob check for patterns ending in *.
func matchesPattern(key, pattern string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
	}
	return key == pattern
}

// Compile-time interface checks.
var (
	_ desire.SpecStore   = (*Store)(nil)
	_ desire.StatusStore = (*Store)(nil)
)
