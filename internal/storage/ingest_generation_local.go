package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
)

func (s *TenantStore) localIngestGenerationKey(tenantID string) string {
	return path.Join(s.Prefix, "control", "tenant-generations", tenantID+".json")
}

func (s *TenantStore) localIngestGeneration(ctx context.Context, tenantID string) (int64, error) {
	files := s.localFileStore()
	if files == nil {
		return 0, nil
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return 0, err
	}
	key := s.localIngestGenerationKey(tenantID)
	r := files.runtime
	r.mu.Lock()
	cached, ok := r.walGenerations[key]
	observed := r.generation
	r.mu.Unlock()
	if ok {
		return cached, nil
	}
	data, err := files.Get(ctx, key)
	generation := int64(1)
	if errors.Is(err, ErrNotFound) {
		// Pre-generation directories may already have been purged. Never bind
		// an unversioned legacy WAL to such a later tenant incarnation.
		if _, markerErr := files.Get(ctx, s.tenantPurgeTombstoneKey(tenantID)); markerErr == nil {
			generation = 2
		} else if !errors.Is(markerErr, ErrNotFound) {
			return 0, markerErr
		}
	} else if err != nil {
		return 0, err
	} else if err := json.Unmarshal(data, &generation); err != nil || generation < 1 {
		return 0, fmt.Errorf("invalid persisted WAL generation for tenant %q", tenantID)
	}
	r.mu.Lock()
	if r.generation == observed {
		if r.walGenerations == nil || len(r.walGenerations) >= 4096 {
			r.walGenerations = make(map[string]int64)
		}
		r.walGenerations[key] = generation
	}
	r.mu.Unlock()
	return generation, nil
}

// The caller pauses WAL admission and holds the exclusive tenant read view.
// This record deliberately survives deleting or replacing the tenant directory.
func (s *TenantStore) advanceLocalIngestGeneration(ctx context.Context, tenantID string) error {
	if s.localFileStore() == nil {
		return nil
	}
	generation, err := s.localIngestGeneration(ctx, tenantID)
	if err != nil {
		return err
	}
	if generation == math.MaxInt64 {
		return fmt.Errorf("tenant WAL generation exhausted")
	}
	data, err := json.Marshal(generation + 1)
	if err != nil {
		return err
	}
	return s.Objects.Put(ctx, s.localIngestGenerationKey(tenantID), data)
}

func (s *TenantStore) validateLocalIngestGeneration(ctx context.Context, tenantID string, expected int64) error {
	if s.localFileStore() == nil || expected == 0 {
		return nil
	}
	current, err := s.localIngestGeneration(ctx, tenantID)
	if err != nil {
		return err
	}
	if expected == legacyUnboundIngestGeneration && current == 1 {
		return nil
	}
	if current != expected {
		return fmt.Errorf("%w: %w: tenant %q WAL generation changed from %d to %d", ErrTenantDeleted, errIngestGenerationFenced, tenantID, expected, current)
	}
	return nil
}
