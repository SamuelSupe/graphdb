package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"
)

const (
	tenantPurgePhaseRunning  = "running"
	tenantPurgePhaseComplete = "complete"
	tenantPurgePhaseCleared  = "cleared"

	tenantPurgePhaseKey      = "purge_phase"
	tenantPurgeOperationKey  = "purge_operation_id"
	tenantPurgeFenceEpochKey = "purge_fence_epoch"
)

type cachedTenantPurgeTombstone struct {
	phase       string
	operationID string
	exists      bool
	checkedAt   time.Time
}

func (s *TenantStore) tenantPurgeTombstoneKey(tenantID string) string {
	return path.Join(s.Prefix, "control", "tenant-purges", objectSegment(tenantID)+".parquet")
}

func (s *TenantStore) beginTenantPurge(
	ctx context.Context,
	tenantID string,
	replaceRunning bool,
) (string, bool, error) {
	operationID, err := newCommitID()
	if err != nil {
		return "", false, err
	}
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		metadata, exists, meta, err := s.getTenantPurgeTombstone(ctx, tenantID)
		if err != nil {
			return "", false, err
		}
		phase, existingOperation := tenantPurgeState(metadata, exists)
		switch phase {
		case tenantPurgePhaseRunning:
			if existingOperation != "" && !replaceRunning {
				return existingOperation, false, nil
			}
		case tenantPurgePhaseComplete:
			return existingOperation, true, nil
		}
		if _, err := s.putTenantPurgeState(ctx, tenantID, tenantPurgePhaseRunning, operationID, metadata, meta); err == nil {
			return operationID, false, nil
		} else if !errors.Is(err, ErrConflict) {
			return "", false, err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return "", false, err
		}
	}
	return "", false, fmt.Errorf("%w: purge state for tenant %q changed while starting", ErrConflict, tenantID)
}

func (s *TenantStore) completeTenantPurge(ctx context.Context, tenantID string, operationID string) error {
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		metadata, exists, meta, err := s.getTenantPurgeTombstone(ctx, tenantID)
		if err != nil {
			return err
		}
		phase, currentOperation := tenantPurgeState(metadata, exists)
		if phase == tenantPurgePhaseComplete && currentOperation == operationID {
			return nil
		}
		if phase != tenantPurgePhaseRunning || currentOperation != operationID {
			return fmt.Errorf("%w: purge operation for tenant %q changed", ErrConflict, tenantID)
		}
		if _, err := s.putTenantPurgeState(ctx, tenantID, tenantPurgePhaseComplete, operationID, metadata, meta); err == nil {
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: purge state for tenant %q changed while completing", ErrConflict, tenantID)
}

func (s *TenantStore) clearTenantPurgeTombstone(ctx context.Context, tenantID string) error {
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		metadata, exists, meta, err := s.getTenantPurgeTombstone(ctx, tenantID)
		if err != nil {
			return err
		}
		phase, operationID := tenantPurgeState(metadata, exists)
		if !exists || phase == tenantPurgePhaseCleared {
			return nil
		}
		if phase == "" {
			hasData, err := s.tenantDataExists(ctx, tenantID)
			if err != nil {
				return err
			}
			if hasData {
				return ErrTenantDeleted
			}
			phase = tenantPurgePhaseComplete
		}
		if phase != tenantPurgePhaseComplete {
			return ErrTenantDeleted
		}
		residual, err := s.tenantResidualObjectsExist(ctx, tenantID)
		if err != nil {
			return err
		}
		if residual {
			return fmt.Errorf("%w: tenant %q still has objects after purge", ErrTenantDeleted, tenantID)
		}
		if _, err := s.putTenantPurgeState(ctx, tenantID, tenantPurgePhaseCleared, operationID, metadata, meta); err == nil {
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: purge state for tenant %q changed while clearing", ErrConflict, tenantID)
}

func (s *TenantStore) reopenCompletedTenantPurge(ctx context.Context, tenantID string) error {
	operationID, err := newCommitID()
	if err != nil {
		return err
	}
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		metadata, exists, meta, err := s.getTenantPurgeTombstone(ctx, tenantID)
		if err != nil {
			return err
		}
		phase, _ := tenantPurgeState(metadata, exists)
		if phase == tenantPurgePhaseRunning {
			return nil
		}
		if phase != tenantPurgePhaseComplete {
			return fmt.Errorf("%w: purge state for tenant %q changed", ErrConflict, tenantID)
		}
		if _, err := s.putTenantPurgeState(ctx, tenantID, tenantPurgePhaseRunning, operationID, metadata, meta); err == nil {
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: purge state for tenant %q changed while reopening", ErrConflict, tenantID)
}

func (s *TenantStore) putTenantPurgeState(ctx context.Context, tenantID string, phase string, operationID string, previous TenantMetadata, meta ObjectMeta) (ObjectMeta, error) {
	now := time.Now().UTC()
	fenceEpoch := tenantPurgeFenceEpoch(previous)
	if lease, _, ok := s.getCachedWriterLeaseAny(tenantID); ok && lease.FenceEpoch > fenceEpoch {
		fenceEpoch = lease.FenceEpoch
	}
	state := map[string]any{tenantPurgePhaseKey: phase, tenantPurgeOperationKey: operationID}
	if fenceEpoch > 0 {
		state[tenantPurgeFenceEpochKey] = fenceEpoch
	}
	record := TenantMetadata{
		TenantID:  tenantID,
		Status:    TenantStatusDeleted,
		Metadata:  state,
		UpdatedAt: now,
		DeletedAt: now,
	}
	data, err := marshalParquetTenantMetadata(ctx, record)
	if err != nil {
		return ObjectMeta{}, err
	}
	condition := PutCondition{IfNoneMatch: !meta.Exists}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	}
	next, err := s.Objects.PutConditional(ctx, s.tenantPurgeTombstoneKey(tenantID), data, condition)
	if err == nil {
		s.deleteCachedTenantPurgeTombstone(tenantID)
	}
	return next, err
}

func tenantPurgeFenceEpoch(metadata TenantMetadata) int64 {
	value, ok := metadata.Metadata[tenantPurgeFenceEpochKey]
	if !ok {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		if typed > 0 {
			return int64(typed)
		}
	}
	return 0
}

func (s *TenantStore) getTenantPurgeTombstone(ctx context.Context, tenantID string) (TenantMetadata, bool, ObjectMeta, error) {
	key := s.tenantPurgeTombstoneKey(tenantID)
	s.clearWriterObjectKey(key)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return TenantMetadata{}, false, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, err
	}
	metadata, err := decodeParquetTenantMetadata(ctx, data)
	if err != nil {
		return TenantMetadata{}, false, ObjectMeta{}, fmt.Errorf("decode tenant purge tombstone: %w", err)
	}
	if metadata.TenantID != tenantID || normalizeTenantStatus(metadata.Status) != TenantStatusDeleted {
		return TenantMetadata{}, false, ObjectMeta{}, fmt.Errorf("tenant purge tombstone mismatch for %q", tenantID)
	}
	return metadata, true, meta, nil
}

func tenantPurgeState(metadata TenantMetadata, exists bool) (string, string) {
	if !exists {
		return tenantPurgePhaseCleared, ""
	}
	phase, _ := metadata.Metadata[tenantPurgePhaseKey].(string)
	operationID, _ := metadata.Metadata[tenantPurgeOperationKey].(string)
	switch phase {
	case tenantPurgePhaseRunning, tenantPurgePhaseComplete, tenantPurgePhaseCleared:
		return phase, operationID
	default:
		return "", operationID
	}
}

func (s *TenantStore) tenantPurgeTombstoneExists(ctx context.Context, tenantID string) (bool, error) {
	metadata, exists, _, err := s.getTenantPurgeTombstone(ctx, tenantID)
	if err != nil || !exists {
		if err == nil {
			s.setCachedTenantPurgeTombstone(tenantID, cachedTenantPurgeTombstone{phase: tenantPurgePhaseCleared, checkedAt: time.Now().UTC()})
		}
		return false, err
	}
	phase, operationID := tenantPurgeState(metadata, true)
	s.setCachedTenantPurgeTombstone(tenantID, cachedTenantPurgeTombstone{
		phase: phase, operationID: operationID, exists: true, checkedAt: time.Now().UTC(),
	})
	return phase != tenantPurgePhaseCleared, nil
}

func (s *TenantStore) tenantPurgeTombstoneExistsCached(ctx context.Context, tenantID string) (bool, error) {
	now := time.Now().UTC()
	if cached, ok := s.getCachedTenantPurgeTombstone(tenantID, now); ok {
		return cached.exists && cached.phase != tenantPurgePhaseCleared, nil
	}
	return s.tenantPurgeTombstoneExists(ctx, tenantID)
}

func (s *TenantStore) getCachedTenantPurgeTombstone(tenantID string, now time.Time) (cachedTenantPurgeTombstone, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.purgeTombstoneCache[tenantID]
	return cached, ok && now.Sub(cached.checkedAt) < s.lifecycleCacheTTL()
}

func (s *TenantStore) setCachedTenantPurgeTombstone(tenantID string, cached cachedTenantPurgeTombstone) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	evictOneCacheEntry(s.purgeTombstoneCache, tenantID, maxWriterMetadataCacheEntries)
	s.purgeTombstoneCache[tenantID] = cached
}

func (s *TenantStore) deleteCachedTenantPurgeTombstone(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.purgeTombstoneCache, tenantID)
}

func (s *TenantStore) lifecycleCacheTTL() time.Duration {
	if s.LifecycleCacheTTL <= 0 {
		return time.Second
	}
	return s.LifecycleCacheTTL
}
