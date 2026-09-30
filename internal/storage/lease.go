package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type WriterLease struct {
	TenantID   string    `json:"tenant_id"`
	OwnerID    string    `json:"owner_id"`
	FenceToken string    `json:"fence_token,omitempty"`
	FenceEpoch int64     `json:"fence_epoch,omitempty"`
	ExpiresAt  time.Time `json:"expires_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (s *TenantStore) acquireWriterLease(ctx context.Context, tenantID string) error {
	if bound, ok := s.writerFenceFromContext(ctx, tenantID); ok {
		return s.ensureBoundWriterLease(ctx, tenantID, bound.fence)
	}
	return s.acquireWriterLeaseMode(ctx, tenantID, false)
}

func (s *TenantStore) acquireWriterLeaseForPurge(ctx context.Context, tenantID string) error {
	return s.acquireWriterLeaseMode(ctx, tenantID, true)
}

func (s *TenantStore) acquireWriterLeaseMode(ctx context.Context, tenantID string, allowPurged bool) error {
	now := mutationTime(ctx)
	if _, _, ok := s.getCachedWriterLease(tenantID); ok && !IsReplicatedContext(ctx) {
		return nil
	}
	if !allowPurged {
		purged, err := s.tenantPurgeTombstoneExists(ctx, tenantID)
		if err != nil {
			return err
		}
		if purged {
			return ErrTenantDeleted
		}
	}
	token, err := mutationID(ctx, "lease")
	if err != nil {
		return fmt.Errorf("create writer fence: %w", err)
	}
	next := WriterLease{
		TenantID:   tenantID,
		OwnerID:    s.InstanceID,
		FenceToken: token,
		FenceEpoch: 1,
		UpdatedAt:  now,
	}
	if allowPurged {
		metadata, exists, _, err := s.getTenantPurgeTombstone(ctx, tenantID)
		if err != nil {
			return err
		}
		lastEpoch := tenantPurgeFenceEpoch(metadata)
		if exists && lastEpoch >= next.FenceEpoch {
			next.FenceEpoch = lastEpoch + 1
		}
	}
	key := s.writerLeaseKey(tenantID)
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		current, meta, err := s.getWriterLease(ctx, tenantID, key)
		switch {
		case errors.Is(err, ErrNotFound):
			if meta, err := s.putLease(ctx, key, next, ObjectMeta{Key: key}); err == nil {
				s.invalidateWriterTakeoverState(tenantID)
				return s.finishWriterLeaseAcquire(ctx, tenantID, next, meta)
			} else if !errors.Is(err, ErrConflict) {
				return err
			}
		case err != nil:
			return err
		default:
			if current.OwnerID == s.InstanceID {
				if current.FenceToken != "" {
					next.FenceToken = current.FenceToken
				}
				if current.FenceEpoch > 0 {
					next.FenceEpoch = current.FenceEpoch
				}
			} else if current.FenceEpoch >= next.FenceEpoch {
				next.FenceEpoch = current.FenceEpoch + 1
			}
			var cached loadedGraph
			retainCache := IsReplicatedContext(ctx) && s.localFileStore() != nil &&
				current.OwnerID == s.InstanceID && current.FenceToken == next.FenceToken && current.FenceEpoch == next.FenceEpoch
			if retainCache {
				cached, retainCache = s.getWriteCache(tenantID)
			}
			if meta, err := s.putLease(ctx, key, next, meta); err == nil {
				if current.OwnerID != s.InstanceID || current.FenceToken != next.FenceToken {
					s.invalidateWriterTakeoverState(tenantID)
				}
				if err := s.finishWriterLeaseAcquire(ctx, tenantID, next, meta); err != nil {
					return err
				}
				// Local lease notifications invalidate all tenant caches. An
				// unchanged fence can retain its immutable graph after checking
				// the manifest identity, including restore/recreation boundaries.
				if retainCache {
					manifest, manifestMeta, err := s.getManifest(ctx, tenantID)
					if err == nil && cachedManifestMatches(cached, manifest, manifestMeta) {
						s.setWriteCache(tenantID, cached)
					}
				}
				return nil
			} else if !errors.Is(err, ErrConflict) {
				return err
			}
		}
		s.deleteCachedWriterLease(tenantID)
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: tenant %q lease changed during acquire", ErrConflict, tenantID)
}

func (s *TenantStore) finishWriterLeaseAcquire(ctx context.Context, tenantID string, lease WriterLease, meta ObjectMeta) error {
	if err := s.publishWriterFence(ctx, tenantID, lease); err != nil {
		s.deleteCachedWriterLease(tenantID)
		return err
	}
	s.setCachedWriterLease(tenantID, lease, meta)
	return nil
}

func (s *TenantStore) releaseWriterLeaseForPurge(ctx context.Context, tenantID string) error {
	key := s.writerLeaseKey(tenantID)
	s.clearWriterObjectKey(key)
	lease, meta, err := s.getWriterLease(ctx, tenantID, key)
	if errors.Is(err, ErrNotFound) {
		s.deleteCachedWriterLease(tenantID)
		return nil
	}
	if err != nil {
		return err
	}
	cached, _, ok := s.getCachedWriterLeaseAny(tenantID)
	if !ok || lease.OwnerID != s.InstanceID || lease.FenceToken != cached.FenceToken || lease.FenceEpoch != cached.FenceEpoch {
		return fmt.Errorf("%w: tenant %q writer lease changed during purge", ErrLeaseHeld, tenantID)
	}
	err = s.Objects.DeleteConditional(ctx, key, PutCondition{IfMatch: meta.ETag})
	if err == nil || errors.Is(err, ErrNotFound) {
		s.deleteCachedWriterLease(tenantID)
		return nil
	}
	if !errors.Is(err, ErrConditionalDeleteUnsupported) {
		return err
	}
	// Some object stores cannot conditionally delete. Retire the lease in
	// place there so a replacement still has to advance the fence epoch.
	now := mutationTime(ctx)
	lease.OwnerID = ""
	lease.ExpiresAt = now.Add(-time.Nanosecond)
	lease.UpdatedAt = now
	_, err = s.putLease(ctx, key, lease, meta)
	if err == nil {
		s.deleteCachedWriterLease(tenantID)
	}
	return err
}

func (s *TenantStore) GetWriterLease(ctx context.Context, tenantID string) (WriterLease, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return WriterLease{}, err
	}
	lease, _, err := s.getWriterLease(ctx, tenantID, s.writerLeaseKey(tenantID))
	return lease, err
}

func (s *TenantStore) getWriterLease(ctx context.Context, tenantID string, key string) (WriterLease, ObjectMeta, error) {
	if cached, meta, ok := s.getCachedWriterLeaseAny(tenantID); ok {
		current, err := objectMeta(ctx, s.Objects, key)
		if err != nil {
			return WriterLease{}, current, err
		}
		if current.ETag == meta.ETag {
			return cached, current, nil
		}
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return WriterLease{}, meta, err
	}
	if !isParquetBytes(data) {
		return WriterLease{}, meta, fmt.Errorf("unsupported writer lease: only parquet leases are readable")
	}
	lease, err := decodeParquetWriterLease(ctx, data)
	if err != nil {
		return WriterLease{}, meta, err
	}
	if lease.TenantID != "" && lease.TenantID != tenantID {
		return WriterLease{}, meta, fmt.Errorf("writer lease tenant mismatch: path tenant %q contains tenant %q", tenantID, lease.TenantID)
	}
	if lease.TenantID == "" {
		lease.TenantID = tenantID
	}
	return lease, meta, nil
}

func (s *TenantStore) putLease(ctx context.Context, key string, lease WriterLease, meta ObjectMeta) (ObjectMeta, error) {
	data, err := marshalParquetWriterLease(ctx, lease)
	if err != nil {
		return ObjectMeta{}, err
	}
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	return s.Objects.PutConditional(ctx, key, data, condition)
}

func (s *TenantStore) retryCount() int {
	if s.MaxRetries < 1 {
		return 1
	}
	return s.MaxRetries
}
