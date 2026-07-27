package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type writerFenceContextKey struct{}

type boundWriterFence struct {
	store    *TenantStore
	tenantID string
	leaseKey string
	fence    writerFenceRef
}

type writerFenceRef struct {
	ownerID string
	token   string
	epoch   int64
}

type tenantGenerationRef struct {
	fenceEpoch int64
	protected  bool
}

func (s *TenantStore) prepareTenantWrite(ctx context.Context, tenantID string) (writerFenceRef, error) {
	if s.coordinated() {
		head, exists, err := s.Coordinator.Head(ctx, tenantID)
		if err != nil {
			return writerFenceRef{}, err
		}
		if !exists {
			return writerFenceRef{}, ErrCoordinatorHeadMissing
		}
		if head.Status != TenantStatusActive {
			return writerFenceRef{}, ErrTenantDeleted
		}
		return writerFenceRef{epoch: head.Generation}, nil
	}
	if bound, ok := s.writerFenceFromContext(ctx, tenantID); ok {
		if err := s.ensureBoundWriterLease(ctx, tenantID, bound.fence); err != nil {
			return writerFenceRef{}, err
		}
		return bound.fence, nil
	}
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return writerFenceRef{}, err
	}
	lease, _, ok := s.getCachedWriterLeaseAny(tenantID)
	if !ok || lease.FenceToken == "" || lease.FenceEpoch <= 0 {
		return writerFenceRef{}, fmt.Errorf("%w: tenant %q has no active writer fence", ErrLeaseHeld, tenantID)
	}
	return writerFenceRef{ownerID: lease.OwnerID, token: lease.FenceToken, epoch: lease.FenceEpoch}, nil
}

func (s *TenantStore) bindCurrentWriterFence(ctx context.Context, tenantID string) (context.Context, error) {
	if bound, ok := s.writerFenceFromContext(ctx, tenantID); ok {
		if err := s.ensureBoundWriterLease(ctx, tenantID, bound.fence); err != nil {
			return nil, err
		}
		return ctx, nil
	}
	return s.rebindCurrentWriterFence(ctx, tenantID)
}

func (s *TenantStore) rebindCurrentWriterFence(ctx context.Context, tenantID string) (context.Context, error) {
	if s.coordinated() {
		return ctx, nil
	}
	lease, _, ok := s.getCachedWriterLeaseAny(tenantID)
	if !ok || lease.FenceToken == "" || lease.FenceEpoch <= 0 {
		return nil, fmt.Errorf("%w: tenant %q has no active writer fence", ErrLeaseHeld, tenantID)
	}
	bound := boundWriterFence{
		store:    s,
		tenantID: tenantID,
		leaseKey: s.writerLeaseKey(tenantID),
		fence: writerFenceRef{
			ownerID: lease.OwnerID,
			token:   lease.FenceToken,
			epoch:   lease.FenceEpoch,
		},
	}
	return context.WithValue(ctx, writerFenceContextKey{}, bound), nil
}

func (s *TenantStore) acquireAndBindWriterFence(ctx context.Context, tenantID string) (context.Context, error) {
	if s.coordinated() {
		return ctx, nil
	}
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return nil, err
	}
	return s.bindCurrentWriterFence(ctx, tenantID)
}

func (s *TenantStore) prepareCreateAndBindWriterFence(ctx context.Context, tenantID string) (context.Context, error) {
	if s.coordinated() {
		_, err := s.ensureCoordinatedTenantHeadForCreate(ctx, tenantID)
		return ctx, err
	}
	if err := s.prepareTenantCreateLease(ctx, tenantID); err != nil {
		return nil, err
	}
	return s.rebindCurrentWriterFence(ctx, tenantID)
}

func (s *TenantStore) writerFenceFromContext(ctx context.Context, tenantID string) (boundWriterFence, bool) {
	bound, ok := ctx.Value(writerFenceContextKey{}).(boundWriterFence)
	return bound, ok && bound.store == s && bound.tenantID == tenantID && bound.leaseKey == s.writerLeaseKey(tenantID)
}

func (s *TenantStore) writerFenceBound(ctx context.Context, tenantID string) bool {
	_, ok := s.writerFenceFromContext(ctx, tenantID)
	return ok
}

func (s *TenantStore) putTenantConditionalIfBound(ctx context.Context, tenantID string, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.writerFenceBound(ctx, tenantID) {
		return s.putTenantConditional(ctx, tenantID, key, data, condition)
	}
	return s.putTenantGenerationConditional(ctx, tenantID, key, data, condition)
}

func (s *TenantStore) writerFenceStillCurrent(ctx context.Context, tenantID string, expected writerFenceRef) error {
	key := s.writerLeaseKey(tenantID)
	s.clearWriterObjectKey(key)
	lease, _, err := s.getWriterLease(ctx, tenantID, key)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: tenant %q writer fence was removed", ErrLeaseHeld, tenantID)
	}
	if err != nil {
		return err
	}
	if !writerLeaseMatchesFence(lease, expected) {
		return fmt.Errorf("%w: tenant %q writer fence changed", ErrLeaseHeld, tenantID)
	}
	return nil
}

func (s *TenantStore) ensureBoundWriterLease(ctx context.Context, tenantID string, expected writerFenceRef) error {
	now := time.Now().UTC()
	if lease, _, ok := s.getCachedWriterLeaseAny(tenantID); ok && writerLeaseMatchesFence(lease, expected) && lease.ExpiresAt.After(now.Add(s.leaseTTL()/3)) {
		return nil
	}
	key := s.writerLeaseKey(tenantID)
	s.clearWriterObjectKey(key)
	lease, meta, err := s.getWriterLease(ctx, tenantID, key)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: tenant %q writer fence was removed", ErrLeaseHeld, tenantID)
	}
	if err != nil {
		return err
	}
	if !writerLeaseMatchesFence(lease, expected) {
		return fmt.Errorf("%w: tenant %q writer fence changed", ErrLeaseHeld, tenantID)
	}
	if lease.ExpiresAt.After(now.Add(s.leaseTTL() / 3)) {
		s.setCachedWriterLease(tenantID, lease, meta)
		return nil
	}
	lease.UpdatedAt = now
	lease.ExpiresAt = now.Add(s.leaseTTL())
	nextMeta, err := s.putLease(ctx, key, lease, meta)
	if errors.Is(err, ErrConflict) {
		return fmt.Errorf("%w: tenant %q writer fence changed while renewing", ErrLeaseHeld, tenantID)
	}
	if err != nil {
		return err
	}
	s.setCachedWriterLease(tenantID, lease, nextMeta)
	return nil
}

func writerLeaseMatchesFence(lease WriterLease, expected writerFenceRef) bool {
	return lease.OwnerID == expected.ownerID && lease.FenceToken == expected.token && lease.FenceEpoch == expected.epoch
}

func (s *TenantStore) putTenantConditional(ctx context.Context, tenantID string, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.coordinated() {
		return s.putTenantGenerationConditional(ctx, tenantID, key, data, condition)
	}
	fence, err := s.prepareTenantWrite(ctx, tenantID)
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := s.writerFenceStillCurrent(ctx, tenantID, fence); err != nil {
		return ObjectMeta{}, err
	}
	meta, err := s.Objects.PutConditional(ctx, key, data, condition)
	if err != nil {
		return meta, err
	}
	checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.writerFenceStillCurrent(checkCtx, tenantID, fence); err != nil {
		rollbackErr := s.Objects.DeleteConditional(checkCtx, key, PutCondition{IfMatch: meta.ETag})
		if rollbackErr != nil && !errors.Is(rollbackErr, ErrConflict) && !errors.Is(rollbackErr, ErrNotFound) {
			return ObjectMeta{}, errors.Join(err, fmt.Errorf("rollback stale tenant write %q: %w", key, rollbackErr))
		}
		return ObjectMeta{}, fmt.Errorf("%w: tenant %q writer fence changed while publishing %q: %v", ErrConflict, tenantID, key, err)
	}
	return meta, nil
}

func (s *TenantStore) putTenantGenerationConditional(ctx context.Context, tenantID string, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.writerFenceBound(ctx, tenantID) {
		return s.putTenantConditional(ctx, tenantID, key, data, condition)
	}
	generation, err := s.currentTenantGeneration(ctx, tenantID)
	if err != nil {
		return ObjectMeta{}, err
	}
	meta, err := s.Objects.PutConditional(ctx, key, data, condition)
	if err != nil {
		return meta, err
	}
	checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.tenantGenerationStillCurrent(checkCtx, tenantID, generation); err != nil {
		rollbackErr := s.Objects.DeleteConditional(checkCtx, key, PutCondition{IfMatch: meta.ETag})
		if rollbackErr != nil && !errors.Is(rollbackErr, ErrConflict) && !errors.Is(rollbackErr, ErrNotFound) {
			return ObjectMeta{}, errors.Join(err, fmt.Errorf("rollback stale tenant generation write %q: %w", key, rollbackErr))
		}
		return ObjectMeta{}, err
	}
	return meta, nil
}

func (s *TenantStore) currentTenantGeneration(ctx context.Context, tenantID string) (tenantGenerationRef, error) {
	if s.coordinated() {
		head, exists, err := s.Coordinator.Head(ctx, tenantID)
		if err != nil {
			return tenantGenerationRef{}, err
		}
		if !exists {
			return tenantGenerationRef{}, ErrCoordinatorHeadMissing
		}
		if head.Status != TenantStatusActive {
			return tenantGenerationRef{}, ErrTenantDeleted
		}
		return tenantGenerationRef{fenceEpoch: head.Generation, protected: true}, nil
	}
	manifest, meta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return tenantGenerationRef{}, err
	}
	if meta.Exists && manifest.WriterFenceEpoch > 0 {
		return tenantGenerationRef{fenceEpoch: manifest.WriterFenceEpoch, protected: true}, nil
	}
	if deleted, err := s.tenantPurgeTombstoneExists(ctx, tenantID); err != nil {
		return tenantGenerationRef{}, err
	} else if deleted {
		return tenantGenerationRef{}, ErrTenantDeleted
	}
	return tenantGenerationRef{}, nil
}

func (s *TenantStore) tenantGenerationStillCurrent(ctx context.Context, tenantID string, expected tenantGenerationRef) error {
	if s.coordinated() {
		head, exists, err := s.Coordinator.Head(ctx, tenantID)
		if err != nil {
			return err
		}
		if !exists || head.Status != TenantStatusActive ||
			!expected.protected || head.Generation != expected.fenceEpoch {
			return ErrTenantDeleted
		}
		return nil
	}
	manifest, meta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return err
	}
	if expected.protected {
		if !meta.Exists || manifest.WriterFenceEpoch != expected.fenceEpoch {
			return ErrTenantDeleted
		}
		return nil
	}
	if deleted, err := s.tenantPurgeTombstoneExists(ctx, tenantID); err != nil {
		return err
	} else if deleted {
		return ErrTenantDeleted
	}
	return nil
}

func (s *TenantStore) putTenantObject(ctx context.Context, tenantID string, key string, data []byte) error {
	condition, err := s.tenantObjectPutCondition(ctx, key)
	if err != nil {
		return err
	}
	_, err = s.putTenantConditional(ctx, tenantID, key, data, condition)
	return err
}

func (s *TenantStore) putTenantGenerationObject(ctx context.Context, tenantID string, key string, data []byte) error {
	condition, err := s.tenantObjectPutCondition(ctx, key)
	if err != nil {
		return err
	}
	_, err = s.putTenantGenerationConditional(ctx, tenantID, key, data, condition)
	return err
}

func (s *TenantStore) tenantObjectPutCondition(ctx context.Context, key string) (PutCondition, error) {
	_, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return PutCondition{IfNoneMatch: true}, nil
	}
	if err != nil {
		return PutCondition{}, err
	}
	return PutCondition{IfMatch: meta.ETag}, nil
}

func (s *TenantStore) putTenantBytesWithMeta(ctx context.Context, tenantID string, key string, data []byte, meta ObjectMeta) error {
	_, err := s.putTenantBytesWithMetaResult(ctx, tenantID, key, data, meta)
	return err
}

func (s *TenantStore) putTenantBytesWithMetaResult(ctx context.Context, tenantID string, key string, data []byte, meta ObjectMeta) (ObjectMeta, error) {
	condition := PutCondition{IfNoneMatch: !meta.Exists}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	}
	return s.putTenantConditional(ctx, tenantID, key, data, condition)
}

func (s *TenantStore) deleteTenantObject(ctx context.Context, tenantID string, key string) error {
	fence, err := s.prepareTenantWrite(ctx, tenantID)
	if err != nil {
		return err
	}
	_, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.writerFenceStillCurrent(ctx, tenantID, fence); err != nil {
		return err
	}
	err = s.Objects.DeleteConditional(ctx, key, PutCondition{IfMatch: meta.ETag})
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (s *TenantStore) deleteTenantGenerationObject(ctx context.Context, tenantID string, key string) error {
	if s.writerFenceBound(ctx, tenantID) {
		return s.deleteTenantObject(ctx, tenantID, key)
	}
	generation, err := s.currentTenantGeneration(ctx, tenantID)
	if err != nil {
		return err
	}
	_, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.tenantGenerationStillCurrent(ctx, tenantID, generation); err != nil {
		return err
	}
	err = s.Objects.DeleteConditional(ctx, key, PutCondition{IfMatch: meta.ETag})
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}
