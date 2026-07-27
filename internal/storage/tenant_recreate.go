package storage

import (
	"context"
	"fmt"
)

func (s *TenantStore) prepareTenantCreateLease(ctx context.Context, tenantID string) error {
	metadata, exists, _, err := s.getTenantPurgeTombstone(ctx, tenantID)
	if err != nil {
		return err
	}
	phase, _ := tenantPurgeState(metadata, exists)
	switch phase {
	case tenantPurgePhaseRunning:
		return ErrTenantDeleted
	case tenantPurgePhaseComplete, "":
		residual, err := s.tenantResidualObjectsExist(ctx, tenantID)
		if err != nil {
			return err
		}
		if residual {
			return fmt.Errorf("%w: tenant %q still has objects after purge", ErrTenantDeleted, tenantID)
		}
		if err := s.acquireWriterLeaseForPurge(ctx, tenantID); err != nil {
			return err
		}
		return s.clearTenantPurgeTombstone(ctx, tenantID)
	default:
		return s.acquireWriterLease(ctx, tenantID)
	}
}

func (s *TenantStore) tenantResidualObjectsExist(ctx context.Context, tenantID string) (bool, error) {
	leaseKey := s.writerLeaseKey(tenantID)
	return objectPrefixMatches(
		ctx, s.Objects, s.tenantObjectPrefix(tenantID),
		func(object ObjectInfo) bool { return object.Key != leaseKey },
	)
}
