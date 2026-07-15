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
	objects, err := s.Objects.List(ctx, s.tenantObjectPrefix(tenantID))
	if err != nil {
		return false, err
	}
	leaseKey := s.writerLeaseKey(tenantID)
	for _, object := range objects {
		if object.Key != leaseKey {
			return true, nil
		}
	}
	return false, nil
}
