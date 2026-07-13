package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
)

type restoreDrillOwnership struct {
	taskID  string
	objects map[string]string
}

func (s *TenantStore) validateRestoreDrillTarget(targetPrefix string, targetTenantID string) error {
	targetRoot := cleanPrefix(targetPrefix)
	if targetRoot == "" {
		return fmt.Errorf("restore drill target prefix is required")
	}
	sourceTenantRoot := path.Join(s.Prefix, "tenants") + "/"
	targetTenantPrefix := path.Join(targetRoot, "tenants", targetTenantID) + "/"
	if targetRoot == s.Prefix || strings.HasPrefix(targetTenantPrefix, sourceTenantRoot) || strings.HasPrefix(sourceTenantRoot, targetTenantPrefix) {
		return fmt.Errorf("restore drill target must be isolated from the source tenant namespace")
	}
	return nil
}

func (s *TenantStore) claimRestoreDrillTarget(ctx context.Context, tenantID string) error {
	if err := s.requireEmptyRestoreDrillTarget(ctx, tenantID); err != nil {
		return err
	}
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return err
	}
	objects, err := s.listRestoreDrillTargetObjects(ctx, tenantID)
	if err != nil {
		return err
	}
	leaseKey := s.writerLeaseKey(tenantID)
	for _, object := range objects {
		if object.Key != leaseKey {
			return fmt.Errorf("%w: restore drill target tenant %q changed while claiming ownership", ErrConflict, tenantID)
		}
	}
	return nil
}

func (s *TenantStore) requireEmptyRestoreDrillTarget(ctx context.Context, tenantID string) error {
	objects, err := s.listRestoreDrillTargetObjects(ctx, tenantID)
	if err != nil {
		return err
	}
	if len(objects) != 0 {
		return fmt.Errorf("%w: restore drill target tenant %q is not empty", ErrConflict, tenantID)
	}
	purged, err := s.tenantPurgeTombstoneExists(ctx, tenantID)
	if err != nil {
		return err
	}
	if purged {
		return fmt.Errorf("%w: restore drill target tenant %q has a purge tombstone", ErrConflict, tenantID)
	}
	tenants, err := s.ListManagedTenants(ctx)
	if err != nil {
		return err
	}
	for _, existing := range tenants {
		if existing == tenantID {
			return fmt.Errorf("%w: restore drill target tenant %q is registered", ErrConflict, tenantID)
		}
	}
	return nil
}

func (s *TenantStore) captureRestoreDrillOwnership(ctx context.Context, tenantID string, taskID string) (restoreDrillOwnership, error) {
	task, err := s.GetTask(ctx, tenantID, taskID)
	if err != nil {
		return restoreDrillOwnership{}, fmt.Errorf("load restore drill ownership task: %w", err)
	}
	if task.ID != taskID || task.TenantID != tenantID || task.Type != TaskTypeTenantRestore || task.Status != TaskStatusSucceeded {
		return restoreDrillOwnership{}, fmt.Errorf("%w: restore drill ownership task mismatch", ErrConflict)
	}
	objects, err := s.restoreDrillObjectETags(ctx, tenantID)
	if err != nil {
		return restoreDrillOwnership{}, err
	}
	return restoreDrillOwnership{taskID: taskID, objects: objects}, nil
}

func (s *TenantStore) cleanupRestoreDrillTarget(ctx context.Context, tenantID string, ownership restoreDrillOwnership) (TenantPurgeReport, error) {
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return TenantPurgeReport{}, err
	}
	current, err := s.captureRestoreDrillOwnership(ctx, tenantID, ownership.taskID)
	if err != nil {
		return TenantPurgeReport{}, err
	}
	if !sameRestoreDrillObjects(ownership.objects, current.objects) {
		return TenantPurgeReport{}, fmt.Errorf("%w: restore drill target tenant %q changed after restore", ErrConflict, tenantID)
	}
	report, err := s.purgeTenantLocked(ctx, tenantID, true)
	if err != nil {
		return report, err
	}
	if err := s.clearTenantPurgeTombstone(ctx, tenantID); err != nil {
		return report, err
	}
	return report, nil
}

func (s *TenantStore) restoreDrillObjectETags(ctx context.Context, tenantID string) (map[string]string, error) {
	objects, err := s.listRestoreDrillTargetObjects(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	prefix := s.tenantObjectPrefix(tenantID)
	out := make(map[string]string, len(objects))
	for _, object := range objects {
		relative := strings.TrimPrefix(object.Key, prefix)
		if strings.HasPrefix(relative, "control/") {
			continue
		}
		etag := object.ETag
		if etag == "" {
			_, meta, err := s.Objects.GetWithMeta(ctx, object.Key)
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("%w: restore drill object %q disappeared", ErrConflict, object.Key)
			}
			if err != nil {
				return nil, err
			}
			etag = meta.ETag
		}
		out[object.Key] = etag
	}
	return out, nil
}

func (s *TenantStore) listRestoreDrillTargetObjects(ctx context.Context, tenantID string) ([]ObjectInfo, error) {
	prefix := s.tenantObjectPrefix(tenantID)
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(prefix)
	}
	return s.Objects.List(ctx, prefix)
}

func sameRestoreDrillObjects(expected map[string]string, current map[string]string) bool {
	if len(expected) != len(current) {
		return false
	}
	for key, etag := range expected {
		if current[key] != etag {
			return false
		}
	}
	return true
}
