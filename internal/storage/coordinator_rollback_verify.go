package storage

import (
	"context"
	"errors"
	"fmt"
)

func (s *TenantStore) loadPostgresCoordinationMarker(
	ctx context.Context,
	namespace string,
) (coordinationMarker, ObjectMeta, error) {
	data, meta, err := s.Objects.GetWithMeta(ctx, s.coordinationMarkerKey())
	if err != nil {
		return coordinationMarker{}, meta, err
	}
	marker, err := decodeCoordinationMarker(data)
	if err != nil {
		return coordinationMarker{}, meta, err
	}
	if marker.Backend != CoordinationPostgres || marker.Namespace != namespace {
		return coordinationMarker{}, meta, fmt.Errorf(
			"coordination marker mismatch: backend=%q namespace=%q",
			marker.Backend, marker.Namespace,
		)
	}
	if meta.ETag == "" {
		return coordinationMarker{}, meta, fmt.Errorf("coordination marker has no ETag for conditional removal")
	}
	return marker, meta, nil
}

func (s *TenantStore) verifyCoordinatorRollback(
	ctx context.Context,
	coordinator WriteCoordinator,
	controller CoordinatorModeController,
) (CoordinatorStatus, []CoordinatorRollbackTenant, error) {
	status, err := coordinator.Status(ctx)
	if err != nil {
		return status, nil, err
	}
	if status.OutboxBacklog != 0 || status.MaxMirrorLag != 0 {
		return status, nil, fmt.Errorf(
			"legacy mirror is not drained: outbox_backlog=%d max_mirror_lag=%d",
			status.OutboxBacklog, status.MaxMirrorLag,
		)
	}
	heads, err := controller.ListHeads(ctx)
	if err != nil {
		return status, nil, err
	}
	if int64(len(heads)) != status.Tenants {
		return status, nil, fmt.Errorf(
			"coordinator tenant count changed during rollback verification: heads=%d status=%d",
			len(heads), status.Tenants,
		)
	}
	local := NewTenantStore(s.Objects, s.Prefix)
	tenants := make([]CoordinatorRollbackTenant, 0, len(heads))
	for _, head := range heads {
		item, err := s.verifyCoordinatorRollbackTenant(ctx, local, head)
		if err != nil {
			return status, tenants, err
		}
		tenants = append(tenants, item)
	}
	return status, tenants, nil
}

func (s *TenantStore) verifyCoordinatorRollbackTenant(
	ctx context.Context,
	local *TenantStore,
	head CoordinationHead,
) (CoordinatorRollbackTenant, error) {
	item := CoordinatorRollbackTenant{
		TenantID:       head.TenantID,
		Status:         head.Status,
		GraphVersion:   head.GraphVersion,
		HeadRevision:   head.Revision,
		LegacyRevision: head.LegacyManifestRevision,
	}
	if head.LegacyManifestRevision != head.Revision {
		return item, fmt.Errorf(
			"tenant %q legacy revision %d does not equal head revision %d",
			head.TenantID, head.LegacyManifestRevision, head.Revision,
		)
	}
	localStatus, err := local.TenantStatus(ctx, head.TenantID)
	if err != nil {
		return item, fmt.Errorf("load tenant %q local status: %w", head.TenantID, err)
	}
	item.LocalTenantStatus = localStatus
	if localStatus != head.Status {
		return item, fmt.Errorf(
			"tenant %q local status %q does not equal coordinator status %q",
			head.TenantID, localStatus, head.Status,
		)
	}
	data, _, err := s.Objects.GetWithMeta(ctx, s.manifestKey(head.TenantID))
	if errors.Is(err, ErrNotFound) && head.Status == TenantStatusDeleted {
		return item, nil
	}
	if err != nil {
		return item, fmt.Errorf("load tenant %q legacy manifest: %w", head.TenantID, err)
	}
	item.LegacyManifestExists = true
	if !isParquetBytes(data) {
		return item, fmt.Errorf("tenant %q legacy manifest is not parquet", head.TenantID)
	}
	item.LegacyManifestHash = objectContentHash(data)
	if item.LegacyManifestHash != head.ManifestHash {
		return item, fmt.Errorf(
			"tenant %q legacy manifest hash %s does not equal head hash %s",
			head.TenantID, item.LegacyManifestHash, head.ManifestHash,
		)
	}
	manifest, err := decodeParquetManifest(ctx, data)
	if err != nil {
		return item, err
	}
	if manifest.TenantID != head.TenantID || manifest.Version != head.GraphVersion {
		return item, fmt.Errorf(
			"tenant %q legacy manifest identifies tenant/version %q/%d, want %q/%d",
			head.TenantID,
			manifest.TenantID,
			manifest.Version,
			head.TenantID,
			head.GraphVersion,
		)
	}
	return item, nil
}
