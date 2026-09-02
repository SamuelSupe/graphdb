package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type CoordinatorRollbackTenant struct {
	TenantID             string `json:"tenant_id"`
	Status               string `json:"status"`
	GraphVersion         int64  `json:"graph_version"`
	HeadRevision         int64  `json:"head_revision"`
	LegacyRevision       int64  `json:"legacy_manifest_revision"`
	LegacyManifestExists bool   `json:"legacy_manifest_exists"`
	LegacyManifestHash   string `json:"legacy_manifest_hash,omitempty"`
	LocalTenantStatus    string `json:"local_tenant_status"`
}

type CoordinatorRollbackReport struct {
	DryRun                bool                        `json:"dry_run"`
	Applied               bool                        `json:"applied"`
	Backend               string                      `json:"backend"`
	Namespace             string                      `json:"namespace"`
	ModeBefore            string                      `json:"mode_before"`
	ModeAfter             string                      `json:"mode_after"`
	SyncedLegacyManifests int                         `json:"synced_legacy_manifests"`
	MarkerRemoved         bool                        `json:"marker_removed"`
	CoordinatorStatus     CoordinatorStatus           `json:"coordinator_status"`
	Tenants               []CoordinatorRollbackTenant `json:"tenants"`
}

func (s *TenantStore) RollbackCoordinator(
	ctx context.Context,
	coordinator WriteCoordinator,
	dryRun bool,
) (report CoordinatorRollbackReport, err error) {
	controller, ok := coordinator.(CoordinatorModeController)
	if !ok || coordinator.Backend() != CoordinationPostgres {
		return report, fmt.Errorf("PostgreSQL coordinator mode control is required")
	}
	report = CoordinatorRollbackReport{
		DryRun:    dryRun,
		Backend:   coordinator.Backend(),
		Namespace: coordinator.Namespace(),
		Tenants:   []CoordinatorRollbackTenant{},
	}
	_, markerMeta, err := s.loadPostgresCoordinationMarker(ctx, coordinator.Namespace())
	if err != nil {
		return report, err
	}
	mode, err := controller.CoordinationMode(ctx)
	if err != nil {
		return report, err
	}
	report.ModeBefore = mode
	report.ModeAfter = mode
	if dryRun {
		status, tenants, err := s.verifyCoordinatorRollback(ctx, coordinator, controller)
		report.CoordinatorStatus = status
		report.Tenants = tenants
		return report, err
	}

	restorePostgresFrom := ""
	switch mode {
	case CoordinationPostgres:
		changed, err := controller.CompareAndSwapCoordinationMode(
			ctx, CoordinationPostgres, CoordinationDraining,
		)
		if err != nil {
			return report, err
		}
		if !changed {
			return report, fmt.Errorf("%w: coordinator mode changed while starting rollback", ErrConflict)
		}
		restorePostgresFrom = CoordinationDraining
		mode = CoordinationDraining
	case CoordinationDraining:
		restorePostgresFrom = CoordinationDraining
	case CoordinationLocal:
		// Resume a prior apply that fenced PostgreSQL but did not remove the marker.
	default:
		return report, fmt.Errorf("cannot roll back coordinator from mode %q", mode)
	}
	defer func() {
		if err == nil || restorePostgresFrom == "" {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 5*time.Second,
		)
		defer cancel()
		restored, restoreErr := controller.CompareAndSwapCoordinationMode(
			rollbackCtx, restorePostgresFrom, CoordinationPostgres,
		)
		if restoreErr != nil {
			err = errors.Join(err, fmt.Errorf("restore PostgreSQL coordination mode: %w", restoreErr))
			return
		}
		if !restored {
			err = errors.Join(err, fmt.Errorf(
				"%w: coordinator mode changed while restoring failed rollback",
				ErrConflict,
			))
			return
		}
		report.ModeAfter = CoordinationPostgres
	}()

	s.SetCoordinator(coordinator)
	if mode != CoordinationLocal {
		report.SyncedLegacyManifests, err = s.SyncLegacyManifests(ctx)
		if err != nil {
			return report, err
		}
	}
	report.CoordinatorStatus, report.Tenants, err = s.verifyCoordinatorRollback(
		ctx, coordinator, controller,
	)
	if err != nil {
		return report, err
	}
	if mode == CoordinationDraining {
		changed, err := controller.CompareAndSwapCoordinationMode(
			ctx, CoordinationDraining, CoordinationLocal,
		)
		if err != nil {
			return report, err
		}
		if !changed {
			return report, fmt.Errorf("%w: coordinator mode changed while completing rollback", ErrConflict)
		}
		restorePostgresFrom = CoordinationLocal
	}
	report.ModeAfter = CoordinationLocal
	if err := s.Objects.DeleteConditional(
		ctx, s.coordinationMarkerKey(), PutCondition{IfMatch: markerMeta.ETag},
	); err != nil {
		return report, fmt.Errorf("remove PostgreSQL coordination marker: %w", err)
	}
	report.MarkerRemoved = true
	report.Applied = true
	restorePostgresFrom = ""
	return report, nil
}
