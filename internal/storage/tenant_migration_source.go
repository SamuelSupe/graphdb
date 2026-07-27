package storage

import (
	"context"
	"fmt"
)

func captureTenantMigrationSource(
	ctx context.Context,
	source *TenantStore,
	tenantID string,
) (Manifest, tenantMigrationContext, error) {
	if !source.coordinated() {
		return captureLocalTenantMigrationSource(ctx, source, tenantID)
	}
	for attempt := 0; attempt < source.CoordinatorRetryLimit+1; attempt++ {
		manifest, meta, err := source.getManifest(ctx, tenantID)
		if err != nil {
			return Manifest{}, tenantMigrationContext{}, err
		}
		token, err := parseCoordinatedHeadToken(meta)
		if err != nil {
			return Manifest{}, tenantMigrationContext{}, err
		}
		snapshot, head, err := source.loadCoordinatedWriteContext(
			ctx, tenantID,
		)
		if err != nil {
			return Manifest{}, tenantMigrationContext{}, err
		}
		if head.Status != TenantStatusActive {
			return Manifest{}, tenantMigrationContext{}, ErrTenantDeleted
		}
		if !sameCoordinationPoint(head, token) {
			if err := coordinatorRetryDelay(ctx, attempt); err != nil {
				return Manifest{}, tenantMigrationContext{}, err
			}
			continue
		}
		writeContext := tenantMigrationContextFromSnapshot(snapshot)
		if err := source.validateTenantMigrationRelationSchemas(
			ctx,
			tenantID,
			manifest,
			meta,
			writeContext,
		); err != nil {
			return Manifest{}, tenantMigrationContext{}, err
		}
		return manifest, writeContext, nil
	}
	return Manifest{}, tenantMigrationContext{}, fmt.Errorf(
		"%w: tenant %q changed while capturing migration source",
		ErrWriteConflict, tenantID,
	)
}

func captureLocalTenantMigrationSource(
	ctx context.Context,
	source *TenantStore,
	tenantID string,
) (Manifest, tenantMigrationContext, error) {
	manifest, meta, err := source.getManifest(ctx, tenantID)
	if err != nil {
		return Manifest{}, tenantMigrationContext{}, err
	}
	if !meta.Exists {
		return Manifest{}, tenantMigrationContext{}, ErrNotFound
	}
	writeContext, err := loadTenantMigrationContext(ctx, source, tenantID)
	if err != nil {
		return Manifest{}, tenantMigrationContext{}, err
	}
	if err := source.validateTenantMigrationRelationSchemas(
		ctx,
		tenantID,
		manifest,
		meta,
		writeContext,
	); err != nil {
		return Manifest{}, tenantMigrationContext{}, err
	}
	return manifest, writeContext, nil
}

func tenantMigrationContextFromSnapshot(
	snapshot WriteContextSnapshot,
) tenantMigrationContext {
	return tenantMigrationContext{
		config:    snapshot.TenantConfig,
		hasConfig: snapshot.TenantConfigConfigured,
		sourcePolicy: sourcePolicyRecord{
			TenantID:     snapshot.TenantID,
			SourcePolicy: snapshot.SourcePolicy,
		},
		hasSourcePolicy: snapshot.SourcePolicyConfigured,
		relationSchemas: append(
			[]RelationSchema(nil),
			snapshot.RelationSchemas.RelationSchemas...,
		),
	}
}
