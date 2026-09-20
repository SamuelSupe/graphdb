package storage

import "context"

func captureTenantMigrationSource(
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
