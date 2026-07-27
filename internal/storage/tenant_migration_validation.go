package storage

import "context"

func (s *TenantStore) validateTenantMigrationRelationSchemas(
	ctx context.Context,
	tenantID string,
	manifest Manifest,
	meta ObjectMeta,
	writeContext tenantMigrationContext,
) error {
	if len(writeContext.relationSchemas) == 0 {
		return nil
	}
	loaded, err := s.loadManifestGraph(ctx, tenantID, manifest, meta)
	if err != nil {
		return err
	}
	catalog := emptyRelationSchemaCatalog(tenantID)
	catalog.RelationSchemas = append(
		[]RelationSchema(nil),
		writeContext.relationSchemas...,
	)
	return validateRelationSchemaGraph(loaded.Graph, catalog)
}
