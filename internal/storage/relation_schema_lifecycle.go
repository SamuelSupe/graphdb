package storage

import "context"

func (s *TenantStore) copyRelationSchemasForLifecycle(ctx context.Context, sourceTenantID string, targetTenantID string, graphVersion int64) error {
	catalog, err := s.GetRelationSchemas(ctx, sourceTenantID)
	if err != nil {
		return err
	}
	return s.putRelationSchemasForLifecycle(ctx, targetTenantID, catalog.RelationSchemas, graphVersion)
}

func (s *TenantStore) putRelationSchemasForLifecycle(ctx context.Context, tenantID string, schemas []RelationSchema, graphVersion int64) error {
	if len(schemas) == 0 {
		return nil
	}
	catalog, meta, err := s.getRelationSchemaCatalogWithMeta(ctx, tenantID)
	if err != nil {
		return err
	}
	catalog.RelationSchemas = append([]RelationSchema(nil), schemas...)
	catalog, err = normalizeRelationSchemaCatalog(catalog)
	if err != nil {
		return err
	}
	prepareRelationSchemaCatalog(&catalog, tenantID, graphVersion)
	return s.putRelationSchemaCatalog(ctx, tenantID, catalog, meta)
}
