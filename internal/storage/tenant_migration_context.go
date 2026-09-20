package storage

import (
	"context"
	"errors"
)

type tenantMigrationContext struct {
	config          TenantConfig
	hasConfig       bool
	sourcePolicy    sourcePolicyRecord
	hasSourcePolicy bool
	relationSchemas []RelationSchema
}

func loadTenantMigrationContext(
	ctx context.Context,
	source *TenantStore,
	tenantID string,
) (tenantMigrationContext, error) {
	var out tenantMigrationContext
	config, configured, err := source.GetTenantConfig(ctx, tenantID)
	if err != nil {
		return out, err
	}
	out.config = config
	out.hasConfig = configured
	policy, configured, err := source.GetSourcePolicy(ctx, tenantID)
	if err != nil {
		return out, err
	}
	out.sourcePolicy = sourcePolicyRecord{TenantID: tenantID, SourcePolicy: policy}
	out.hasSourcePolicy = configured
	schemas, err := source.GetRelationSchemas(ctx, tenantID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return out, err
	}
	out.relationSchemas = append([]RelationSchema(nil), schemas.RelationSchemas...)
	return out, nil
}
