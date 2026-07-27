package storage

import (
	"context"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) putLocalLifecycleWriteContext(
	ctx context.Context,
	tenantID string,
	record TenantBackupRecord,
	graphVersion int64,
) error {
	if record.Config != nil {
		meta, err := s.putTenantConfigRecordWithMeta(
			ctx,
			tenantID,
			tenantConfigRecord{TenantID: tenantID, Config: *record.Config},
			ObjectMeta{Key: s.tenantConfigKey(tenantID)},
		)
		if err != nil {
			return err
		}
		s.setCachedTenantConfig(tenantID, *record.Config, true, meta)
	}
	if record.SourcePolicy != nil {
		policy, err := graph.NormalizeSourcePolicy(*record.SourcePolicy)
		if err != nil {
			return err
		}
		meta, err := s.putSourcePolicyRecordWithMeta(
			ctx,
			tenantID,
			sourcePolicyRecord{TenantID: tenantID, SourcePolicy: policy},
			ObjectMeta{Key: s.sourcePolicyKey(tenantID)},
		)
		if err != nil {
			return err
		}
		s.setCachedSourcePolicy(tenantID, policy, true, meta)
	}
	return s.putRelationSchemasForLifecycle(
		ctx, tenantID, record.RelationSchemas, graphVersion,
	)
}
