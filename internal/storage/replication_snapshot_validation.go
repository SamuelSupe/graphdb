package storage

import (
	"context"
	"fmt"
)

func (s *ReplicationSnapshotSource) validateTenantGraphs(ctx context.Context) error {
	return validateReplicationSnapshotTenantGraphs(ctx, NewFileStore(s.root), s.TenantPrefix)
}

func validateReplicationSnapshotTenantGraphs(ctx context.Context, files *FileStore, prefix string) error {
	if prefix == "" {
		return nil
	}
	if err := files.validateReplicationPrefix(ctx, prefix); err != nil {
		return err
	}
	// Read the captured files rather than the live store or its warm graph cache.
	// Streaming builders run this work after capture has released application.
	cold := NewTenantStore(files, prefix)
	cold.MaxWriteCacheTenants = 0
	tenants, err := cold.listTenantsByPrefix(ctx)
	if err != nil {
		return err
	}
	for _, tenant := range tenants {
		g, _, err := cold.Load(ctx, tenant)
		if err != nil {
			return fmt.Errorf("replication snapshot tenant %q: %w", tenant, err)
		}
		schemas, err := cold.GetRelationSchemas(ctx, tenant)
		if err == nil {
			err = validateRelationSchemaGraph(g, schemas)
		}
		if err != nil {
			return fmt.Errorf("replication snapshot tenant %q: %w", tenant, err)
		}
	}
	return nil
}
