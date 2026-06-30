package storage

import (
	"context"
	"fmt"

	"graphdb/internal/graph"
)

func (s *TenantStore) refreshParquetIndexesAfterCommit(ctx context.Context, tenantID string, previous IndexCatalog, previousMeta ObjectMeta, g *graph.Graph, version int64) error {
	if previous.Version != version-1 {
		return fmt.Errorf("index catalog version %d does not match previous graph version %d", previous.Version, version-1)
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	definitions, err := s.getIndexDefinitions(ctx, tenantID)
	if err != nil {
		return err
	}
	catalog, err := buildIndexCatalogWithDefinitions(g, version, definitions)
	if err != nil {
		return err
	}
	catalog.TenantID = tenantID
	s.decorateIndexCatalog(&catalog, tenantID, IndexFormatParquet)

	indexes, err := buildSecondaryIndexesWithDefinitions(g, version, definitions)
	if err != nil {
		return err
	}
	if err := s.writeSecondaryIndexesWithFormat(ctx, tenantID, indexes, IndexFormatParquet); err != nil {
		return err
	}
	if err := s.writeEdgeShardsWithFormat(ctx, tenantID, buildEdgeShards(g, version), IndexFormatParquet); err != nil {
		return err
	}
	if err := s.writeEntityPagesWithFormat(ctx, tenantID, g, version, buildEntityPages(g, version), IndexFormatParquet); err != nil {
		return err
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	if err := s.putIndexCatalogWithMeta(ctx, tenantID, catalog, previousMeta); err != nil {
		return err
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	return nil
}
