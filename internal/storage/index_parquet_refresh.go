package storage

import (
	"context"
	"fmt"

	"graphdb/internal/graph"
)

func (s *TenantStore) refreshParquetIndexesAfterCommit(ctx context.Context, tenantID string, previous IndexCatalog, previousMeta ObjectMeta, before *graph.Graph, g *graph.Graph, version int64) error {
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
	s.reuseUnchangedIndexCatalogObjects(tenantID, &catalog, previous)

	indexes, err := buildSecondaryIndexesWithDefinitions(g, version, definitions)
	if err != nil {
		return err
	}
	if err := s.writeChangedParquetSecondaryIndexesFast(ctx, tenantID, indexes, catalog, version); err != nil {
		return err
	}
	edgeShards := buildEdgeShards(g, version)
	if err := s.writeChangedParquetEdgeShardsFast(ctx, tenantID, edgeShards, catalog, version); err != nil {
		return err
	}
	entityPages := buildEntityPages(g, version)
	if err := s.writeChangedParquetEntityPagesFast(ctx, tenantID, entityPages, catalog, before, g, version); err != nil {
		return err
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	nextMeta, err := s.putIndexCatalogWithMetaFast(ctx, tenantID, catalog, previousMeta)
	if err != nil {
		s.deleteCachedIndexCatalog(tenantID)
		return err
	}
	s.setCachedIndexCatalog(tenantID, catalog, nextMeta)
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	return nil
}
