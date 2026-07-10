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
	artifacts, err := buildIndexArtifactsWithDefinitions(g, version, definitions)
	if err != nil {
		return err
	}
	catalog := artifacts.Catalog
	catalog.TenantID = tenantID
	s.decorateIndexCatalog(&catalog, tenantID, IndexFormatParquet)
	s.reuseUnchangedIndexCatalogObjects(tenantID, &catalog, previous)

	if err := s.writeChangedParquetSecondaryIndexesFast(ctx, tenantID, artifacts.Indexes, catalog, version); err != nil {
		return err
	}
	if err := s.writeChangedParquetEdgeShardsFast(ctx, tenantID, artifacts.EdgeShards, catalog, version); err != nil {
		return err
	}
	if err := s.writeChangedParquetEntityPagesFast(ctx, tenantID, artifacts.EntityPages, catalog, before, g, version); err != nil {
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
