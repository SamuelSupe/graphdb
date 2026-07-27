package storage

import (
	"context"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) refreshParquetIndexesAfterCommit(ctx context.Context, tenantID string, previous IndexCatalog, previousMeta ObjectMeta, before *graph.Graph, g *graph.Graph, report graph.ApplyReport, version int64) error {
	if previous.Version != version-1 {
		return fmt.Errorf("index catalog version %d does not match previous graph version %d", previous.Version, version-1)
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	artifacts, err := s.buildIncrementalIndexArtifacts(ctx, tenantID, previous, before, g, report, version)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		definitions, definitionErr := s.getIndexDefinitions(ctx, tenantID)
		if definitionErr != nil {
			return definitionErr
		}
		artifacts, err = buildIndexArtifactsWithDefinitions(g, version, definitions)
		if err != nil {
			return err
		}
		artifacts.Catalog.TenantID = tenantID
		s.decorateIndexCatalog(&artifacts.Catalog, tenantID, IndexFormatParquet)
		s.reuseUnchangedIndexCatalogObjects(tenantID, &artifacts.Catalog, previous)
	}
	catalog := artifacts.Catalog

	if len(artifacts.IncrementalIndexWrites) > 0 {
		if err := s.writeIncrementalSecondaryIndexObjects(ctx, tenantID, artifacts.IncrementalIndexWrites); err != nil {
			return err
		}
	} else if err := s.writeChangedParquetSecondaryIndexesFast(ctx, tenantID, artifacts.Indexes, catalog, version); err != nil {
		return err
	}
	if err := s.writeChangedParquetEdgeShardsFast(ctx, tenantID, artifacts.EdgeShards, catalog, version); err != nil {
		return err
	}
	if err := s.writeChangedParquetEntityPagesFast(ctx, tenantID, artifacts.EntityPages, catalog, before, g, report.AffectedEntityIDs, version); err != nil {
		return err
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	_, err = s.putIndexCatalogWithMetaFast(ctx, tenantID, catalog, previousMeta)
	if err != nil {
		s.deleteCachedIndexCatalog(tenantID)
		return err
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	if err := s.refreshReverseIndexAfterCommit(
		ctx,
		tenantID,
		before,
		g,
		report.AffectedEdgeIDs,
		version,
	); err != nil {
		if err := s.rebuildReverseIndex(
			ctx,
			tenantID,
			g,
			version,
		); err != nil {
			return fmt.Errorf("refresh reverse index: %w", err)
		}
	}
	return nil
}
