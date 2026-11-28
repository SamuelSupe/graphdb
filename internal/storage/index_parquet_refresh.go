package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (s *TenantStore) refreshParquetIndexesAfterCommit(ctx context.Context, tenantID string, previous IndexCatalog, previousMeta ObjectMeta, before *graph.Graph, g *graph.Graph, report graph.ApplyReport, version int64) error {
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	artifacts, err := s.buildIncrementalIndexArtifacts(ctx, tenantID, previous, before, g, report, version)
	if err != nil {
		if errors.Is(err, errAdjacencyIndex) {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		definitions, definitionErr := s.getIndexDefinitions(ctx, tenantID)
		if definitionErr != nil {
			return definitionErr
		}
		mutator := s.catalogMutator()
		rebuildCtx := mutator.WithAdjacencyInheritance(ctx, previous, true)
		artifacts, err = s.buildAndWriteIndexArtifacts(rebuildCtx, tenantID, g, version, definitions)
		if err != nil {
			return err
		}
		artifacts.Catalog.TenantID = tenantID
		s.decorateIndexCatalog(&artifacts.Catalog, tenantID)
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
	mutator := s.catalogMutator()
	reverseCtx := mutator.WithAdjacencyInheritance(ctx, previous, true)
	if err := s.refreshReverseIndexAfterCommit(
		reverseCtx,
		tenantID,
		before,
		g,
		report.AffectedEdgeIDs,
		previous.Version,
		version,
	); err != nil {
		if errors.Is(err, errAdjacencyIndex) {
			return err
		}
		if err := s.rebuildReverseIndex(
			reverseCtx,
			tenantID,
			g,
			version,
		); err != nil {
			return fmt.Errorf("refresh reverse index: %w", err)
		}
	}
	if err := s.ensureIncrementalIndexCurrent(ctx, tenantID, version); err != nil {
		return err
	}
	_, err = mutator.PublishForwardCatalog(ctx, tenantID, catalog, previousMeta, true)
	return err
}
