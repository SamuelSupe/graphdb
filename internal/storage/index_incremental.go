package storage

import (
	"context"
	"errors"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) updateIndexesAfterCommit(ctx context.Context, tenantID string, before *graph.Graph, after *graph.Graph, mutations graph.Mutations, report graph.ApplyReport, version int64, rebuildOnGap bool) error {
	if !canIncrementIndexes(mutations) {
		return nil
	}
	catalog, catalogMeta, err := s.getIndexCatalogForWriteWithMeta(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if catalog.Version >= version {
		return nil
	}
	if catalog.Version != version-1 {
		if !rebuildOnGap {
			return fmt.Errorf("index catalog version %d does not match previous graph version %d", catalog.Version, version-1)
		}
		_, err := s.RebuildIndexes(ctx, tenantID)
		return err
	}
	return s.refreshParquetIndexesAfterCommit(ctx, tenantID, catalog, catalogMeta, before, after, report, version)
}

func (s *TenantStore) ensureIncrementalIndexCurrent(ctx context.Context, tenantID string, version int64) error {
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return err
	}
	current, err := s.currentManifestForWriteAdmission(ctx, tenantID)
	if err != nil {
		return err
	}
	if current.Version != version {
		return fmt.Errorf("%w: manifest for tenant %q changed while updating indexes", ErrConflict, tenantID)
	}
	return nil
}

func canIncrementIndexes(mutations graph.Mutations) bool {
	if len(mutations.UpsertCITypes) > 0 || len(mutations.DeleteCITypes) > 0 ||
		len(mutations.UpsertRelationTypes) > 0 ||
		len(mutations.DeleteRelationTypes) > 0 {
		return false
	}
	return true
}
