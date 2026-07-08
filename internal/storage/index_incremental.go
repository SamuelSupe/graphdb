package storage

import (
	"context"
	"errors"
	"fmt"

	"graphdb/internal/graph"
)

func (s *TenantStore) updateIndexesAfterCommit(ctx context.Context, tenantID string, before *graph.Graph, after *graph.Graph, mutations graph.Mutations, report graph.ApplyReport, version int64) error {
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
	return s.refreshParquetIndexesAfterCommit(ctx, tenantID, catalog, catalogMeta, before, after, version)
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
		len(mutations.UpsertRelationTypes) > 0 || len(mutations.DeleteRelationTypes) > 0 ||
		len(mutations.MergeEntities) > 0 || len(mutations.SplitEntities) > 0 {
		return false
	}
	return true
}
