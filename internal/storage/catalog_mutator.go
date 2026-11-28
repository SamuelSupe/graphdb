package storage

import (
	"context"
	"fmt"
)

// CatalogMutator is the single publication and inheritance pipeline for derived
// index catalogs (incremental refresh, fallback rebuild, catch-up rebuild,
// schema-change rebuild, and background range/adjacency compaction).
type CatalogMutator struct {
	store *TenantStore
}

func (s *TenantStore) catalogMutator() CatalogMutator {
	return CatalogMutator{store: s}
}

// WithAdjacencyInheritance enforces the single canonical AdjacencyRoot rule:
// if an explicit rebuild enabled adjacency OR the previous catalog already
// opted into AdjacencyRoot (and store flags allow it), build opts inherit
// AdjacencyEnabled=true. Legacy tenants that never opted in are never
// unexpectedly converted by catch-up or fallback rebuilds.
func (m CatalogMutator) WithAdjacencyInheritance(ctx context.Context, previous IndexCatalog, found bool) context.Context {
	s := m.store
	env := s.executionEnv(ctx)
	if !env.BuildOpts.AdjacencyEnabled && s.WriteIndexAdjacencyPages && s.WriteIndexRangePages && found && indexCatalogHasAdjacency(previous) {
		return s.withBuildOpts(ctx, func(opts *IndexBuildOpts) {
			opts.AdjacencyEnabled = true
		})
	}
	return ctx
}

// BuildEntityRecordJobs constructs synchronized EntityRecord write jobs for all
// entities rewritten into packed/merged range pages whenever WriteEntityRecords
// is enabled on the store.
func (m CatalogMutator) BuildEntityRecordJobs(tenantID string, groups []entityPageDataPackGroup, metas []ObjectMeta) []entityRecordWriteJob {
	s := m.store
	if !s.WriteEntityRecords || len(groups) == 0 {
		return nil
	}
	var jobs []entityRecordWriteJob
	for index, group := range groups {
		var etag string
		if index < len(metas) {
			etag = metas[index].ETag
		}
		for _, page := range group.Pages {
			pageHash := entityPageContentHash(page)
			for _, entity := range page.Entities {
				record := newEntityRecord(tenantID, entity, page.Shard, pageHash, etag, page.Version, page.UpdatedAt)
				jobs = append(jobs, entityRecordWriteJob{
					Key:    s.entityRecordKey(tenantID, entity.ID),
					Record: record,
				})
			}
		}
	}
	return jobs
}

// PublishEntityRecords persists synchronized EntityRecord metadata before the
// referencing IndexCatalog becomes visible.
func (m CatalogMutator) PublishEntityRecords(ctx context.Context, jobs []entityRecordWriteJob) error {
	if len(jobs) == 0 {
		return nil
	}
	return m.store.putEntityRecordBatch(ctx, jobs)
}

// PublishForwardCatalog atomically publishes a forward IndexCatalog and
// invalidates the cached catalog entry on any CAS/I/O error.
func (m CatalogMutator) PublishForwardCatalog(ctx context.Context, tenantID string, catalog IndexCatalog, previousMeta ObjectMeta, fast bool) (ObjectMeta, error) {
	s := m.store
	var meta ObjectMeta
	var err error
	if fast {
		meta, err = s.putIndexCatalogWithMetaFast(ctx, tenantID, catalog, previousMeta)
	} else {
		meta, err = s.putIndexCatalogWithMeta(ctx, tenantID, catalog, previousMeta)
	}
	if err != nil {
		s.deleteCachedIndexCatalog(tenantID)
		return ObjectMeta{}, err
	}
	return meta, nil
}

// PublishReverseCatalog atomically publishes a ReverseIndexCatalog after
// verifying version alignment with the forward catalog, invalidating the cache
// on error.
func (m CatalogMutator) PublishReverseCatalog(ctx context.Context, tenantID string, reverse ReverseIndexCatalog, expectedVersion int64, previousMeta ObjectMeta) error {
	s := m.store
	if expectedVersion > 0 && reverse.Version != expectedVersion {
		return fmt.Errorf("reverse index catalog version %d does not match expected version %d", reverse.Version, expectedVersion)
	}
	if err := s.putReverseIndexCatalogWithMeta(ctx, tenantID, reverse, previousMeta); err != nil {
		s.deleteCachedReverseIndexCatalog(tenantID)
		return err
	}
	return nil
}
