package storage

import (
	"context"
	"fmt"
	"sort"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// ReadCurrentSchema replays only schema mutations. A sharded snapshot does not
// require loading its entity or edge pages into the control process.
func (s *TenantStore) ReadCurrentSchema(ctx context.Context, tenant string) (*graph.Graph, Manifest, error) {
	manifest, err := s.CurrentManifest(ctx, tenant)
	if err != nil {
		return nil, manifest, err
	}
	g := graph.New()
	if manifest.SnapshotCatalogKey != "" {
		catalog, err := s.getShardedSnapshotCatalog(ctx, tenant, manifest.SnapshotCatalogKey)
		if err != nil {
			return nil, manifest, err
		}
		if catalog.Version != manifest.SnapshotVersion || catalog.Version > manifest.Version || s.validateTenantObjectKey(tenant, catalog.Schema.Key) != nil {
			return nil, manifest, fmt.Errorf("invalid schema snapshot reference")
		}
		data, err := s.Objects.Get(ctx, catalog.Schema.Key)
		if err != nil {
			return nil, manifest, err
		}
		schema, err := decodeSnapshotSchemaObject(ctx, data)
		if err != nil {
			return nil, manifest, err
		}
		if schema.TenantID != tenant || schema.Version != catalog.Version || snapshotSchemaContentHash(schema) != catalog.Schema.ContentHash {
			return nil, manifest, fmt.Errorf("schema snapshot integrity mismatch")
		}
		g, err = graph.FromSnapshot(graph.Snapshot{Version: schema.Version, CITypes: schema.CITypes, RelationTypes: schema.RelationTypes})
		if err != nil {
			return nil, manifest, err
		}
	} else if manifest.SnapshotKey != "" {
		objects, err := s.Objects.List(ctx, manifest.SnapshotKey)
		if err != nil {
			return nil, manifest, err
		}
		found := false
		for _, object := range objects {
			if object.Key == manifest.SnapshotKey {
				found = true
				if object.Size > 64<<20 {
					return nil, manifest, fmt.Errorf("%w: compact the legacy snapshot into sharded pages before conversion", ErrInvalidArgument)
				}
			}
		}
		if !found {
			return nil, manifest, fmt.Errorf("schema snapshot is missing")
		}
		record, err := s.loadSnapshotRecord(ctx, manifest.SnapshotKey)
		if err != nil {
			return nil, manifest, err
		}
		if record.TenantID != tenant || record.Version != manifest.SnapshotVersion {
			return nil, manifest, fmt.Errorf("schema snapshot identity mismatch")
		}
		g, err = graph.FromSnapshot(graph.Snapshot{Version: record.Version, CITypes: record.CITypes, RelationTypes: record.RelationTypes})
		if err != nil {
			return nil, manifest, err
		}
	}
	apply := func(key string, commit graph.Commit) error {
		if commit.TenantID != tenant {
			return errTenantCommitMismatch(tenant, key, commit.TenantID)
		}
		commit.Mutations = graph.Mutations{UpsertCITypes: commit.Mutations.UpsertCITypes, DeleteCITypes: commit.Mutations.DeleteCITypes, UpsertRelationTypes: commit.Mutations.UpsertRelationTypes, DeleteRelationTypes: commit.Mutations.DeleteRelationTypes}
		return applyManifestCommit(g, key, commit)
	}
	segments := append([]CommitSegmentRef(nil), manifest.CommitSegments...)
	sort.Slice(segments, func(i, j int) bool { return segments[i].FirstVersion < segments[j].FirstVersion })
	for _, ref := range segments {
		items, err := s.loadCommitSegment(ctx, tenant, ref)
		if err != nil {
			return nil, manifest, err
		}
		for _, item := range items {
			if err := apply(item.Key, item.Commit); err != nil {
				return nil, manifest, err
			}
		}
	}
	for _, key := range manifest.CommitKeys {
		commit, err := s.getCommitObject(ctx, key)
		if err != nil {
			return nil, manifest, err
		}
		if err := apply(key, commit); err != nil {
			return nil, manifest, err
		}
	}
	if g.Version != manifest.Version {
		return nil, manifest, fmt.Errorf("schema replay did not reach the published version")
	}
	return g, manifest, nil
}
