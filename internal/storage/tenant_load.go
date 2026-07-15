package storage

import (
	"context"
	"errors"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

const manifestObjectMissingReloads = 2

func (s *TenantStore) Load(ctx context.Context, tenantID string) (*graph.Graph, Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, Manifest{}, err
	}
	return s.loadAtLeast(ctx, tenantID, 0)
}

func (s *TenantStore) LoadAtLeast(ctx context.Context, tenantID string, minVersion int64) (*graph.Graph, Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, Manifest{}, err
	}
	return s.loadAtLeast(ctx, tenantID, minVersion)
}

func (s *TenantStore) loadAtLeast(ctx context.Context, tenantID string, minVersion int64) (*graph.Graph, Manifest, error) {
	if cached, ok := s.getWriteCache(tenantID); ok {
		if cached.Manifest.Version >= minVersion {
			if minVersion > 0 {
				return cloneLoadedGraph(cached)
			}
			manifest, _, err := s.getManifest(ctx, tenantID)
			if err != nil {
				return nil, Manifest{}, err
			}
			if cached.Manifest.Version >= manifest.Version {
				return cloneLoadedGraph(cached)
			}
		}
	}
	loaded, err := s.loadWithMeta(ctx, tenantID)
	if err != nil {
		return nil, Manifest{}, err
	}
	if minVersion > 0 && loaded.Manifest.Version < minVersion {
		return nil, Manifest{}, fmt.Errorf("loaded graph version %d is below required version %d", loaded.Manifest.Version, minVersion)
	}
	return loaded.Graph, loaded.Manifest, nil
}

func cloneLoadedGraph(loaded loadedGraph) (*graph.Graph, Manifest, error) {
	g, err := cloneGraph(loaded.Graph)
	if err != nil {
		return nil, Manifest{}, err
	}
	return g, loaded.Manifest, nil
}

func (s *TenantStore) CurrentManifest(ctx context.Context, tenantID string) (Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Manifest{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	return manifest, err
}

func (s *TenantStore) loadWithMeta(ctx context.Context, tenantID string) (loaded loadedGraph, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.load_with_meta", tenantTraceAttr(tenantID))
	defer func() {
		if err == nil {
			span.SetAttributes(append(manifestTraceAttrs("graphdb.manifest", loaded.Manifest), graphTraceAttrs("graphdb.graph", loaded.Graph)...)...)
		}
		endStorageSpan(span, err)
	}()
	if err := ValidateTenantID(tenantID); err != nil {
		return loadedGraph{}, err
	}

	manifestCtx, manifestSpan := startStorageSpan(ctx, "graphdb.storage.load.get_manifest", tenantTraceAttr(tenantID))
	manifest, meta, err := s.getManifest(manifestCtx, tenantID)
	if err == nil {
		manifestSpan.SetAttributes(manifestTraceAttrs("graphdb.manifest", manifest)...)
	}
	endStorageSpan(manifestSpan, err)
	if err != nil {
		return loadedGraph{}, err
	}
	for attempt := 0; ; attempt++ {
		span.SetAttributes(attribute.Int("graphdb.load.manifest_attempt", attempt+1))
		loaded, err = s.loadManifestGraph(ctx, tenantID, manifest, meta)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, ErrNotFound) || attempt >= manifestObjectMissingReloads {
			return loadedGraph{}, err
		}
		reloadCtx, reloadSpan := startStorageSpan(ctx, "graphdb.storage.load.reload_manifest_after_missing_object",
			tenantTraceAttr(tenantID),
			attribute.Int("graphdb.load.manifest_attempt", attempt+1),
		)
		nextManifest, nextMeta, manifestErr := s.getManifest(reloadCtx, tenantID)
		if manifestErr == nil {
			reloadSpan.SetAttributes(manifestTraceAttrs("graphdb.manifest", nextManifest)...)
		}
		endStorageSpan(reloadSpan, manifestErr)
		if manifestErr != nil {
			return loadedGraph{}, manifestErr
		}
		if sameManifestReadSet(manifest, nextManifest) {
			return loadedGraph{}, err
		}
		manifest, meta = nextManifest, nextMeta
	}
}

func (s *TenantStore) loadManifestGraph(ctx context.Context, tenantID string, manifest Manifest, meta ObjectMeta) (loaded loadedGraph, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.load_manifest_graph",
		append([]attribute.KeyValue{
			tenantTraceAttr(tenantID),
		}, manifestTraceAttrs("graphdb.manifest", manifest)...)...,
	)
	defer func() {
		if err == nil {
			span.SetAttributes(graphTraceAttrs("graphdb.graph", loaded.Graph)...)
		}
		endStorageSpan(span, err)
	}()
	g := graph.New()
	if manifest.SnapshotCatalogKey != "" {
		snapshotCtx, snapshotSpan := startStorageSpan(ctx, "graphdb.storage.load_snapshot_catalog",
			tenantTraceAttr(tenantID),
			attribute.Int64("graphdb.snapshot.version", manifest.SnapshotVersion),
		)
		if err := s.validateTenantObjectKey(tenantID, manifest.SnapshotCatalogKey); err != nil {
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		catalog, err := s.getShardedSnapshotCatalog(snapshotCtx, tenantID, manifest.SnapshotCatalogKey)
		if err != nil {
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		snapshotSpan.SetAttributes(
			attribute.Int64("graphdb.snapshot.catalog_version", catalog.Version),
			attribute.Int("graphdb.snapshot.entity_pages", len(catalog.EntityPages)),
			attribute.Int("graphdb.snapshot.edge_shards", len(catalog.EdgeShards)),
		)
		if manifest.SnapshotVersion != 0 && catalog.Version != manifest.SnapshotVersion {
			err := fmt.Errorf("snapshot catalog version mismatch: manifest snapshot version %d catalog version %d", manifest.SnapshotVersion, catalog.Version)
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		if catalog.Version > manifest.Version {
			err := fmt.Errorf("snapshot catalog version %d is ahead of manifest version %d", catalog.Version, manifest.Version)
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		snapshot, err := s.loadSnapshotFromCatalog(snapshotCtx, tenantID, catalog)
		if err != nil {
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		g, err = graph.FromSnapshot(snapshot)
		endStorageSpan(snapshotSpan, err)
		if err != nil {
			return loadedGraph{}, err
		}
	} else if manifest.SnapshotKey != "" {
		snapshotCtx, snapshotSpan := startStorageSpan(ctx, "graphdb.storage.load_snapshot_record",
			tenantTraceAttr(tenantID),
			attribute.Int64("graphdb.snapshot.version", manifest.SnapshotVersion),
		)
		if err := s.validateTenantObjectKey(tenantID, manifest.SnapshotKey); err != nil {
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		record, err := s.loadSnapshotRecord(snapshotCtx, manifest.SnapshotKey)
		if err != nil {
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		if record.TenantID != "" && record.TenantID != tenantID {
			err := fmt.Errorf("snapshot tenant mismatch: key tenant %q object %q contains tenant %q", tenantID, manifest.SnapshotKey, record.TenantID)
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		snapshot := record.Snapshot
		if err := validateSnapshotObjectIdentity(manifest.SnapshotKey, snapshot); err != nil {
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		if manifest.SnapshotVersion != 0 && snapshot.Version != manifest.SnapshotVersion {
			err := fmt.Errorf("snapshot version mismatch: manifest snapshot version %d object version %d", manifest.SnapshotVersion, snapshot.Version)
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		if snapshot.Version > manifest.Version {
			err := fmt.Errorf("snapshot version %d is ahead of manifest version %d", snapshot.Version, manifest.Version)
			endStorageSpan(snapshotSpan, err)
			return loadedGraph{}, err
		}
		g, err = graph.FromSnapshot(snapshot)
		snapshotSpan.SetAttributes(
			attribute.Int64("graphdb.snapshot.version", snapshot.Version),
			attribute.Int("graphdb.snapshot.entities", len(snapshot.Entities)),
			attribute.Int("graphdb.snapshot.edges", len(snapshot.Edges)),
		)
		endStorageSpan(snapshotSpan, err)
		if err != nil {
			return loadedGraph{}, err
		}
	}

	segmentsCtx, segmentsSpan := startStorageSpan(ctx, "graphdb.storage.load_commit_segments",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.commit_segments.count", len(manifest.CommitSegments)),
	)
	segmentItems := 0
	for _, segment := range manifest.CommitSegments {
		items, err := s.loadCommitSegment(segmentsCtx, tenantID, segment)
		if err != nil {
			segmentsSpan.SetAttributes(attribute.Int("graphdb.commit_segments.items", segmentItems))
			endStorageSpan(segmentsSpan, err)
			return loadedGraph{}, err
		}
		segmentItems += len(items)
		for _, item := range items {
			if err := applyManifestCommit(g, item.Key, item.Commit); err != nil {
				segmentsSpan.SetAttributes(attribute.Int("graphdb.commit_segments.items", segmentItems))
				endStorageSpan(segmentsSpan, err)
				return loadedGraph{}, err
			}
		}
	}
	segmentsSpan.SetAttributes(attribute.Int("graphdb.commit_segments.items", segmentItems))
	endStorageSpan(segmentsSpan, nil)

	commitsCtx, commitsSpan := startStorageSpan(ctx, "graphdb.storage.load_commit_tail",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.commit_keys.count", len(manifest.CommitKeys)),
	)
	appliedCommits := 0
	for _, commitKey := range manifest.CommitKeys {
		if err := s.validateTenantObjectKey(tenantID, commitKey); err != nil {
			commitsSpan.SetAttributes(attribute.Int("graphdb.commit_keys.applied", appliedCommits))
			endStorageSpan(commitsSpan, err)
			return loadedGraph{}, err
		}
		commit, err := s.getCommitObject(commitsCtx, commitKey)
		if err != nil {
			err = fmt.Errorf("load commit %q: %w", commitKey, err)
			commitsSpan.SetAttributes(attribute.Int("graphdb.commit_keys.applied", appliedCommits))
			endStorageSpan(commitsSpan, err)
			return loadedGraph{}, err
		}
		if commit.TenantID != tenantID {
			err := errTenantCommitMismatch(tenantID, commitKey, commit.TenantID)
			commitsSpan.SetAttributes(attribute.Int("graphdb.commit_keys.applied", appliedCommits))
			endStorageSpan(commitsSpan, err)
			return loadedGraph{}, err
		}
		if err := applyManifestCommit(g, commitKey, commit); err != nil {
			commitsSpan.SetAttributes(attribute.Int("graphdb.commit_keys.applied", appliedCommits))
			endStorageSpan(commitsSpan, err)
			return loadedGraph{}, err
		}
		appliedCommits++
	}
	commitsSpan.SetAttributes(attribute.Int("graphdb.commit_keys.applied", appliedCommits))
	endStorageSpan(commitsSpan, nil)

	if g.Version != manifest.Version {
		return loadedGraph{}, fmt.Errorf("manifest version mismatch: manifest version %d loaded graph version %d", manifest.Version, g.Version)
	}
	if _, err := g.ContentFingerprint(); err != nil {
		return loadedGraph{}, err
	}
	return loadedGraph{Graph: g, Manifest: manifest, Meta: meta, DataMD5: manifest.DataMD5}, nil
}

func sameManifestReadSet(a, b Manifest) bool {
	if a.Version != b.Version ||
		a.SnapshotKey != b.SnapshotKey ||
		a.SnapshotCatalogKey != b.SnapshotCatalogKey ||
		a.SnapshotVersion != b.SnapshotVersion {
		return false
	}
	if !commitSegmentsEqual(a.CommitSegments, b.CommitSegments) || len(a.CommitKeys) != len(b.CommitKeys) {
		return false
	}
	for i := range a.CommitKeys {
		if a.CommitKeys[i] != b.CommitKeys[i] {
			return false
		}
	}
	return true
}

func applyManifestCommit(g *graph.Graph, commitKey string, commit graph.Commit) error {
	if err := validateCommitObjectIdentity(commitKey, commit); err != nil {
		return err
	}
	if commit.Version <= g.Version {
		return nil
	}
	if commit.Version != g.Version+1 {
		return fmt.Errorf("non-contiguous commit version %d after graph version %d in %q", commit.Version, g.Version, commitKey)
	}
	return g.ApplyCommitInPlaceForStorage(commit)
}
