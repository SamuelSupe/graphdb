package storage

import (
	"context"
	"errors"
	"fmt"

	"graphdb/internal/graph"
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

func (s *TenantStore) loadWithMeta(ctx context.Context, tenantID string) (loadedGraph, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return loadedGraph{}, err
	}
	manifest, meta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return loadedGraph{}, err
	}
	for attempt := 0; ; attempt++ {
		loaded, err := s.loadManifestGraph(ctx, tenantID, manifest, meta)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, ErrNotFound) || attempt >= manifestObjectMissingReloads {
			return loadedGraph{}, err
		}
		nextManifest, nextMeta, manifestErr := s.getManifest(ctx, tenantID)
		if manifestErr != nil {
			return loadedGraph{}, manifestErr
		}
		if sameManifestReadSet(manifest, nextManifest) {
			return loadedGraph{}, err
		}
		manifest, meta = nextManifest, nextMeta
	}
}

func (s *TenantStore) loadManifestGraph(ctx context.Context, tenantID string, manifest Manifest, meta ObjectMeta) (loadedGraph, error) {
	g := graph.New()
	if manifest.SnapshotCatalogKey != "" {
		if err := s.validateTenantObjectKey(tenantID, manifest.SnapshotCatalogKey); err != nil {
			return loadedGraph{}, err
		}
		catalog, err := s.getShardedSnapshotCatalog(ctx, tenantID, manifest.SnapshotCatalogKey)
		if err != nil {
			return loadedGraph{}, err
		}
		if manifest.SnapshotVersion != 0 && catalog.Version != manifest.SnapshotVersion {
			return loadedGraph{}, fmt.Errorf("snapshot catalog version mismatch: manifest snapshot version %d catalog version %d", manifest.SnapshotVersion, catalog.Version)
		}
		if catalog.Version > manifest.Version {
			return loadedGraph{}, fmt.Errorf("snapshot catalog version %d is ahead of manifest version %d", catalog.Version, manifest.Version)
		}
		snapshot, err := s.loadSnapshotFromCatalog(ctx, tenantID, catalog)
		if err != nil {
			return loadedGraph{}, err
		}
		g, err = graph.FromSnapshot(snapshot)
		if err != nil {
			return loadedGraph{}, err
		}
	} else if manifest.SnapshotKey != "" {
		if err := s.validateTenantObjectKey(tenantID, manifest.SnapshotKey); err != nil {
			return loadedGraph{}, err
		}
		record, err := s.loadSnapshotRecord(ctx, manifest.SnapshotKey)
		if err != nil {
			return loadedGraph{}, err
		}
		if record.TenantID != "" && record.TenantID != tenantID {
			return loadedGraph{}, fmt.Errorf("snapshot tenant mismatch: key tenant %q object %q contains tenant %q", tenantID, manifest.SnapshotKey, record.TenantID)
		}
		snapshot := record.Snapshot
		if err := validateSnapshotObjectIdentity(manifest.SnapshotKey, snapshot); err != nil {
			return loadedGraph{}, err
		}
		if manifest.SnapshotVersion != 0 && snapshot.Version != manifest.SnapshotVersion {
			return loadedGraph{}, fmt.Errorf("snapshot version mismatch: manifest snapshot version %d object version %d", manifest.SnapshotVersion, snapshot.Version)
		}
		if snapshot.Version > manifest.Version {
			return loadedGraph{}, fmt.Errorf("snapshot version %d is ahead of manifest version %d", snapshot.Version, manifest.Version)
		}
		g, err = graph.FromSnapshot(snapshot)
		if err != nil {
			return loadedGraph{}, err
		}
	}
	for _, segment := range manifest.CommitSegments {
		items, err := s.loadCommitSegment(ctx, tenantID, segment)
		if err != nil {
			return loadedGraph{}, err
		}
		for _, item := range items {
			if err := applyManifestCommit(g, item.Key, item.Commit); err != nil {
				return loadedGraph{}, err
			}
		}
	}
	for _, commitKey := range manifest.CommitKeys {
		if err := s.validateTenantObjectKey(tenantID, commitKey); err != nil {
			return loadedGraph{}, err
		}
		commit, err := s.getCommitObject(ctx, commitKey)
		if err != nil {
			return loadedGraph{}, fmt.Errorf("load commit %q: %w", commitKey, err)
		}
		if commit.TenantID != tenantID {
			return loadedGraph{}, errTenantCommitMismatch(tenantID, commitKey, commit.TenantID)
		}
		if err := applyManifestCommit(g, commitKey, commit); err != nil {
			return loadedGraph{}, err
		}
	}
	if g.Version != manifest.Version {
		return loadedGraph{}, fmt.Errorf("manifest version mismatch: manifest version %d loaded graph version %d", manifest.Version, g.Version)
	}
	return loadedGraph{Graph: g, Manifest: manifest, Meta: meta}, nil
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
	return g.ApplyCommit(commit)
}
