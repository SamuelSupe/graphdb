package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestCatchUpWriteCacheAppliesOnlyMissingCommits(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	cached, ok := store.getWriteCache("tenant-a")
	if !ok {
		t.Fatal("v1 graph is not cached")
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	current, ok := store.getWriteCache("tenant-a")
	if !ok {
		t.Fatal("v2 graph is not cached")
	}
	cached.Meta = coordinatedManifestMeta("manifest-v1", CoordinationHead{
		Revision: 1, Generation: 1, WriteContextRevision: 1,
	})
	current.Meta = coordinatedManifestMeta("manifest-v2", CoordinationHead{
		Revision: 2, Generation: 1, WriteContextRevision: 1,
	})

	caughtUp, applied, err := store.catchUpWriteCache(
		ctx, "tenant-a", cached, current.Manifest, current.Meta,
	)
	if err != nil {
		t.Fatalf("catch up cache: %v", err)
	}
	if !applied || caughtUp.Manifest.Version != 2 || caughtUp.Graph.Version != 2 {
		t.Fatalf("caught up graph = %#v applied=%v", caughtUp.Manifest, applied)
	}
	if _, ok := caughtUp.Graph.GetEntity("host:b"); !ok {
		t.Fatal("caught up graph is missing host:b")
	}
	if _, ok := cached.Graph.GetEntity("host:b"); ok {
		t.Fatal("catch up mutated the source cache")
	}
}

func TestCatchUpWriteCacheReusesGraphForHeadOnlyRevision(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	cached := cachedGraph(7)
	cached.Meta = coordinatedManifestMeta("manifest-v7-r7", CoordinationHead{
		Revision: 7, Generation: 2, WriteContextRevision: 1,
	})
	manifest := cached.Manifest
	manifest.SnapshotVersion = 7
	manifest.SnapshotKey = "snapshot-v7"
	manifest.SnapshotCatalogKey = "catalog-v7"
	meta := coordinatedManifestMeta("manifest-v7-r8", CoordinationHead{
		Revision: 8, Generation: 2, WriteContextRevision: 1,
	})

	caughtUp, applied, err := store.catchUpWriteCache(
		context.Background(), "tenant-a", cached, manifest, meta,
	)
	if err != nil {
		t.Fatalf("catch up head-only revision: %v", err)
	}
	if !applied || caughtUp.Graph != cached.Graph || caughtUp.Meta.ETag != meta.ETag {
		t.Fatalf("head-only catch up applied=%v graph_reused=%v meta=%#v", applied, caughtUp.Graph == cached.Graph, caughtUp.Meta)
	}
}

func TestCatchUpWriteCacheRejectsNewerSnapshotThanCache(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	cached := cachedGraph(7)
	cached.Meta = coordinatedManifestMeta("manifest-v7", CoordinationHead{
		Revision: 7, Generation: 1,
	})
	manifest := Manifest{Version: 9, SnapshotVersion: 8}
	meta := coordinatedManifestMeta("manifest-v9", CoordinationHead{
		Revision: 9, Generation: 1,
	})

	_, applied, err := store.catchUpWriteCache(
		context.Background(), "tenant-a", cached, manifest, meta,
	)
	if err != nil {
		t.Fatalf("catch up newer snapshot: %v", err)
	}
	if applied {
		t.Fatal("cache older than the current snapshot should require a full load")
	}
}
