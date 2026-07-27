package storage

import (
	"context"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type fixedHeadCoordinator struct {
	WriteCoordinator
	head CoordinationHead
}

func (c fixedHeadCoordinator) Backend() string {
	return CoordinationPostgres
}

func (c fixedHeadCoordinator) Namespace() string {
	return "test"
}

func (c fixedHeadCoordinator) Head(
	context.Context,
	string,
) (CoordinationHead, bool, error) {
	return c.head, true, nil
}

func TestCoordinatedWriteCacheHitSkipsManifestObjectRead(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       7,
		HeadCommitID:  "commit-7",
	}
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	head := CoordinationHead{
		TenantID:             "tenant-a",
		Generation:           3,
		Status:               TenantStatusActive,
		Revision:             11,
		GraphVersion:         manifest.Version,
		ManifestKey:          "test/tenants/tenant-a/coordination/manifests/v7.parquet",
		ManifestHash:         objectContentHash(data),
		CommitID:             manifest.HeadCommitID,
		WriteContextRevision: 2,
	}
	if err := base.Put(ctx, head.ManifestKey, data); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	store.SetCoordinator(fixedHeadCoordinator{head: head})
	g := graph.New()
	g.Version = manifest.Version
	store.setWriteCache("tenant-a", loadedGraph{
		Graph:    g,
		Manifest: manifest,
		Meta:     coordinatedManifestMeta(head.ManifestKey, head),
	})
	objects.reset()

	loaded, err := store.loadForWriteLocked(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load for write: %v", err)
	}
	if loaded.Graph != g {
		t.Fatal("write cache hit did not reuse the cached graph")
	}
	if reads := objects.countContains("/coordination/manifests/"); reads != 0 {
		t.Fatalf("manifest object reads = %d, want 0 for unchanged PG head", reads)
	}

	objects.reset()
	readGraph, readManifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load cached graph: %v", err)
	}
	if readGraph.Version != manifest.Version || readManifest.Version != manifest.Version {
		t.Fatalf(
			"cached load version = graph %d manifest %d, want %d",
			readGraph.Version,
			readManifest.Version,
			manifest.Version,
		)
	}
	if reads := objects.countContains("/coordination/manifests/"); reads != 0 {
		t.Fatalf("cached graph manifest object reads = %d, want 0 for unchanged PG head", reads)
	}
}

func TestCoordinatedReaderCacheRevalidationSkipsManifestObjectRead(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       7,
		HeadCommitID:  "commit-7",
	}
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	head := CoordinationHead{
		TenantID:             "tenant-a",
		Generation:           3,
		Status:               TenantStatusActive,
		Revision:             11,
		GraphVersion:         manifest.Version,
		ManifestKey:          "test/tenants/tenant-a/coordination/manifests/v7.parquet",
		ManifestHash:         objectContentHash(data),
		CommitID:             manifest.HeadCommitID,
		WriteContextRevision: 2,
	}
	if err := base.Put(ctx, head.ManifestKey, data); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	store.SetCoordinator(fixedHeadCoordinator{head: head})
	g := graph.New()
	g.Version = manifest.Version
	cache := NewReaderCache(store, time.Second)
	cache.entries["tenant-a"] = cacheEntry{
		graph:      g,
		manifest:   manifest,
		meta:       coordinatedManifestMeta(head.ManifestKey, head),
		cachedAt:   time.Now().Add(-2 * time.Second),
		expiresAt:  time.Now().Add(-time.Second),
		lastAccess: time.Now(),
	}
	objects.reset()

	loaded, loadedManifest, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load reader cache: %v", err)
	}
	if loaded.Version != manifest.Version || loadedManifest.Version != manifest.Version {
		t.Fatalf(
			"loaded version = graph %d manifest %d, want %d",
			loaded.Version,
			loadedManifest.Version,
			manifest.Version,
		)
	}
	if reads := objects.countContains("/coordination/manifests/"); reads != 0 {
		t.Fatalf("manifest object reads = %d, want 0 for unchanged PG head", reads)
	}
}

func TestCoordinatedCurrentVersionSkipsManifestObjectRead(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       7,
		HeadCommitID:  "commit-7",
	}
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	head := CoordinationHead{
		TenantID:             "tenant-a",
		Generation:           3,
		Status:               TenantStatusActive,
		Revision:             11,
		GraphVersion:         manifest.Version,
		ManifestKey:          "test/tenants/tenant-a/coordination/manifests/v7.parquet",
		ManifestHash:         objectContentHash(data),
		CommitID:             manifest.HeadCommitID,
		WriteContextRevision: 2,
	}
	if err := base.Put(ctx, head.ManifestKey, data); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	store.SetCoordinator(fixedHeadCoordinator{head: head})
	objects.reset()

	version, err := store.CurrentVersion(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current version: %v", err)
	}
	if version != manifest.Version {
		t.Fatalf("current version = %d, want %d", version, manifest.Version)
	}
	if reads := objects.countContains("/coordination/manifests/"); reads != 0 {
		t.Fatalf("manifest object reads = %d, want 0 for PG head version lookup", reads)
	}
}
