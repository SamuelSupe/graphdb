package storage

import (
	"context"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPackedEntityPagesShareRawObjectCacheAcrossRequests(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	store.WriteEntityRecords = false
	entities := entitiesForDistinctShards("host", "packed-cache", 8, entityShardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	keys := entityPageObjectKeys(catalog.EntityPages)
	if len(keys) != 1 || !strings.Contains(keys[0], "/packs/") {
		t.Fatalf("test requires one packed entity object, keys=%#v", keys)
	}

	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 16})
	for _, entity := range entities[:2] {
		lookup := &PersistedIndexLookup{
			Store: store, TenantID: "tenant-a",
			Version: catalog.Version, Catalog: catalog,
		}
		got, ok, err := lookup.GetEntity(ctx, entity.ID, nil)
		if err != nil || !ok || got.ID != entity.ID {
			t.Fatalf("lookup %q got=%#v ok=%v err=%v", entity.ID, got, ok, err)
		}
	}
	if got := objects.GetWithMetaCount(keys[0]); got != 1 {
		t.Fatalf("packed object reads = %d, want one shared raw-cache read", got)
	}
	store.indexCache.mu.Lock()
	entries := len(store.indexCache.data)
	store.indexCache.mu.Unlock()
	if entries != 1 {
		t.Fatalf("raw cache entries = %d, want one physical packed object", entries)
	}
}

func TestPackedRawObjectCacheKeepsLogicalVerificationSeparate(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 4})
	key := "test/tenants/tenant-a/indexes/parquet/versions/v7/entities/pages/packs/pack.parquet"
	data := []byte("packed parquet bytes")
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("seed packed object: %v", err)
	}
	meta, err := objectMeta(ctx, objects, key)
	if err != nil {
		t.Fatalf("load packed object metadata: %v", err)
	}

	store.putVerifiedCachedIndexObject(
		"entity_page", "tenant-a", 7, key, "content-a", "schema-a", data, meta,
	)
	store.indexCache.revalidateTTL = 0
	if _, _, verified, ok, err := store.cachedIndexObjectWithVerification(
		ctx, "entity_page", "tenant-a", 7, key, "content-b", "schema-a",
	); err != nil || !ok || verified {
		t.Fatalf("second logical page cache verified=%v ok=%v err=%v, want shared but unverified", verified, ok, err)
	}
	if _, _, verified, ok, err := store.cachedIndexObjectWithVerification(
		ctx, "entity_page", "tenant-a", 7, key, "content-a", "schema-a",
	); err != nil || !ok || !verified {
		t.Fatalf("first logical page lost verification verified=%v ok=%v err=%v", verified, ok, err)
	}
	store.markCachedIndexObjectVerified(
		"entity_page", "tenant-a", 7, key, "content-b", "schema-a",
	)
	if _, _, verified, ok, err := store.cachedIndexObjectWithVerification(
		ctx, "entity_page", "tenant-a", 7, key, "content-b", "schema-a",
	); err != nil || !ok || !verified {
		t.Fatalf("verified second logical page cache verified=%v ok=%v err=%v", verified, ok, err)
	}
}
