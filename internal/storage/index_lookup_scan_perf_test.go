package storage

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"graphdb/internal/graph"
)

func TestVisitEntitiesStopsAfterFirstPageAndKeepsCacheReadOnly(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	mutations := graph.Mutations{}
	for i := 0; i < 64; i++ {
		mutations.UpsertEntities = append(mutations.UpsertEntities, graph.Entity{
			ID:     fmt.Sprintf("host:scan-%03d", i),
			Kind:   "host",
			Fields: graph.Fields{"region": "original"},
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if len(catalog.EntityPages) < 2 {
		t.Fatalf("test requires multiple logical pages, got %d", len(catalog.EntityPages))
	}
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 16})
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	var first graph.Entity
	ok, err := lookup.VisitEntities(ctx, "host", []string{"region"}, "", func(entity graph.Entity) (bool, error) {
		first = entity
		return false, nil
	})
	if err != nil || !ok {
		t.Fatalf("first visit ok=%v err=%v", ok, err)
	}
	store.entityPageCache.mu.Lock()
	cachedPages := len(store.entityPageCache.data)
	store.entityPageCache.mu.Unlock()
	if got := cachedPages; got != 1 {
		t.Fatalf("decoded page cache entries = %d, want 1 after early stop", got)
	}
	first.Fields["region"] = "mutated"
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ok, err = lookup.VisitEntities(ctx, "host", []string{"region"}, "", func(entity graph.Entity) (bool, error) {
		if entity.Fields["region"] != "original" {
			t.Fatalf("caller mutation reached borrowed cache page: %#v", entity.Fields)
		}
		return false, nil
	})
	if err != nil || !ok {
		t.Fatalf("second visit ok=%v err=%v", ok, err)
	}
	ids := make([]string, 0, len(mutations.UpsertEntities))
	for _, entity := range mutations.UpsertEntities {
		ids = append(ids, entity.ID)
	}
	sort.Slice(ids, func(i, j int) bool {
		return scanKey(entityShardID(ids[i]), ids[i]) < scanKey(entityShardID(ids[j]), ids[j])
	})
	afterID := ids[len(ids)/2]
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 16})
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	var resumed graph.Entity
	ok, err = lookup.VisitEntities(ctx, "host", []string{"region"}, afterID, func(entity graph.Entity) (bool, error) {
		resumed = entity
		return false, nil
	})
	if err != nil || !ok {
		t.Fatalf("cursor visit ok=%v err=%v", ok, err)
	}
	if resumed.ID != afterID {
		t.Fatalf("cursor visit started at %q, want %q", resumed.ID, afterID)
	}
	store.entityPageCache.mu.Lock()
	cachedPages = len(store.entityPageCache.data)
	store.entityPageCache.mu.Unlock()
	if cachedPages != 1 {
		t.Fatalf("cursor visit decoded %d pages, want only cursor page", cachedPages)
	}
}

func TestVisitEntitiesResumesFromLegacyCatalogShard(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	findID := func(prefix string, shard string) string {
		for i := 0; i < 10000; i++ {
			candidate := fmt.Sprintf("host:%s-%04d", prefix, i)
			if legacyIndexShardID(candidate) == shard {
				return candidate
			}
		}
		return ""
	}
	afterID := findID("legacy-cursor", "80")
	currentCollisionID := findID("legacy-current-collision", "00")
	laterID := findID("legacy-later", "01")
	if afterID == "" || currentCollisionID == "" || laterID == "" {
		t.Fatal("failed to find legacy shard cursor fixture")
	}
	entities := []graph.Entity{
		{ID: currentCollisionID, Kind: "host"},
		{ID: laterID, Kind: "host"},
		{ID: afterID, Kind: "host"},
	}
	schema := parquetEntityPageSchemaHash()
	catalog := IndexCatalog{Version: 1}
	for _, entity := range entities {
		shard := legacyIndexShardID(entity.ID)
		page := EntityPageData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: "tenant-a", Shard: shard, Version: 1, Entities: []graph.Entity{entity}}
		key := fmt.Sprintf("test/legacy-cursor-%s.parquet", shard)
		writeParquetEntityPageForTest(t, ctx, store, key, page)
		hash := entityPageContentHash(page)
		catalog.EntityPages = append(catalog.EntityPages, EntityPageSpec{Shard: shard, Format: IndexFormatParquet, ContentHash: hash, SchemaHash: schema, EntityCount: 1, Objects: []IndexObject{{Role: "page", Key: key, ContentHash: hash, SchemaHash: schema}}})
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: 1, Catalog: catalog}
	var got []string
	ok, err := lookup.VisitEntities(ctx, "host", nil, afterID, func(entity graph.Entity) (bool, error) {
		got = append(got, entity.ID)
		return true, nil
	})
	if err != nil || !ok {
		t.Fatalf("legacy cursor visit ok=%v err=%v", ok, err)
	}
	sort.Slice(entities, func(i, j int) bool {
		return scanKey(entityShardID(entities[i].ID), entities[i].ID) < scanKey(entityShardID(entities[j].ID), entities[j].ID)
	})
	want := make([]string, 0, len(entities))
	seenCursor := false
	for _, entity := range entities {
		if entity.ID == afterID {
			seenCursor = true
		}
		if seenCursor {
			want = append(want, entity.ID)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy cursor resume = %q, want canonical tail %q", got, want)
	}
}
