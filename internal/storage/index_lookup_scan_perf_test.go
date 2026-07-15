package storage

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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

func TestVisitEntitiesSkipsPackedPagesWhenKindIsAbsent(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	store.WriteEntityRecords = false
	entities := entitiesForDistinctShards("host", "query-packed-scan", 8, entityShardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	keys := entityPageObjectKeys(catalog.EntityPages)
	if len(catalog.EntityPages) != len(entities) || len(keys) != 1 {
		t.Fatalf("test requires one packed object across logical pages: pages=%d keys=%#v", len(catalog.EntityPages), keys)
	}

	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects
	recorder := installStorageSpanRecorder(t)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	visited := 0
	available, err := lookup.VisitEntities(ctx, "system", nil, "", func(graph.Entity) (bool, error) {
		visited++
		return true, nil
	})
	if err != nil || !available || visited != 0 {
		t.Fatalf("visit entities available=%v visited=%d err=%v", available, visited, err)
	}
	if got := objects.GetWithMetaCount(keys[0]); got != 1 {
		t.Fatalf("packed entity object reads = %d, want one", got)
	}

	span := requireStorageSpan(t, recorder.Ended(), "graphdb.storage.index_lookup.visit_entities")
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.unique_objects", int64(1))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.object_loads", int64(1))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.candidate_object_scans", int64(1))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.candidate_filter_requests", int64(len(catalog.EntityPages)))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.candidate_scan_reuses", int64(len(catalog.EntityPages)-1))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.parquet_decodes", int64(0))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.pages_skipped_by_kind", int64(len(catalog.EntityPages)))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.kind_candidate_found", false)
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.candidate_pruned_all_pages", true)
	requireStorageSpan(t, recorder.Ended(), "graphdb.storage.index_lookup.visit_entities.candidate_filter")
}

func TestVisitEntitiesDecodesOnlyMatchingPackedShards(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	store.WriteEntityRecords = false
	entities := entitiesForDistinctShards("host", "query-packed-match", 8, entityShardID)
	entities[0].Kind = "system"
	entities[1].Kind = "system"
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	keys := entityPageObjectKeys(catalog.EntityPages)
	if len(keys) != 1 {
		t.Fatalf("test requires one packed object, keys=%#v", keys)
	}

	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects
	recorder := installStorageSpanRecorder(t)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	var got []string
	available, err := lookup.VisitEntities(ctx, "system", nil, "", func(entity graph.Entity) (bool, error) {
		got = append(got, entity.ID)
		return true, nil
	})
	if err != nil || !available {
		t.Fatalf("visit entities available=%v err=%v", available, err)
	}
	sort.Strings(got)
	want := []string{entities[0].ID, entities[1].ID}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("visited IDs = %q, want %q", got, want)
	}
	if count := objects.GetWithMetaCount(keys[0]); count != 1 {
		t.Fatalf("packed entity object reads = %d, want one", count)
	}

	span := requireStorageSpan(t, recorder.Ended(), "graphdb.storage.index_lookup.visit_entities")
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.candidate_object_scans", int64(1))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.parquet_decodes", int64(2))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.kind_matched", int64(2))
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.pages_skipped_by_kind", int64(len(catalog.EntityPages)-2))
	requireStorageSpan(t, recorder.Ended(), "graphdb.storage.index_lookup.visit_entities.decode_page")
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
