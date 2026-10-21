package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestEntityCursorSkipsEarlierPageObjects(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "test")
	catalog := IndexCatalog{TenantID: "tenant-a", Version: 1}
	for i, shard := range []string{"10", "20", "30", "40"} {
		page := EntityPageData{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      "tenant-a",
			Shard:         shard,
			Version:       1,
			Entities:      []graph.Entity{{ID: fmt.Sprintf("host:%d", i), Kind: "host"}},
		}
		key := fmt.Sprintf("test/pages/%s.parquet", shard)
		writeParquetEntityPageForTest(t, ctx, writer, key, page)
		hash := entityPageContentHash(page)
		schema := parquetEntityPageSchemaHash()
		catalog.EntityPages = append(catalog.EntityPages, EntityPageSpec{
			Shard:       shard,
			Format:      IndexFormatParquet,
			ContentHash: hash,
			SchemaHash:  schema,
			EntityCount: 1,
			Objects: []IndexObject{{
				Role: "page", Key: key, ContentHash: hash, SchemaHash: schema,
			}},
		})
	}

	reads := newCountingReadStore(objects)
	reader := NewTenantStore(reads, "test")
	options := EntityScanOptions{Limit: 1}
	options.Cursor = encodeScanCursor(scanCursor{
		Version:     catalog.Version,
		CatalogHash: scanCatalogContentHash(catalog),
		After:       scanKey("30", ""),
		Query:       entityScanQueryHash(options),
	})
	result, err := reader.ListEntitiesFromCatalog(ctx, "tenant-a", catalog, options)
	if err != nil {
		t.Fatalf("list entities from cursor: %v", err)
	}
	if len(result.Entities) != 1 || result.Entities[0].ID != "host:2" {
		t.Fatalf("cursor result = %#v, want host:2", result.Entities)
	}
	for _, spec := range catalog.EntityPages[:2] {
		key := requireIndexObjectKey(t, spec.Objects, "page")
		if got := exactObjectReads(reads, key); got != 0 {
			t.Fatalf("earlier page %q reads = %d, want 0", key, got)
		}
	}
}

func TestEdgeCursorSkipsEarlierShardObjects(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "test")
	catalog := IndexCatalog{TenantID: "tenant-a", Version: 1}
	for i, relationType := range []string{"a", "b", "c", "d"} {
		shard := EdgeShardData{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      "tenant-a",
			RelationType:  relationType,
			Shard:         "10",
			Version:       1,
			Edges: []graph.Edge{{
				ID: fmt.Sprintf("edge:%d", i), Type: relationType, From: fmt.Sprintf("from:%d", i), To: "target",
			}},
		}
		key := fmt.Sprintf("test/edges/%s.parquet", relationType)
		writeParquetEdgeShardForTest(t, ctx, writer, key, shard)
		hash := edgeShardContentHash(shard)
		schema := parquetEdgeShardSchemaHash()
		catalog.EdgeShards = append(catalog.EdgeShards, EdgeShard{
			RelationType: relationType,
			Shard:        "10",
			Format:       IndexFormatParquet,
			ContentHash:  hash,
			SchemaHash:   schema,
			EdgeCount:    1,
			Objects: []IndexObject{{
				Role: "shard", Key: key, ContentHash: hash, SchemaHash: schema,
			}},
		})
	}

	reads := newCountingReadStore(objects)
	reader := NewTenantStore(reads, "test")
	options := EdgeScanOptions{Limit: 1}
	options.Cursor = encodeScanCursor(scanCursor{
		Version:     catalog.Version,
		CatalogHash: scanCatalogContentHash(catalog),
		After:       scanKey(edgeShardTargetKey("c", "10"), ""),
		Query:       edgeScanQueryHash(options),
	})
	result, err := reader.ListEdgesFromCatalog(ctx, "tenant-a", catalog, options)
	if err != nil {
		t.Fatalf("list edges from cursor: %v", err)
	}
	if len(result.Edges) != 1 || result.Edges[0].ID != "edge:2" {
		t.Fatalf("cursor result = %#v, want edge:2", result.Edges)
	}
	for _, spec := range catalog.EdgeShards[:2] {
		key := requireIndexObjectKey(t, spec.Objects, "shard")
		if got := exactObjectReads(reads, key); got != 0 {
			t.Fatalf("earlier shard %q reads = %d, want 0", key, got)
		}
	}
}

func TestCursorPagesReuseCompiledScanCatalog(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	catalog := IndexCatalog{
		TenantID: "tenant-a",
		Version:  7,
		EntityPages: []EntityPageSpec{
			{Shard: "20"},
			{Shard: "10"},
		},
		EdgeShards: []EdgeShard{
			{RelationType: "runs_on", Shard: "20"},
			{RelationType: "depends_on", Shard: "10"},
		},
	}
	first, err := store.compiledScanCatalog("tenant-a", catalog, "")
	if err != nil {
		t.Fatalf("compile catalog: %v", err)
	}
	second, err := store.compiledScanCatalog("tenant-a", catalog, first.contentHash)
	if err != nil {
		t.Fatalf("reuse catalog: %v", err)
	}
	if first != second {
		t.Fatal("cursor page rebuilt the compiled catalog")
	}
	if got := first.entityShards; len(got) != 2 || got[0] != "10" || got[1] != "20" {
		t.Fatalf("compiled entity shards = %#v", got)
	}
	if got := first.edgeTargets; len(got) != 2 || got[0].RelationType != "depends_on" || got[1].RelationType != "runs_on" {
		t.Fatalf("compiled edge targets = %#v", got)
	}
}

func TestCursorSnapshotReusesTrustedCatalogWithoutMetadataRead(t *testing.T) {
	reads := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(reads, "test")
	catalog := IndexCatalog{TenantID: "tenant-a", Version: 3, EntityPages: []EntityPageSpec{{Shard: "10"}}}
	hash, err := indexCatalogContentHash(catalog)
	if err != nil {
		t.Fatalf("catalog hash: %v", err)
	}
	store.setCachedIndexCatalog("tenant-a", catalog, ObjectMeta{Key: store.indexCatalogKey("tenant-a"), ETag: "trusted"})
	reads.Reset()
	got, err := store.GetIndexCatalogSnapshot(context.Background(), "tenant-a", catalog.Version, hash)
	if err != nil {
		t.Fatalf("catalog snapshot: %v", err)
	}
	if got.Version != catalog.Version || len(got.EntityPages) != 1 {
		t.Fatalf("catalog snapshot = %#v", got)
	}
	if reads.CountContains("/indexes/") != 0 {
		t.Fatal("cursor snapshot repeated an index catalog metadata read")
	}
}

func exactObjectReads(store *countingReadStore, key string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.gets[key]
}

func TestRangeRoutingHasNoPerLookupAllocations(t *testing.T) {
	objects := make([]IndexObject, 4096)
	for i := range objects {
		lower := ""
		if i > 0 {
			lower = fmt.Sprintf("id:%08d", i*10)
		}
		objects[i] = IndexObject{Role: encodeRangeRole(indexRange{Lower: lower, Bytes: 1024}), Key: fmt.Sprint(i), RowCount: 1, Format: IndexFormatParquet, ContentHash: "hash", SchemaHash: "schema"}
	}
	finishRangeBounds(objects)
	spec := EntityPageSpec{Shard: "00", Codec: parquetEntityRangeCodec, Objects: objects, EntityCount: len(objects), ContentHash: rangeObjectsHash(objects)}
	store := NewTenantStore(NewMemoryStore(), "test")
	compiled, err := store.compiledScanCatalog("tenant-a", IndexCatalog{Version: 1, EntityPages: []EntityPageSpec{spec}}, "")
	if err != nil {
		t.Fatal(err)
	}
	routed := compiled.entitySpecs["00"].Objects
	if got := rangeObjectIndex(routed, "id:00020005"); got != 2000 {
		t.Fatal(got)
	}
	allocations := testing.AllocsPerRun(100, func() { rangeObjectIndex(routed, "id:00020005") })
	if allocations != 0 {
		t.Fatalf("each routing lookup allocates %.0f times decoding immutable boundaries", allocations)
	}
	before, _ := json.Marshal(objects)
	after, _ := json.Marshal(routed)
	if !bytes.Equal(before, after) || rangeObjectsHash(routed) != spec.ContentHash {
		t.Fatal("memory routing changed persisted references or their hash")
	}
	changed := append([]IndexObject(nil), routed...)
	changed[1].Role = encodeRangeRole(indexRange{Lower: "id:00000005", Bytes: 1024})
	finishRangeBounds(changed)
	if err := validateRangeObjects(changed, len(changed), rangeObjectsHash(changed)); err != nil {
		t.Fatal(err)
	}
	if rangeObjectIndex(changed, "id:00000007") != 1 || rangeObjectIndex(routed, "id:00000007") != 0 {
		t.Fatal("copied catalog reused stale boundaries or mutated the old routing")
	}
}

func TestRangeEntityScanPrunesBeforePayloadDecode(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteIndexRangePages = true
	var entities []graph.Entity
	for i := 0; i < 1024; i++ {
		entities = append(entities, graph.Entity{ID: fmt.Sprintf("host:%06d", i), Kind: "host", Source: "agent", Fields: graph.Fields{"blob": strings.Repeat("x", 1024)}})
	}
	pages, err := prepareBoundedEntityPages(ctx, "tenant-a", []EntityPageData{{Shard: "00", Version: 1, Entities: entities}})
	if err != nil {
		t.Fatal(err)
	}
	catalog := IndexCatalog{TenantID: "tenant-a", Version: 1, EntityPages: entityPageSpecs(pages)}
	store.decorateIndexCatalog(&catalog, "tenant-a")
	if err := store.writeParquetEntityPages(ctx, "tenant-a", pages, 1); err != nil {
		t.Fatal(err)
	}
	recorder := installStorageSpanRecorder(t)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: 1, Catalog: catalog}
	if ok, err := lookup.VisitEntities(ctx, "service", nil, "", func(graph.Entity) (bool, error) { t.Fatal("absent kind returned an entity"); return false, nil }); err != nil || !ok {
		t.Fatalf("visit: %v %v", ok, err)
	}
	span := requireStorageSpan(t, recorder.Ended(), "graphdb.storage.index_lookup.visit_entities")
	assertStorageSpanAttribute(t, span, "graphdb.index_lookup.parquet_decodes", int64(0))
	if storageSpanInt64(t, span, "graphdb.index_lookup.candidate_row_groups_skipped") == 0 {
		t.Fatal("range visitor bypassed pruning")
	}
	for _, options := range []EntityScanOptions{{Kind: "service"}, {Source: "absent"}} {
		stats := newEntityScanTraceStats()
		result, ok, err := store.listEntitiesFromPages(ctx, "tenant-a", 1, catalog, options, scanCursor{}, stats)
		if err != nil || !ok || len(result.Entities) != 0 {
			t.Fatalf("scan: %+v %v %v", result, ok, err)
		}
		if stats.parquetDecodes != 0 || stats.candidateRowGroupsSkip == 0 {
			t.Fatalf("filtered range decoded payload instead of pruning: decodes=%d skipped=%d", stats.parquetDecodes, stats.candidateRowGroupsSkip)
		}
	}
}
