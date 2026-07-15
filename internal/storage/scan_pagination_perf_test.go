package storage

import (
	"context"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
