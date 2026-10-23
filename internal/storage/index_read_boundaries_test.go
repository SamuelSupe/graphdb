package storage

import (
	"context"
	"fmt"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"testing"
)

func TestAdjacencyCachedNodeStillEnforcesTenantScope(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	refs, err := store.saveAdjacencyNodes(ctx, store.adjacencyPrefix("tenant-a", 1, "out"), adjacencyNode{Codec: adjacencyCodec, Entries: []adjacencyKey{{Endpoint: "host:a", ID: "edge:a"}}}, 0)
	if err != nil || len(refs) != 1 {
		t.Fatalf("build: %+v %v", refs, err)
	}
	if _, err := store.loadAdjacencyNode(ctx, "tenant-a", refs[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.loadAdjacencyNode(ctx, "tenant-b", refs[0]); err == nil {
		t.Fatal("cache hit bypassed tenant prefix validation")
	}
}
func TestLegacyEdgePaginationUsesCatalogShard(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	from := ""
	for i := range 1000 {
		id := fmt.Sprintf("host:%d", i)
		if legacyIndexShardID(id) > "3f" {
			from = id
			break
		}
	}
	if from == "" {
		t.Fatal("legacy fixture")
	}
	shard := EdgeShardData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: "tenant-a", RelationType: "links", Shard: legacyIndexShardID(from), Version: 1, Edges: []graph.Edge{{ID: "edge:a", Type: "links", From: from, To: "host:b"}, {ID: "edge:b", Type: "links", From: from, To: "host:c"}}}
	key := "test/legacy-edge.parquet"
	writeParquetEdgeShardForTest(t, ctx, store, key, shard)
	hash := edgeShardContentHash(shard)
	catalog := IndexCatalog{LayoutVersion: CurrentObjectLayoutVersion, TenantID: "tenant-a", Version: 1, EdgeShards: []EdgeShard{{RelationType: "links", Shard: shard.Shard, Format: IndexFormatParquet, EdgeCount: 2, ContentHash: hash, Objects: []IndexObject{{Role: "shard", Key: key, ContentHash: hash}}}}}
	options := EdgeScanOptions{Limit: 1}
	page, ok, err := store.listEdgesFromShards(ctx, "tenant-a", 1, catalog, options, scanCursor{})
	if err != nil || !ok || len(page.Edges) != 1 || page.NextCursor == "" {
		t.Fatalf("first: %+v %v %v", page, ok, err)
	}
	cursor, err := decodeScanCursor(page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	// Resume a cursor emitted by the former64-bucket encoder over a256-bucket catalog.
	cursor.After = scanKey("links"+"\x00"+edgeShardID(from), page.Edges[0].ID)
	next, ok, err := store.listEdgesFromShards(ctx, "tenant-a", 1, catalog, options, cursor)
	if err != nil || !ok || len(next.Edges) != 1 || next.Edges[0].ID == page.Edges[0].ID || next.NextCursor != "" {
		t.Fatalf("legacy cursor repeated: first=%+v next=%+v ok=%v err=%v", page, next, ok, err)
	}
}
func TestReadMemoDoesNotRetainCallerCancellation(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	parent := WithQueryReadMemo(context.Background())
	ctx, cancel := context.WithCancel(parent)
	cancel()
	if _, err := store.QueryManifest(ctx, "tenant-a"); err == nil {
		t.Fatal("expected cancellation")
	}
	if _, err := store.QueryManifest(parent, "tenant-a"); err != nil {
		t.Fatalf("independent caller inherited cancellation: %v", err)
	}
}
