package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPersistedIndexLookupReusesDecodedEdgeShardAcrossRequests(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	from := sameEntityShardIDs(t, "service:edge-cache-", 2)
	mutations := graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: from[0], Kind: "service"},
			{ID: from[1], Kind: "service"},
			{ID: "host:edge-cache-a", Kind: "host"},
			{ID: "host:edge-cache-b", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{
			{Type: "runs_on", From: from[0], To: "host:edge-cache-a"},
			{Type: "runs_on", From: from[1], To: "host:edge-cache-b"},
		},
	}
	if _, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	shardID := edgeShardID(from[0])
	spec := requireEdgeShardSpec(t, catalog, "runs_on", shardID)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	first, ok, err := lookup.OutEdges(ctx, from[0], nil)
	if err != nil || !ok || len(first) != 1 {
		t.Fatalf("first lookup edges=%#v ok=%v err=%v", first, ok, err)
	}
	key := firstIndexObjectKey(spec.Objects, "shard", store.parquetEdgeShardVersionKey("tenant-a", catalog.Version, "runs_on", shardID))
	if err := base.Delete(ctx, key); err != nil {
		t.Fatalf("delete backing shard: %v", err)
	}
	second, ok, err := lookup.OutEdges(ctx, from[1], nil)
	if err != nil || !ok || len(second) != 1 || second[0].To != "host:edge-cache-b" {
		t.Fatalf("cached lookup edges=%#v ok=%v err=%v", second, ok, err)
	}
	store.dropCachedIndexObject(
		"edge_shard",
		"tenant-a",
		catalog.Version,
		key,
		spec.ContentHash,
		spec.SchemaHash,
	)
	nextRequest := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a",
		Version: catalog.Version, Catalog: catalog,
	}
	third, ok, err := nextRequest.OutEdges(ctx, from[0], nil)
	if err != nil || !ok || len(third) != 1 ||
		third[0].To != "host:edge-cache-a" {
		t.Fatalf(
			"shared decoded lookup edges=%#v ok=%v err=%v",
			third, ok, err,
		)
	}
}
