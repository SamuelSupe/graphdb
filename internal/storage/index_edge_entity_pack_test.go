package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"graphdb/internal/graph"
)

func TestEntityPagePackingMergesSmallPages(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	entities := entitiesForDistinctShards("host", "host", 8, entityShardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if len(catalog.EntityPages) < 2 {
		t.Fatalf("test expected multiple logical pages, catalog=%#v", catalog.EntityPages)
	}
	keys := entityPageObjectKeys(catalog.EntityPages)
	if len(keys) != 1 || !strings.Contains(keys[0], "/entities/pages/packs/") {
		t.Fatalf("entity pages should share one pack object, keys=%#v pages=%#v", keys, catalog.EntityPages)
	}
	objects, err := store.Objects.List(ctx, "test/tenants/tenant-a/indexes/parquet/versions/v1/entities/pages/")
	if err != nil {
		t.Fatalf("list entity pages: %v", err)
	}
	if parquetObjects := countParquetObjects(objects); parquetObjects != 1 {
		t.Fatalf("entity page parquet object count = %d, want 1; objects=%#v", parquetObjects, objects)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	for _, entity := range entities {
		got, ok, err := lookup.GetEntity(ctx, entity.ID, nil)
		if err != nil || !ok || got.ID != entity.ID {
			t.Fatalf("entity lookup %s got=%#v ok=%v err=%v", entity.ID, got, ok, err)
		}
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func TestEdgeShardPackingMergesSmallShards(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	services := entitiesForDistinctShards("service", "service", 8, edgeShardID)
	hosts := make([]graph.Entity, 0, len(services))
	edges := make([]graph.Edge, 0, len(services))
	for i, service := range services {
		host := graph.Entity{ID: fmt.Sprintf("host:%02d", i), Kind: "host"}
		hosts = append(hosts, host)
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:%02d", i),
			Type: "runs_on",
			From: service.ID,
			To:   host.ID,
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
		}},
		UpsertEntities: append(services, hosts...),
		UpsertEdges:    edges,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	shards := relationEdgeShards(catalog, "runs_on")
	if len(shards) < 2 {
		t.Fatalf("test expected multiple logical edge shards, catalog=%#v", catalog.EdgeShards)
	}
	keys := edgeShardObjectKeys(shards)
	if len(keys) != 1 || !strings.Contains(keys[0], "/edges/runs_on/packs/") {
		t.Fatalf("edge shards should share one pack object, keys=%#v shards=%#v", keys, shards)
	}
	objects, err := store.Objects.List(ctx, "test/tenants/tenant-a/indexes/parquet/versions/v1/edges/runs_on/")
	if err != nil {
		t.Fatalf("list edge shards: %v", err)
	}
	if parquetObjects := countParquetObjects(objects); parquetObjects != 1 {
		t.Fatalf("edge shard parquet object count = %d, want 1; objects=%#v", parquetObjects, objects)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	for _, edge := range edges {
		got, ok, err := lookup.OutEdges(ctx, edge.From, map[string]struct{}{"runs_on": {}})
		if err != nil || !ok || len(got) != 1 || got[0].From != edge.From {
			t.Fatalf("edge lookup from=%s got=%#v ok=%v err=%v", edge.From, got, ok, err)
		}
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func entitiesForDistinctShards(kind string, prefix string, count int, shardFn func(string) string) []graph.Entity {
	seen := map[string]struct{}{}
	out := make([]graph.Entity, 0, count)
	for i := 0; len(out) < count; i++ {
		id := fmt.Sprintf("%s:%04d", prefix, i)
		shard := shardFn(id)
		if _, ok := seen[shard]; ok {
			continue
		}
		seen[shard] = struct{}{}
		out = append(out, graph.Entity{ID: id, Kind: kind})
	}
	return out
}

func entityPageObjectKeys(pages []EntityPageSpec) []string {
	objects := make([]IndexObject, 0, len(pages))
	for _, page := range pages {
		objects = append(objects, page.Objects...)
	}
	return uniqueObjectKeys(objects)
}

func relationEdgeShards(catalog IndexCatalog, relationType string) []EdgeShard {
	out := make([]EdgeShard, 0)
	for _, shard := range catalog.EdgeShards {
		if shard.RelationType == relationType {
			out = append(out, shard)
		}
	}
	return out
}

func edgeShardObjectKeys(shards []EdgeShard) []string {
	objects := make([]IndexObject, 0, len(shards))
	for _, shard := range shards {
		objects = append(objects, shard.Objects...)
	}
	return uniqueObjectKeys(objects)
}

func countParquetObjects(objects []ObjectInfo) int {
	count := 0
	for _, object := range objects {
		if strings.HasSuffix(object.Key, ".parquet") {
			count++
		}
	}
	return count
}
