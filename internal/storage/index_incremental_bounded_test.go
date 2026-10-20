package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestIncrementalEntityPageRewriteIsBounded(t *testing.T) {
	ctx := context.Background()
	var entities []graph.Entity
	for i := 0; len(entities) < 4096; i++ {
		id := fmt.Sprintf("host:bounded-%d", i)
		if entityShardID(id) == "00" {
			entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"value": i}})
		}
	}
	before, err := graph.FromSnapshot(graph.Snapshot{Version: 1, Entities: entities})
	if err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	artifacts, err := buildIndexArtifactsWithDefinitions(before, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.decorateIndexCatalog(&artifacts.Catalog, "tenant-a")
	if err := store.writeParquetEntityPages(ctx, "tenant-a", artifacts.EntityPages, 1); err != nil {
		t.Fatal(err)
	}
	id := entities[0].ID
	after, _, err := before.ApplyCommitStorageCopyWithOptions(graph.Commit{ID: "one-change", Version: 2, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: id, Kind: "host", Fields: graph.Fields{"value": -1}}}}}, graph.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pages, _, err := store.buildIncrementalEntityPages(ctx, "tenant-a", 1, artifacts.Catalog.EntityPages, before, after, []string{id}, 2, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, page := range pages {
		rows += len(page.Entities)
	}
	if rows > 1024 {
		t.Fatalf("one changed entity rewrites %d entities; want at most 1024 after a fresh index build", rows)
	}
}
