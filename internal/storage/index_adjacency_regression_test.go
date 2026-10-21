package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestSparseRangeAdjacencyDoesNotReadUnrelatedPayloadPages(t *testing.T) {
	for _, count := range []int{1024, 8192, 65536} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/reverse=%v", count, reverse), func(t *testing.T) {
				ctx := context.WithValue(context.Background(), adjacencyBuildKey{}, true)
				var endpoints []string
				for i := 0; len(endpoints) < 3; i++ {
					id := fmt.Sprintf("host:sparse-%d", i)
					if edgeShardID(id) == "00" {
						endpoints = append(endpoints, id)
					}
				}
				var entities []graph.Entity
				for _, id := range endpoints {
					entities = append(entities, graph.Entity{ID: id, Kind: "host"})
				}
				var edges []graph.Edge
				for i := 0; i < count; i++ {
					endpoint := endpoints[0]
					if i == count/2 {
						endpoint = endpoints[1]
					}
					leaf := fmt.Sprintf("host:leaf-%06d", i)
					entities = append(entities, graph.Entity{ID: leaf, Kind: "host"})
					e := graph.Edge{ID: fmt.Sprintf("edge:%06d", i), Type: "links", From: endpoint, To: leaf}
					if reverse {
						e.From, e.To = e.To, e.From
					}
					edges = append(edges, e)
				}
				g, err := graph.FromSnapshot(graph.Snapshot{Version: 1, Entities: entities, Edges: edges, RelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}})
				if err != nil {
					t.Fatal(err)
				}
				objects := NewMemoryStore()
				writer := NewTenantStore(objects, "test")
				writer.WriteIndexRangePages = true
				writer.WriteIndexAdjacencyPages = true
				defer writer.ShutdownTasks(ctx)
				artifacts, err := writer.buildAndWriteIndexArtifacts(ctx, "tenant-a", g, 1, nil)
				if err != nil {
					t.Fatal(err)
				}
				writer.decorateIndexCatalog(&artifacts.Catalog, "tenant-a")
				if err := writer.writeParquetEdgeShards(ctx, "tenant-a", artifacts.EdgeShards); err != nil {
					t.Fatal(err)
				}
				rev, _, err := writer.prepareReverseIndex(ctx, "tenant-a", g, 1)
				if err != nil {
					t.Fatal(err)
				}
				for _, endpoint := range endpoints[1:] {
					counted := &countingMetaReadStore{ObjectStore: objects}
					reader := NewTenantStore(counted, "test")
					defer reader.ShutdownTasks(ctx)
					lookup := &PersistedIndexLookup{Store: reader, TenantID: "tenant-a", Version: 1, Catalog: artifacts.Catalog, ReverseCatalog: &rev}
					want := 0
					if endpoint == endpoints[1] {
						want = 1
					}
					for attempt := 0; attempt < 2; attempt++ {
						var got []graph.Edge
						var ok bool
						if reverse {
							got, ok, err = lookup.InEdges(ctx, endpoint, nil)
						} else {
							got, ok, err = lookup.OutEdges(ctx, endpoint, nil)
						}
						if err != nil || !ok || len(got) != want {
							t.Fatalf("adjacency cold/warm=%d: got=%d ok=%v err=%v", attempt, len(got), ok, err)
						}
					}
					loaded := 0
					specs := artifacts.Catalog.EdgeShards
					if reverse {
						specs = rev.EdgeShards
					}
					for _, spec := range specs {
						for _, object := range spec.Objects {
							loaded += counted.GetWithMetaCount(object.Key)
						}
					}
					if loaded > want {
						t.Fatalf("degree %d reads %d payload pages from a %d-edge bucket; want <=%d", want, loaded, count, want)
					}
				}
			})
		}
	}
}

func TestRangeDeleteMergesNonemptyNeighborPages(t *testing.T) {
	ctx := context.Background()
	var entities []graph.Entity
	for i := 0; len(entities) < 4096; i++ {
		id := fmt.Sprintf("host:merge-%07d", i)
		if entityShardID(id) == "00" {
			entities = append(entities, graph.Entity{ID: id, Kind: "host"})
		}
	}
	before, err := graph.FromSnapshot(graph.Snapshot{Version: 1, Entities: entities})
	if err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteIndexRangePages = true
	defer store.ShutdownTasks(ctx)
	artifacts, err := store.buildAndWriteIndexArtifacts(ctx, "tenant-a", before, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.decorateIndexCatalog(&artifacts.Catalog, "tenant-a")
	if err := store.writeParquetEntityPages(ctx, "tenant-a", artifacts.EntityPages, 1); err != nil {
		t.Fatal(err)
	}
	var deleted []string
	for i, e := range entities {
		if i%1024 >= 200 {
			deleted = append(deleted, e.ID)
		}
	}
	after, _, err := before.ApplyCommitStorageCopyWithOptions(graph.Commit{ID: "shrink", Version: 2, Mutations: graph.Mutations{DeleteEntities: deleted}}, graph.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, specs, err := store.buildIncrementalEntityPages(ctx, "tenant-a", 1, artifacts.Catalog.EntityPages, before, after, deleted, 2, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 || specs[0].EntityCount != 800 {
		t.Fatalf("entities lost: %+v", specs)
	}
	if got := len(specs[0].Objects); got != 2 {
		t.Fatalf("delete leaves %d nonempty pages; want two bounded local merges", got)
	}
}
