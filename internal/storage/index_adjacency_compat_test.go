package storage

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestListEdgesOldCursorSurvivesAdjacencyMigrationAndMerge(t *testing.T) {
	for _, ranges := range []bool{false, true} {
		t.Run(fmt.Sprintf("old_ranges=%v", ranges), func(t *testing.T) {
			ctx := context.Background()
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			s := NewTenantStore(files, "test")
			s.WriteIndexRangePages = ranges
			s.WriteEntityRecords = false
			defer s.ShutdownTasks(ctx)
			entities := []graph.Entity{{ID: "host:hub", Kind: "host"}}
			var edges []graph.Edge
			for i := 0; i < 2050; i++ {
				id := fmt.Sprintf("host:compat-%06d", i)
				entities = append(entities, graph.Entity{ID: id, Kind: "host"})
				edges = append(edges, graph.Edge{Type: "links", From: entities[0].ID, To: id, Source: "fixture"})
			}
			if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities, UpsertEdges: edges, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			old, err := s.RebuildIndexes(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			oldHash, err := indexCatalogContentHash(old)
			if err != nil {
				t.Fatal(err)
			}
			types := []EdgeScanOptions{{Limit: 257}, {Limit: 257, Type: "links"}, {Limit: 257, From: "host:hub"}, {Limit: 257, FromShard: edgeShardID("host:hub")}, {Limit: 257, Source: "fixture"}}
			var first []EdgeScanResult
			var expected [][]string
			for _, opts := range types {
				p, err := s.ListEdges(ctx, "tenant-a", opts)
				if err != nil || p.NextCursor == "" {
					t.Fatalf("old first page: %v", err)
				}
				first = append(first, p)
				var ids []string
				for {
					for _, e := range p.Edges {
						ids = append(ids, e.ID)
					}
					if p.NextCursor == "" {
						break
					}
					opts.Cursor = p.NextCursor
					p, err = s.ListEdges(ctx, "tenant-a", opts)
					if err != nil {
						t.Fatal(err)
					}
				}
				expected = append(expected, ids)
			}
			s.WriteIndexRangePages = true
			s.WriteIndexAdjacencyPages = true
			migrated, err := s.RebuildIndexes(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if len(migrated.EdgeShards) == 0 || migrated.EdgeShards[0].AdjacencyRoot == nil {
				t.Fatal("explicit migration did not publish adjacency")
			}
			pinned, err := s.GetIndexCatalogSnapshot(ctx, "tenant-a", old.Version, oldHash)
			if err != nil {
				t.Fatal(err)
			}
			if hash, _ := indexCatalogContentHash(pinned); hash != oldHash {
				t.Fatal("old catalog was normalized to a new hash")
			}
			// A later deletion triggers local merges; old cursors retain all old IDs.
			result, err := s.CommitWithReport(ctx, "tenant-a", graph.Mutations{DeleteEntities: entityIDs(entities[301:])}, CommitOptions{})
			if err != nil || len(result.IndexWarnings) != 0 {
				t.Fatalf("delete: %+v %v", result, err)
			}
			for i, opts := range types {
				p := first[i]
				var ids []string
				for {
					for _, e := range p.Edges {
						ids = append(ids, e.ID)
					}
					if p.NextCursor == "" {
						break
					}
					opts.Cursor = p.NextCursor
					p, err = s.ListEdges(ctx, "tenant-a", opts)
					if err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(ids, expected[i]) {
					t.Fatalf("old cursor %d lost order/data: got=%d want=%d", i, len(ids), len(expected[i]))
				}
			}
			// A reader with the feature flag off still reads the new layout.
			reader := NewTenantStore(files, "test")
			defer reader.ShutdownTasks(ctx)
			current, err := reader.GetIndexCatalog(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			reverse, err := reader.GetReverseIndexCatalog(ctx, "tenant-a", current.Version)
			if err != nil {
				t.Fatal(err)
			}
			lookup := &PersistedIndexLookup{Store: reader, TenantID: "tenant-a", Version: current.Version, Catalog: current, ReverseCatalog: &reverse}
			got, ok, err := lookup.OutEdges(ctx, "host:hub", nil)
			if err != nil || !ok || len(got) != 300 {
				t.Fatalf("new-layout reader: %d %v %v", len(got), ok, err)
			}
			// Controlled downgrade publishes payload-only catalogs; it does not
			// claim that an old binary can consume a newly issued cursor.
			s.WriteIndexAdjacencyPages = false
			s.WriteIndexRangePages = ranges
			downgraded, err := s.RebuildIndexes(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range downgraded.EdgeShards {
				if spec.AdjacencyRoot != nil {
					t.Fatal("downgrade retained adjacency root")
				}
			}
			page, err := s.ListEdges(ctx, "tenant-a", EdgeScanOptions{Limit: 500})
			if err != nil || len(page.Edges) != 300 {
				t.Fatalf("downgrade scan: %d %v", len(page.Edges), err)
			}
			if err := s.Objects.Delete(ctx, s.indexCatalogVersionHashKey("tenant-a", old.Version, oldHash)); err != nil {
				t.Fatal(err)
			}
			s.deleteCachedIndexCatalog("tenant-a")
			fresh := NewTenantStore(files, "test")
			defer fresh.ShutdownTasks(ctx)
			opts := types[0]
			opts.Cursor = first[0].NextCursor
			if _, err := fresh.ListEdges(ctx, "tenant-a", opts); err == nil {
				t.Fatal("expired cursor silently switched snapshots")
			}
		})
	}
}

func entityIDs(entities []graph.Entity) []string {
	ids := make([]string, 0, len(entities))
	for _, e := range entities {
		ids = append(ids, e.ID)
	}
	return ids
}
