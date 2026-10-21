package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestAdjacencyPublicationFailurePreservesAcceptedGraphAndOldRoot(t *testing.T) {
	for _, mode := range []string{"adjacency_write", "catalog_cas", "uncertain_publication"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base := NewMemoryStore()
			s := NewTenantStore(base, "test")
			s.WriteIndexRangePages = true
			s.WriteIndexAdjacencyPages = true
			defer s.ShutdownTasks(ctx)
			if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}, {ID: "host:c", Kind: "host"}}, UpsertEdges: []graph.Edge{{Type: "links", From: "host:a", To: "host:b"}}, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			previous, err := s.RebuildIndexes(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			rule := faultMatrixRule{name: mode, op: "write", contains: "/adjacency/", err: ErrObjectStoreUnavailable, remaining: -1}
			if mode == "catalog_cas" {
				rule.op = "put_conditional"
				rule.contains = "/indexes/catalog.parquet"
				rule.err = ErrConflict
			}
			faults := newFaultMatrixStore(base, rule)
			s.Objects = faults
			var uncertain *uncertainRangeCatalogStore
			if mode == "uncertain_publication" {
				uncertain = &uncertainRangeCatalogStore{ObjectStore: base}
				s.Objects = uncertain
			}
			result, err := s.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{{Type: "links", From: "host:a", To: "host:c"}}}, CommitOptions{})
			if err != nil || result.Version != 2 || len(result.IndexWarnings) == 0 {
				t.Fatalf("accepted outcome: %+v %v", result, err)
			}
			if uncertain == nil && faults.fired(mode) == 0 || uncertain != nil && !uncertain.failed.Load() {
				t.Fatal("fault did not fire")
			}
			reader := NewTenantStore(base, "test")
			defer reader.ShutdownTasks(ctx)
			lookup := &PersistedIndexLookup{Store: reader, TenantID: "tenant-a", Version: previous.Version, Catalog: previous}
			if edges, ok, err := lookup.OutEdges(ctx, "host:a", nil); err != nil || !ok || len(edges) != 1 {
				t.Fatalf("old root damaged: %d %v %v", len(edges), ok, err)
			}
			g, _, err := reader.Load(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if g.Edges.Len() != 2 {
				t.Fatal("derived index failure lost committed edge")
			}
			s.Objects = base
			if _, err := s.RebuildIndexes(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAdjacencyPinnedGCAndMissingRootNeverMeanEmpty(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	s := NewTenantStore(files, "test")
	s.WriteIndexRangePages = true
	s.WriteIndexAdjacencyPages = true
	s.WriteEntityRecords = false
	defer s.ShutdownTasks(ctx)
	entities := []graph.Entity{{ID: "host:hub", Kind: "host"}}
	var edges []graph.Edge
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("host:gc-%d", i)
		entities = append(entities, graph.Entity{ID: id, Kind: "host"})
		edges = append(edges, graph.Edge{Type: "links", From: "host:hub", To: id})
	}
	if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities, UpsertEdges: edges[:10], UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	old, err := s.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.PinReadView(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEdges: edges[10:]}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Compact(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatal(err)
	}
	cold := NewTenantStore(files, "test")
	defer cold.ShutdownTasks(ctx)
	q := &PersistedIndexLookup{Store: cold, TenantID: "tenant-a", Version: old.Version, Catalog: old}
	if got, ok, err := q.OutEdges(ctx, "host:hub", nil); err != nil || !ok || len(got) != 10 {
		t.Fatalf("pinned root: %d %v %v", len(got), ok, err)
	}
	release()
	for i := 0; i < 2; i++ {
		if _, err := s.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := files.Get(ctx, s.adjacencyObjectKey(*old.EdgeShards[0].AdjacencyRoot)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old tree not reclaimed: %v", err)
	}
	current, err := s.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	root := current.EdgeShards[0].AdjacencyRoot
	if root == nil {
		t.Fatal("current tree lost during compaction")
	}
	if err := files.Delete(ctx, s.adjacencyObjectKey(*root)); err != nil {
		t.Fatal(err)
	}
	missing := NewTenantStore(files, "test")
	defer missing.ShutdownTasks(ctx)
	q = &PersistedIndexLookup{Store: missing, TenantID: "tenant-a", Version: current.Version, Catalog: current}
	if got, ok, err := q.OutEdges(ctx, "host:hub", nil); err == nil {
		t.Fatalf("missing root returned adjacency: %d %v", len(got), ok)
	}
}

func TestAdjacencyMigrationNeverRunsOnBusinessIndexGap(t *testing.T) {
	ctx := context.Background()
	objects := &adjacencyCostStore{ObjectStore: NewMemoryStore()}
	s := NewTenantStore(objects, "test")
	s.WriteIndexRangePages = true
	defer s.ShutdownTasks(ctx)
	if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}, {ID: "host:b", Kind: "host"}, {ID: "host:c", Kind: "host"}}, UpsertEdges: []graph.Edge{{Type: "links", From: "host:a", To: "host:b"}}, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Cardinality: graph.ManyToMany}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	old, err := s.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	s.WriteIndexAdjacencyPages = true
	if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{{Type: "links", From: "host:a", To: "host:c"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	before, _, err := s.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the existing full-build fallback for a lagging catalog. It
	// must not silently turn a business request into format migration.
	writeParquetIndexCatalogForTest(t, ctx, s, "tenant-a", old)
	objects.reset()
	result, err := s.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "changed"}}}}, CommitOptions{})
	if err != nil || result.Version != 3 {
		t.Fatalf("gap recovery: %+v %v", result, err)
	}
	after, _, err := s.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	// Background catch-up, unlike an isolated direct commit, is permitted to
	// rebuild on a gap. Exercise that same boundary with captured graph inputs.
	if err := s.updateIndexesAfterCommit(ctx, "tenant-a", before, after, graph.Mutations{}, graph.ApplyReport{}, 2, 3, false, true); err != nil {
		t.Fatal(err)
	}
	for key := range objects.writes {
		if isAdjacencyObjectKey(key) {
			t.Fatal("business gap recovery built adjacency tree")
		}
	}
	current, err := s.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range current.EdgeShards {
		if spec.AdjacencyRoot != nil {
			t.Fatal("business gap recovery migrated bucket")
		}
	}
	current, err = s.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if current.EdgeShards[0].AdjacencyRoot == nil {
		t.Fatal("explicit rebuild did not migrate")
	}
}
