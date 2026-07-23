package storage

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

func TestPersistedReverseIndexSupportsLazyBidirectionalTraversal(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{{Type: "runs_on", From: "service:api", To: "host:app"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := store.GetReverseIndexCatalog(ctx, "tenant-a", catalog.Version)
	if err != nil {
		t.Fatal(err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog, ReverseCatalog: &reverse}
	out, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}})
	if err != nil || !ok || len(out) != 1 || out[0].To != "host:app" {
		t.Fatalf("out=%#v ok=%v err=%v", out, ok, err)
	}
	in, ok, err := lookup.InEdges(ctx, "host:app", map[string]struct{}{"runs_on": {}})
	if err != nil || !ok || len(in) != 1 || in[0].From != "service:api" {
		t.Fatalf("in=%#v ok=%v err=%v", in, ok, err)
	}

	stats := catalog.PlannerStats()
	for _, shard := range reverse.EdgeShards {
		stats.ReverseEdgeShards = append(stats.ReverseEdgeShards, query.PlannerEdgeStat{RelationType: shard.RelationType, Shard: shard.Shard, EdgeCount: shard.EdgeCount})
	}
	empty := graph.New()
	empty.Version = catalog.Version
	options := query.ExecuteOptions{PlannerStats: stats, IndexLookup: lookup, EntityLookup: lookup}
	response, err := query.ExecuteContextWithOptions(ctx, empty, query.Request{
		Op: "traverse", ID: "service:api", Direction: "out", RelationType: "runs_on", Depth: 1, Limit: 10,
	}, options)
	if err != nil || len(response.Results) != 1 || response.Results[0].Path == nil {
		t.Fatalf("lazy outgoing response=%#v err=%v", response, err)
	}
	response, err = query.ExecuteContextWithOptions(ctx, empty, query.Request{
		Op: "impact", ID: "service:api", Direction: "out", RelationType: "runs_on", Depth: 1, Limit: 10,
	}, options)
	if err != nil || len(response.Results) != 1 || response.Results[0].Path == nil {
		t.Fatalf("lazy impact response=%#v err=%v", response, err)
	}
	response, err = query.ExecuteContextWithOptions(ctx, empty, query.Request{
		Op: "traverse", ID: "host:app", Direction: "in", RelationType: "runs_on", Depth: 1, Limit: 10,
	}, options)
	if err != nil || len(response.Results) != 1 || response.Results[0].Path == nil || response.Results[0].Path.Entities[1].ID != "service:api" {
		t.Fatalf("lazy reverse response=%#v err=%v", response, err)
	}
	response, err = query.ExecuteContextWithOptions(ctx, empty, query.Request{
		Op:    "pattern",
		Kind:  "host",
		Where: []query.Filter{{Field: "id", Op: "eq", Value: "host:app"}},
		Path: query.PathFilter{Steps: []query.PathStep{{
			Direction: "in", RelationTypes: []string{"runs_on"}, NodeKinds: []string{"service"},
		}}},
		Limit: 10,
	}, options)
	if err != nil || len(response.Results) != 1 || response.Results[0].Path == nil || response.Results[0].Path.Entities[1].ID != "service:api" {
		t.Fatalf("lazy pattern response=%#v err=%v", response, err)
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "service:worker", Kind: "service"}},
		UpsertEdges:    []graph.Edge{{Type: "runs_on", From: "service:worker", To: "host:app"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	updatedCatalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	updatedReverse, err := store.GetReverseIndexCatalog(ctx, "tenant-a", updatedCatalog.Version)
	if err != nil {
		t.Fatal(err)
	}
	updatedLookup := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a", Version: updatedCatalog.Version,
		Catalog: updatedCatalog, ReverseCatalog: &updatedReverse,
	}
	in, ok, err = updatedLookup.InEdges(ctx, "host:app", map[string]struct{}{"runs_on": {}})
	if err != nil || !ok || len(in) != 2 {
		t.Fatalf("updated reverse edges=%#v ok=%v err=%v", in, ok, err)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{CleanupIndexOrphans: true}); err != nil {
		t.Fatal(err)
	}
	objects, err := store.Objects.List(ctx, store.reverseIndexPrefix("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	versionFragment := "/v" + strconv.FormatInt(updatedCatalog.Version, 10) + "/"
	for _, object := range objects {
		if object.Key == store.reverseIndexCatalogKey("tenant-a") {
			continue
		}
		if !strings.Contains(object.Key, versionFragment) {
			t.Fatalf("obsolete reverse index object remains: %s", object.Key)
		}
	}
}
