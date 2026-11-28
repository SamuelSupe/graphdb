package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func TestReverseIndexGCUsesCurrentCatalogAndRetainsUnpublishedVersions(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := NewWriterObjectCache(base, WriterObjectCacheConfig{MaxKeys: 128, NegativeTTL: time.Minute})
	store := NewTenantStore(objects, "test")
	const tenant = "tenant-a"
	if _, err := store.InitTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetReverseIndexCatalog(ctx, tenant, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reverse catalog before publication: %v", err)
	}
	orphan := store.reverseEdgeShardVersionKey(tenant, 1, "rel", "orphan")
	online := store.reverseEdgeShardVersionKey(tenant, 1, "rel", "live")
	current := store.reverseEdgeShardVersionKey(tenant, 2, "rel", "new")
	future := store.reverseEdgeShardVersionKey(tenant, 3, "rel", "future")
	for _, key := range []string{orphan, online, current, future} {
		if err := base.Put(ctx, key, []byte("page")); err != nil {
			t.Fatal(err)
		}
	}
	data, err := json.Marshal(ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion, TenantID: tenant, Version: 2,
		EdgeShards: []EdgeShard{{
			RelationType: "rel", Shard: "live", EdgeCount: 1, ContentHash: "hash",
			Objects: []IndexObject{{Key: online, ContentHash: "hash"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Put(ctx, store.reverseIndexCatalogKey(tenant), data); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunGC(ctx, tenant, GCOptions{CleanupIndexOrphans: true}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{online, current, future} {
		if _, err := base.Get(ctx, key); err != nil {
			t.Fatalf("retained object %s: %v", key, err)
		}
	}
	if _, err := base.Get(ctx, orphan); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired orphan: %v", err)
	}
}

func TestPersistedReverseIndexSupportsLazyBidirectionalTraversal(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true, ImpactDirection: "forward"},
			{Name: "owned_by", FromKind: "service", ToKind: "team", Directed: true, ImpactDirection: "reverse"},
		},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app", Kind: "host"},
			{ID: "team:platform", Kind: "team"},
		},
		UpsertEdges: []graph.Edge{
			{Type: "runs_on", From: "service:api", To: "host:app"},
			{Type: "owned_by", From: "service:api", To: "team:platform"},
		},
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
		stats.ReverseEdgeShards = append(stats.ReverseEdgeShards, query.PlannerEdgeStat{
			RelationType:    shard.RelationType,
			ImpactDirection: shard.ImpactDirection,
			Shard:           shard.Shard,
			EdgeCount:       shard.EdgeCount,
		})
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
		Op: "impact", ID: "team:platform", Direction: "out", RelationType: "owned_by", Depth: 1, Limit: 10,
	}, options)
	if err != nil || len(response.Results) != 1 || response.Results[0].Path == nil ||
		response.Results[0].Path.Entities[1].ID != "service:api" {
		t.Fatalf("lazy reverse impact response=%#v err=%v", response, err)
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
	referenced := map[string]struct{}{
		store.reverseIndexCatalogKey("tenant-a"): {},
	}
	for _, spec := range updatedReverse.EdgeShards {
		for _, object := range spec.Objects {
			referenced[object.Key] = struct{}{}
		}
	}
	for _, object := range objects {
		if _, ok := referenced[object.Key]; !ok {
			t.Fatalf("unreferenced reverse index object remains: %s", object.Key)
		}
	}
}
