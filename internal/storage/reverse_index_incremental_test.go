package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestReverseIndexAdvancesWithoutFullRewrite(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	targetA, targetB := distinctEdgeShardTargets()
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name:            "link",
			FromKind:        "node",
			ToKind:          "node",
			ImpactDirection: "forward",
			Cardinality:     graph.ManyToMany,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "node:a", Kind: "node"},
			{ID: "node:b", Kind: "node"},
			{ID: "node:c", Kind: "node"},
			{ID: targetA, Kind: "node"},
			{ID: targetB, Kind: "node"},
		},
		UpsertEdges: []graph.Edge{
			{Type: "link", From: "node:a", To: targetA},
			{Type: "link", From: "node:b", To: targetB},
		},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "node:a", Kind: "node",
			Fields: graph.Fields{"state": "ready"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	reverse, err := store.GetReverseIndexCatalog(
		ctx,
		"tenant-a",
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range reverse.EdgeShards {
		if spec.ImpactDirection != "forward" {
			t.Fatalf("impact direction = %q", spec.ImpactDirection)
		}
		if len(spec.Objects) != 1 ||
			!strings.Contains(spec.Objects[0].Key, "/v1/") {
			t.Fatalf(
				"entity-only commit rewrote reverse shard: %#v",
				spec,
			)
		}
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEdges: []graph.Edge{{
			Type: "link", From: "node:c", To: targetA,
		}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	reverse, err = store.GetReverseIndexCatalog(
		ctx,
		"tenant-a",
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	changed := reverseSpecForShard(
		t,
		reverse,
		"link",
		edgeShardID(targetA),
	)
	if !strings.Contains(changed.Objects[0].Key, "/v3/") ||
		changed.EdgeCount != 2 {
		t.Fatalf("changed reverse shard = %#v", changed)
	}
	unchanged := reverseSpecForShard(
		t,
		reverse,
		"link",
		edgeShardID(targetB),
	)
	if !strings.Contains(unchanged.Objects[0].Key, "/v1/") ||
		unchanged.EdgeCount != 1 {
		t.Fatalf("unchanged reverse shard = %#v", unchanged)
	}
}

func TestReverseIndexMovesAndDeletesEdgeAcrossTargetShards(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	targetA, targetB := distinctEdgeShardTargets()
	oldID := graph.CanonicalEdgeIDParts("link", "node:source", targetA)
	newID := graph.CanonicalEdgeIDParts("link", "node:source", targetB)
	oldEdge := graph.Edge{
		ID: oldID, Type: "link", From: "node:source", To: targetA,
		Fields: graph.Fields{"state": "old"},
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "link", FromKind: "node", ToKind: "node",
			ImpactDirection: "forward", Cardinality: graph.ManyToMany,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "node:source", Kind: "node"},
			{ID: targetA, Kind: "node"},
			{ID: targetB, Kind: "node"},
		},
		UpsertEdges: []graph.Edge{oldEdge},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	forwardV1, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	reverseV1, err := store.GetReverseIndexCatalog(ctx, "tenant-a", forwardV1.Version)
	if err != nil {
		t.Fatalf("load v1 reverse catalog: %v", err)
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		DeleteEdges: []string{oldID},
		UpsertEdges: []graph.Edge{{
			ID: newID, Type: "link", From: "node:source", To: targetB,
			Fields: graph.Fields{"state": "moved"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("move endpoint: %v", err)
	}
	forwardV2, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load v2 index catalog: %v", err)
	}
	reverseV2, err := store.GetReverseIndexCatalog(ctx, "tenant-a", forwardV2.Version)
	if err != nil {
		t.Fatalf("load v2 reverse catalog: %v", err)
	}
	if _, ok := findReverseSpec(reverseV2, "link", edgeShardID(targetA)); ok {
		t.Fatalf("v2 retained removed target shard: %#v", reverseV2.EdgeShards)
	}
	newSpec, ok := findReverseSpec(reverseV2, "link", edgeShardID(targetB))
	if !ok || newSpec.EdgeCount != 1 || len(newSpec.Objects) != 1 ||
		!strings.Contains(newSpec.Objects[0].Key, "/v2/") {
		t.Fatalf("v2 moved target shard = %#v", newSpec)
	}

	lookupV2 := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a", Version: forwardV2.Version,
		Catalog: forwardV2, ReverseCatalog: &reverseV2,
	}
	in, _, err := lookupV2.InEdges(ctx, targetA, map[string]struct{}{"link": {}})
	if err != nil || len(in) != 0 {
		t.Fatalf("v2 old target reverse edges=%#v err=%v", in, err)
	}
	in, ok, err = lookupV2.InEdges(ctx, targetB, map[string]struct{}{"link": {}})
	if err != nil || !ok || len(in) != 1 || in[0].ID != newID ||
		in[0].To != targetB || in[0].Fields["state"] != "moved" {
		t.Fatalf("v2 new target reverse edges=%#v ok=%v err=%v", in, ok, err)
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		DeleteEdges: []string{newID},
	}, CommitOptions{}); err != nil {
		t.Fatalf("delete moved edge: %v", err)
	}
	forwardV3, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load v3 index catalog: %v", err)
	}
	reverseV3, err := store.GetReverseIndexCatalog(ctx, "tenant-a", forwardV3.Version)
	if err != nil {
		t.Fatalf("load v3 reverse catalog: %v", err)
	}
	if _, ok := findReverseSpec(reverseV3, "link", edgeShardID(targetB)); ok {
		t.Fatalf("v3 retained deleted target shard: %#v", reverseV3.EdgeShards)
	}
	lookupV3 := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a", Version: forwardV3.Version,
		Catalog: forwardV3, ReverseCatalog: &reverseV3,
	}
	in, _, err = lookupV3.InEdges(ctx, targetB, map[string]struct{}{"link": {}})
	if err != nil || len(in) != 0 {
		t.Fatalf("v3 deleted target reverse edges=%#v err=%v", in, err)
	}

	// The current catalog is replaced in place, but the v1 catalog snapshot must
	// still address its immutable v1 object after later endpoint changes.
	lookupV1 := &PersistedIndexLookup{
		Store: store, TenantID: "tenant-a", Version: forwardV1.Version,
		Catalog: forwardV1, ReverseCatalog: &reverseV1,
	}
	in, ok, err = lookupV1.InEdges(ctx, targetA, map[string]struct{}{"link": {}})
	if err != nil || !ok || len(in) != 1 || in[0].ID != oldID ||
		in[0].To != targetA || in[0].Fields["state"] != "old" {
		t.Fatalf("v1 preserved reverse edges=%#v ok=%v err=%v", in, ok, err)
	}
}

func findReverseSpec(catalog ReverseIndexCatalog, relationType string, shard string) (EdgeShard, bool) {
	for _, spec := range catalog.EdgeShards {
		if spec.RelationType == relationType && spec.Shard == shard {
			return spec, true
		}
	}
	return EdgeShard{}, false
}

func distinctEdgeShardTargets() (string, string) {
	first := "node:target-0"
	for index := 1; ; index++ {
		candidate := fmt.Sprintf("node:target-%d", index)
		if edgeShardID(candidate) != edgeShardID(first) {
			return first, candidate
		}
	}
}

func reverseSpecForShard(
	t *testing.T,
	catalog ReverseIndexCatalog,
	relationType string,
	shard string,
) EdgeShard {
	t.Helper()
	for _, spec := range catalog.EdgeShards {
		if spec.RelationType == relationType &&
			spec.Shard == shard {
			return spec
		}
	}
	t.Fatalf(
		"missing reverse shard %s/%s in %#v",
		relationType,
		shard,
		catalog.EdgeShards,
	)
	return EdgeShard{}
}
