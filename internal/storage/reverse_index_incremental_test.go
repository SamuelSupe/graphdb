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
