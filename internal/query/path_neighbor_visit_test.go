package query

import (
	"context"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestTraverseStopsIndexedEdgeVisitorAtPageBoundary(t *testing.T) {
	edges := pathVisitorEdges(100)
	lookup := &countingOutEdgeLookup{edges: edges}
	g := graph.New()
	g.Version = 1

	response, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		Request{
			Op:        "traverse",
			ID:        "service:start",
			Direction: "out",
			Depth:     1,
			Limit:     1,
		},
		pathVisitorOptions(len(edges), lookup),
	)
	if err != nil {
		t.Fatalf("traverse: %v", err)
	}
	if len(response.Results) != 1 || response.NextCursor == "" {
		t.Fatalf("response = %#v, want one result and cursor", response)
	}
	if lookup.outCalls != 0 {
		t.Fatalf("OutEdges calls = %d, want 0", lookup.outCalls)
	}
	if lookup.visited != 2 {
		t.Fatalf(
			"visited edges = %d, want page plus lookahead",
			lookup.visited,
		)
	}
}

func TestShortestPathStopsIndexedEdgeVisitorAtTarget(t *testing.T) {
	edges := pathVisitorEdges(100)
	lookup := &countingOutEdgeLookup{edges: edges}
	g := graph.New()
	g.Version = 1

	response, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		Request{
			Op:        "shortest_path",
			ID:        "service:start",
			TargetID:  "host:000",
			Direction: "out",
			Depth:     1,
		},
		pathVisitorOptions(len(edges), lookup),
	)
	if err != nil {
		t.Fatalf("shortest path: %v", err)
	}
	if len(response.Results) != 1 ||
		response.Results[0].Path == nil {
		t.Fatalf("response = %#v, want one path", response)
	}
	if lookup.outCalls != 0 {
		t.Fatalf("OutEdges calls = %d, want 0", lookup.outCalls)
	}
	if lookup.visited != 1 {
		t.Fatalf("visited edges = %d, want 1", lookup.visited)
	}
}

func pathVisitorEdges(count int) []graph.Edge {
	edges := make([]graph.Edge, 0, count)
	for index := 0; index < count; index++ {
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:%03d", index),
			Type: "links",
			From: "service:start",
			To:   fmt.Sprintf("host:%03d", index),
		})
	}
	return edges
}

func pathVisitorOptions(
	edgeCount int,
	lookup *countingOutEdgeLookup,
) ExecuteOptions {
	return ExecuteOptions{
		PlannerStats: PlannerStats{
			Version: 1,
			EdgeShards: []PlannerEdgeStat{{
				RelationType: "links",
				Shard:        plannerEdgeShardID("service:start"),
				EdgeCount:    edgeCount,
			}},
			EntityPages: []PlannerEntityPageStat{{
				Shard: "00", EntityCount: edgeCount + 1,
			}},
		},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
}
