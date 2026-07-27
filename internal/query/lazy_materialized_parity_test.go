package query

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestLazyAndMaterializedPathQueriesStayEquivalent(t *testing.T) {
	full := parityGraph(t)
	lookup := &graphParityLookup{graph: full}
	lazy := graph.New()
	lazy.Version = full.Version
	options := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version: full.Version,
			EdgeShards: []PlannerEdgeStat{
				{RelationType: "feeds", ImpactDirection: "reverse", Shard: "00", EdgeCount: 2},
				{RelationType: "links", ImpactDirection: "forward", Shard: "00", EdgeCount: 6},
				{RelationType: "peer", ImpactDirection: "both", Shard: "00", EdgeCount: 1},
			},
			ReverseEdgeShards: []PlannerEdgeStat{
				{RelationType: "feeds", ImpactDirection: "reverse", Shard: "00", EdgeCount: 2},
				{RelationType: "links", ImpactDirection: "forward", Shard: "00", EdgeCount: 6},
				{RelationType: "peer", ImpactDirection: "both", Shard: "00", EdgeCount: 1},
			},
			EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: 7}},
		},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
	requests := []Request{
		{Op: "match", Kind: "node", Limit: 2},
		{
			Op: "match", Kind: "node", Limit: 2,
			Where:   []Filter{{Field: "score", Op: "gte", Value: 2}},
			Project: []string{"id", "score"},
		},
		{
			Op: "match", Kind: "node", Limit: 2,
			Sort:      []SortSpec{{Field: "score", Desc: true}},
			Aggregate: []Aggregation{{Op: "count"}, {Name: "avg_score", Op: "avg", Field: "score"}},
			GroupBy:   []string{"group"},
			Having:    []Filter{{Field: "count", Op: "gte", Value: 2}},
		},
		{Op: "neighbors", ID: "node:0", Direction: "both", Limit: 2},
		{Op: "neighbors", ID: "node:0", Direction: "in", RelationTypes: []string{"links", "feeds"}, Limit: 2},
		{
			Op: "neighbors", ID: "node:0", Direction: "both", Limit: 2,
			Where:     []Filter{{Field: "group", Op: "eq", Value: 0}},
			EdgeWhere: []Filter{{Field: "type", Op: "in", Value: []any{"links", "feeds"}}},
		},
		{
			Op: "neighbors", ID: "node:0", Direction: "both", Limit: 2,
			Sort:      []SortSpec{{Field: "score", Desc: true}},
			Aggregate: []Aggregation{{Op: "count"}},
			GroupBy:   []string{"group"},
		},
		{Op: "traverse", ID: "node:0", Direction: "out", Depth: 3, Limit: 3},
		{Op: "traverse", ID: "node:0", Direction: "both", Depth: 2, Limit: 4},
		{
			Op: "traverse", ID: "node:0", Depth: 2, Limit: 3,
			Path: PathFilter{Steps: []PathStep{
				{Direction: "out", RelationTypes: []string{"links"}},
				{Direction: "in", RelationTypes: []string{"feeds", "links"}},
			}},
		},
		{
			Op: "traverse", ID: "node:0", Direction: "both", Depth: 3,
			Sort: []SortSpec{{Field: "end_id", Desc: true}}, Limit: 2,
			Aggregate: []Aggregation{{Name: "paths", Op: "count"}},
		},
		{Op: "shortest_path", ID: "node:0", TargetID: "node:6", Direction: "both", Depth: 5},
		{Op: "impact", ID: "node:0", Depth: 3, Limit: 4},
		{
			Op: "pattern", Kind: "node", Limit: 3,
			Path: PathFilter{Steps: []PathStep{
				{Direction: "out", RelationTypes: []string{"links"}},
				{Direction: "out", RelationTypes: []string{"feeds", "links", "peer"}},
			}},
		},
	}
	for index, request := range requests {
		assertQueryParity(t, index, full, lazy, request, options)
	}
}

func assertQueryParity(
	t *testing.T,
	index int,
	full *graph.Graph,
	lazy *graph.Graph,
	request Request,
	options ExecuteOptions,
) {
	t.Helper()
	for page := 0; page < 100; page++ {
		materialized, err := ExecuteContextWithOptions(
			context.Background(), full, request, ExecuteOptions{},
		)
		if err != nil {
			t.Fatalf("request %d page %d materialized: %v", index, page, err)
		}
		indexed, err := ExecuteContextWithOptions(
			context.Background(), lazy, request, options,
		)
		if err != nil {
			t.Fatalf("request %d page %d lazy: %v", index, page, err)
		}
		materialized.Stats = Stats{}
		materialized.Profile = nil
		materialized.Plan = nil
		indexed.Stats = Stats{}
		indexed.Profile = nil
		indexed.Plan = nil
		materializedJSON, _ := json.Marshal(materialized)
		indexedJSON, _ := json.Marshal(indexed)
		if !bytes.Equal(materializedJSON, indexedJSON) {
			t.Fatalf(
				"request %d page %d mismatch\nmaterialized=%s\nlazy=%s",
				index, page, materializedJSON, indexedJSON,
			)
		}
		if materialized.NextCursor == "" {
			return
		}
		request.Cursor = materialized.NextCursor
	}
	t.Fatalf("request %d pagination did not terminate", index)
}

func parityGraph(t *testing.T) *graph.Graph {
	t.Helper()
	entities := make([]graph.Entity, 0, 7)
	for index := 0; index < 7; index++ {
		entities = append(entities, graph.Entity{
			ID: "node:" + string(rune('0'+index)), Kind: "node",
			Fields: graph.Fields{"group": index % 2, "score": index},
		})
	}
	g, err := graph.FromSnapshot(graph.Snapshot{
		Version:  1,
		CITypes:  []graph.CIType{{Name: "node"}},
		Entities: entities,
		RelationTypes: []graph.RelationType{
			{Name: "links", Directed: true, FromKind: "node", ToKind: "node", ImpactDirection: "forward"},
			{Name: "feeds", Directed: true, FromKind: "node", ToKind: "node", ImpactDirection: "reverse"},
			{Name: "peer", Directed: true, FromKind: "node", ToKind: "node", ImpactDirection: "both"},
		},
		Edges: []graph.Edge{
			{ID: "edge:01", Type: "links", From: "node:0", To: "node:1"},
			{ID: "edge:02", Type: "links", From: "node:2", To: "node:0"},
			{ID: "edge:03", Type: "links", From: "node:0", To: "node:0"},
			{ID: "edge:04", Type: "feeds", From: "node:1", To: "node:3"},
			{ID: "edge:05", Type: "links", From: "node:1", To: "node:4"},
			{ID: "edge:06", Type: "feeds", From: "node:4", To: "node:0"},
			{ID: "edge:07", Type: "peer", From: "node:3", To: "node:5"},
			{ID: "edge:08", Type: "links", From: "node:5", To: "node:6"},
			{ID: "edge:09", Type: "links", From: "node:2", To: "node:4"},
		},
	})
	if err != nil {
		t.Fatalf("build parity graph: %v", err)
	}
	return g
}
