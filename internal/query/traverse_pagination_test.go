package query

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestTraverseCursorContinuesPastSecondPage(t *testing.T) {
	g := traversalPagingGraph(t)
	request := Request{
		Op: "traverse", ID: "service:start", Direction: "out", Depth: 1, Limit: 1,
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	request.Cursor = first.NextCursor
	second, err := Execute(g, request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.NextCursor == "" {
		t.Fatal("second page omitted cursor for the remaining third path")
	}
	request.Cursor = second.NextCursor
	third, err := Execute(g, request)
	if err != nil {
		t.Fatalf("third page: %v", err)
	}
	if len(third.Results) != 1 || pathEnd(*third.Results[0].Path).ID != "host:c" {
		t.Fatalf("third page = %#v, want host:c", third.Results)
	}
}

func TestTraverseAcceptsLegacyEdgeOnlyPathCursor(t *testing.T) {
	g := traversalPagingGraph(t)
	reference, err := Execute(g, Request{
		Op: "traverse", ID: "service:start", Direction: "out",
		Depth: 1, Limit: 2,
	})
	if err != nil {
		t.Fatalf("reference page: %v", err)
	}
	request := Request{
		Op: "traverse", ID: "service:start", Direction: "out",
		Depth: 1, Limit: 1,
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	request.Cursor = encodeCursor(cursorState{
		Version: g.Version,
		After:   legacyPathIdentity(*first.Results[0].Path),
		Query:   cursorQueryHash(request),
	})
	second, err := Execute(g, request)
	if err != nil {
		t.Fatalf("legacy cursor page: %v", err)
	}
	if len(second.Results) != 1 ||
		legacyPathIdentity(*second.Results[0].Path) !=
			legacyPathIdentity(*reference.Results[1].Path) {
		t.Fatalf(
			"legacy cursor page = %v, want %v",
			pathResultIDs(second.Results),
			pathResultIDs(reference.Results[1:2]),
		)
	}
}

func TestTraverseSortAndAggregateUseFullBoundedPathSet(t *testing.T) {
	g := traversalPagingGraph(t)
	response, err := Execute(g, Request{
		Op:        "traverse",
		ID:        "service:start",
		Direction: "out",
		Depth:     1,
		Sort:      []SortSpec{{Field: "end_id", Desc: true}},
		Aggregate: []Aggregation{{Op: "count"}},
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("traverse: %v", err)
	}
	if len(response.Results) != 1 || pathEnd(*response.Results[0].Path).ID != "host:c" {
		t.Fatalf("first sorted result = %#v, want host:c", response.Results)
	}
	if response.Aggregates["count"] != 3 {
		t.Fatalf("count = %#v, want 3", response.Aggregates["count"])
	}
}

func TestTraversePaginationContinuesPastInternalLookahead(t *testing.T) {
	g := largeTraversalPagingGraph(t, 1003)
	request := Request{
		Op: "traverse", ID: "service:start", Direction: "out",
		Depth: 1, Limit: 1000,
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first.Results) != 1000 || first.NextCursor == "" {
		t.Fatalf(
			"first page results=%d cursor=%q, want 1000 and a cursor",
			len(first.Results), first.NextCursor,
		)
	}
	request.Cursor = first.NextCursor
	second, err := Execute(g, request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second.Results) != 3 || second.NextCursor != "" {
		t.Fatalf(
			"second page results=%d cursor=%q, want final 3 results",
			len(second.Results), second.NextCursor,
		)
	}
}

func TestTraverseGlobalSortAndAggregateExceedLookahead(t *testing.T) {
	g := largeTraversalPagingGraph(t, 1100)
	response, err := Execute(g, Request{
		Op: "traverse", ID: "service:start", Direction: "out", Depth: 1,
		Sort:      []SortSpec{{Field: "end_id", Desc: true}},
		Aggregate: []Aggregation{{Op: "count"}},
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("traverse: %v", err)
	}
	if response.Aggregates["count"] != 1100 {
		t.Fatalf("count = %#v, want 1100", response.Aggregates["count"])
	}
	if len(response.Results) != 1 ||
		pathEnd(*response.Results[0].Path).ID != "host:1099" {
		t.Fatalf(
			"first sorted result = %#v, want host:1099",
			response.Results,
		)
	}
}

func TestTraverseDefaultOrderDoesNotLoseDeeperPaths(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "traverse-depth-order",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{
				Name: "links", AllowCrossKind: true, Directed: true,
			}},
			UpsertEntities: []graph.Entity{
				{ID: "service:start", Kind: "service"},
				{ID: "host:a", Kind: "host"},
				{ID: "host:b", Kind: "host"},
				{ID: "host:c", Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "edge:a", Type: "links", From: "service:start", To: "host:a"},
				{ID: "edge:b", Type: "links", From: "service:start", To: "host:b"},
				{ID: "edge:c", Type: "links", From: "service:start", To: "host:c"},
			},
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	rootNeighbors := g.Neighbors("service:start", "out", "links")
	if len(rootNeighbors) != 3 {
		t.Fatalf("root neighbors = %d, want 3", len(rootNeighbors))
	}
	firstEdge := rootNeighbors[0].Edge.ID
	firstTarget := rootNeighbors[0].Entity.ID
	if err := g.ApplyCommit(graph.Commit{
		ID:      "traverse-depth-order-child",
		Version: 2,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: "process:a", Kind: "process",
			}},
			UpsertEdges: []graph.Edge{{
				ID: "edge:z", Type: "links",
				From: firstTarget, To: "process:a",
			}},
		},
	}); err != nil {
		t.Fatalf("seed child path: %v", err)
	}
	childEdge := graph.CanonicalEdgeIDParts(
		"links", firstTarget, "process:a",
	)
	request := Request{
		Op: "traverse", ID: "service:start", Direction: "out",
		Depth: 2, Limit: 2,
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if got := pathResultIDs(first.Results); fmt.Sprint(got) !=
		fmt.Sprint([]string{
			"path:" + firstEdge,
			"path:" + firstEdge + ">" + childEdge,
		}) {
		t.Fatalf("first page = %v, want globally ordered path prefix", got)
	}
	request.Cursor = first.NextCursor
	second, err := Execute(g, request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if got := pathResultIDs(second.Results); fmt.Sprint(got) !=
		fmt.Sprint([]string{
			"path:" + rootNeighbors[1].Edge.ID,
			"path:" + rootNeighbors[2].Edge.ID,
		}) {
		t.Fatalf("second page = %v, want remaining paths", got)
	}
}

func pathResultIDs(results []Result) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, legacyPathIdentity(*result.Path))
	}
	return ids
}

func traversalPagingGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "traverse-pages",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "service:start", Kind: "service"},
				{ID: "host:a", Kind: "host", Fields: graph.Fields{"rank": 1}},
				{ID: "host:b", Kind: "host", Fields: graph.Fields{"rank": 2}},
				{ID: "host:c", Kind: "host", Fields: graph.Fields{"rank": 3}},
			},
			UpsertEdges: []graph.Edge{
				{ID: "edge:a", Type: "runs_on", From: "service:start", To: "host:a"},
				{ID: "edge:b", Type: "runs_on", From: "service:start", To: "host:b"},
				{ID: "edge:c", Type: "runs_on", From: "service:start", To: "host:c"},
			},
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	return g
}

func largeTraversalPagingGraph(t *testing.T, count int) *graph.Graph {
	t.Helper()
	entities := make([]graph.Entity, 0, count+1)
	entities = append(entities, graph.Entity{
		ID: "service:start", Kind: "service",
	})
	edges := make([]graph.Edge, 0, count)
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("%04d", index)
		entities = append(entities, graph.Entity{
			ID: "host:" + id, Kind: "host",
		})
		edges = append(edges, graph.Edge{
			ID: "edge:" + id, Type: "runs_on",
			From: "service:start", To: "host:" + id,
		})
	}
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID: "large-traverse-pages", Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: entities,
			UpsertEdges:    edges,
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	return g
}
