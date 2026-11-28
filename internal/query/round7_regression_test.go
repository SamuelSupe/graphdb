package query

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type round7BothEdgeVisitor struct {
	edges   []graph.Edge
	visited int
}

func (v *round7BothEdgeVisitor) VisitBothEdges(_ context.Context, _ string, _ map[string]struct{}, startEdgeID string, visit func(graph.Edge, string) (bool, error)) (bool, error) {
	for _, e := range v.edges {
		if e.ID < startEdgeID {
			continue
		}
		v.visited++
		more, err := visit(e, "out")
		if err != nil || !more {
			return true, err
		}
	}
	return true, nil
}

func TestRound7LazyBothNeighborsCursorOvershootStopsEarly(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "c1",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}},
			UpsertEntities: []graph.Entity{
				{ID: "h0", Kind: "host"},
				{ID: "h1", Kind: "host"},
				{ID: "h2", Kind: "host"},
				{ID: "h3", Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "e2", Type: "links", From: "h0", To: "h2"},
				{ID: "e3", Type: "links", From: "h0", To: "h3"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	req := Request{Op: "neighbors", ID: "h0", Direction: "both", Limit: 1}
	cursor := cursorState{
		Version: 1,
		After:   "edge:out:e1", // Deleted/missing edge before e2!
		Query:   cursorQueryHash(req),
	}
	visitor := &round7BothEdgeVisitor{
		edges: []graph.Edge{
			{ID: "e2", Type: "links", From: "h0", To: "h2"},
			{ID: "e3", Type: "links", From: "h0", To: "h3"},
		},
	}
	b, cancel := newBudget(context.Background(), req, nil, nil, nil, PlannerStats{})
	defer cancel()

	_, _, _, err := pageBothNeighborsFromVisitor(g, req, cursor, b, visitor)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid on cursor overshoot, got %v", err)
	}
	if visitor.visited != 1 {
		t.Fatalf("expected early exit after first overshoot edge (visited=1), got %d", visitor.visited)
	}

	// Malformed non-legacy cursor rejected before visiting any edge.
	badCursor := cursorState{Version: 1, After: "not-an-edge-cursor", Query: cursorQueryHash(req)}
	visitor.visited = 0
	_, _, _, err = pageBothNeighborsFromVisitor(g, req, badCursor, b, visitor)
	if !errors.Is(err, ErrInvalid) || visitor.visited != 0 {
		t.Fatalf("expected immediate ErrInvalid with 0 visits for malformed cursor, got err=%v visited=%d", err, visitor.visited)
	}
}

func TestRound7SupportsLazyReadPatternImpactValidatesSteps(t *testing.T) {
	req := Request{
		Op:                "pattern",
		Kind:              "service",
		DirectionStrategy: "impact",
		Path: PathFilter{
			Steps: []PathStep{
				{RelationTypes: []string{"calls"}},
			},
		},
	}
	// Both forward and reverse edge indexes exist, but calls lacks ImpactDirection -> must return false.
	missingImpact := PlannerStats{
		Version:                   1,
		EntityPages:               []PlannerEntityPageStat{{Shard: "00", EntityCount: 1}},
		ForwardEdgeIndexAvailable: true,
		ReverseEdgeIndexAvailable: true,
		EdgeShards:                []PlannerEdgeStat{{RelationType: "calls", Shard: "00", EdgeCount: 1}},
		ReverseEdgeShards:         []PlannerEdgeStat{{RelationType: "calls", Shard: "00", EdgeCount: 1}},
	}
	if SupportsLazyRead(req, missingImpact) {
		t.Fatalf("expected SupportsLazyRead(pattern, impact) to be false when ImpactDirection is missing")
	}

	// Forward-only impact relation with only forward edge index -> must return true.
	forwardOnlyImpact := PlannerStats{
		Version:                   1,
		EntityPages:               []PlannerEntityPageStat{{Shard: "00", EntityCount: 1}},
		ForwardEdgeIndexAvailable: true,
		EdgeShards:                []PlannerEdgeStat{{RelationType: "calls", ImpactDirection: "forward", Shard: "00", EdgeCount: 1}},
	}
	if !SupportsLazyRead(req, forwardOnlyImpact) {
		t.Fatalf("expected SupportsLazyRead(pattern, impact) to be true for forward-only impact relation with forward index")
	}
}

func TestRound7PathIdentityOrderMatchesDFSForPrefixOverlappingEdgeIDs(t *testing.T) {
	// Verify compareResults orders prefix-overlapping edge IDs ("e1", "e1>e8", "e1>e9", "e10")
	// identically to DFS emission order (whereas string-comparing '>' puts "e10" before "e1>e8").
	mkPath := func(edges []string) Result {
		edgeList := make([]graph.Edge, len(edges))
		entList := make([]graph.Entity, len(edges)+1)
		entList[0] = graph.Entity{ID: "a", Kind: "host"}
		for i, id := range edges {
			edgeList[i] = graph.Edge{ID: id, Type: "links"}
			entList[i+1] = graph.Entity{ID: "n-" + id, Kind: "host"}
		}
		return Result{Path: &graph.Path{Entities: entList, Edges: edgeList}}
	}
	results := []Result{
		mkPath([]string{"e10"}),
		mkPath([]string{"e1", "e9"}),
		mkPath([]string{"e1"}),
		mkPath([]string{"e1", "e8"}),
	}
	sortResults(results, nil)
	got := make([]string, len(results))
	for i, r := range results {
		got[i] = legacyPathIdentity(*r.Path)
	}
	want := []string{"path:e1", "path:e1>e8", "path:e1>e9", "path:e10"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted path identity order = %v, want %v", got, want)
	}
}

func TestRound7PatternEnforcesEntityPredicate(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "c1",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "service", ToKind: "service", Directed: true}},
			UpsertEntities: []graph.Entity{
				{ID: "s1", Kind: "service"},
				{ID: "s2", Kind: "service"},
				{ID: "s3", Kind: "service"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "e1", Type: "calls", From: "s1", To: "s3"},
				{ID: "e2", Type: "calls", From: "s2", To: "s3"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	req := Request{
		Op:   "pattern",
		Kind: "service",
		Path: PathFilter{Steps: []PathStep{{Direction: "out", RelationTypes: []string{"calls"}}}},
	}
	resp, err := ExecuteContextWithOptions(context.Background(), g, req, ExecuteOptions{
		EntityPredicate: func(e graph.Entity) bool { return e.ID == "s2" || e.ID == "s3" },
	})
	if err != nil {
		t.Fatalf("ExecuteWithOptions failed: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Path.Entities[0].ID != "s2" {
		t.Fatalf("expected only start node s2 allowed by EntityPredicate, got %+v", resp.Results)
	}
}

func TestRound7AggregateMatchRespectsShardCursorOrder(t *testing.T) {
	g := graph.New()
	entities := []graph.Entity{
		{ID: "h-a", Kind: "host", Fields: graph.Fields{"env": "prod"}},
		{ID: "h-b", Kind: "host", Fields: graph.Fields{"env": "prod"}},
		{ID: "h-c", Kind: "host", Fields: graph.Fields{"env": "prod"}},
		{ID: "h-d", Kind: "host", Fields: graph.Fields{"env": "prod"}},
	}
	if err := g.ApplyCommit(graph.Commit{
		ID:      "c1",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{{
				Name: "host",
				Fields: map[string]graph.FieldSpec{
					"env": {Type: "string", Indexed: true},
				},
			}},
			UpsertEntities: entities,
		},
	}); err != nil {
		t.Fatal(err)
	}

	ordered := orderEntities(entities, EntityPageOrderShard)
	req := Request{
		Op:        "match",
		Kind:      "host",
		Where:     []Filter{{Field: "env", Op: "eq", Value: "prod"}},
		Aggregate: []Aggregation{{Name: "cnt", Op: "count"}},
		Limit:     1,
	}
	cursor := encodeCursor(cursorState{
		Version: 1,
		After:   "entity:" + ordered[0].ID,
		Order:   EntityPageOrderShard,
		Query:   cursorQueryHash(req),
	})
	req.Cursor = cursor

	resp, err := Execute(g, req)
	if err != nil {
		t.Fatalf("Execute with shard cursor failed: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Entity.ID != ordered[1].ID {
		t.Fatalf("expected next entity in shard order %q, got %+v", ordered[1].ID, resp.Results)
	}
}
