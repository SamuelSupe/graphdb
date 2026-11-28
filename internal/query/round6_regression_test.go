package query

import (
	"context"
	"errors"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type round6StubIndexLookup struct{}

func (round6StubIndexLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (round6StubIndexLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

type round6StubEntityLookup struct {
	entities map[string]graph.Entity
}

func (l round6StubEntityLookup) GetEntity(_ context.Context, id string, _ []string) (graph.Entity, bool, error) {
	e, ok := l.entities[id]
	return e, ok, nil
}

func (l round6StubEntityLookup) VisitEntities(_ context.Context, kind string, _ []string, after string, visit func(graph.Entity) (bool, error)) (bool, error) {
	for _, e := range l.entities {
		if kind != "" && e.Kind != kind {
			continue
		}
		if e.ID <= after {
			continue
		}
		more, err := visit(e)
		if err != nil || !more {
			return true, err
		}
	}
	return true, nil
}

func TestRound6StreamCursorOrderMismatchReturnsMaterializedFallback(t *testing.T) {
	g := &graph.Graph{Version: 1}
	req := Request{Op: "match", Kind: "host", Limit: 1}
	req.Cursor = encodeCursor(cursorState{
		Version: g.Version,
		After:   "h1",
		Order:   EntityPageOrderShard,
		Query:   cursorQueryHash(req),
	})
	stats := PlannerStats{
		Version:             g.Version,
		EntityScanAvailable: true,
		EntityPages:         []PlannerEntityPageStat{{Shard: "00", EntityCount: 1}},
	}
	lookup := round6StubEntityLookup{entities: map[string]graph.Entity{
		"h1": {ID: "h1", Kind: "host", Fields: graph.Fields{"name": "alpha"}},
	}}
	handled, err := StreamContextWithOptions(context.Background(), g, req, ExecuteOptions{
		PlannerStats:              stats,
		IndexLookup:               round6StubIndexLookup{},
		EntityLookup:              lookup,
		AuthoritativeEntityLookup: true,
	}, func(any) error { return nil })
	if !handled {
		t.Fatalf("expected StreamContextWithOptions to handle match request")
	}
	if !errors.Is(err, ErrMaterializedFallback) {
		t.Fatalf("expected ErrMaterializedFallback on cursor order mismatch, got %v", err)
	}
}

func TestRound6SupportsLazyReadImpactStrategyRequiresImpactIndexes(t *testing.T) {
	forwardOnlyStats := PlannerStats{
		Version:                   1,
		EntityPages:               []PlannerEntityPageStat{{Shard: "00", EntityCount: 2}},
		ForwardEdgeIndexAvailable: true,
		EdgeShards: []PlannerEdgeStat{
			{RelationType: "depends_on", ImpactDirection: "reverse", Shard: "00", EdgeCount: 1},
		},
	}
	bothStats := forwardOnlyStats
	bothStats.ReverseEdgeIndexAvailable = true
	bothStats.ReverseEdgeShards = []PlannerEdgeStat{
		{RelationType: "depends_on", ImpactDirection: "reverse", Shard: "00", EdgeCount: 1},
	}

	cases := []Request{
		{Op: "traverse", ID: "svc-1", DirectionStrategy: "impact", Depth: 2},
		{Op: "shortest_path", ID: "svc-1", TargetID: "svc-2", DirectionStrategy: "impact", Depth: 2},
		{Op: "neighbors", ID: "svc-1", DirectionStrategy: "impact"},
	}
	for _, req := range cases {
		if SupportsLazyRead(req, forwardOnlyStats) {
			t.Fatalf("expected SupportsLazyRead(%s, impact) to be false without reverse edge index", req.Op)
		}
		if !SupportsLazyRead(req, bothStats) {
			t.Fatalf("expected SupportsLazyRead(%s, impact) to be true with required edge indexes", req.Op)
		}
	}
}

func TestRound6ShortestPathBFSClampsSteppedDepth(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "c1",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{
				{Name: "links", FromKind: "host", ToKind: "host", Directed: true},
			},
			UpsertEntities: []graph.Entity{
				{ID: "n1", Kind: "host"},
				{ID: "n2", Kind: "host"},
				{ID: "n3", Kind: "host"},
				{ID: "n4", Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "e1", Type: "links", From: "n1", To: "n2"},
				{ID: "e2", Type: "links", From: "n2", To: "n3"},
				{ID: "e3", Type: "links", From: "n3", To: "n4"},
			},
		},
	}); err != nil {
		t.Fatalf("ApplyCommit failed: %v", err)
	}
	steps := []PathStep{
		{Direction: "out", RelationTypes: []string{"links"}},
		{Direction: "out", RelationTypes: []string{"links"}},
	}

	// Depth < len(Path.Steps): should short-circuit immediately with 0 visited.
	shortReq := Request{
		Op:       "shortest_path",
		ID:       "n1",
		TargetID: "n3",
		Depth:    1,
		Path:     PathFilter{Steps: steps},
	}
	shortBudget, cancelShort := newBudget(context.Background(), shortReq, nil, nil, nil, PlannerStats{})
	defer cancelShort()
	start, _ := g.Entities.Get("n1")
	target, _ := g.Entities.Get("n3")
	if _, found, err := shortestPathBFS(g, start, target, shortReq, shortBudget); err != nil || found {
		t.Fatalf("expected not found for depth < len(steps), got found=%v err=%v", found, err)
	}
	if shortBudget.visited != 0 {
		t.Fatalf("expected 0 visited on short-circuit, got %d", shortBudget.visited)
	}

	// Depth > len(Path.Steps) targeting unreachable n4 (3 hops away, steps only 2):
	// BFS must clamp depth to 2 and never expand from n3 -> n4.
	deepReq := Request{
		Op:       "shortest_path",
		ID:       "n1",
		TargetID: "n4",
		Depth:    6,
		Path:     PathFilter{Steps: steps},
	}
	deepBudget, cancelDeep := newBudget(context.Background(), deepReq, nil, nil, nil, PlannerStats{})
	defer cancelDeep()
	target4, _ := g.Entities.Get("n4")
	if _, found, err := shortestPathBFS(g, start, target4, deepReq, deepBudget); err != nil || found {
		t.Fatalf("expected not found for 3-hop target with 2 steps, got found=%v err=%v", found, err)
	}
	// Only n1->n2 (depth 1) and n2->n3 (depth 2) should be visited; n3->n4 must not be expanded.
	if deepBudget.visited != 2 {
		t.Fatalf("expected clamped BFS to visit exactly 2 neighbors (not expand depth >= 2), got %d", deepBudget.visited)
	}
}

func TestRound6GQLQuotedStringNotMatchedAsKeyword(t *testing.T) {
	req, err := ParseGQL(`FIND host WHERE "EXISTS" = "AND" OR name = "END" LIMIT 5`)
	if err != nil {
		t.Fatalf("ParseGQL failed on quoted keyword tokens: %v", err)
	}
	if req.WhereExpr == nil || req.WhereExpr.Op != "or" || len(req.WhereExpr.Children) != 2 {
		t.Fatalf("unexpected WhereExpr: %+v", req.WhereExpr)
	}
	left := req.WhereExpr.Children[0]
	if left.Field != "EXISTS" || left.Op != "eq" || left.Value != "AND" {
		t.Fatalf("unexpected left expr: %+v", left)
	}
	right := req.WhereExpr.Children[1]
	if right.Field != "name" || right.Op != "eq" || right.Value != "END" {
		t.Fatalf("unexpected right expr: %+v", right)
	}
}
