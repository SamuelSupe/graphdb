package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestExecutorUsesIDLookupPlan(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:      "match",
		Where:   []Filter{{Field: "id", Op: "eq", Value: "service:api"}},
		Profile: true,
	})
	if err != nil {
		t.Fatalf("id lookup: %v", err)
	}
	if response.Plan == nil || response.Plan.Strategy != "entity-id" {
		t.Fatalf("plan = %#v", response.Plan)
	}
	if response.Stats.Scanned != 1 || len(response.Results) != 1 {
		t.Fatalf("response = %#v", response)
	}
}

func TestPlannerUsesPersistedIndexStats(t *testing.T) {
	g := seedCMDBGraph(t)
	plan := PlanQueryWithStats(g, Request{
		Op:   "match",
		Kind: "host",
		Where: []Filter{
			{Field: "region", Op: "eq", Value: "us-east-1"},
			{Field: "hostname", Op: "eq", Value: "app-01"},
		},
	}, PlannerStats{
		Version: g.Version,
		Indexes: []PlannerIndexStat{
			{Kind: "host", Field: "region", Status: "ready", EntryCount: 3, DistinctValues: 2, TopValues: []PlannerValueStat{{Value: "s:us-east-1", Count: 2}}},
			{Kind: "host", Field: "hostname", Status: "ready", EntryCount: 3, DistinctValues: 3},
		},
	})
	if plan.Strategy != "field-index" || plan.IndexField != "hostname" || plan.StatsSource != "persisted-catalog" {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestPlannerJSONNumberStatsUseIndexCanonicalValue(t *testing.T) {
	g := graph.New()
	g.Version = 9
	plan := PlanQueryWithStats(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "cpu", Op: "eq", Value: json.Number("16.0")}},
	}, PlannerStats{
		Version: g.Version,
		Indexes: []PlannerIndexStat{{
			Kind:           "host",
			Field:          "cpu",
			Status:         "ready",
			EntryCount:     1000,
			DistinctValues: 100,
			TopValues:      []PlannerValueStat{{Value: "n:16", Count: 700}},
		}},
	})
	if plan.Strategy != "field-index" || plan.EstimatedRows != 700 || plan.EstimatedCost != 700 {
		t.Fatalf("plan = %#v, want json.Number to use canonical n:16 top-value stats", plan)
	}
}

func TestProfileIncludesOperators(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:      "match",
		Kind:    "host",
		Where:   []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Profile: true,
	})
	if err != nil {
		t.Fatalf("profile: %v", err)
	}
	if len(response.Profile) == 0 || !hasOperator(response.Profile, "index-seek") || !hasOperator(response.Profile, "filter-project") {
		t.Fatalf("profile = %#v", response.Profile)
	}
}

func TestMaterializedMatchWithoutSortStopsAfterPageBoundary(t *testing.T) {
	g := seedPruneGraph(t, 50)
	response, err := Execute(g, Request{Op: "match", Kind: "host", Limit: 5})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(response.Results) != 5 || response.NextCursor == "" {
		t.Fatalf("response = %#v", response)
	}
	if response.Stats.Scanned != 6 {
		t.Fatalf("scanned = %d, want limit+1", response.Stats.Scanned)
	}
}

func TestNeighborsWithoutSortStopsAfterPageBoundary(t *testing.T) {
	g := seedPruneGraph(t, 50)
	response, err := Execute(g, Request{Op: "neighbors", ID: "service:start", Direction: "out", Limit: 5})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 5 || response.NextCursor == "" {
		t.Fatalf("response = %#v", response)
	}
	if response.Stats.Visited != 6 {
		t.Fatalf("visited = %d, want limit+1", response.Stats.Visited)
	}
	next, err := Execute(g, Request{Op: "neighbors", ID: "service:start", Direction: "out", Limit: 5, Cursor: response.NextCursor})
	if err != nil {
		t.Fatalf("neighbors page 2: %v", err)
	}
	if len(next.Results) != 5 {
		t.Fatalf("next page = %#v", next.Results)
	}
	if next.Results[0].Edge.ID == response.Results[0].Edge.ID {
		t.Fatalf("cursor repeated first page edge %q", next.Results[0].Edge.ID)
	}
}

func TestNeighborsCursorUsesEdgeIdentityWhenSameEntityHasMultipleEdges(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "multi-edge-neighbor",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{
				{Name: "depends_on", FromKind: "service", ToKind: "host", Directed: true, Cardinality: graph.ManyToMany},
				{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true, Cardinality: graph.ManyToMany},
				{Name: "observes", FromKind: "service", ToKind: "host", Directed: true, Cardinality: graph.ManyToMany},
			},
			UpsertEntities: []graph.Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "host:shared", Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "collector-edge-1", Type: "depends_on", From: "service:api", To: "host:shared"},
				{ID: "collector-edge-2", Type: "runs_on", From: "service:api", To: "host:shared"},
				{ID: "collector-edge-3", Type: "observes", From: "service:api", To: "host:shared"},
			},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	seen := map[string]struct{}{}
	cursor := ""
	for page := 0; page < 3; page++ {
		response, err := Execute(g, Request{Op: "neighbors", ID: "service:api", Direction: "out", Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", page+1, err)
		}
		if len(response.Results) != 1 {
			t.Fatalf("page %d results = %#v, want one edge", page+1, response.Results)
		}
		edgeID := response.Results[0].Edge.ID
		if _, ok := seen[edgeID]; ok {
			t.Fatalf("page %d repeated edge %q", page+1, edgeID)
		}
		seen[edgeID] = struct{}{}
		cursor = response.NextCursor
		if page < 2 && cursor == "" {
			t.Fatalf("page %d missing cursor", page+1)
		}
	}
	if cursor != "" {
		t.Fatalf("last cursor = %q, want empty", cursor)
	}
}

func TestStableCursorRejectsDifferentEarlyPageQuery(t *testing.T) {
	g := seedGraph(t)
	first, err := Execute(g, Request{Op: "match", Kind: "person", Limit: 1})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("expected cursor")
	}
	_, err = Execute(g, Request{Op: "match", Kind: "company", Limit: 1, Cursor: first.NextCursor})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func TestStableCursorRejectsDifferentSortedQuery(t *testing.T) {
	g := seedGraph(t)
	first, err := Execute(g, Request{
		Op:    "match",
		Kind:  "person",
		Sort:  []SortSpec{{Field: "name"}},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("first sorted page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("expected cursor")
	}
	_, err = Execute(g, Request{
		Op:     "match",
		Kind:   "company",
		Sort:   []SortSpec{{Field: "name"}},
		Limit:  1,
		Cursor: first.NextCursor,
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func TestStableCursorRejectsChangedFilterWithSameResultSetAnchor(t *testing.T) {
	g := seedPruneGraph(t, 3)
	first, err := Execute(g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "id", Op: "prefix", Value: "host:"}},
		Sort:  []SortSpec{{Field: "id"}},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("expected cursor")
	}
	_, err = Execute(g, Request{
		Op:     "match",
		Kind:   "host",
		Sort:   []SortSpec{{Field: "id"}},
		Limit:  1,
		Cursor: first.NextCursor,
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func TestBlockingSortAddsCandidateCost(t *testing.T) {
	g := seedPruneGraph(t, 50)
	plan := PlanQuery(g, Request{Op: "match", Kind: "host", Sort: []SortSpec{{Field: "id"}}})
	if plan.EstimatedRows != len(g.Entities) {
		t.Fatalf("estimated rows = %d", plan.EstimatedRows)
	}
	if plan.EstimatedCost <= plan.EstimatedRows {
		t.Fatalf("estimated cost = %d, want sort cost included", plan.EstimatedCost)
	}
	if plan.Steps[len(plan.Steps)-1].Name != "sort" || plan.Steps[len(plan.Steps)-1].Cost != len(g.Entities) {
		t.Fatalf("sort step = %#v", plan.Steps[len(plan.Steps)-1])
	}
}

func TestPlannerFanoutEstimateUsesAdjacencyCount(t *testing.T) {
	g := seedPruneGraph(t, 50)
	request := Request{
		Op:           "neighbors",
		ID:           "service:start",
		Direction:    "out",
		RelationType: "connects_to",
		Path:         PathFilter{NodeKinds: []string{"service", "database"}},
	}
	plan := PlanQuery(g, request)
	if plan.EstimatedRows != 1 || plan.EstimatedCost != 2 {
		t.Fatalf("plan = %#v, want one allowed neighbor plus start lookup", plan)
	}
	response, err := Execute(g, request)
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "service:api" {
		t.Fatalf("neighbors = %#v, want only node-kind allowed neighbor", response.Results)
	}
}

func TestPlannerFanoutEstimateUsesPersistedEdgeShardStats(t *testing.T) {
	g := graph.New()
	g.Version = 7
	stats := PlannerStats{
		Version:    7,
		EdgeShards: []PlannerEdgeStat{{RelationType: "runs_on", Shard: plannerEdgeShardID("service:api"), EdgeCount: 500}},
	}
	plan := PlanQueryWithStats(g, Request{
		Op:           "neighbors",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
	}, stats)
	if plan.EstimatedRows != 101 || plan.EstimatedCost != 102 {
		t.Fatalf("plan = %#v, want one page plus lookahead estimate", plan)
	}
}

func TestPlannerCapsPageableOutNeighborShardEstimate(t *testing.T) {
	g := graph.New()
	g.Version = 7
	plan := PlanQueryWithStats(g, Request{
		Op:           "neighbors",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
		Limit:        10,
	}, PlannerStats{
		Version: 7,
		EdgeShards: []PlannerEdgeStat{{
			RelationType: "runs_on",
			Shard:        plannerEdgeShardID("service:api"),
			EdgeCount:    1_000_000,
		}},
	})
	if plan.EstimatedRows != 11 || plan.EstimatedCost != 12 {
		t.Fatalf("plan = %#v, want page boundary cost 12", plan)
	}
}

func TestPlannerCapsPageableInNeighborShardEstimate(t *testing.T) {
	g := graph.New()
	g.Version = 7
	plan := PlanQueryWithStats(g, Request{
		Op:           "neighbors",
		ID:           "host:app",
		Direction:    "in",
		RelationType: "runs_on",
		Limit:        10,
	}, PlannerStats{
		Version: 7,
		ReverseEdgeShards: []PlannerEdgeStat{{
			RelationType: "runs_on",
			Shard:        plannerEdgeShardID("host:app"),
			EdgeCount:    1_000_000,
		}},
	})
	if plan.EstimatedRows != 11 || plan.EstimatedCost != 12 {
		t.Fatalf("plan = %#v, want page boundary cost 12", plan)
	}
}

func TestPlannerCapsPageableBothNeighborShardEstimate(t *testing.T) {
	g := graph.New()
	g.Version = 7
	plan := PlanQueryWithStats(g, Request{
		Op:           "neighbors",
		ID:           "service:api",
		Direction:    "both",
		RelationType: "links",
		Limit:        10,
	}, PlannerStats{
		Version: 7,
		EdgeShards: []PlannerEdgeStat{{
			RelationType: "links",
			Shard:        plannerEdgeShardID("service:api"),
			EdgeCount:    1_000_000,
		}},
		ReverseEdgeShards: []PlannerEdgeStat{{
			RelationType: "links",
			Shard:        plannerEdgeShardID("service:api"),
			EdgeCount:    1_000_000,
		}},
	})
	if plan.EstimatedRows != 11 || plan.EstimatedCost != 12 {
		t.Fatalf("plan = %#v, want page boundary cost 12", plan)
	}
}

func TestAdmissionRejectsLazyEdgeQueryFromPersistedShardStats(t *testing.T) {
	g := graph.New()
	g.Version = 7
	options := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:    7,
			EdgeShards: []PlannerEdgeStat{{RelationType: "runs_on", Shard: plannerEdgeShardID("service:api"), EdgeCount: 500}},
		},
	}
	_, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:           "neighbors",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
		CostLimit:    10,
	}, options)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("neighbors err = %v, want ErrLimitExceeded", err)
	}
	_, err = ExecuteContextWithOptions(context.Background(), g, Request{
		Op:           "traverse",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
		Depth:        2,
		CostLimit:    10,
	}, options)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("traverse err = %v, want ErrLimitExceeded", err)
	}
}

func TestPlannerUsesEachTraversalStepDirectionAndRelation(t *testing.T) {
	g := graph.New()
	g.Version = 7
	shard := plannerEdgeShardID("node:start")
	stats := PlannerStats{
		Version: 7,
		EdgeShards: []PlannerEdgeStat{{
			RelationType: "forward_link",
			Shard:        shard,
			EdgeCount:    1,
		}},
		ReverseEdgeShards: []PlannerEdgeStat{{
			RelationType: "reverse_link",
			Shard:        shard,
			EdgeCount:    1000,
		}},
	}
	request := Request{
		Op:        "shortest_path",
		ID:        "node:start",
		TargetID:  "node:target",
		Direction: "out",
		Depth:     2,
		Path: PathFilter{Steps: []PathStep{
			{
				Direction:     "out",
				RelationTypes: []string{"forward_link"},
			},
			{
				Direction:     "in",
				RelationTypes: []string{"reverse_link"},
			},
		}},
	}
	plan := PlanQueryWithStats(g, request, stats)
	if plan.EstimatedRows != 1001 || plan.EstimatedCost != 1001 {
		t.Fatalf(
			"plan rows/cost = %d/%d, want 1001/1001",
			plan.EstimatedRows, plan.EstimatedCost,
		)
	}
	request.CostLimit = 100
	_, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		request,
		ExecuteOptions{PlannerStats: stats},
	)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("admission err = %v, want ErrLimitExceeded", err)
	}
}

func TestPlannerImpactEstimateUsesRelationDirection(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "impact-plan",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "team:platform", Kind: "team"},
			},
			UpsertEdges: []graph.Edge{{
				ID:   "edge:api-owner",
				Type: "owned_by",
				From: "service:api",
				To:   "team:platform",
			}},
		},
	}); err != nil {
		t.Fatalf("seed impact plan graph: %v", err)
	}
	plan := PlanQuery(g, Request{Op: "impact", ID: "service:api", Direction: "both", Depth: 1})
	if plan.EstimatedRows != 1 || plan.EstimatedCost != 1 {
		t.Fatalf("plan = %#v, want impact cost clamped to 1 for no-impact relation", plan)
	}
}

func TestAdmissionRejectsEstimatedOverLimit(t *testing.T) {
	g := seedCMDBGraph(t)
	_, err := Execute(g, Request{Op: "match", CostLimit: 2})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestNeighborCostLimitCountsFilteredOutEdges(t *testing.T) {
	g := seedFilteredOutEdgeGraph(t, 20)
	_, err := Execute(g, Request{
		Op:           "neighbors",
		ID:           "service:start",
		Direction:    "out",
		RelationType: "wanted",
		CostLimit:    3,
	})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("neighbors err = %v, want ErrLimitExceeded", err)
	}
}

func hasOperator(operators []OperatorStat, name string) bool {
	for _, operator := range operators {
		if operator.Name == name {
			return true
		}
	}
	return false
}

func TestPathPruningKeepsTraversalWithinBudget(t *testing.T) {
	g := seedPruneGraph(t, 50)
	response, err := Execute(g, Request{
		Op:        "traverse",
		ID:        "service:start",
		Direction: "out",
		Depth:     2,
		Path:      PathFilter{NodeKinds: []string{"service", "database"}, EndKind: "database"},
		CostLimit: 60,
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("pruned traverse: %v", err)
	}
	if len(response.Results) != 1 {
		t.Fatalf("results = %#v", response.Results)
	}
	if response.Stats.Visited > 60 {
		t.Fatalf("visited = %d, pruning did not cap expansion", response.Stats.Visited)
	}
}

func seedPruneGraph(t *testing.T, hostBranches int) *graph.Graph {
	t.Helper()
	g := graph.New()
	entities := []graph.Entity{
		{ID: "service:start", Kind: "service"},
		{ID: "service:api", Kind: "service"},
		{ID: "db:main", Kind: "database"},
	}
	edges := []graph.Edge{{ID: "edge:start-api", Type: "connects_to", From: "service:start", To: "service:api"}}
	edges = append(edges, graph.Edge{ID: "edge:api-db", Type: "connects_to", From: "service:api", To: "db:main"})
	for i := 0; i < hostBranches; i++ {
		hostID := fmt.Sprintf("host:%03d", i)
		entities = append(entities, graph.Entity{ID: hostID, Kind: "host"})
		edges = append(edges,
			graph.Edge{ID: fmt.Sprintf("edge:start-host-%03d", i), Type: "connects_to", From: "service:start", To: hostID},
			graph.Edge{ID: fmt.Sprintf("edge:host-db-%03d", i), Type: "connects_to", From: hostID, To: "db:main"},
		)
	}
	err := g.ApplyCommit(graph.Commit{
		ID:      "prune",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: entities,
			UpsertEdges:    edges,
		},
	})
	if err != nil {
		t.Fatalf("seed prune graph: %v", err)
	}
	return g
}

func seedFilteredOutEdgeGraph(t *testing.T, ignoredEdges int) *graph.Graph {
	t.Helper()
	g := graph.New()
	entities := []graph.Entity{{ID: "service:start", Kind: "service"}}
	edges := make([]graph.Edge, 0, ignoredEdges)
	for i := 0; i < ignoredEdges; i++ {
		hostID := fmt.Sprintf("host:ignored-%03d", i)
		entities = append(entities, graph.Entity{ID: hostID, Kind: "host"})
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:ignored-%03d", i),
			Type: "ignored",
			From: "service:start",
			To:   hostID,
		})
	}
	if err := g.ApplyCommit(graph.Commit{
		ID:      "filtered-out-edges",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{
				{Name: "ignored", FromKind: "service", ToKind: "host", Directed: true},
				{Name: "wanted", FromKind: "service", ToKind: "host", Directed: true},
			},
			UpsertEntities: entities,
			UpsertEdges:    edges,
		},
	}); err != nil {
		t.Fatalf("seed filtered-out graph: %v", err)
	}
	return g
}
