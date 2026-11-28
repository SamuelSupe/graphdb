package query

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestRound5SteppedTraverseStopsAtStepLength(t *testing.T) {
	g := graph.New()
	now := time.Now().UTC()
	var entities []graph.Entity
	var edges []graph.Edge
	entities = append(entities,
		graph.Entity{ID: "n:start", Kind: "host"},
		graph.Entity{ID: "n:mid", Kind: "service"},
		graph.Entity{ID: "n:end", Kind: "db"},
	)
	edges = append(edges,
		graph.Edge{Type: "depends_on", From: "n:start", To: "n:mid"},
		graph.Edge{Type: "depends_on", From: "n:mid", To: "n:end"},
	)
	for i := 0; i < 40; i++ {
		a := fmt.Sprintf("n:extra-a-%d", i)
		b := fmt.Sprintf("n:extra-b-%d", i)
		entities = append(entities, graph.Entity{ID: a, Kind: "host"}, graph.Entity{ID: b, Kind: "host"})
		edges = append(edges,
			graph.Edge{Type: "depends_on", From: "n:end", To: a},
			graph.Edge{Type: "depends_on", From: a, To: b},
		)
	}
	if err := g.ApplyCommit(graph.Commit{
		Version:   1,
		CreatedAt: now,
		Mutations: graph.Mutations{UpsertEntities: entities, UpsertEdges: edges},
	}); err != nil {
		t.Fatal(err)
	}

	resp, err := Execute(g, Request{
		Op:    "traverse",
		ID:    "n:start",
		Depth: 5,
		Path: PathFilter{
			Steps: []PathStep{
				{NodeKinds: []string{"service"}},
				{NodeKinds: []string{"db"}},
			},
		},
		Aggregate: []Aggregation{{Op: "count"}},
		CostLimit: 25,
	})
	if err != nil {
		t.Fatalf("stepped traverse expanded past len(Steps) and failed: %v", err)
	}
	if resp.Aggregates["count"] != 1 {
		t.Fatalf("count = %v, want 1", resp.Aggregates["count"])
	}
}

type round5EmptyEntityLookup struct{}

func (round5EmptyEntityLookup) GetEntity(context.Context, string, []string) (graph.Entity, bool, error) {
	return graph.Entity{}, false, nil
}
func (round5EmptyEntityLookup) GetEntities(context.Context, []string, []string) (map[string]graph.Entity, error) {
	return map[string]graph.Entity{}, nil
}
func (round5EmptyEntityLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}
func (round5EmptyEntityLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func TestRound5MissingEntityLazyReturnsFallbackAndAuthoritativeStreamSucceeds(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := round5EmptyEntityLookup{}
	opts := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:             1,
			EntityScanAvailable: true,
			EntityPages:         []PlannerEntityPageStat{{Shard: "00", EntityCount: 1}},
		},
		IndexLookup:               lookup,
		EntityLookup:              lookup,
		AuthoritativeEntityLookup: false,
	}
	_, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:    "match",
		Where: []Filter{{Field: "id", Op: "eq", Value: "missing"}},
	}, opts)
	if !errors.Is(err, ErrMaterializedFallback) || !errors.Is(err, ErrIndexUnavailable) {
		t.Fatalf("err = %v, want ErrMaterializedFallback (wrapping ErrIndexUnavailable)", err)
	}

	authOpts := opts
	authOpts.AuthoritativeEntityLookup = true
	results := 0
	ok, err := StreamContextWithOptions(context.Background(), g, Request{
		Op:    "match",
		Where: []Filter{{Field: "id", Op: "eq", Value: "missing"}},
	}, authOpts, func(item any) error {
		if _, isResult := item.(Result); isResult {
			results++
		}
		return nil
	})
	if err != nil || !ok || results != 0 {
		t.Fatalf("authoritative stream miss = (ok=%v, results=%d, err=%v), want (true, 0, nil)", ok, results, err)
	}
}

func TestRound5EntityPredicateEnforcedAcrossMaterializedMatch(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		Version:   1,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "host:1", Kind: "host", Fields: graph.Fields{"tier": "prod"}},
				{ID: "host:2", Kind: "host", Fields: graph.Fields{"tier": "dev"}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	opts := ExecuteOptions{
		EntityPredicate: func(e graph.Entity) bool { return e.ID == "host:1" },
	}
	resp, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:   "match",
		Kind: "host",
	}, opts)
	if err != nil {
		t.Fatalf("materialized match with EntityPredicate failed: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Entity.ID != "host:1" {
		t.Fatalf("EntityPredicate not enforced on materialized match: got %+v", resp.Results)
	}

	aggResp, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:        "match",
		Kind:      "host",
		Aggregate: []Aggregation{{Op: "count"}},
	}, opts)
	if err != nil {
		t.Fatalf("bounded materialized match with EntityPredicate failed: %v", err)
	}
	if aggResp.Aggregates["count"] != 1 {
		t.Fatalf("EntityPredicate not enforced on bounded aggregate: count=%v, want 1", aggResp.Aggregates["count"])
	}
}
