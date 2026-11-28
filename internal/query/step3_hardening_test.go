package query

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type stubProjectedEntityLookup struct {
	entities map[string]graph.Entity
	lastProj [][]string
}

func (s *stubProjectedEntityLookup) GetEntity(_ context.Context, id string, fields []string) (graph.Entity, bool, error) {
	s.lastProj = append(s.lastProj, append([]string(nil), fields...))
	e, ok := s.entities[id]
	if !ok {
		return graph.Entity{}, false, nil
	}
	if len(fields) == 0 {
		return graph.CopyEntity(e), true, nil
	}
	out := graph.CopyEntity(e)
	filtered := graph.Fields{}
	for _, f := range fields {
		if v, exists := e.Fields[f]; exists {
			filtered[f] = v
		}
	}
	out.Fields = filtered
	return out, true, nil
}

func (s *stubProjectedEntityLookup) VisitEntities(_ context.Context, kind string, fields []string, afterID string, visit func(graph.Entity) (bool, error)) (bool, error) {
	s.lastProj = append(s.lastProj, append([]string(nil), fields...))
	for _, id := range []string{"ci:1", "ci:2"} {
		if id <= afterID {
			continue
		}
		e, ok, _ := s.GetEntity(context.Background(), id, fields)
		if !ok || (kind != "" && e.Kind != kind) {
			continue
		}
		cont, err := visit(e)
		if err != nil || !cont {
			return true, err
		}
	}
	return true, nil
}

func TestLazyProjectionDisabledWhenEntityPredicatePresent(t *testing.T) {
	lookup := &stubProjectedEntityLookup{
		entities: map[string]graph.Entity{
			"ci:1": {ID: "ci:1", Kind: "host", Fields: graph.Fields{"name": "public", "secret_tag": "allow"}},
			"ci:2": {ID: "ci:2", Kind: "host", Fields: graph.Fields{"name": "hidden", "secret_tag": "deny"}},
		},
	}
	skeleton := graph.New()
	skeleton.Version = 1

	resp, err := ExecuteContextWithOptions(context.Background(), skeleton, Request{
		Op:      "match",
		Kind:    "host",
		Project: []string{"name"},
	}, ExecuteOptions{
		EntityLookup: lookup,
		PlannerStats: PlannerStats{Version: 1, EntityScanAvailable: true},
		EntityPredicate: func(e graph.Entity) bool {
			return e.Fields["secret_tag"] == "allow"
		},
	})
	if err != nil {
		t.Fatalf("ExecuteWithOptions: %v", err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Entity == nil || resp.Results[0].Entity.ID != "ci:1" {
		t.Fatalf("expected only ci:1 to pass EntityPredicate, got %+v", resp.Results)
	}
	for _, proj := range lookup.lastProj {
		if len(proj) != 0 {
			t.Fatalf("expected full materialization (nil fields) when EntityPredicate is active, got %v", proj)
		}
	}
}

func TestEstimateFanoutSupportsGlobalUnshardedAndAsymmetricBoth(t *testing.T) {
	g := graph.New()
	g.Version = 5
	stats := PlannerStats{
		Version:                   5,
		ForwardEdgeIndexAvailable: true,
		ReverseEdgeIndexAvailable: true,
		EntityPages:               []PlannerEntityPageStat{{Shard: "", EntityCount: 100}},
		EdgeShards:                []PlannerEdgeStat{{RelationType: "runs_on", Shard: "", EdgeCount: 500}},
	}
	fanout, ok := estimateFanoutFromStats(g, Request{Op: "neighbors", ID: "ci:1", Direction: "both"}, stats)
	if !ok {
		t.Fatal("expected asymmetric both with global unsharded stats to succeed")
	}
	if fanout != 5 {
		t.Fatalf("expected average degree 500/100=5, got %d", fanout)
	}
}

func TestNeighborsAliasPaginationPreservesCursorHash(t *testing.T) {
	g := graph.New()
	now := time.Now().UTC()
	if err := g.ApplyCommit(graph.Commit{
		Version:   1,
		CreatedAt: now,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host"}},
			UpsertEntities: []graph.Entity{
				{ID: "ci:root", Kind: "host"},
				{ID: "ci:alias", Kind: "host"},
				{ID: "ci:n1", Kind: "host"},
				{ID: "ci:n2", Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "e:1", Type: "links", From: "ci:root", To: "ci:n1"},
				{ID: "e:2", Type: "links", From: "ci:root", To: "ci:n2"},
			},
			MergeEntities: []graph.MergeRequest{{TargetID: "ci:root", SourceIDs: []string{"ci:alias"}}},
		},
	}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	aliasLookup := &stubProjectedEntityLookup{
		entities: map[string]graph.Entity{
			"ci:alias": {ID: "ci:root", Kind: "host"},
		},
	}
	page1, err := ExecuteContextWithOptions(context.Background(), g, Request{Op: "neighbors", ID: "ci:alias", Direction: "out", Limit: 1}, ExecuteOptions{EntityLookup: aliasLookup})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Results) != 1 || page1.NextCursor == "" {
		t.Fatalf("expected 1 result and next cursor, got len=%d cursor=%q", len(page1.Results), page1.NextCursor)
	}
	page2, err := ExecuteContextWithOptions(context.Background(), g, Request{Op: "neighbors", ID: "ci:alias", Direction: "out", Limit: 1, Cursor: page1.NextCursor}, ExecuteOptions{EntityLookup: aliasLookup})
	if err != nil {
		t.Fatalf("page 2 with alias cursor failed: %v", err)
	}
	if len(page2.Results) != 1 || page2.Results[0].Entity.ID == page1.Results[0].Entity.ID {
		t.Fatalf("expected second distinct neighbor on page 2, got %+v", page2.Results)
	}
}

func TestValuesEqualRecursiveAndGQLRangeError(t *testing.T) {
	if !valuesEqual([]any{int64(80), "http"}, []any{float64(80), "http"}) {
		t.Fatal("expected recursive valuesEqual to match int64 and float64 inside slice")
	}
	if _, err := tokenizeGQL("MATCH host WHERE x = 1e999"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid for overflowing float literal, got %v", err)
	}
	req1 := Request{Op: "match", Kind: "host", MinVersion: 1, AllowStale: false}
	req2 := Request{Op: "match", Kind: "host", MinVersion: 9, AllowStale: true}
	if cursorQueryHash(req1) != cursorQueryHash(req2) {
		t.Fatal("cursorQueryHash must ignore MinVersion and AllowStale")
	}
}

func TestParseGQLRejectsLargeORChainWithoutQuadraticAmplification(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("FIND host WHERE a=1")
	for sb.Len() < 60*1024 {
		sb.WriteString(" OR a=1")
	}
	start := time.Now()
	if _, err := ParseGQL(sb.String()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid for oversized OR chain, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("ParseGQL took too long on chained OR query: %v", elapsed)
	}
}
