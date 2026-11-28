package query

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestParseCursorRejectsOffsetWithoutAfter(t *testing.T) {
	payload, _ := json.Marshal(cursorState{Version: 1, Offset: 10})
	raw := base64.RawURLEncoding.EncodeToString(payload)
	if _, err := parseCursor(raw); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid for offset without after, got %v", err)
	}
}

func TestMatchSortUsesSmallAndEmptyKindCardinality(t *testing.T) {
	g := graph.New()
	entities := []graph.Entity{{ID: "small:1", Kind: "small"}}
	for i := range 32 {
		entities = append(entities, graph.Entity{ID: fmt.Sprintf("large:%d", i), Kind: "large"})
	}
	if err := g.ApplyCommit(graph.Commit{ID: "seed", Version: 1, Mutations: graph.Mutations{UpsertEntities: entities}}); err != nil {
		t.Fatal(err)
	}
	for kind, want := range map[string]int{"small": 1, "empty": 0} {
		response, err := Execute(g, Request{
			Op: "match", Kind: kind, CostLimit: 8, Sort: []SortSpec{{Field: "id"}},
		})
		if err != nil || len(response.Results) != want {
			t.Fatalf("sorted match for %q: got %d results, %v", kind, len(response.Results), err)
		}
	}
}

func TestShortestPathEnforcesBudgetAndKeysEntitiesByID(t *testing.T) {
	g := graph.New()
	now := time.Unix(1700000000, 0).UTC()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "c1",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes:       []graph.CIType{{Name: "svc"}},
			UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKinds: []string{"svc"}, ToKinds: []string{"svc"}}},
			UpsertEntities: []graph.Entity{
				{ID: "svc:a", Kind: "svc"},
				{ID: "svc:b", Kind: "svc"},
				{ID: "svc:c", Kind: "svc"},
				{ID: "svc:d", Kind: "svc"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "e1", Type: "calls", From: "svc:a", To: "svc:b"},
				{ID: "e2", Type: "calls", From: "svc:b", To: "svc:c"},
				{ID: "e3", Type: "calls", From: "svc:c", To: "svc:d"},
			},
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	res, err := Execute(g, Request{
		Op:       "shortest_path",
		ID:       "svc:a",
		TargetID: "svc:d",
		Depth:    4,
	})
	if err != nil {
		t.Fatalf("shortest_path failed: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].Path == nil || len(res.Results[0].Path.Entities) != 4 {
		t.Fatalf("unexpected shortest path result: %+v", res.Results)
	}

	_, err = Execute(g, Request{
		Op:        "shortest_path",
		ID:        "svc:a",
		TargetID:  "svc:d",
		Depth:     4,
		CostLimit: 2,
	})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expected ErrLimitExceeded for tight CostLimit, got %v", err)
	}
}

func TestPatternEmptyKindAdmittedAndOversizedGroupKeyRejected(t *testing.T) {
	g := graph.New()
	now := time.Unix(1700000000, 0).UTC()
	var entities []graph.Entity
	var edges []graph.Edge
	for i := range 20 {
		id := fmt.Sprintf("pod:%d", i)
		entities = append(entities, graph.Entity{ID: id, Kind: "pod", Fields: graph.Fields{"blob": fmt.Sprintf("v-%d", i)}})
		if i > 0 {
			for j := range 10 {
				edges = append(edges, graph.Edge{
					ID:   fmt.Sprintf("e:%d:%d", i, j),
					Type: "links",
					From: fmt.Sprintf("pod:%d", j%i),
					To:   id,
				})
			}
		}
	}
	entities = append(entities, graph.Entity{
		ID:     "pod:huge",
		Kind:   "pod",
		Fields: graph.Fields{"blob": string(make([]byte, maxAggregateKeyBytes+128))},
	})
	if err := g.ApplyCommit(graph.Commit{
		ID:      "seed",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes:       []graph.CIType{{Name: "pod"}, {Name: "empty_kind"}},
			UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKinds: []string{"pod"}, ToKinds: []string{"pod"}}},
			UpsertEntities:      entities,
			UpsertEdges:         edges,
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	// Empty kind in multi-hop pattern must estimate 0 paths and pass tight CostLimit.
	patternReq, err := ParseGQL(`MATCH empty_kind PATH STEP OUT STEP OUT LIMIT 10`)
	if err != nil {
		t.Fatal(err)
	}
	patternReq.CostLimit = 2
	resp, err := Execute(g, patternReq)
	if err != nil || len(resp.Results) != 0 {
		t.Fatalf("empty kind pattern rejected or non-empty: %+v, err=%v", resp, err)
	}

	// Oversized group_by scalar key must be bounded with ErrLimitExceeded.
	_, err = Execute(g, Request{
		Op:      "match",
		Kind:    "pod",
		GroupBy: []string{"blob"},
	})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expected ErrLimitExceeded for oversized group_by key, got %v", err)
	}

	// Oversized count_by key must also be bounded with ErrLimitExceeded.
	_, err = Execute(g, Request{
		Op:        "match",
		Kind:      "pod",
		Aggregate: []Aggregation{{Op: "count_by", Field: "blob"}},
	})
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expected ErrLimitExceeded for oversized count_by key, got %v", err)
	}
}
