package query

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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
