package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound3PartitionTreeBalancedOnLargeSequentialInsertsAndChurn(t *testing.T) {
	ctx := context.Background()
	tree := Tree{
		Objects: storage.NewMemoryStore(),
		Prefix:  "test/tree",
	}
	payload := json.RawMessage(`"` + strings.Repeat("x", 16<<10) + `"`)

	var root Root
	var err error
	// Insert 1500 large entries sequentially across batches to trigger repeated branch splits.
	for batch := 0; batch < 15; batch++ {
		changes := make([]Row, 0, 100)
		for i := 0; i < 100; i++ {
			id := batch*100 + i
			changes = append(changes, Row{Key: fmt.Sprintf("k:%06d", id), Value: payload})
		}
		root, err = tree.Update(ctx, root, changes)
		if err != nil {
			t.Fatalf("batch %d update failed: %v", batch, err)
		}
	}
	if root.Count != 1500 {
		t.Fatalf("expected 1500 entries, got %d", root.Count)
	}
	// Check tree depth stays shallow and uniform (never approaching depth > 32).
	depth := 0
	var checkDepth func(ref Root, d int) error
	checkDepth = func(ref Root, d int) error {
		if d > depth {
			depth = d
		}
		page, err := tree.load(ctx, ref)
		if err != nil {
			return err
		}
		for _, child := range page.Children {
			if err := checkDepth(child, d+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := checkDepth(root, 0); err != nil {
		t.Fatalf("tree integrity check failed: %v", err)
	}
	if depth > 4 {
		t.Fatalf("tree became unbalanced after sequential large inserts: depth=%d", depth)
	}

	// Run 30 rounds of add+delete churn and verify underfull coalescing keeps depth bounded.
	for round := 0; round < 30; round++ {
		changes := make([]Row, 0, 60)
		for i := 0; i < 40; i++ {
			id := round*40 + i
			changes = append(changes, Row{Key: fmt.Sprintf("k:%06d", id), Value: nil})
		}
		for i := 0; i < 20; i++ {
			id := 1500 + round*20 + i
			changes = append(changes, Row{Key: fmt.Sprintf("k:%06d", id), Value: payload})
		}
		root, err = tree.Update(ctx, root, changes)
		if err != nil {
			t.Fatalf("churn round %d failed: %v", round, err)
		}
	}
	depth = 0
	if err := checkDepth(root, 0); err != nil {
		t.Fatalf("tree integrity check after churn failed: %v", err)
	}
	if depth > 4 {
		t.Fatalf("tree depth grew after churn: depth=%d", depth)
	}
}

func TestRound3PlanDeleteEdgeRequestsMatchesGraphPrecedence(t *testing.T) {
	ctx := context.Background()
	engine, _ := newLocalGraph(t)
	seed := CommitRequest{
		IdempotencyKey: "seed-edge",
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{
				{Name: "connects", FromKind: "host", ToKind: "host"},
			},
			UpsertEntities: []graph.Entity{
				{ID: "host:a", Kind: "host"},
				{ID: "host:b", Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "edge:real", Type: "connects", From: "host:a", To: "host:b"},
			},
		},
	}
	if _, err := engine.Submit(ctx, seed); err != nil {
		t.Fatalf("seed commit failed: %v", err)
	}
	// DeleteEdgeRequest provides BOTH a non-matching ID and a matching (Type, From, To) with whitespace.
	// Graph semantics prioritize (Type, From, To) over ID; partition planner must match!
	delReq := CommitRequest{
		IdempotencyKey: "del-edge",
		Mutations: graph.Mutations{
			DeleteEdgeRequests: []graph.EdgeDeleteRequest{
				{ID: "edge:wrong-id", Type: " connects ", From: " host:a ", To: " host:b "},
			},
		},
	}
	if _, err := engine.Submit(ctx, delReq); err != nil {
		t.Fatalf("delete edge commit failed: %v", err)
	}
	g, err := engine.Remote.Describe(ctx, engine.Home, engine.Tenant)
	if err != nil {
		t.Fatalf("describe failed: %v", err)
	}
	if g.EdgeCount != 0 {
		t.Fatalf("expected 0 edges remaining after delete, got %d", g.EdgeCount)
	}
}
