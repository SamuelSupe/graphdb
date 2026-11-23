package partition

import (
	"context"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestMoveRootsRespectsHistoryCutoff(t *testing.T) {
	ctx := context.Background()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	now := time.Unix(1700000000, 0).UTC()
	cutoff := now.Add(5 * time.Minute)
	g := Graph{
		Format:        FormatVersion,
		Tenant:        "tenant-a",
		Generation:    1,
		Version:       10,
		State:         "active",
		HistoryCutoff: cutoff,
	}
	g.Roots[0] = Root{Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", First: "a", Last: "z", Count: 1}
	// Pin has KeepUntil > now, but KeepUntil <= HistoryCutoff, and its view was pruned.
	prunedPin := Pin{
		ID:         "11111111111111111111111111111111",
		Generation: 1,
		Version:    2,
		ExpiresAt:  now.Add(2 * time.Minute),
		KeepUntil:  cutoff,
	}
	if err := store.save(ctx, store.pinKey(g.Tenant, g.Generation, prunedPin.ID), prunedPin); err != nil {
		t.Fatal(err)
	}
	roots, err := store.MoveRoots(ctx, g, 0, now)
	if err != nil {
		t.Fatalf("MoveRoots should skip pin invalidated by HistoryCutoff, got: %v", err)
	}
	if len(roots) != 1 || roots[0] != g.Roots[0] {
		t.Fatalf("unexpected MoveRoots result: %+v", roots)
	}
}
