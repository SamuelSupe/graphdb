package partition

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound5DataMoveRetireAdvancesOlderRetiredEpoch(t *testing.T) {
	ctx := context.Background()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	old := DataState{Tenant: "t1", Generation: 1, Partition: 3, Epoch: 1, State: "retired", LastTxn: "11111111111111111111111111111111"}
	if err := store.save(ctx, store.dataKey("t1", 1, 3), old); err != nil {
		t.Fatal(err)
	}
	res, err := store.Apply(ctx, Action{
		Operation:  "data_move_retire",
		Tenant:     "t1",
		Generation: 1,
		Partition:  3,
		Epoch:      3,
		Txn:        "22222222222222222222222222222222",
	})
	if err != nil {
		t.Fatalf("data_move_retire on older retired state failed: %v", err)
	}
	got := res.(DataState)
	if got.State != "retired" || got.Epoch != 3 || got.LastTxn != "22222222222222222222222222222222" {
		t.Fatalf("unexpected retired state: %+v", got)
	}
}

func TestRound5SyncGroupsFencesStaleGraphMoveBegin(t *testing.T) {
	ctx := context.Background()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	g1 := Group{ID: "g1", ClusterID: "c1", Peers: map[uint64]string{1: "http://1", 2: "http://2", 3: "http://3"}}
	g2 := Group{ID: "g2", ClusterID: "c2", Peers: map[uint64]string{1: "http://4", 2: "http://5", 3: "http://6"}}
	g := Graph{
		Format:         FormatVersion,
		Tenant:         "t1",
		Generation:     1,
		State:          "active",
		CatalogVersion: 5,
		Groups:         map[string]Group{"g1": g1, "g2": g2},
	}
	for i := range g.Partitions {
		g.Partitions[i] = Placement{Group: "g1", Epoch: 1}
	}
	if err := store.save(ctx, store.graphKey("t1"), g); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Apply(ctx, Action{
		Operation:      "sync_groups",
		Tenant:         "t1",
		Generation:     1,
		CatalogVersion: 6,
		Target:         &Group{ID: "g2"},
	}); err != nil {
		t.Fatalf("sync_groups prune fence failed: %v", err)
	}

	if _, err := store.Apply(ctx, Action{
		Operation:      "graph_move_begin",
		Tenant:         "t1",
		Generation:     1,
		Partition:      0,
		Txn:            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Target:         &g2,
		CatalogVersion: 5,
	}); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("stale graph_move_begin err = %v, want ErrConflict", err)
	}

	if _, err := store.Apply(ctx, Action{
		Operation:      "graph_move_begin",
		Tenant:         "t1",
		Generation:     1,
		Partition:      0,
		Txn:            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Target:         &g2,
		CatalogVersion: 7,
	}); err != nil {
		t.Fatalf("fresh graph_move_begin failed: %v", err)
	}
	if _, err := store.Apply(ctx, Action{
		Operation:      "sync_groups",
		Tenant:         "t1",
		Generation:     1,
		CatalogVersion: 8,
		Target:         &Group{ID: "g2"},
	}); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("sync_groups prune during active move err = %v, want ErrConflict", err)
	}
}

func TestRound5PartitionImpactV0Supported(t *testing.T) {
	ctx := context.Background()
	engine, _ := newLocalGraph(t)
	g, err := engine.Remote.Describe(ctx, engine.Home, engine.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if g.Version != 0 {
		t.Fatalf("expected fresh partitioned graph at version 0, got %d", g.Version)
	}
	_, err = ExecuteQuery(ctx, View{Graph: g, Remote: engine.Remote}, query.Request{
		Op:            "impact",
		ID:            "svc:api",
		RelationTypes: []string{"contains"},
		Depth:         2,
	})
	if err == nil || strings.Contains(err.Error(), "distributed execution does not support") || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("v0 partitioned impact err = %v, want normal entity-not-found error (not unsupported query)", err)
	}
}
