package partition

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestImportChunkRejectsConflictingRepeatedEntityIdentity(t *testing.T) {
	view := View{Graph: Graph{State: "initializing", Version: 1}}
	_, err := BuildImportPlan(t.Context(), view, ImportChunk{Entities: []graph.Entity{
		{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "first"}},
		{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "second"}},
	}})
	if !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("conflicting import chunk: %v", err)
	}
}

func TestSyncGroupsUpdatesStoredMoveAndKeepsNewerCatalogRevision(t *testing.T) {
	ctx := t.Context()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "test"}
	old := Group{ID: "shard-a", ClusterID: "raft-a", Peers: map[uint64]string{1: "a", 2: "b", 3: "c"}}
	current := Group{ID: old.ID, ClusterID: old.ClusterID, Peers: map[uint64]string{4: "d", 5: "e", 6: "f"}}
	g := Graph{Format: FormatVersion, Tenant: "tenant-a", Generation: 1, LayoutRevision: 4,
		Groups: map[string]Group{old.ID: old}, Move: &Move{Source: old}}
	if err := store.save(ctx, store.graphKey(g.Tenant), g); err != nil {
		t.Fatal(err)
	}
	action := Action{Operation: "sync_groups", Tenant: g.Tenant, Generation: g.Generation,
		CatalogVersion: 9, Groups: map[string]Group{old.ID: current}}
	if _, err := store.Apply(ctx, action); err != nil {
		t.Fatal(err)
	}
	action.CatalogVersion, action.Groups[old.ID] = 8, old
	if _, err := store.Apply(ctx, action); err != nil {
		t.Fatal(err)
	}
	updated, err := store.GetGraph(ctx, g.Tenant)
	if err != nil || updated.CatalogVersion != 9 || updated.LayoutRevision != 4 ||
		updated.Groups[old.ID].Peers[4] != "d" || updated.Move.Source.Peers[4] != "d" {
		t.Fatalf("updated graph topology: %+v, %v", updated.Groups, err)
	}
}

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

func TestConversionCancellationAndActivationChooseOneGeneration(t *testing.T) {
	ctx := t.Context()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "test"}
	g := Graph{Format: FormatVersion, Tenant: "tenant-a", Generation: 2, ConversionAttempt: 15}
	for i := range g.Partitions {
		g.Partitions[i] = Placement{Group: "home", Epoch: 1}
	}
	initialize := Action{Operation: "initialize", Tenant: g.Tenant, Generation: g.Generation, Graph: &g}
	cancel := Action{Operation: "cancel_conversion", Tenant: g.Tenant, Generation: g.Generation, ConversionAttempt: g.ConversionAttempt}
	if _, err := store.Apply(ctx, cancel); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(ctx, initialize); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("late initialize after cancellation: %v", err)
	}
	g.Generation, g.ConversionAttempt = 3, 20
	initialize.Generation = g.Generation
	if _, err := store.Apply(ctx, initialize); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(ctx, Action{Operation: "activate", Tenant: g.Tenant, Generation: g.Generation}); err != nil {
		t.Fatal(err)
	}
	cancel.Generation, cancel.ConversionAttempt = g.Generation, g.ConversionAttempt
	decision, err := store.Apply(ctx, cancel)
	if err != nil || decision != (ConversionCancellation{Attempt: g.ConversionAttempt, Generation: g.Generation, Activated: true}) {
		t.Fatalf("cancellation after activation: %+v %v", decision, err)
	}
	if _, err := store.Apply(ctx, Action{Operation: "reclaim_conversion", Tenant: g.Tenant, Generation: g.Generation}); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("reclamation accepted for active generation: %v", err)
	}
	if current, err := store.GetGraph(ctx, g.Tenant); err != nil || current.Generation != g.Generation || current.State != "active" {
		t.Fatalf("active generation disappeared after cancellation: %+v %v", current, err)
	}
}

func TestSnapshotAcceptsInterruptedReclamationOfCancelledLegacyConversion(t *testing.T) {
	ctx := t.Context()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "test"}
	const tenant = "tenant-a"
	const generation = 2
	g := Graph{Format: FormatVersion, Tenant: tenant, Generation: generation, State: "initializing"}
	if err := store.save(ctx, store.graphKey(tenant), g); err != nil {
		t.Fatal(err)
	}
	input := Input{Stage: "00000000000000000000000000000001", Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Bytes: 1}
	txn := Transaction{Tenant: tenant, Generation: generation, Identity: "import", Inputs: []Input{input}}
	if err := store.save(ctx, store.txnKey(tenant, generation, txn.Identity), txn); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateSnapshot(ctx, "home"); err == nil {
		t.Fatal("a live transaction with a missing input passed snapshot validation")
	}
	cancel := Action{Operation: "cancel_conversion", Tenant: tenant, Generation: generation}
	if _, err := store.Apply(ctx, cancel); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateSnapshot(ctx, "home"); err != nil {
		t.Fatalf("snapshot cannot resume an interrupted reclamation: %v", err)
	}
	if _, err := store.Apply(ctx, cancel); err != nil {
		t.Fatalf("legacy cancellation replay failed: %v", err)
	}
}

type localBatchRemote struct {
	*localRemote
}

func (r localBatchRemote) ReadBatch(ctx context.Context, g Group, reqs []ReadRequest) ([]ReadResult, error) {
	return r.stores[g.ID].ReadBatch(ctx, reqs)
}

func TestBuildPlanBatchesEdgeUpsertsAndGuardsPostConversionReclaim(t *testing.T) {
	ctx := t.Context()
	engine, rawRemote := newLocalGraph(t)
	remote := localBatchRemote{localRemote: rawRemote}
	engine.Remote = remote

	var seedEntities []graph.Entity
	for i := range 64 {
		seedEntities = append(seedEntities, graph.Entity{
			ID:   "host:" + string(rune('a'+i/26)) + string(rune('a'+i%26)),
			Kind: "host",
		})
	}
	if _, err := engine.Submit(ctx, CommitRequest{
		Mutations: graph.Mutations{
			UpsertCITypes:       []graph.CIType{{Name: "host"}},
			UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKinds: []string{"host"}, ToKinds: []string{"host"}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(ctx, CommitRequest{
		Mutations: graph.Mutations{
			UpsertEntities: seedEntities,
		},
	}); err != nil {
		t.Fatal(err)
	}
	desc, err := remote.Describe(ctx, engine.Home, engine.Tenant)
	if err != nil {
		t.Fatal(err)
	}

	// BuildPlan with 2,200 edges connecting existing entities previously exceeded 8,192 single-key RPCs.
	var edges []graph.Edge
	for i := range 2200 {
		from := seedEntities[i%len(seedEntities)].ID
		to := seedEntities[(i*7+3)%len(seedEntities)].ID
		if from == to {
			to = seedEntities[(i+1)%len(seedEntities)].ID
		}
		edges = append(edges, graph.Edge{
			ID:   graph.CanonicalEdgeIDParts("links", from, to) + ":" + string(rune('0'+i%10)) + string(rune('a'+(i/10)%26)) + string(rune('a'+(i/260)%26)),
			Type: "links",
			From: from,
			To:   to,
		})
	}
	budget := &ReadBudget{Requests: 256, Bytes: 64 << 20}
	view := View{Graph: desc, Remote: remote, Budget: budget}
	if _, err := BuildPlan(ctx, view, CommitRequest{Mutations: graph.Mutations{UpsertEdges: edges}}, time.Unix(1700000000, 0).UTC()); err != nil {
		t.Fatalf("batched BuildPlan for 2200 edges exceeded tight RPC budget (remaining=%d): %v", budget.Requests, err)
	}

	// Non-home data shard (no graph.json) must refuse reclaim_conversion when DataState has post-conversion lifecycle state.
	dataShard := Store{Objects: storage.NewMemoryStore(), Prefix: "test"}
	if err := dataShard.save(ctx, dataShard.dataKey("tenant-a", 5, 0), DataState{
		Tenant:     "tenant-a",
		Generation: 5,
		Partition:  0,
		Epoch:      2,
		State:      "active",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := dataShard.Apply(ctx, Action{Operation: "reclaim_conversion", Tenant: "tenant-a", Generation: 5}); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("expected ErrConflict on post-conversion non-home data shard reclaim, got %v", err)
	}
}
