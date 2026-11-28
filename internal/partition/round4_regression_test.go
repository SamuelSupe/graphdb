package partition

import (
	"context"
	"errors"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound4PartitionImpactQuerySupported(t *testing.T) {
	ctx := context.Background()
	engine, _ := newLocalGraph(t)
	if _, err := engine.Submit(ctx, CommitRequest{
		IdempotencyKey: "seed-impact",
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "cluster:1", Kind: "cluster"},
				{ID: "svc:api", Kind: "service"},
			},
			UpsertEdges: []graph.Edge{
				{Type: "contains", From: "cluster:1", To: "svc:api"},
			},
		},
	}); err != nil {
		t.Fatalf("seed impact commit failed: %v", err)
	}
	g, err := engine.Remote.Describe(ctx, engine.Home, engine.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ExecuteQuery(ctx, View{Graph: g, Remote: engine.Remote}, query.Request{
		Op:    "impact",
		ID:    "svc:api",
		Depth: 2,
	})
	if err != nil {
		t.Fatalf("partitioned impact query failed: %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("expected 1 impacted path, got %+v", res.Results)
	}
}

func TestRound4PartitionDeleteEntityBySourceWithRequiredField(t *testing.T) {
	ctx := context.Background()
	engine, _ := newLocalGraph(t)
	if _, err := engine.Submit(ctx, CommitRequest{
		IdempotencyKey: "seed-req",
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{
				{Name: "Server", Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Required: true}}},
			},
			UpsertEntities: []graph.Entity{
				{ID: "srv:1", Kind: "Server", Source: "cmdb", ExternalID: "ext-1", Fields: graph.Fields{"hostname": "web-01"}},
			},
		},
	}); err != nil {
		t.Fatalf("seed required-field commit failed: %v", err)
	}
	if _, err := engine.Submit(ctx, CommitRequest{
		IdempotencyKey: "del-req",
		Mutations: graph.Mutations{
			DeleteEntityRequests: []graph.EntityDeleteRequest{
				{Kind: "Server", Source: "cmdb", ExternalID: "ext-1"},
			},
		},
	}); err != nil {
		t.Fatalf("DeleteEntityRequest by source identity on required-field CIType failed: %v", err)
	}
	g, err := engine.Remote.Describe(ctx, engine.Home, engine.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if g.EntityCount != 0 {
		t.Fatalf("expected 0 entities remaining after delete, got %d", g.EntityCount)
	}
}

func TestRound4ConversionCancelAllowedAfterRecoveryGC(t *testing.T) {
	ctx := context.Background()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "test"}

	if err := store.save(ctx, store.dataKey("tenant-a", 2, 0), DataState{
		Tenant:         "tenant-a",
		Generation:     2,
		Partition:      0,
		Epoch:          1,
		State:          "active",
		LastGC:         "gc-recovery-1",
		LastGCRecovery: true,
	}); err != nil {
		t.Fatal(err)
	}
	states, _, err := store.collectAbandonableStates(ctx, "tenant-a", 2)
	if err != nil || len(states) != 1 {
		t.Fatalf("recovery GC blocked conversion cancellation: states=%v err=%v", states, err)
	}

	if err := store.save(ctx, store.dataKey("tenant-b", 2, 0), DataState{
		Tenant:         "tenant-b",
		Generation:     2,
		Partition:      0,
		Epoch:          1,
		State:          "active",
		LastGC:         "gc-normal-1",
		LastGCRecovery: false,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.collectAbandonableStates(ctx, "tenant-b", 2); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("post-activation GC should still block abandonment: err=%v", err)
	}
}
