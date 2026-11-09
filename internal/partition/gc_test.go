package partition

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestDistributedGraphGCProtectsPinsAndConcurrentPreparedWrites(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	commit := func(key string, m graph.Mutations) {
		t.Helper()
		if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: key, Mutations: m}); err != nil {
			t.Fatal(err)
		}
	}
	commit("schema", graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"value": {Type: "string"}}}}})
	commit("data", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "a", Kind: "host"}, {ID: "b", Kind: "host"}, {ID: "c", Kind: "host"}}})
	describe := func() Graph {
		t.Helper()
		g, err := remote.Describe(ctx, engine.Home, "t")
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	request := query.Request{Op: "match", Kind: "host", Limit: 1}
	first, err := ExecutePinnedQuery(ctx, remote, engine.Home, describe(), request)
	if err != nil || first.NextCursor == "" {
		t.Fatalf("pin: %+v %v", first, err)
	}
	for i := range 20 {
		commit(fmt.Sprintf("history-%d", i), graph.Mutations{UpsertEntities: []graph.Entity{{ID: "b", Kind: "host", Fields: graph.Fields{"value": fmt.Sprint(i)}}}})
	}
	pages := func() int {
		count := 0
		for _, store := range remote.stores {
			_ = store.walkObjects(ctx, store.namespace("t", 1)+"/partitions/", func(object storage.ObjectInfo) error {
				if len(object.Key) > 0 && validDigest(path.Base(object.Key)) {
					count++
				}
				return nil
			})
		}
		return count
	}
	before := pages()
	gcID := "11111111111111111111111111111111"
	apply := func(a Action) Graph {
		t.Helper()
		a.Tenant = "t"
		a.Generation = 1
		a.Txn = gcID
		if a.At.IsZero() {
			a.At = time.Now().UTC()
		}
		var result Graph
		if err := remote.Action(ctx, engine.Home, a, &result); err != nil {
			t.Fatalf("%s: %v", a.Operation, err)
		}
		return result
	}
	start := func(at time.Time) {
		t.Helper()
		g := apply(Action{Operation: "graph_gc_begin", At: at})
		for g.GC.Phase == "collect" {
			g = apply(Action{Operation: "graph_gc_collect", At: at, ExpectedCursor: g.GC.Cursor})
		}
		var ids []uint8
		for i := range g.Partitions {
			id := uint8(i)
			roots, err := remote.stores[engine.Home.ID].GCRoots(ctx, "t", gcID, id)
			if err != nil {
				t.Fatal(err)
			}
			group, err := g.GroupFor(id)
			if err != nil {
				t.Fatal(err)
			}
			if err := remote.Action(ctx, group, Action{Operation: "data_gc_begin", Tenant: "t", Generation: 1, Partition: id, Epoch: g.Partitions[id].Epoch, Txn: gcID, Roots: roots}, nil); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		g = apply(Action{Operation: "graph_gc_started", Participants: ids})
		if g.GC.Phase == "prune" {
			commit(fmt.Sprintf("while-pruning-%s", gcID), graph.Mutations{
				UpsertEntities: []graph.Entity{{ID: "c", Kind: "host", Fields: graph.Fields{"value": "while-pruning"}}},
			})
		}
		for g.GC.Phase == "prune" {
			if g.GC.BlocksWrites() {
				t.Fatal("view pruning still fences writes after data GC is armed")
			}
			g = apply(Action{Operation: "graph_gc_prune", ExpectedCursor: g.GC.Cursor})
		}
	}
	finish := func() {
		t.Helper()
		g := describe()
		var ids []uint8
		for i := range g.Partitions {
			id := uint8(i)
			group, err := g.GroupFor(id)
			if err != nil {
				t.Fatal(err)
			}
			for count := 0; ; count++ {
				if count > 1000 {
					t.Fatal("GC did not converge")
				}
				var state DataState
				if err := remote.Action(ctx, group, Action{Operation: "data_gc_step", Tenant: "t", Generation: 1, Partition: id, Epoch: g.Partitions[id].Epoch, Txn: gcID}, &state); err != nil {
					t.Fatal(err)
				}
				if state.GC == nil {
					break
				}
			}
			ids = append(ids, id)
		}
		apply(Action{Operation: "graph_gc_done", Participants: ids})
	}
	start(time.Now().UTC())
	// Begin a new prepare after mark has started, then collect before recovering
	// its response. The prepare itself must protect every newly referenced page.
	id := Bucket("b")
	g := describe()
	group, _ := g.GroupFor(id)
	for {
		var state DataState
		if err := remote.Action(ctx, group, Action{Operation: "data_gc_step", Tenant: "t", Generation: 1, Partition: id, Epoch: 1, Txn: gcID}, &state); err != nil {
			t.Fatal(err)
		}
		if state.GC.Phase == "sweep" {
			break
		}
	}
	remote.lose = "prepare"
	pending := CommitRequest{IdempotencyKey: "during-gc", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "b", Kind: "host", Fields: graph.Fields{"value": "after mark"}}}}}
	if _, err := engine.Submit(ctx, pending); err == nil {
		t.Fatal("expected simulated response loss")
	}
	finish()
	if _, err := engine.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	replay, err := engine.Submit(ctx, pending)
	if err != nil || !replay.IdempotentReplay {
		t.Fatalf("prepared write lost in GC: %+v %v", replay, err)
	}
	if after := pages(); after >= before {
		t.Fatalf("GC retained unreferenced history: before=%d after=%d", before, after)
	}
	request.Cursor = first.NextCursor
	second, err := ExecutePinnedQuery(ctx, remote, engine.Home, describe(), request)
	if err != nil || second.Version != first.Version || len(second.Results) != 1 || second.Results[0].Entity.ID != "b" {
		t.Fatalf("GC lost pinned old page: %+v %v", second, err)
	}
	for name, store := range remote.stores {
		if err := store.ValidateSnapshot(ctx, name); err != nil {
			t.Fatalf("snapshot after GC: %v", err)
		}
	}
	gcID = "22222222222222222222222222222222"
	start(time.Now().Add(CursorLifetime + MaxQueryLifetime + time.Minute))
	finish()
	if _, err := ExecutePinnedQuery(ctx, remote, engine.Home, describe(), request); err == nil {
		t.Fatal("expired GC pin continued on a newer graph")
	}
}
