package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestDistributedGraphMoveCapturesWritesPinsAndCancellation(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	submit := func(key string, m graph.Mutations) storage.CommitResult {
		t.Helper()
		result, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: key, Mutations: m})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	submit("schema", graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"name": {Type: "string"}}}}, UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true}}})
	left := "left"
	for Bucket(left)%2 != 0 {
		left += "x"
	}
	submit("data", graph.Mutations{UpsertEntities: []graph.Entity{{ID: left, Kind: "host"}, {ID: "right", Kind: "host"}, {ID: "third", Kind: "host"}}, UpsertEdges: []graph.Edge{{From: left, To: "right", Type: "calls"}, {From: left, To: "third", Type: "calls"}}})
	describe := func() Graph {
		t.Helper()
		g, err := remote.Describe(ctx, engine.Home, "t")
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := describe()
	request := query.Request{Op: "neighbors", ID: left, Direction: "both", Limit: 1}
	first, err := ExecutePinnedQuery(ctx, remote, engine.Home, g, request)
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	id := Bucket(left)
	moveID := "11111111111111111111111111111111"
	control := func(a Action) Graph {
		t.Helper()
		a.Tenant = "t"
		a.Generation = 1
		a.Txn = moveID
		current := describe()
		if current.Move != nil {
			a.Phase = current.Move.Phase
			a.ExpectedCursor = current.Move.Cursor
		}
		if a.At.IsZero() {
			a.At = time.Now().UTC()
		}
		var result Graph
		if err := remote.Action(ctx, engine.Home, a, &result); err != nil {
			t.Fatalf("%s: %v", a.Operation, err)
		}
		return result
	}
	target := g.Groups["b"]
	control(Action{Operation: "graph_move_begin", Partition: id, Target: &target})
	data := func(group Group, op string, epoch uint64, root Root, sequence uint64) DataState {
		t.Helper()
		var result DataState
		if err := remote.Action(ctx, group, Action{Operation: op, Tenant: "t", Generation: 1, Partition: id, Txn: moveID, Epoch: epoch, Base: root, Sequence: sequence}, &result); err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return result
	}
	source := g.Groups["a"]
	data(target, "data_move_reserve", 2, Root{}, 0)
	data(source, "data_move_capture", 1, Root{}, 0)
	control(Action{Operation: "graph_move_progress", Purpose: "copy"})
	copyPages := func(purpose string) {
		t.Helper()
		cursor := ""
		for {
			exported, err := remote.stores[source.ID].ExportPages(ctx, Action{Tenant: "t", Generation: 1, Partition: id, Epoch: 1, Txn: moveID, Cursor: cursor, Purpose: purpose})
			if err != nil {
				t.Fatal(err)
			}
			for _, object := range exported.Objects {
				if err := remote.Action(ctx, target, Action{Operation: "data_move_page", Tenant: "t", Generation: 1, Partition: id, Epoch: 2, Txn: moveID, Digest: object.Hash, Data: object.Data}, nil); err != nil {
					t.Fatal(err)
				}
			}
			if exported.Next == "" {
				break
			}
			cursor = exported.Next
		}
	}
	copyPages("")
	// This page was not in the baseline. A new coordinator must recover it from
	// the replicated capture root, even when its hash sorts before the cursor.
	updated := submit("during-copy", graph.Mutations{UpsertEntities: []graph.Entity{{ID: left, Kind: "host", Fields: graph.Fields{"name": "updated"}}}})
	g = control(Action{Operation: "graph_move_progress", Purpose: "fence"})
	data(source, "data_move_fence", 1, g.Move.Root, g.Move.Sequence)
	control(Action{Operation: "graph_move_progress", Purpose: "final_copy"})
	if _, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "blocked", Kind: "host"}}}}); !errors.Is(err, ErrGraphBusy) {
		t.Fatalf("write crossed source fence: %v", err)
	}
	// Reopen storage handles, as after source/coordinator leader replacement.
	remote.stores["a"] = Store{Objects: remote.stores["a"].Objects, Prefix: "graphdb"}
	if err := remote.stores["a"].ValidateSnapshot(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	copyPages("delta")
	g = control(Action{Operation: "graph_move_progress", Purpose: "install"})
	installed := data(target, "data_move_install", 2, g.Move.Root, g.Move.Sequence)
	g = control(Action{Operation: "graph_move_switch", DataState: &installed})
	if g.Version != updated.Version || g.DataHash != updated.DataHash || g.Partitions[id].Group != "b" {
		t.Fatalf("physical move changed logical graph: %+v", g)
	}
	entity, found, err := (&Lookup{View: View{Graph: g, Remote: remote}}).GetEntity(ctx, left, nil)
	if err != nil || !found || entity.Fields["name"] != "updated" {
		t.Fatalf("move lost concurrent write: %+v %v", entity, err)
	}
	request.Cursor = first.NextCursor
	page, err := ExecutePinnedQuery(ctx, remote, engine.Home, g, request)
	if err != nil || len(page.Results) != 1 || page.Version != first.Version {
		t.Fatalf("move lost pinned view: %+v %v", page, err)
	}
	retireAt := g.Move.RetireAfter
	for data(source, "data_move_retire", 1, Root{}, 0).State != "retired" {
	}
	control(Action{Operation: "graph_move_complete", At: retireAt.Add(time.Second)})
	if err := remote.stores["a"].ValidateSnapshot(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	// Cancelling another move uses a newer source epoch; it never resurrects
	// writes addressed to either of the old source or staged target epochs.
	moveID = "22222222222222222222222222222222"
	control(Action{Operation: "graph_move_begin", Partition: id, Target: &source})
	g = control(Action{Operation: "graph_move_cancel"})
	restored := data(target, "data_move_thaw", 2, Root{}, g.Move.Sequence)
	data(source, "data_move_reserve", 3, Root{}, 0)
	for data(source, "data_move_retire", 3, Root{}, 0).State != "retired" {
	}
	g = control(Action{Operation: "graph_move_cancelled", DataState: &restored})
	if g.Move.BlocksWrites() || g.Partitions[id].Epoch != 4 || g.Partitions[id].Group != "b" {
		t.Fatalf("cancelled layout: %+v", g.Partitions[id])
	}
	submit("after-cancel", graph.Mutations{UpsertEntities: []graph.Entity{{ID: left, Kind: "host", Fields: graph.Fields{"name": "after cancel"}}}})
	if _, err := remote.stores["b"].Read(ctx, ReadRequest{Tenant: "t", Generation: 1, Partition: id, Epoch: 2, Root: g.Roots[id], Keys: []string{EntityKey(left)}}); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("stale epoch remained readable: %v", err)
	}
}

func TestMoveTransfersReachablePagesAndVerifiesBoundedSteps(t *testing.T) {
	ctx := context.Background()
	source := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	target := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	a := Action{Tenant: "t", Generation: 1, Partition: 7, Txn: "11111111111111111111111111111111", Revision: ActionRevision, Epoch: 1}
	apply := func(store Store, action Action) DataState {
		t.Helper()
		value, err := store.Apply(ctx, action)
		if err != nil {
			t.Fatal(err)
		}
		state, _ := value.(DataState)
		return state
	}
	a.Operation = "reserve"
	state := apply(source, a)
	tree := source.Tree("t", 1, 7)
	garbage, err := tree.Update(ctx, Root{}, []Row{{Key: "unreachable", Value: json.RawMessage(`1`)}})
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]Row, MaxChanges)
	for i := range rows {
		rows[i] = Row{Key: fmt.Sprintf("k-%05d", i), Value: json.RawMessage(`1`)}
	}
	state.Root, err = tree.Update(ctx, Root{}, rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.save(ctx, source.dataKey("t", 1, 7), state); err != nil {
		t.Fatal(err)
	}
	a.Operation = "data_move_capture"
	a.Purpose = "reachable"
	a.Roots = []Root{state.Root}
	apply(source, a)
	reserve := a
	reserve.Epoch = 2
	reserve.Operation = "data_move_reserve"
	apply(target, reserve)
	for {
		exported, err := source.ExportPages(ctx, a)
		if err != nil || len(exported.Objects) == 0 {
			t.Fatalf("export failed or stalled: %d %v", len(exported.Objects), err)
		}
		for _, page := range exported.Objects {
			if page.Hash == garbage.Hash {
				t.Fatal("export copied an unreachable historical page")
			}
		}
		stage := reserve
		stage.Operation = "data_move_pages"
		stage.Pages = exported.Objects
		apply(target, stage)
		if exported.Next == "" {
			break
		}
		a.Cursor = exported.Next
	}
	install := reserve
	install.Operation = "data_move_install"
	install.Base = state.Root
	install.Sequence = 2
	step, err := target.VerifyMoveStep(ctx, install)
	if err != nil || step == nil || len(step.Stack) == 0 {
		t.Fatalf("large install was not checkpointed: %v", err)
	}
	install.Verify = step
	pending := apply(target, install)
	if pending.State != "importing" || pending.MoveVerify == nil {
		t.Fatal("partial verification published the target")
	}
	nextRoot := step.Stack[len(step.Stack)-1]
	key := target.Tree("t", 1, 7).pageKey(nextRoot.Hash)
	data, err := target.Objects.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Objects.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := target.VerifyMoveStep(ctx, install); !errors.Is(err, ErrPartitionCorrupt) {
		t.Fatalf("missing page was not detected before proposal: %v", err)
	}
	if err := target.Objects.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	for count := 0; count < 8; count++ {
		install.Verify, err = target.VerifyMoveStep(ctx, install)
		if err != nil {
			t.Fatal(err)
		}
		if installed := apply(target, install); installed.State == "active" {
			if installed.Root != state.Root {
				t.Fatal("installed the wrong root")
			}
			return
		}
	}
	t.Fatal("verification did not resume to completion")
}
