package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestDistributedGraphGCProtectsPinsAndConcurrentPreparedWrites(t *testing.T) {
	for _, revision := range []int{ActionRevision, InputGCRevision, BatchGCRevision, BufferedGCRevision} {
		t.Run(fmt.Sprint(revision), func(t *testing.T) { testDistributedGraphGC(t, revision) })
	}
}

func testDistributedGraphGC(t *testing.T, revision int) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	remote.revision = revision
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
		for g.GC.Phase == "inputs" {
			plan, err := remote.stores[engine.Home.ID].InputGCPlan(ctx, g)
			if err != nil {
				t.Fatal(err)
			}
			g = apply(plan)
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
	if revision == BufferedGCRevision {
		active := describe()
		var prepared int
		for _, bucket := range active.Active.Participants {
			owner := active.Partitions[bucket].Group
			state, err := remote.stores[owner].Data(ctx, "t", 1, bucket)
			if err != nil || state.GC == nil || !state.GC.Buffered {
				t.Fatalf("missing buffered GC state: %+v %v", state, err)
			}
			if state.Prepared != nil {
				prepared++
				if state.GC.Marked.Hash != "" || !slices.Contains(state.GC.Pending, state.Prepared.Root.Hash) {
					t.Fatal("prepare did not protect its new root in partition state")
				}
			}
		}
		if prepared == 0 {
			t.Fatal("response loss did not leave a prepared participant")
		}
		for name, store := range remote.stores {
			if err := store.ValidateSnapshot(ctx, name); err != nil {
				t.Fatalf("snapshot with buffered protection: %v", err)
			}
		}
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

func TestBufferedGCSweepsWithBothMarkRepresentations(t *testing.T) {
	ctx := context.Background()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	action := Action{Tenant: "t", Generation: 1, Epoch: 1, Txn: "11111111111111111111111111111111", Revision: BufferedGCRevision}
	for _, operation := range []string{"reserve", "data_gc_begin", "data_gc_step"} {
		action.Operation = operation
		if _, err := store.Apply(ctx, action); err != nil {
			t.Fatal(err)
		}
	}
	state, err := store.Data(ctx, "t", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	tree := store.Tree("t", 1, 0)
	var protected []Row
	for i := range gcPendingLimit + 3 {
		page, err := tree.Update(ctx, Root{}, []Row{{Key: fmt.Sprint(i), Value: []byte(`true`)}})
		if err != nil {
			t.Fatal(err)
		}
		protected = append(protected, Row{Key: page.Hash, Value: []byte(`true`)})
	}
	if err := store.recordNewPages(ctx, &state, protected[:gcPendingLimit+1], BufferedGCRevision); err != nil {
		t.Fatal(err)
	}
	if err := store.recordNewPages(ctx, &state, protected[gcPendingLimit+1:], BufferedGCRevision); err != nil {
		t.Fatal(err)
	}
	if state.GC.Marked.Hash == "" || len(state.GC.Pending) != 2 {
		t.Fatal("protection should span the mark tree and the pending list")
	}
	if err := store.save(ctx, store.dataKey("t", 1, 0), state); err != nil {
		t.Fatal(err)
	}
	obsolete, err := tree.Update(ctx, Root{}, []Row{{Key: "obsolete", Value: []byte(`true`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateSnapshot(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	pendingKey := tree.pageKey(state.GC.Pending[0])
	data, err := store.Objects.Get(ctx, pendingKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Objects.Delete(ctx, pendingKey); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateSnapshot(ctx, "a"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("snapshot accepted a missing pending page: %v", err)
	}
	if err := store.Objects.Put(ctx, pendingKey, data); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		value, err := store.Apply(ctx, action)
		if err != nil {
			t.Fatal(err)
		}
		state = value.(DataState)
		if state.GC == nil {
			break
		}
	}
	if state.GC != nil || state.LastGC != action.Txn {
		t.Fatal("sweep did not finish within its bounded steps")
	}
	if _, err := store.Objects.Get(ctx, tree.pageKey(obsolete.Hash)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("obsolete page survived sweep: %v", err)
	}
	for _, row := range protected {
		if _, err := store.Objects.Get(ctx, tree.pageKey(row.Key)); err != nil {
			t.Fatalf("protected page disappeared: %v", err)
		}
	}
}

type noSweepListing struct {
	storage.ObjectStore
}

func (noSweepListing) ListPage(context.Context, string, string, int) ([]storage.ObjectInfo, string, error) {
	return nil, "", errors.New("planned sweep listed objects during application")
}

func TestPlannedGCSweepRechecksConcurrentPrepare(t *testing.T) {
	ctx := context.Background()
	store := Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
	step := Action{Tenant: "t", Generation: 1, Epoch: 1, Txn: "11111111111111111111111111111111", Revision: BufferedGCRevision}
	for _, op := range []string{"reserve", "data_gc_begin", "data_gc_step"} {
		step.Operation = op
		if _, err := store.Apply(ctx, step); err != nil {
			t.Fatal(err)
		}
	}
	tree := store.Tree("t", 1, 0)
	changes := []Row{{Key: "reused", Value: []byte(`true`)}}
	reused, err := tree.Update(ctx, Root{}, changes)
	if err != nil {
		t.Fatal(err)
	}
	obsolete, err := tree.Update(ctx, Root{}, []Row{{Key: "obsolete", Value: []byte(`true`)}})
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Data(ctx, "t", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PlanGCSweep(ctx, &step, state); err != nil || step.GCSweep == nil || len(step.GCSweep.Pages) != 2 {
		t.Fatalf("expected both obsolete page candidates: %+v %v", step.GCSweep, err)
	}

	// A newer transaction reuses an immutable page after it was listed.
	prepare := step
	prepare.GCSweep = nil
	prepare.Operation, prepare.Txn, prepare.Sequence = "prepare", "22222222222222222222222222222222", 1
	prepare.Changes = changes
	encoded, err := json.Marshal(changes)
	if err != nil {
		t.Fatal(err)
	}
	prepare.Digest = fmt.Sprintf("%x", sha256.Sum256(encoded))
	if _, err := store.ApplyFragment(ctx, prepare); err != nil {
		t.Fatal(err)
	}
	stale := step
	stale.ExpectedCursor = "different"
	if _, err := store.ApplyFragment(ctx, stale); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("stale sweep advanced: %v", err)
	}
	if _, err := store.Objects.Get(ctx, tree.pageKey(obsolete.Hash)); err != nil {
		t.Fatalf("stale sweep removed page: %v", err)
	}
	store.Objects = noSweepListing{store.Objects}
	result, err := store.ApplyFragment(ctx, step)
	if err != nil {
		t.Fatal(err)
	}
	state = result.(DataState)
	if state.GC.Phase != "cleanup" || state.Prepared == nil || state.Prepared.Root != reused {
		t.Fatalf("unexpected state after sweep: %+v", state)
	}
	if _, err := store.Objects.Get(ctx, tree.pageKey(obsolete.Hash)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("obsolete page survived sweep: %v", err)
	}
	store.Objects = tree.Objects
	if err := store.ValidateSnapshot(ctx, "a"); err != nil {
		t.Fatalf("planned sweep lost the prepared page: %v", err)
	}
}

type reclaimBeforeBeginRemote struct {
	Remote
	store     Store
	reclaimed bool
}

func (r *reclaimBeforeBeginRemote) Action(ctx context.Context, group Group, action Action, result any) error {
	if action.Operation == "begin" && !r.reclaimed {
		g, err := r.store.GetGraph(ctx, action.Tenant)
		if err != nil {
			return err
		}
		g.GC = &GraphGC{}
		plan, err := r.store.InputGCPlan(ctx, g)
		if err != nil {
			return err
		}
		if err := r.store.reclaimInputs(ctx, g, plan); err != nil {
			return err
		}
		r.reclaimed = true
	}
	return r.Remote.Action(ctx, group, action, result)
}

func TestGraphInputGCProtectsRecoveryAndRestagesUnclaimedInputs(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	remote.revision = InputGCRevision
	store := remote.stores[engine.Home.ID]
	collector := &reclaimBeforeBeginRemote{Remote: remote, store: store}
	engine.Remote = collector
	if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "schema", Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}}}); err != nil {
		t.Fatalf("request could not restage after GC won the begin race: %v", err)
	}
	if !collector.reclaimed {
		t.Fatal("test did not cross the staging / begin race")
	}
	remote.blocked = "prepare"
	request := CommitRequest{IdempotencyKey: "pending", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}}
	if _, err := engine.Submit(ctx, request); err == nil {
		t.Fatal("prepare did not pause")
	}
	g, err := store.GetGraph(ctx, "t")
	if err != nil || g.Active == nil || len(g.Active.Inputs) == 0 {
		t.Fatalf("test did not retain an active input: %v", err)
	}
	for i := range 40 {
		payload := []byte("orphan")
		if i < 10 {
			payload = make([]byte, MaxValueBytes)
		}
		_, err := store.Apply(ctx, Action{Operation: "stage_input", Tenant: "t", Generation: 1, Txn: fmt.Sprintf("%032x", i+1), Data: payload, Digest: fmt.Sprintf("%x", sha256.Sum256(payload))})
		if err != nil {
			t.Fatal(err)
		}
	}
	cursor := ""
	steps := 0
	for {
		g.GC = &GraphGC{Cursor: cursor}
		plan, err := store.InputGCPlan(ctx, g)
		if err != nil {
			t.Fatal(err)
		}
		err = store.reclaimInputs(ctx, g, plan)
		if err != nil {
			t.Fatal(err)
		}
		cursor = plan.Cursor
		steps++
		if cursor == "" {
			break
		}
		if steps > 10 {
			t.Fatal("input GC did not advance")
		}
	}
	if steps < 2 {
		t.Fatal("input GC did not respect its batch budget")
	}
	remaining, err := store.Objects.List(ctx, path.Join(store.namespace("t", 1), "inputs")+"/")
	if err != nil || len(remaining) != len(g.Active.Inputs) {
		t.Fatalf("orphan inputs retained or active inputs deleted: %d, %v", len(remaining), err)
	}
	if err := store.ValidateSnapshot(ctx, engine.Home.ID); err != nil {
		t.Fatalf("GC broke the active transaction's snapshot: %v", err)
	}
	// Missing active input is corruption, and must retain the snapshot/recovery
	// failure rather than being mistaken for a request that can be restaged.
	input := g.Active.Inputs[0]
	payload, err := store.Input(ctx, "t", 1, input)
	if err != nil {
		t.Fatal(err)
	}
	key := store.inputKey("t", 1, input.Stage, input.Digest)
	if err := store.Objects.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateSnapshot(ctx, engine.Home.ID); err == nil || errors.Is(err, ErrInputExpired) {
		t.Fatalf("snapshot accepted or misclassified a missing active input: %v", err)
	}
	if err := store.Objects.Put(ctx, key, payload); err != nil {
		t.Fatal(err)
	}
	remote.blocked = ""
	if _, err := engine.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Submit(ctx, request)
	if err != nil || !result.IdempotentReplay || result.Version != 2 {
		t.Fatalf("GC changed the pending transaction's result: %+v %v", result, err)
	}
}
