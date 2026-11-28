package partition

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type localRemote struct {
	mu       sync.Mutex
	stores   map[string]Store
	lose     string
	blocked  string
	revision int
}

func (r *localRemote) Transaction(ctx context.Context, g Group, tenant string, generation int64, identity string) (Transaction, error) {
	return r.stores[g.ID].Transaction(ctx, tenant, generation, identity)
}

func (r *localRemote) Pin(ctx context.Context, g Group, tenant string, generation, version int64, digest, id string) (PinnedView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id != "" {
		return r.stores[g.ID].Pinned(ctx, tenant, id, digest, time.Now().UTC())
	}
	id, err := transactionID()
	if err != nil {
		return PinnedView{}, err
	}
	value, err := r.stores[g.ID].Apply(ctx, Action{Operation: "pin", Tenant: tenant, Generation: generation, BaseVersion: version, Digest: digest, Txn: id, At: time.Now().UTC()})
	if err != nil {
		return PinnedView{}, err
	}
	return value.(PinnedView), nil
}

func (r *localRemote) Describe(ctx context.Context, g Group, tenant string) (Graph, error) {
	return r.stores[g.ID].GetGraph(ctx, tenant)
}
func (r *localRemote) Input(ctx context.Context, g Group, tenant string, generation int64, input Input) ([]byte, error) {
	return r.stores[g.ID].Input(ctx, tenant, generation, input)
}
func (r *localRemote) Read(ctx context.Context, g Group, input ReadRequest) (ReadResult, error) {
	return r.stores[g.ID].Read(ctx, input)
}
func (r *localRemote) Action(ctx context.Context, g Group, a Action, result any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	a.Revision = ActionRevision
	if r.revision != 0 {
		a.Revision = r.revision
	}
	if r.blocked == a.Operation {
		return context.DeadlineExceeded
	}
	if a.Operation == "data_move_install" {
		verified, err := r.stores[g.ID].VerifyMoveStep(ctx, a)
		if err != nil {
			return err
		}
		a.Verify = verified
	}
	value, err := r.stores[g.ID].Apply(ctx, a)
	if err != nil {
		return err
	}
	if r.lose == a.Operation {
		r.lose = ""
		return context.DeadlineExceeded
	}
	if result != nil {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, result)
	}
	return nil
}

func newLocalGraph(t *testing.T) (Engine, *localRemote) {
	t.Helper()
	ctx := context.Background()
	r := &localRemote{stores: map[string]Store{}}
	groups := map[string]Group{}
	for _, id := range []string{"a", "b"} {
		r.stores[id] = Store{Objects: storage.NewMemoryStore(), Prefix: "graphdb"}
		groups[id] = Group{ID: id, ClusterID: id, Peers: map[uint64]string{1: "http://one", 2: "http://two", 3: "http://three"}}
	}
	g := Graph{Format: FormatVersion, Tenant: "t", Generation: 1, Groups: groups}
	for i := range g.Partitions {
		id := []string{"a", "b"}[i%2]
		g.Partitions[i] = Placement{Group: id, Epoch: 1}
		if _, err := r.stores[id].Apply(ctx, Action{Operation: "reserve", Tenant: "t", Generation: 1, Partition: uint8(i), Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.stores["a"].Apply(ctx, Action{Operation: "initialize", Tenant: "t", Generation: 1, Graph: &g}); err != nil {
		t.Fatal(err)
	}
	engine := Engine{Remote: r, Home: groups["a"], Tenant: "t"}
	if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "initialize"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.stores["a"].Apply(ctx, Action{Operation: "activate", Tenant: "t", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	return engine, r
}

type commitReadCounter struct {
	Remote
	describes int
	stages    int
	begins    int
}

func (r *commitReadCounter) Describe(ctx context.Context, group Group, tenant string) (Graph, error) {
	r.describes++
	return r.Remote.Describe(ctx, group, tenant)
}

func (r *commitReadCounter) Action(ctx context.Context, group Group, action Action, result any) error {
	switch action.Operation {
	case "stage_input":
		r.stages++
	case "begin":
		r.begins++
	}
	return r.Remote.Action(ctx, group, action, result)
}

func TestSubmitWithGraphReusesDescriptorAndRefreshesStaleBase(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stale     bool
		expected  bool
		describes int
		begins    int
	}{
		{name: "current", describes: 1, begins: 1},
		{name: "stale base", stale: true, describes: 2, begins: 2},
		{name: "newer expected version", stale: true, expected: true, describes: 2, begins: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			engine, remote := newLocalGraph(t)
			schema := CommitRequest{Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}}}
			if _, err := engine.Submit(ctx, schema); err != nil {
				t.Fatal(err)
			}
			initial, err := remote.Describe(ctx, engine.Home, engine.Tenant)
			if err != nil {
				t.Fatal(err)
			}
			version := initial.Version
			if tc.stale {
				intervening, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "other", Kind: "host"}}}})
				if err != nil {
					t.Fatal(err)
				}
				version = intervening.Version
			}
			counter := &commitReadCounter{Remote: remote}
			engine.Remote, engine.Generation = counter, initial.Generation
			request := CommitRequest{IdempotencyKey: "with-graph", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "new", Kind: "host"}}}}
			if tc.expected {
				request.ExpectedVersion = &version
			}
			result, err := engine.SubmitWithGraph(ctx, request, &initial)
			if err != nil || result.Version != version+1 {
				t.Fatalf("commit: %+v %v", result, err)
			}
			if counter.describes != tc.describes || counter.stages != 1 || counter.begins != tc.begins {
				t.Fatalf("describes=%d stages=%d begins=%d", counter.describes, counter.stages, counter.begins)
			}
			replay, err := engine.SubmitWithGraph(ctx, request, &initial)
			if err != nil || !replay.IdempotentReplay || replay.Version != result.Version || replay.DataHash != result.DataHash {
				t.Fatalf("replay: %+v %v", replay, err)
			}
		})
	}
}

func TestPlanBatchesDirectoryReads(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	g, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil {
		t.Fatal(err)
	}
	g.CITypes = []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Unique: true}}}}
	at := time.Unix(1700000000, 0).UTC()
	seed := CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "retained", Kind: "host", Source: "collector", ExternalID: "retained", Fields: graph.Fields{"name": "retained"}},
	}}}
	plan, err := BuildPlan(ctx, View{Graph: g, Remote: remote}, seed, at)
	if err != nil {
		t.Fatal(err)
	}
	bucket := Bucket("retained")
	var retained graph.Entity
	if err := decodeRecord(plan.Changes[bucket][EntityKey("retained")], &retained); err != nil {
		t.Fatal(err)
	}
	retained.MergedFrom = []string{"former"}
	if err := plan.Changes.Set(bucket, EntityKey(retained.ID), retained); err != nil {
		t.Fatal(err)
	}
	aliasKey := EntityAliasKey("former")
	if err := plan.Changes.Set(Bucket(aliasKey), aliasKey, []string{retained.ID}); err != nil {
		t.Fatal(err)
	}
	for _, bucket := range plan.Changes.Participants() {
		owner, err := g.GroupFor(bucket)
		if err != nil {
			t.Fatal(err)
		}
		g.Roots[bucket], err = remote.stores[owner.ID].Tree("t", 1, bucket).Update(ctx, Root{}, plan.Changes.Rows(bucket))
		if err != nil {
			t.Fatal(err)
		}
	}
	g.Version++
	g.EntityCount = 1
	request := CommitRequest{Mutations: graph.Mutations{DeleteEntities: []string{"former"}}}
	for i := range 16 {
		id := fmt.Sprintf("new-%02d", i)
		request.Mutations.UpsertEntities = append(request.Mutations.UpsertEntities,
			graph.Entity{ID: id, Kind: "host", Source: "collector", ExternalID: id, Fields: graph.Fields{"name": id}})
	}
	single := &countedReadRemote{Remote: remote}
	batched := &scanRPCRemote{Remote: remote, stores: remote.stores}
	// Capture the rows produced before batching, including the alias removal.
	const wantDigest = "a07533ee7f5dc7745651cc64c123924f2d18183ed0cc42ae94869935b5a2743d"
	for _, reader := range []Remote{single, batched} {
		plan, err := BuildPlan(ctx, View{Graph: g, Remote: reader}, request, at.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(plan.Changes)
		if err != nil {
			t.Fatal(err)
		}
		if digest := fmt.Sprintf("%x", sha256.Sum256(data)); digest != wantDigest {
			t.Fatalf("%T changed the transaction rows: %s", reader, digest)
		}
	}
	batchedCalls := batched.calls.Load()
	t.Logf("single fragment RPCs: %d, batched fragment RPCs: %d", single.calls, batchedCalls)
	if batchedCalls > 12 || int64(single.calls) <= batchedCalls {
		t.Fatal("plan did not batch primary, identity and alias reads")
	}
}

func seedHubGraph(t *testing.T, degree int) (Engine, *localRemote, Graph) {
	t.Helper()
	ctx := t.Context()
	engine, remote := newLocalGraph(t)
	g, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil {
		t.Fatal(err)
	}
	g.Version = 1
	g.CITypes = []graph.CIType{{Name: "host"}}
	g.RelationTypes = []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true}}
	g.EdgeCount, g.EntityCount = int64(degree), int64(degree+2)
	g.EdgeTypeCounts = map[string]int64{"calls": int64(degree)}
	schema, err := graph.FromSnapshot(graph.Snapshot{Version: g.Version, CITypes: g.CITypes, RelationTypes: g.RelationTypes})
	if err != nil {
		t.Fatal(err)
	}
	seed := &changeBuilder{changes: ChangeSet{}, ownerEdits: map[string]map[string]bool{}}
	addEntity := func(id string) {
		t.Helper()
		if err := seed.entity(schema, graph.Entity{ID: id, Kind: "host", Version: g.Version}, true); err != nil {
			t.Fatal(err)
		}
		g.EntityCounts[Bucket(id)]++
	}
	addEntity("hub")
	addEntity("retained")
	for i := range degree {
		id := fmt.Sprintf("source-%05d", i)
		addEntity(id)
		edge := graph.Edge{Type: "calls", From: id, To: "hub", Version: g.Version}
		edge.ID = graph.CanonicalEdgeID(edge)
		if err := seed.edge(schema, edge, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, bucket := range seed.changes.Participants() {
		group, err := g.GroupFor(bucket)
		if err != nil {
			t.Fatal(err)
		}
		store := remote.stores[group.ID]
		tree := store.Tree(g.Tenant, g.Generation, bucket)
		rows := seed.changes.Rows(bucket)
		state, err := store.Data(ctx, g.Tenant, g.Generation, bucket)
		if err != nil {
			t.Fatal(err)
		}
		for chunk := range slices.Chunk(rows, MaxChanges) {
			state.Root, err = tree.Update(ctx, state.Root, chunk)
			if err != nil {
				t.Fatal(err)
			}
		}
		g.Roots[bucket] = state.Root
		digests, err := tree.LogicalBuckets(ctx, state.Root, rows)
		if err != nil {
			t.Fatal(err)
		}
		for index, digest := range digests {
			g.HashBuckets[index] = digest
		}
		if err := store.save(ctx, store.dataKey(g.Tenant, g.Generation, bucket), state); err != nil {
			t.Fatal(err)
		}
	}
	g.DataHash, err = graph.ContentHashFromBuckets(g.HashBuckets)
	if err != nil {
		t.Fatal(err)
	}
	home := remote.stores[engine.Home.ID]
	if err := home.save(ctx, home.graphKey(g.Tenant), g); err != nil {
		t.Fatal(err)
	}
	return engine, remote, g
}

func TestDistributedMergeAndDeleteHubWithinReadBudget(t *testing.T) {
	ctx := t.Context()
	const degree = 5001
	engine, remote, g := seedHubGraph(t, degree)
	batched := &scanRPCRemote{Remote: remote, stores: remote.stores}
	remote.revision = LargeFragmentRevision
	engine.Remote = batched
	merge, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "merge-hub", Mutations: graph.Mutations{
		MergeEntities: []graph.MergeRequest{{TargetID: "retained", SourceIDs: []string{"hub"}}},
	}})
	if err != nil || merge.Version != g.Version+1 {
		t.Fatalf("hub merge result %+v (%v)", merge, err)
	}
	merged, err := remote.Describe(ctx, engine.Home, g.Tenant)
	if err != nil || merged.EntityCount != degree+1 || merged.EdgeCount != degree || merged.Active != nil {
		t.Fatalf("merged entity count=%d edge count=%d active=%v (%v)", merged.EntityCount, merged.EdgeCount, merged.Active != nil, err)
	}
	t.Logf("%d fragment read RPCs to merge %d incoming edges", batched.calls.Load(), degree)
	if batched.calls.Load() > 2048 {
		t.Fatal("merging a hub consumed an excessive fraction of the read budget")
	}
	batched.calls.Store(0)
	split := CommitRequest{Mutations: graph.Mutations{SplitEntities: []graph.SplitRequest{
		{SourceID: "retained", Entities: []graph.Entity{{ID: "split-a", Kind: "host"}, {ID: "split-b", Kind: "host"}}},
	}}}
	plan, err := BuildPlan(ctx, View{Graph: merged, Remote: batched}, split, time.Now().UTC())
	if err != nil || plan.EntityCount != degree+2 || plan.EdgeCount != 0 {
		t.Fatalf("hub split plan has %d entities and %d edges (%v)", plan.EntityCount, plan.EdgeCount, err)
	}
	t.Logf("%d fragment read RPCs to plan splitting a hub with %d incoming edges", batched.calls.Load(), degree)
	batched.calls.Store(0)
	result, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "delete-hub", Mutations: graph.Mutations{DeleteEntities: []string{"retained"}}})
	if err != nil || result.Version != g.Version+2 {
		t.Fatalf("hub delete result %+v (%v)", result, err)
	}
	after, err := remote.Describe(ctx, engine.Home, g.Tenant)
	if err != nil || after.EntityCount != degree || after.EdgeCount != 0 || after.Active != nil {
		t.Fatalf("published entity count=%d edge count=%d active=%v (%v)", after.EntityCount, after.EdgeCount, after.Active != nil, err)
	}
	t.Logf("%d fragment read RPCs for %d incoming edges", batched.calls.Load(), degree)
	if batched.calls.Load() > 1024 {
		t.Fatal("deleting a hub consumed an excessive fraction of the read budget")
	}
}

func TestDistributedHubBudgetFailureCanRetryOnNewVersion(t *testing.T) {
	ctx := t.Context()
	const degree = 8200
	engine, remote, initial := seedHubGraph(t, degree)
	remote.revision = MutationBudgetRestartRevision
	batched := &scanRPCRemote{Remote: remote, stores: remote.stores}
	engine.Remote = batched
	request := CommitRequest{IdempotencyKey: "delete-hub", Mutations: graph.Mutations{DeleteEntities: []string{"hub"}}}
	remote.lose = "complete"
	var budget *MutationBudgetError
	if _, err := engine.Submit(ctx, request); !errors.As(err, &budget) {
		t.Fatalf("expected budget failure after lost completion response, got %v", err)
	}
	failed, err := remote.Transaction(ctx, engine.Home, initial.Tenant, initial.Generation, "direct:delete-hub")
	if err != nil || failed.State != "retryable" || len(failed.Inputs) != 0 || len(failed.Participants) != 0 {
		t.Fatalf("missing completed retryable receipt: %+v (%v)", failed, err)
	}
	g, err := remote.Describe(ctx, engine.Home, initial.Tenant)
	if err != nil || g.Active != nil || g.Version != initial.Version || g.Roots != initial.Roots || g.DataHash != initial.DataHash {
		t.Fatalf("budget failure changed the graph: version=%d active=%v (%v)", g.Version, g.Active != nil, err)
	}
	batched.calls.Store(0)
	if _, err := engine.Submit(ctx, request); !errors.As(err, &budget) || batched.calls.Load() != 0 {
		t.Fatalf("same version repeated planning: %d reads (%v)", batched.calls.Load(), err)
	}
	changed := request
	changed.Mutations = graph.Mutations{DeleteEntities: []string{"retained"}}
	if _, err := engine.Submit(ctx, changed); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("budget receipt allowed idempotency key to accept different content: %v", err)
	}
	edges := make([]string, degree-5001)
	for i := range edges {
		edges[i] = graph.CanonicalEdgeIDParts("calls", fmt.Sprintf("source-%05d", i), "hub")
	}
	for i, chunk := range [][]string{edges[:1600], edges[1600:]} {
		if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: fmt.Sprintf("trim-%d", i), Mutations: graph.Mutations{DeleteEdges: chunk}}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := engine.SubmitWithGraph(ctx, request, &initial)
	if err != nil || result.Version != initial.Version+3 || result.IdempotentReplay {
		t.Fatalf("retry against the new graph version: %+v (%v)", result, err)
	}
	replay, err := engine.Submit(ctx, request)
	if err != nil || !replay.IdempotentReplay || replay.Version != result.Version || replay.DataHash != result.DataHash {
		t.Fatalf("successful retry did not publish exactly once: %+v (%v)", replay, err)
	}
	final, err := remote.Describe(ctx, engine.Home, initial.Tenant)
	if err != nil || final.Active != nil || final.EntityCount != initial.EntityCount-1 || final.EdgeCount != 0 {
		t.Fatalf("unexpected graph after retry: entities=%d edges=%d active=%v (%v)", final.EntityCount, final.EdgeCount, final.Active != nil, err)
	}
}

func TestDistributedBudgetRetryPreservesLegacyReceiptsAndExpectedVersion(t *testing.T) {
	for _, test := range []struct {
		name     string
		revision int
	}{
		{name: "legacy completion", revision: LargeFragmentRevision},
		{name: "version precondition", revision: MutationBudgetRestartRevision},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			engine, remote := newLocalGraph(t)
			remote.revision = test.revision
			schema, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}}})
			if err != nil {
				t.Fatal(err)
			}
			request := CommitRequest{IdempotencyKey: "bounded", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "one", Kind: "host"}}}}
			if test.revision == MutationBudgetRestartRevision {
				request.ExpectedVersion = &schema.Version
			}
			engine.Remote = readBudgetRejectingRemote{remote}
			var budget *MutationBudgetError
			if _, err := engine.Submit(ctx, request); !errors.As(err, &budget) {
				t.Fatalf("first planning failure: %v", err)
			}
			engine.Remote = remote
			if _, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "other", Kind: "host"}}}}); err != nil {
				t.Fatal(err)
			}
			_, err = engine.Submit(ctx, request)
			if test.revision == MutationBudgetRestartRevision {
				if !errors.Is(err, storage.ErrConflict) {
					t.Fatalf("retry bypassed expected_version: %v", err)
				}
			} else if !errors.As(err, &budget) {
				t.Fatalf("legacy completion did not retain original failure: %v", err)
			}
		})
	}
}

func TestImportPlanBatchesRepeatedEndpoints(t *testing.T) {
	ctx := t.Context()
	engine, remote := newLocalGraph(t)
	mutations := graph.Mutations{
		UpsertCITypes:       []graph.CIType{{Name: "host"}},
		UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true}},
		UpsertEntities:      []graph.Entity{{ID: "source", Kind: "host"}},
	}
	chunk := ImportChunk{}
	for i := range 16 {
		id := fmt.Sprintf("target-%02d", i)
		mutations.UpsertEntities = append(mutations.UpsertEntities, graph.Entity{ID: id, Kind: "host"})
		edge := graph.Edge{Type: "calls", From: "source", To: id}
		edge.ID = graph.CanonicalEdgeID(edge)
		chunk.Edges = append(chunk.Edges, edge)
	}
	if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "seed-import", Mutations: mutations}); err != nil {
		t.Fatal(err)
	}
	g, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil {
		t.Fatal(err)
	}
	g.State = "initializing"
	batched := &scanRPCRemote{Remote: remote, stores: remote.stores}
	single := &countedReadRemote{Remote: remote}
	want, err := BuildImportPlan(ctx, View{Graph: g, Remote: single}, chunk)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildImportPlan(ctx, View{Graph: g, Remote: batched}, chunk)
	if err != nil || !reflect.DeepEqual(got.Changes, want.Changes) {
		t.Fatalf("batched import changed the planned rows: %v", err)
	}
	t.Logf("single fragment RPCs: %d, batched fragment RPCs: %d", single.calls, batched.calls.Load())
	if batched.calls.Load() > 4 || batched.calls.Load() >= int64(single.calls) {
		t.Fatal("import plan did not batch primary edge and endpoint reads")
	}
	chunk.Edges[0].To = "missing"
	chunk.Edges[0].ID = graph.CanonicalEdgeID(chunk.Edges[0])
	if _, err := BuildImportPlan(ctx, View{Graph: g, Remote: batched}, chunk); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("import accepted an edge with a missing endpoint: %v", err)
	}
}

func TestDistributedGraphCommitQueryAndRecovery(t *testing.T) {
	for _, lost := range []string{"begin", "prepare", "prepared", "commit", "finalize", "complete"} {
		t.Run(lost, func(t *testing.T) {
			ctx := context.Background()
			engine, remote := newLocalGraph(t)
			mutations := graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Unique: true}}}}, UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true, Cardinality: graph.ManyToOne}}}
			if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "schema", Mutations: mutations}); err != nil {
				t.Fatal(err)
			}
			left := "left"
			right := "right"
			for Bucket(left)%2 == Bucket(right)%2 {
				right += "x"
			}
			request := CommitRequest{IdempotencyKey: "write", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: left, Kind: "host", Fields: graph.Fields{"name": "a"}}, {ID: right, Kind: "host", Fields: graph.Fields{"name": "b"}}}, UpsertEdges: []graph.Edge{{Type: "calls", From: left, To: right}}}}
			remote.lose = lost
			resolved, err := engine.Submit(ctx, request)
			unknown := lost == "begin" || lost == "prepare" || lost == "prepared"
			if unknown && !errors.Is(err, context.DeadlineExceeded) || !unknown && (err != nil || resolved.Version != 2) {
				t.Fatalf("lost response recovery: %+v %v", resolved, err)
			}
			if lost == "prepare" {
				active, err := remote.Describe(ctx, engine.Home, "t")
				if err != nil || active.Active == nil {
					t.Fatalf("missing durable prepare: %v", err)
				}
				ref := active.Active.Inputs[0]
				homeStore := remote.stores[engine.Home.ID]
				key := homeStore.inputKey("t", active.Generation, ref.Stage, ref.Digest)
				bytes, err := homeStore.Objects.Get(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				if err := homeStore.Objects.Delete(ctx, key); err != nil {
					t.Fatal(err)
				}
				if err := homeStore.ValidateSnapshot(ctx, engine.Home.ID); err == nil {
					t.Fatal("snapshot accepted a lost durable input")
				}
				if err := homeStore.Objects.Put(ctx, key, bytes); err != nil {
					t.Fatal(err)
				}
				checked := false
				for _, id := range active.Active.Participants {
					group, err := active.GroupFor(id)
					if err != nil {
						t.Fatal(err)
					}
					store := remote.stores[group.ID]
					state, err := store.Data(ctx, "t", active.Generation, id)
					if err != nil {
						t.Fatal(err)
					}
					if state.Prepared == nil || state.Prepared.Root.Hash == "" {
						continue
					}
					pageKey := store.Tree("t", active.Generation, id).pageKey(state.Prepared.Root.Hash)
					page, err := store.Objects.Get(ctx, pageKey)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.Objects.Delete(ctx, pageKey); err != nil {
						t.Fatal(err)
					}
					if err := store.ValidateSnapshot(ctx, group.ID); err == nil {
						t.Fatal("snapshot accepted a lost prepared page")
					}
					if err := store.Objects.Put(ctx, pageKey, page); err != nil {
						t.Fatal(err)
					}
					checked = true
					break
				}
				if !checked {
					t.Fatal("response-loss scenario did not leave a durable prepared root")
				}
			}
			restarted := Engine{Remote: remote, Home: engine.Home, Tenant: "t"}
			current, reloadErr := remote.Describe(ctx, engine.Home, "t")
			if reloadErr != nil {
				t.Fatal(reloadErr)
			}
			if lost == "prepared" && (current.Active == nil || len(current.Active.Prepared) == 0) {
				t.Fatal("lost prepare acknowledgement did not leave a durable control receipt")
			}
			if current.Active != nil {
				if _, err := restarted.Resume(ctx); err != nil {
					t.Fatal(err)
				}
			}
			result, err := restarted.Submit(ctx, request)
			if err != nil || !result.IdempotentReplay || result.Version != 2 {
				t.Fatalf("replayed result: %+v %v", result, err)
			}
			g, err := remote.Describe(ctx, engine.Home, "t")
			if err != nil {
				t.Fatal(err)
			}
			if g.EntityCount != 2 || g.EdgeCount != 1 {
				t.Fatalf("record counts: %d/%d", g.EntityCount, g.EdgeCount)
			}
			view := View{Graph: g, Remote: remote}
			for _, q := range []query.Request{{Op: "match", Kind: "host", Limit: 10}, {Op: "match", Limit: 10}, {Op: "neighbors", ID: left, Direction: "out", Limit: 10}, {Op: "neighbors", ID: right, Direction: "in", Limit: 10}, {Op: "shortest_path", ID: left, TargetID: right, Direction: "out", Depth: 3}} {
				response, err := ExecuteQuery(ctx, view, q)
				if err != nil {
					t.Fatalf("query %+v: %v", q, err)
				}
				want := 1
				if q.Op == "match" {
					want = 2
				}
				if len(response.Results) != want {
					t.Fatalf("query %+v returned %d", q, len(response.Results))
				}
			}
			baseline := graph.New()
			if err := baseline.ApplyCommit(graph.Commit{ID: "schema", Version: 1, Mutations: mutations}); err != nil {
				t.Fatal(err)
			}
			stored, err := remote.stores["a"].Transaction(ctx, "t", 1, "direct:write")
			if err != nil {
				t.Fatal(err)
			}
			if err := baseline.ApplyCommit(graph.Commit{ID: "write", Version: 2, CreatedAt: stored.At, Mutations: request.Mutations}); err != nil {
				t.Fatal(err)
			}
			hash, err := baseline.ContentHash()
			if err != nil || hash != g.DataHash {
				t.Fatalf("logical hash differs: %s vs %s (%v)", hash, g.DataHash, err)
			}
			_, err = restarted.Submit(ctx, CommitRequest{IdempotencyKey: "duplicate", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "duplicate", Kind: "host", Fields: graph.Fields{"name": "a"}}}}})
			if !errors.Is(err, storage.ErrInvalidArgument) {
				t.Fatalf("cross-partition unique constraint was ignored: %v", err)
			}
			_, err = restarted.Submit(ctx, CommitRequest{IdempotencyKey: "cardinality", Mutations: graph.Mutations{UpsertEdges: []graph.Edge{{Type: "calls", From: left, To: left}}}})
			if !errors.Is(err, storage.ErrInvalidArgument) {
				t.Fatalf("cross-partition cardinality was ignored: %v", err)
			}
			latest, err := remote.Describe(ctx, engine.Home, "t")
			if err != nil || latest.Version != 2 || latest.Active != nil {
				t.Fatalf("aborted transactions changed graph: %+v %v", latest, err)
			}
			deleteRequest := CommitRequest{IdempotencyKey: "delete", Mutations: graph.Mutations{DeleteEntities: []string{right}}}
			pageRequest := query.Request{Op: "match", Kind: "host", Limit: 1}
			page, err := ExecutePinnedQuery(ctx, remote, engine.Home, g, pageRequest)
			if err != nil || page.NextCursor == "" || len(page.Results) != 1 {
				t.Fatalf("first pinned page: %+v %v", page, err)
			}
			if _, err := restarted.Submit(ctx, deleteRequest); err != nil {
				t.Fatal(err)
			}
			oldResponse, err := ExecuteQuery(ctx, view, query.Request{Op: "neighbors", ID: left, Direction: "out", Limit: 10})
			if err != nil || len(oldResponse.Results) != 1 {
				t.Fatalf("old immutable view changed: %+v %v", oldResponse, err)
			}
			latest, err = remote.Describe(ctx, engine.Home, "t")
			if err != nil {
				t.Fatal(err)
			}
			pageRequest.Cursor = page.NextCursor
			secondPage, err := ExecutePinnedQuery(ctx, remote, engine.Home, latest, pageRequest)
			if err != nil || secondPage.Version != 2 || len(secondPage.Results) != 1 || secondPage.Results[0].Entity.ID == page.Results[0].Entity.ID {
				t.Fatalf("paging switched graph versions: %+v %v", secondPage, err)
			}
			altered := pageRequest
			altered.Kind = "another-kind"
			if _, err := ExecutePinnedQuery(ctx, remote, engine.Home, latest, altered); !errors.Is(err, storage.ErrInvalidArgument) {
				t.Fatalf("cursor accepted different query: %v", err)
			}
			pins, err := remote.stores["a"].Objects.List(ctx, remote.stores["a"].pinPrefix("t", 1))
			if err != nil || len(pins) != 1 {
				t.Fatalf("persistent read pins: %v %v", pins, err)
			}
			var pin Pin
			if err := remote.stores["a"].load(ctx, pins[0].Key, &pin); err != nil {
				t.Fatal(err)
			}
			if _, err := remote.stores["a"].Pinned(ctx, "t", pin.ID, pin.Query, pin.ExpiresAt); !errors.Is(err, storage.ErrInvalidArgument) {
				t.Fatalf("expired cursor remained usable: %v", err)
			}
			response, err := ExecuteQuery(ctx, View{Graph: latest, Remote: remote}, query.Request{Op: "neighbors", ID: left, Direction: "out", Limit: 10})
			if err != nil || len(response.Results) != 0 {
				t.Fatalf("cascade deletion failed: %+v %v", response, err)
			}
			if latest.EntityCount != 1 || latest.EdgeCount != 0 {
				t.Fatal(fmt.Sprintf("counts after deletion: %d/%d", latest.EntityCount, latest.EdgeCount))
			}
			maximum := 1
			latest.Config.Quota.MaxEntitiesPerTenant = &maximum
			if err := remote.stores["a"].save(ctx, remote.stores["a"].graphKey("t"), latest); err != nil {
				t.Fatal(err)
			}
			extra := "quota"
			for Bucket(extra)%2 == Bucket(left)%2 {
				extra += "x"
			}
			quotaRequest := CommitRequest{IdempotencyKey: "quota", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: extra, Kind: "host", Fields: graph.Fields{"name": "quota"}}}}}
			for range 2 {
				if _, err := restarted.Submit(ctx, quotaRequest); !errors.Is(err, ErrQuotaExceeded) {
					t.Fatalf("global quota not preserved: %v", err)
				}
			}
			if current, err := remote.Describe(ctx, engine.Home, "t"); err != nil || current.Version != latest.Version || current.EntityCount != 1 {
				t.Fatalf("quota failure changed graph: %+v %v", current, err)
			}

		})
	}
}

func TestDistributedGraphNormalizedEdgeConstraints(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	commit := func(key string, m graph.Mutations) {
		t.Helper()
		if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: key, Mutations: m}); err != nil {
			t.Fatal(err)
		}
	}
	commit("schema", graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}, UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true, Cardinality: graph.ManyToOne}}})
	commit("data", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "a", Kind: "host"}, {ID: "b", Kind: "host"}, {ID: "c", Kind: "host"}}, UpsertEdges: []graph.Edge{{From: "a", To: "b", Type: "calls"}}})
	if _, err := engine.Submit(ctx, CommitRequest{IdempotencyKey: "normalized-cardinality", Mutations: graph.Mutations{UpsertEdges: []graph.Edge{{From: " a ", To: " c ", Type: " calls "}}}}); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("canonical cardinality directory was bypassed by unnormalized input: %v", err)
	}
	commit("normalized-delete", graph.Mutations{DeleteEdgeRequests: []graph.EdgeDeleteRequest{{Type: " calls ", From: " a ", To: " b "}}})
	g, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil || g.EdgeCount != 0 || g.EdgeTypeCounts["calls"] != 0 {
		t.Fatalf("normalized endpoint deletion: version=%d edges=%d counts=%v error=%v", g.Version, g.EdgeCount, g.EdgeTypeCounts, err)
	}
}

func TestDistributedGraphScanDoesNotLoadAllPartitionPayloads(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	if _, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"payload": {Type: "string"}}}}}}); err != nil {
		t.Fatal(err)
	}
	used := map[uint8]bool{}
	var entities []graph.Entity
	for i := 0; len(entities) < 64; i++ {
		id := fmt.Sprintf("node-%03d", i)
		bucket := Bucket(id)
		if used[bucket] {
			continue
		}
		used[bucket] = true
		entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"payload": strings.Repeat("x", 2048)}})
	}
	if _, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{UpsertEntities: entities}}); err != nil {
		t.Fatal(err)
	}
	g, err := remote.Describe(ctx, engine.Home, "t")
	if err != nil {
		t.Fatal(err)
	}
	response, err := ExecuteQuery(ctx, View{Graph: g, Remote: remote, Budget: &ReadBudget{Requests: 2048, Bytes: 32 << 10}}, query.Request{Op: "match", Kind: "host", Limit: 1})
	if err != nil || len(response.Results) != 1 || len(response.Results[0].Entity.Fields["payload"].(string)) != 2048 {
		t.Fatalf("bounded scan fetched every partition payload: results=%d error=%v", len(response.Results), err)
	}
}

func TestSharedReadLeaseSurvivesLegacyPinCapacityAndRenews(t *testing.T) {
	ctx := context.Background()
	engine, remote := newLocalGraph(t)
	if _, err := engine.Submit(ctx, CommitRequest{Mutations: graph.Mutations{
		UpsertCITypes:  []graph.CIType{{Name: "host"}},
		UpsertEntities: []graph.Entity{{ID: "a", Kind: "host"}, {ID: "b", Kind: "host"}, {ID: "c", Kind: "host"}},
	}}); err != nil {
		t.Fatal(err)
	}
	store := remote.stores[engine.Home.ID]
	g, err := store.GetGraph(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(ctx, "t", g.Generation, g.Version); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("commit created an unleased historical view: %v", err)
	}
	now := time.Now().UTC()
	for i := range 4096 {
		id := fmt.Sprintf("%032x", i+1)
		pin := Pin{ID: id, Generation: g.Generation, Version: g.Version, Query: strings.Repeat("a", 64), ExpiresAt: now.Add(CursorLifetime), KeepUntil: now.Add(CursorLifetime + MaxQueryLifetime)}
		if err := store.save(ctx, store.pinKey("t", g.Generation, id), pin); err != nil {
			t.Fatal(err)
		}
	}
	request := query.Request{Op: "match", Kind: "host", Limit: 1}
	page, err := ExecutePinnedQuery(ctx, remote, engine.Home, g, request)
	if err != nil || page.NextCursor == "" {
		t.Fatalf("legacy pin capacity blocked a new read: %+v %v", page, err)
	}
	g, err = store.GetGraph(ctx, "t")
	if err != nil || g.ReadLease == nil {
		t.Fatalf("no reusable lease: %v", err)
	}
	// A ready lease does not invoke Action, including for abandoned first pages.
	remote.lose = "pin"
	for range 40 {
		if _, err := ExecutePinnedQuery(ctx, remote, engine.Home, g, request); err != nil {
			t.Fatal(err)
		}
	}
	remote.lose = ""
	pins, err := store.Objects.List(ctx, store.pinPrefix("t", g.Generation))
	if err != nil || len(pins) != 4097 {
		t.Fatalf("first pages created individual pins: %d %v", len(pins), err)
	}
	lease := *g.ReadLease
	lease.ExpiresAt = now.Add(time.Minute)
	lease.KeepUntil = lease.ExpiresAt.Add(MaxQueryLifetime)
	if err := store.save(ctx, store.pinKey("t", g.Generation, lease.ID), lease); err != nil {
		t.Fatal(err)
	}
	request.Cursor = page.NextCursor
	next, err := ExecutePinnedQuery(ctx, remote, engine.Home, g, request)
	if err != nil || len(next.Results) != 1 || next.Results[0].Entity.ID != "b" || next.NextCursor == "" {
		t.Fatalf("continuation failed: %+v %v", next, err)
	}
	renewed, err := store.Pinned(ctx, "t", lease.ID, "", now)
	if err != nil || !renewed.Pin.ExpiresAt.After(lease.ExpiresAt) {
		t.Fatalf("cursor did not renew its live lease: %v", err)
	}
}
