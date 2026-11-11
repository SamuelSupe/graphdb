package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestDistributedGraphCommitQueryAndRecovery(t *testing.T) {
	for _, lost := range []string{"begin", "prepare", "commit", "finalize", "complete"} {
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
			unknown := lost == "begin" || lost == "prepare"
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
