package ha

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestPartitionSnapshotRoundTripWithMetadataNames(t *testing.T) {
	const prefix, tenant = "archive/pins", "views"
	open := func() *Application {
		t.Helper()
		files, err := storage.OpenFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { files.Close() })
		return &Application{Files: files, Store: storage.NewTenantStore(files, prefix), ShardID: "home", MaxSnapshotBytes: 4 << 20}
	}
	source, target := open(), open()
	ctx := t.Context()
	g := partition.Graph{Tenant: tenant, Format: partition.FormatVersion, Generation: 1}
	for i := range g.Partitions {
		g.Partitions[i] = partition.Placement{Group: "data", Epoch: 1}
	}
	_, err := source.Files.ApplyReplicated(ctx, 1, "init", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		store := source.partitionStore()
		if _, err := store.Apply(ctx, partition.Action{Operation: "initialize", Tenant: tenant, Generation: 1, Graph: &g}); err != nil {
			return nil, err
		}
		_, err := store.Apply(ctx, partition.Action{Operation: "activate", Tenant: tenant, Generation: 1})
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Restore(ctx, 1, snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := target.partitionStore().GetGraph(ctx, tenant)
	if err != nil || restored.State != "active" || restored.Generation != g.Generation || restored.Partitions != g.Partitions {
		t.Fatalf("restored graph: generation=%d err=%v", restored.Generation, err)
	}
	if view, err := target.partitionStore().Snapshot(ctx, tenant, g.Generation, g.Version); err != nil || view.Partitions != g.Partitions {
		t.Fatalf("restored read view: version=%d err=%v", view.Version, err)
	}
}

func TestPendingGCPartitionsSpansPlacements(t *testing.T) {
	var g partition.Graph
	// An uneven layout can leave all the lowest pending IDs on one group.
	for i := range g.Partitions {
		group := "a"
		if i >= 224 {
			group = "b"
		}
		g.Partitions[i] = partition.Placement{Group: group, Epoch: 2}
	}
	for _, tc := range []struct {
		name, phase string
		versions    []int64
		wantB       int
	}{
		{name: "running", phase: "running", wantB: 31},
		{name: "starting with one protected view", phase: "starting", versions: []int64{3}, wantB: 31},
		{name: "starting with pinned history", phase: "starting", versions: []int64{1, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g.GC = &partition.GraphGC{Phase: tc.phase, Versions: tc.versions}
			if tc.phase == "starting" {
				g.GC.Started[0], g.GC.Started[255] = true, true
			} else {
				g.GC.Done[0], g.GC.Done[255] = true, true
			}
			counts := map[string]int{}
			for _, id := range pendingGCPartitions(g, partition.GCSchedulingProtocol) {
				if id == 0 || id == 255 {
					t.Fatalf("scheduled completed partition %d", id)
				}
				counts[g.Partitions[id].Group]++
			}
			if counts["a"] != 32 || counts["b"] != tc.wantB {
				t.Fatalf("GC scheduled unexpected per-group counts: %v", counts)
			}
		})
	}
}

func newTestPartitionedGraph(t *testing.T, wal bool, homeProtocol int) (a, b, catalog *testCluster, router *sharding.Router, call func(string, string, string, any, int) *httptest.ResponseRecorder) {
	t.Helper()
	catalog = newTestClusterRole(t, false, "", true)
	a = newTestClusterRole(t, wal, "a", false)
	b = newTestClusterRole(t, wal, "b", false)
	for _, group := range []*testCluster{catalog, a, b} {
		for i := range group.nodes {
			group.stop(i)
		}
		for i, node := range group.nodes {
			node.cfg.Raft.Protocol = 5
			if group == a || group == b && homeProtocol >= partition.BatchProtocol {
				node.cfg.Raft.Protocol = homeProtocol
			}
			node.cfg.Raft.SnapshotEntries = 1000
			if wal {
				node.cfg.IngestFlushInterval = time.Hour
			}
			group.start(i)
		}
		group.leader(-1)
	}
	definition := func(group *testCluster, id string) sharding.Shard {
		cfg := group.nodes[0].cfg.Raft
		return sharding.Shard{ID: id, ClusterID: cfg.ClusterID, Peers: cfg.Peers}
	}
	token := catalog.nodes[0].cfg.Raft.Token
	router = sharding.NewRouter(definition(catalog, "catalog"), token)
	t.Cleanup(router.Client.HTTP.CloseIdleConnections)
	call = func(method, path, tenant string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var response *httptest.ResponseRecorder
		for {
			request := httptest.NewRequest(method, path, bytes.NewReader(encoded)).WithContext(ctx)
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			if tenant != "" {
				request.Header.Set("X-Tenant-ID", tenant)
			}
			response = httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code == want || response.Code < 500 || ctx.Err() != nil {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(30 * time.Millisecond):
			}
		}
		if response.Code != want {
			t.Fatalf("%s %s returned %d: %s", method, path, response.Code, response.Body.String())
		}
		return response
	}
	for _, item := range []struct {
		group *testCluster
		id    string
	}{{a, "a"}, {b, "b"}} {
		call(http.MethodPost, "/v1/cluster/shards", "", definition(item.group, item.id), http.StatusAccepted)
	}
	call(http.MethodPost, "/v1/tenants", "", map[string]any{"tenant_id": "big"}, http.StatusOK)

	call(http.MethodPost, "/v1/cluster/graphs", "", map[string]any{"tenant_id": "big", "data_groups": []string{"a", "b"}}, http.StatusAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		var state sharding.Catalog
		if err := router.Client.JSON(ctx, definition(catalog, "catalog"), http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
			t.Fatal(err)
		}
		p := state.Tenants["big"]
		if p.Mode == "partitioned" && p.State == "active" {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("partition enable did not complete: %+v", p)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return a, b, catalog, router, call
}

func TestHAPartitionTopologyFollowsCatalogPeerUpdate(t *testing.T) {
	_, data, _, _, call := newTestPartitionedGraph(t, false, partition.GroupTopologyProtocol)
	var reads atomic.Int64
	cfg := data.nodes[0].cfg.Raft
	updated := sharding.Shard{ID: "b", ClusterID: cfg.ClusterID, Peers: make(map[uint64]string, len(cfg.Peers))}
	for id, address := range cfg.Peers {
		original, err := url.Parse(address)
		if err != nil {
			t.Fatal(err)
		}
		forward := httputil.NewSingleHostReverseProxy(original)
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/cluster/graph/read" || r.URL.Path == "/cluster/graph/read-batch" {
				reads.Add(1)
			}
			forward.ServeHTTP(w, r)
		}))
		t.Cleanup(peer.Close)
		updated.Peers[id] = peer.URL
	}
	call(http.MethodPost, "/v1/cluster/shards", "", updated, http.StatusAccepted)
	var g partition.Graph
	deadline := time.Now().Add(15 * time.Second)
	for {
		response := call(http.MethodGet, "/v1/cluster/graphs/big", "", nil, http.StatusOK)
		g = partition.Graph{}
		if err := json.Unmarshal(response.Body.Bytes(), &g); err != nil {
			t.Fatal(err)
		}
		if g.CatalogVersion > 0 && maps.Equal(g.Groups["b"].Peers, updated.Peers) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("graph still reports old peers at catalog version %d: %v", g.CatalogVersion, g.Groups["b"].Peers)
		}
		time.Sleep(50 * time.Millisecond)
	}
	id := "remote"
	for g.Partitions[partition.Bucket(id)].Group != "b" {
		id += "x"
	}
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{IdempotencyKey: "catalog-peers", Mutations: graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}}, UpsertEntities: []graph.Entity{{ID: id, Kind: "host"}},
	}}, http.StatusOK)
	before := reads.Load()
	call(http.MethodGet, "/v1/entities/"+id, "big", nil, http.StatusOK)
	if reads.Load() == before {
		t.Fatal("graph read used the old fragment addresses")
	}
}

func TestHACancelCopiedConversionRestoresSourceAndAllowsRetry(t *testing.T) {
	a, b, catalog, router, call := newTestPartitionedGraph(t, false, partition.ConversionCancellationProtocol)
	for i := range catalog.nodes {
		catalog.stop(i)
	}
	for i, node := range catalog.nodes {
		node.cfg.Raft.Protocol = partition.ConversionCancellationProtocol
		catalog.start(i)
	}
	catalog.leader(-1)
	const tenant = "cancel-copy"
	const graphPath = "/v1/cluster/graphs/" + tenant
	const entityPath = "/v1/entities/retained"
	call(http.MethodPost, "/v1/tenants", "", map[string]any{"tenant_id": tenant}, http.StatusOK)
	call(http.MethodPost, "/v1/commits", tenant, partition.CommitRequest{IdempotencyKey: "source", Mutations: graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}}, UpsertEntities: []graph.Entity{{ID: "retained", Kind: "host"}},
	}}, http.StatusOK)
	call(http.MethodPost, "/v1/indexes/rebuild", tenant, nil, http.StatusOK)

	var seen atomic.Int64
	var pause atomic.Bool
	pause.Store(true)
	hold := func(w http.ResponseWriter, _ *http.Request, _ *Cluster, action partition.Action) bool {
		if action.Operation != "activate" || action.Tenant != tenant || !pause.Load() {
			return false
		}
		seen.Store(action.Generation)
		http.Error(w, "hold graph activation", http.StatusServiceUnavailable)
		return true
	}
	for _, group := range []*testCluster{a, b} {
		for _, node := range group.nodes {
			node.graphAction.Store(&hold)
		}
	}
	enable := map[string]any{"tenant_id": tenant, "data_groups": []string{"a", "b"}}
	call(http.MethodPost, "/v1/cluster/graphs", "", enable, http.StatusAccepted)
	deadline := time.Now().Add(20 * time.Second)
	for seen.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	abandoned := seen.Load()
	if abandoned == 0 {
		t.Fatal("copy did not reach the activation boundary")
	}
	for _, node := range a.nodes {
		node.blockConversionReclaim.Store(true)
	}
	call(http.MethodPost, graphPath+"/cancel", "", nil, http.StatusAccepted)
	var restored sharding.Placement
	deadline = time.Now().Add(20 * time.Second)
	for {
		response := call(http.MethodGet, graphPath, "", nil, http.StatusOK)
		restored = sharding.Placement{}
		if err := json.Unmarshal(response.Body.Bytes(), &restored); err != nil {
			t.Fatal(err)
		}
		if restored.State == "active" && restored.Mode != "partitioned" && restored.ConversionCleanup != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancelled conversion did not restore source routing: %s", response.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if restored.GraphGenerationFloor != abandoned {
		t.Fatalf("abandoned generation fence = %d, want %d", restored.GraphGenerationFloor, abandoned)
	}
	call(http.MethodGet, entityPath, tenant, nil, http.StatusOK)
	call(http.MethodPost, "/v1/commits", tenant, partition.CommitRequest{IdempotencyKey: "source-while-cleaning", Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "retained", Kind: "host", Fields: graph.Fields{"state": "during-cleanup"}}},
	}}, http.StatusOK)
	call(http.MethodPost, "/v1/indexes/rebuild", tenant, nil, http.StatusOK)
	old := catalog.leader(-1)
	catalog.stop(old)
	catalog.leader(old)
	for _, node := range a.nodes {
		node.blockConversionReclaim.Store(false)
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		response := call(http.MethodGet, graphPath, "", nil, http.StatusOK)
		restored = sharding.Placement{}
		if err := json.Unmarshal(response.Body.Bytes(), &restored); err != nil {
			t.Fatal(err)
		}
		if restored.State == "active" && restored.Mode != "partitioned" && restored.ConversionCleanup == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("new catalog leader did not finish reclamation: %s", response.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	remote := sharding.GraphRemote{Client: router.Client}
	for _, group := range []*testCluster{a, b} {
		node := group.nodes[group.leader(-1)]
		prefix := fmt.Sprintf("graphdb/control/graphs/%s/%d/", tenant, abandoned)
		if objects, err := node.files.List(t.Context(), prefix); err != nil || len(objects) != 0 {
			t.Fatalf("abandoned group %s objects=%d err=%v", node.cfg.Raft.ShardID, len(objects), err)
		}
		address := partition.Group{ID: node.cfg.Raft.ShardID, ClusterID: node.cfg.Raft.ClusterID, Peers: node.cfg.Raft.Peers}
		if err := remote.Action(t.Context(), address, partition.Action{Operation: "reserve_partitions", Tenant: tenant, Generation: abandoned, Epoch: 1, Participants: []uint8{0}}, nil); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("late reservation for cancelled generation: %v", err)
		}
	}

	pause.Store(false)
	call(http.MethodPost, "/v1/cluster/graphs", "", enable, http.StatusAccepted)
	deadline = time.Now().Add(20 * time.Second)
	for {
		response := call(http.MethodGet, graphPath, "", nil, http.StatusOK)
		var g partition.Graph
		if err := json.Unmarshal(response.Body.Bytes(), &g); err != nil {
			t.Fatal(err)
		}
		if g.Format == partition.FormatVersion && g.State == "active" {
			if g.Generation <= abandoned || g.Version != 2 {
				t.Fatalf("unexpected graph after retry: generation=%d version=%d", g.Generation, g.Version)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("conversion retry did not finish: %s", response.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	call(http.MethodGet, entityPath, tenant, nil, http.StatusOK)
	call(http.MethodPost, graphPath+"/cancel", "", nil, http.StatusConflict)
}

func TestHAPartitionGCRecoversUnacknowledgedFragments(t *testing.T) {
	a, b, _, _, call := newTestPartitionedGraph(t, false, partition.GCSchedulingProtocol)
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{IdempotencyKey: "schema",
		Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}}}, http.StatusOK)
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{IdempotencyKey: "entity",
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "retained", Kind: "host"}}}}, http.StatusOK)
	const graphPath = "/v1/cluster/graphs/big"
	describe := func() partition.Graph {
		t.Helper()
		response := call(http.MethodGet, graphPath, "", nil, http.StatusOK)
		var g partition.Graph
		if err := json.Unmarshal(response.Body.Bytes(), &g); err != nil {
			t.Fatal(err)
		}
		return g
	}
	id := partition.Bucket("retained")
	previous := describe().Roots[id]
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{IdempotencyKey: "update",
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "retained", Kind: "host", Fields: graph.Fields{"value": "current"}}}}}, http.StatusOK)
	before := describe()
	if previous.Hash == "" || previous.Hash == before.Roots[id].Hash {
		t.Fatal("update did not produce an obsolete fragment root")
	}
	old := a.leader(-1)
	completed := make(chan struct{}, 1)
	hook := func(_ http.ResponseWriter, r *http.Request, _ *Cluster, action partition.Action) bool {
		if action.Operation != "graph_gc_done" || len(action.Participants) <= partition.MaxFragmentBatch {
			return false
		}
		// The data groups finished this turn. Stop the home leader before it
		// can record their completion in the graph descriptor.
		select {
		case completed <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return true
	}
	a.nodes[old].graphAction.Store(&hook)
	call(http.MethodPost, graphPath+"/gc", "", nil, http.StatusAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("GC did not finish a turn spanning more than one batch of partitions")
	}
	a.stop(old)
	a.leader(old)
	for {
		g := describe()
		if g.GC == nil && g.LastGCAt.After(before.LastGCAt) {
			if g.Version != before.Version || g.DataHash != before.DataHash {
				t.Fatal("GC recovery changed the published graph")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the new leader did not finish the interrupted GC")
		case <-time.After(50 * time.Millisecond):
		}
	}
	call(http.MethodGet, "/v1/entities/retained", "big", nil, http.StatusOK)
	owner := a
	if before.Partitions[id].Group == "b" {
		owner = b
	}
	node := owner.nodes[owner.leader(-1)]
	tree := (partition.Store{Prefix: node.store.Prefix}).Tree("big", before.Generation, id)
	if _, err := node.files.Get(ctx, tree.Prefix+"/"+previous.Hash[:2]+"/"+previous.Hash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("recovered GC retained the obsolete page: %v", err)
	}
}

func TestHAPartitionSnapshotBudgetAllowsFinalizationAndGC(t *testing.T) {
	t.Run("after_publication", func(t *testing.T) { testHAPartitionSnapshotBudgetRecovery(t, false) })
	t.Run("durable_prepare", func(t *testing.T) { testHAPartitionSnapshotBudgetRecovery(t, true) })
}

func testHAPartitionSnapshotBudgetRecovery(t *testing.T, beforePublication bool) {
	t.Helper()
	a, b, _, router, call := newTestPartitionedGraph(t, false, partition.MutationBudgetRestartProtocol)
	for i := range a.nodes {
		a.stop(i)
	}
	for i, replica := range a.nodes {
		replica.cfg.Raft.SnapshotEntries = 4
		replica.cfg.Raft.ElectionTicks = 30
		a.start(i)
	}
	leader := a.nodes[a.leader(-1)]
	cluster := leader.cluster
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	remote := sharding.GraphRemote{Client: router.Client}
	home := cluster.graphHome()
	describe := func() partition.Graph {
		t.Helper()
		g, err := remote.Describe(ctx, home, "big")
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	initial := describe()
	id := "large-old-page"
	for initial.Partitions[partition.Bucket(id)].Group != home.ID {
		id += "x"
	}
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{Mutations: graph.Mutations{
		UpsertCITypes:  []graph.CIType{{Name: "host"}},
		UpsertEntities: []graph.Entity{{ID: id, Kind: "host", Fields: graph.Fields{"payload": strings.Repeat("x", 640<<10)}}},
	}}, http.StatusOK)
	base := describe()
	bucket := partition.Bucket(id)
	old := base.Roots[bucket]
	tree := cluster.App.partitionStore().Tree("big", base.Generation, bucket)
	oldPage := tree.Prefix + "/" + old.Hash[:2] + "/" + old.Hash

	groups := []*testCluster{a, b}
	blockFinalization := func(block bool) {
		for _, group := range groups {
			for _, replica := range group.nodes {
				replica.blockGraphFinalize.Store(block)
			}
		}
	}
	blockFinalization(true)
	defer blockFinalization(false)
	var prepare atomic.Pointer[partition.Action]
	if beforePublication {
		hook := func(w http.ResponseWriter, r *http.Request, cluster *Cluster, action partition.Action) bool {
			if action.Partition == bucket {
				switch action.Operation {
				case "prepare":
					prepare.Store(&action)
				case "prepared":
					http.Error(w, "home prepare registration paused", http.StatusServiceUnavailable)
					return true
				}
			}
			return false
		}
		for _, replica := range a.nodes {
			replica.graphAction.Store(&hook)
			defer replica.graphAction.Store(nil)
		}
	}
	request := partition.CommitRequest{IdempotencyKey: "replace-large-page", Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: id, Kind: "host", Fields: graph.Fields{"payload": "small"}}},
	}}
	engine := cluster.partitionEngine("big", base.Generation, !beforePublication)
	attempt, stop := context.WithTimeout(ctx, 5*time.Second)
	_, err := engine.Submit(attempt, request)
	stop()
	var unknown *partition.OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected the paused transaction to report an unknown outcome: %v", err)
	}
	paused := describe()
	if beforePublication {
		if paused.Version != base.Version || paused.Active == nil || paused.Active.State != "preparing" || paused.Active.Prepared[bucket].Txn != "" {
			t.Fatalf("transaction did not stop before home prepare registration: version=%d transaction=%+v", paused.Version, paused.Active)
		}
	} else if paused.Version != base.Version+1 || paused.Active == nil || paused.Active.State != "committed" {
		t.Fatalf("transaction did not stop after publication: version=%d transaction=%+v", paused.Version, paused.Active)
	}

	probe := partition.Action{Operation: "reserve", Tenant: "big", Generation: base.Generation, Partition: bucket, Epoch: base.Partitions[bucket].Epoch}
	if err := exceedPartitionSnapshotBudget(ctx, a, cluster, probe, 512<<10); err != nil {
		t.Fatal(err)
	}
	blockFinalization(false)
	if beforePublication {
		original := prepare.Load()
		if original == nil || original.Txn != paused.Active.ID {
			t.Fatal("original prepare request not captured")
		}
		var receipt partition.Prepared
		if err := remote.Action(ctx, home, *original, &receipt); err != nil || receipt.Txn != original.Txn || receipt.Digest != original.Digest {
			t.Fatalf("single replay did not return the durable prepare: %+v (%v)", receipt, err)
		}
		batch, err := remote.FragmentBatch(ctx, home, []partition.Action{*original})
		var batched partition.Prepared
		if err != nil || len(batch) != 1 || json.Unmarshal(batch[0], &batched) != nil || batched.Txn != receipt.Txn || batched.Root != receipt.Root {
			t.Fatalf("batch replay did not return the durable prepare: %+v (%v)", batched, err)
		}
		missing := *original
		for id, pos := range base.Partitions {
			if uint8(id) != bucket && pos.Group == home.ID && base.Roots[id].Hash == "" && !slices.Contains(paused.Active.Participants, uint8(id)) {
				missing.Partition, missing.Epoch, missing.Base = uint8(id), pos.Epoch, base.Roots[id]
				break
			}
		}
		if missing.Partition == bucket {
			t.Fatal("no unprepared partition in the local group")
		}
		missing.Changes, missing.Digest = nil, fmt.Sprintf("%x", sha256.Sum256([]byte("null")))
		if _, err := remote.FragmentBatch(ctx, home, []partition.Action{*original, missing}); !errors.Is(err, partition.ErrShardUnavailable) {
			t.Fatalf("snapshot gate accepted a batch requiring new preparation: %v", err)
		}
		mismatch := *original
		mismatch.Digest = missing.Digest
		if err := remote.Action(ctx, home, mismatch, nil); !errors.Is(err, storage.ErrInvalidArgument) {
			t.Fatalf("prepare replay accepted changed data digest: %v", err)
		}
		incomplete := partition.Action{Operation: "commit", Tenant: "big", Generation: base.Generation, Txn: paused.Active.ID,
			Sequence: paused.Active.Sequence, Result: &storage.CommitResult{Manifest: storage.Manifest{Version: base.Version + 1}}}
		if err := remote.Action(ctx, home, incomplete, nil); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("publication bypassed the missing prepare receipt: %v", err)
		}
		for _, replica := range a.nodes {
			replica.graphAction.Store(nil)
		}
	} else {
		prepared, ok := paused.Active.Prepared[bucket]
		if !ok {
			t.Fatal("published transaction has no receipt for the local partition")
		}
		finalize := partition.Action{Operation: "finalize", Tenant: "big", Generation: base.Generation, Partition: bucket,
			Epoch: probe.Epoch, Txn: paused.Active.ID, Sequence: paused.Active.Sequence, Prepared: &prepared}
		if _, err := remote.FragmentBatch(ctx, home, []partition.Action{finalize}); err != nil {
			t.Fatalf("snapshot budget blocked batched finalization: %v", err)
		}
	}
	response := call(http.MethodPost, "/v1/commits", "big", request, http.StatusOK)
	var replay storage.CommitResult
	if err := json.Unmarshal(response.Body.Bytes(), &replay); err != nil || !replay.IdempotentReplay || replay.Version != base.Version+1 {
		t.Fatalf("published result after cleanup: %+v (%v)", replay, err)
	}
	if !beforePublication && paused.DataHash != replay.DataHash {
		t.Fatal("finalization changed the published digest")
	}
	if g := describe(); g.Active != nil || g.Version != replay.Version || g.DataHash != replay.DataHash {
		t.Fatalf("recovered result does not match the graph at version %d, active=%t", g.Version, g.Active != nil)
	}
	newWrite := partition.CommitRequest{IdempotencyKey: "after-gc", Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "after-gc", Kind: "host"}}}}
	call(http.MethodPost, "/v1/commits", "big", newWrite, http.StatusServiceUnavailable)
	call(http.MethodPost, "/v1/cluster/graphs/big/gc", "", nil, http.StatusAccepted)
	for {
		g, err := remote.Describe(ctx, home, "big")
		if err == nil && g.GC == nil && cluster.Node.Status()["snapshot_error"] == nil {
			break
		}
		if err != nil && !errors.Is(err, partition.ErrShardUnavailable) && ctx.Err() == nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("GC did not restore writes: phase=%+v snapshot=%v error=%v", g.GC, cluster.Node.Status()["snapshot_error"], err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if _, err := leader.files.Get(ctx, oldPage); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("obsolete immutable page was retained: %v", err)
	}
	entity := call(http.MethodGet, "/v1/entities/"+id, "big", nil, http.StatusOK)
	if !bytes.Contains(entity.Body.Bytes(), []byte(`"payload":"small"`)) {
		t.Fatalf("GC removed the current entity: %s", entity.Body.String())
	}
	call(http.MethodPost, "/v1/commits", "big", newWrite, http.StatusOK)
}

func exceedPartitionSnapshotBudget(ctx context.Context, group *testCluster, leader *Cluster, probe partition.Action, budget int64) error {
	for _, replica := range group.nodes {
		app := replica.cluster.App
		app.mu.Lock()
		app.MaxSnapshotBytes = budget
		app.mu.Unlock()
	}
	// Re-reserve the same partition and epoch to advance the Raft index
	// without changing the graph or making any page obsolete.
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		if _, err := leader.partitionAction(probeCtx, probe); err != nil {
			if errors.Is(err, replication.ErrSnapshotTooLarge) {
				return nil
			}
			return err
		}
		select {
		case <-probeCtx.Done():
			return fmt.Errorf("did not reach the snapshot budget: %v", leader.Node.Status()["snapshot_error"])
		case <-time.After(30 * time.Millisecond):
		}
	}
}

func TestHAPartitionedGraphRoutesAndLeaderRecovery(t *testing.T) {
	a, b, catalog, router, call := newTestPartitionedGraph(t, false, 5)
	definition := func(group *testCluster, id string) sharding.Shard {
		cfg := group.nodes[0].cfg.Raft
		return sharding.Shard{ID: id, ClusterID: cfg.ClusterID, Peers: cfg.Peers}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	oversized := make([]string, 257)
	for i := range oversized {
		oversized[i] = fmt.Sprintf("group-%d", i)
	}
	call(http.MethodPost, "/v1/cluster/graphs", "", map[string]any{"tenant_id": "big", "data_groups": oversized}, http.StatusBadRequest)

	request := partition.CommitRequest{IdempotencyKey: "schema", Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}, UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true}}}}
	call(http.MethodPost, "/v1/commits", "big", request, http.StatusOK)
	// Keep an ordinary tenant's read view open on the same home leader while
	// the partitioned tenant publishes, acquires a pin, and aborts a write.
	readLeader := a.nodes[a.leader(-1)].cluster
	if _, err := readLeader.shardAction(ctx, sharding.Action{Operation: "own", Tenant: "reporting", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	seed := httptest.NewRequest(http.MethodPost, "/v1/commits", bytes.NewBufferString(`{"mutations":{"upsert_entities":[{"id":"report","kind":"host"}]}}`)).WithContext(ctx)
	seed.Header.Set("Content-Type", "application/json")
	seed.Header.Set("X-Tenant-ID", "reporting")
	seed.Header.Set(sharding.EpochHeader, "1")
	seedResponse := httptest.NewRecorder()
	readLeader.App.Handler.ServeHTTP(seedResponse, seed)
	if seedResponse.Code != http.StatusOK {
		t.Fatalf("seed reporting tenant: %d %s", seedResponse.Code, seedResponse.Body.String())
	}
	readCtx, stopRead := context.WithCancel(ctx)
	read := httptest.NewRequest(http.MethodGet, "/v1/entities", nil).WithContext(readCtx)
	read.Header.Set("X-Tenant-ID", "reporting")
	read.Header.Set(sharding.EpochHeader, "1")
	readResponse := httptest.NewRecorder()
	readEntered, finishRead, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readDone)
		readLeader.ServeRoute(readResponse, read, false, false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(readEntered)
			select {
			case <-r.Context().Done():
			case <-finishRead:
				g, manifest, err := readLeader.App.Store.Load(r.Context(), "reporting")
				if err != nil || manifest.Version != 1 || g.Entities.Len() != 1 {
					http.Error(w, fmt.Sprintf("reporting view changed: version=%d error=%v", manifest.Version, err), http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}
		}))
	}()
	defer func() { stopRead(); <-readDone }()
	select {
	case <-readEntered:
	case <-readDone:
		t.Fatalf("materialized read never acquired its view: %d %s", readResponse.Code, readResponse.Body.String())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	remote := sharding.GraphRemote{Client: router.Client}
	home := partition.Group{ID: "a", ClusterID: a.nodes[0].cfg.Raft.ClusterID, Peers: a.nodes[0].cfg.Raft.Peers}
	schema, err := remote.Describe(ctx, home, "big")
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := partition.AcquireReadView(ctx, remote, home, schema)
	if err != nil || pinned.Graph.Version != 1 {
		t.Fatalf("pin before partition commit: version=%d error=%v", pinned.Graph.Version, err)
	}
	left, right := "left", "right"
	for partition.Bucket(left)%2 == partition.Bucket(right)%2 {
		right += "x"
	}
	used := map[uint8]bool{partition.Bucket(left): true, partition.Bucket(right): true}
	pageIDs := []string{"a-page-only", "z-page-only"}
	for i, id := range pageIDs {
		for used[partition.Bucket(id)] {
			id += "x"
		}
		used[partition.Bucket(id)] = true
		pageIDs[i] = id
	}
	request = partition.CommitRequest{IdempotencyKey: "first", Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: left, Kind: "host"}, {ID: right, Kind: "host"}, {ID: pageIDs[0], Kind: "host"}, {ID: pageIDs[1], Kind: "host"}},
		UpsertEdges:    []graph.Edge{{From: left, To: right, Type: "calls"}},
	}}
	first := call(http.MethodPost, "/v1/commits", "big", request, http.StatusOK)
	var committed storage.CommitResult
	if err := json.Unmarshal(first.Body.Bytes(), &committed); err != nil || committed.Version != 2 {
		t.Fatalf("commit: %s (%v)", first.Body.String(), err)
	}
	history, err := remote.Pin(ctx, home, "big", schema.Generation, pinned.Pin.Version, pinned.Pin.Query, pinned.Pin.ID)
	if err != nil || history.Graph.Version != pinned.Graph.Version || history.Graph.DataHash != pinned.Graph.DataHash || history.Graph.Roots != pinned.Graph.Roots {
		t.Fatalf("partition commit changed the pinned graph: version=%d error=%v", history.Graph.Version, err)
	}
	for _, q := range []query.Request{{Op: "neighbors", ID: left, Direction: "out", Limit: 10}, {Op: "neighbors", ID: right, Direction: "in", Limit: 10}, {Op: "shortest_path", ID: left, TargetID: right, Direction: "out", Depth: 3}} {
		response := call(http.MethodPost, "/v1/query", "big", q, http.StatusOK)
		var result query.Response
		if json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Results) != 1 || result.Version != 2 {
			t.Fatalf("query: %s", response.Body.String())
		}
	}
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{IdempotencyKey: "missing-endpoint", Mutations: graph.Mutations{
		UpsertEdges: []graph.Edge{{From: left, To: "absent", Type: "calls"}},
	}}, http.StatusBadRequest)
	close(finishRead)
	select {
	case <-readDone:
		if readResponse.Code != http.StatusNoContent {
			t.Fatalf("partition transaction interrupted the other tenant's read: %d %s", readResponse.Code, readResponse.Body.String())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The second page may exhaust some partitions before reaching the cursor.
	// Both public entry points must continue the same immutable view.
	var listed struct {
		Version    int64          `json:"version"`
		NextCursor string         `json:"next_cursor"`
		Entities   []graph.Entity `json:"entities"`
	}
	listing := "/v1/entities?limit=2"
	page := call(http.MethodGet, listing, "big", nil, http.StatusOK)
	if err := json.Unmarshal(page.Body.Bytes(), &listed); err != nil || listed.NextCursor == "" || len(listed.Entities) != 2 {
		t.Fatalf("entity first page: %s", page.Body.String())
	}
	lastID := listed.Entities[1].ID
	page = call(http.MethodGet, listing+"&cursor="+url.QueryEscape(listed.NextCursor), "big", nil, http.StatusOK)
	listed.Entities = nil
	listed.NextCursor = ""
	if err := json.Unmarshal(page.Body.Bytes(), &listed); err != nil || listed.Version != 2 || len(listed.Entities) != 2 || listed.Entities[0].ID <= lastID || listed.NextCursor != "" {
		t.Fatalf("entity second page: %s", page.Body.String())
	}
	match := query.Request{Op: "match", Kind: "host", Limit: 2}
	page = call(http.MethodPost, "/v1/query", "big", match, http.StatusOK)
	var matched query.Response
	if err := json.Unmarshal(page.Body.Bytes(), &matched); err != nil || matched.NextCursor == "" {
		t.Fatalf("match first page: %s", page.Body.String())
	}
	match.Cursor = matched.NextCursor
	page = call(http.MethodPost, "/v1/query", "big", match, http.StatusOK)
	matched = query.Response{}
	if err := json.Unmarshal(page.Body.Bytes(), &matched); err != nil || matched.Version != 2 || len(matched.Results) != 2 || matched.Results[0].Entity.ID <= lastID {
		t.Fatalf("match second page: %s", page.Body.String())
	}
	// Exercise the real private multi-key path under a fixed published root.
	descriptor, err := remote.Describe(ctx, home, "big")
	if err != nil {
		t.Fatal(err)
	}
	missing := partition.Action{Operation: "begin", Tenant: "missing-graph", Generation: 1}
	if err := remote.Action(ctx, home, missing, nil); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing graph scope: %v", err)
	}
	leftBucket := partition.Bucket(left)
	owner, err := descriptor.GroupFor(leftBucket)
	if err != nil {
		t.Fatal(err)
	}
	stale := partition.Action{Operation: "prepare", Tenant: "big", Generation: descriptor.Generation, Partition: leftBucket,
		Epoch: descriptor.Partitions[leftBucket].Epoch + 1, Txn: "obsolete", Sequence: descriptor.Sequence + 1}
	if err := remote.Action(ctx, owner, stale, nil); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("stale partition epoch: %v", err)
	}
	keys := []string{partition.EntityKey(left), partition.EdgeKey(graph.CanonicalEdgeID(graph.Edge{From: left, To: right, Type: "calls"}))}
	batch, err := (partition.View{Graph: descriptor, Remote: remote}).Read(ctx, partition.Bucket(left), partition.ReadRequest{Keys: keys})
	if err != nil || len(batch.Rows) != 2 || batch.Rows[0].Key != keys[0] || batch.Rows[1].Key != keys[1] {
		t.Fatalf("published multi-key read: rows=%d error=%v", len(batch.Rows), err)
	}
	// Snapshot the application state, including input chunks and prepared/data
	// roots, and verify installation into a fresh independent local directory.
	leader := a.leader(-1)
	snapshot, err := a.nodes[leader].cluster.App.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := a.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	restored := &Application{Files: files, Store: storage.NewTenantStore(files, "graphdb"), ShardID: "a", MaxSnapshotBytes: 256 << 20}
	if err := restored.Restore(ctx, checkpoint.Index, snapshot); err != nil {
		t.Fatal(err)
	}
	state, err := restored.partitionStore().GetGraph(ctx, "big")
	if err != nil || state.DataHash != committed.DataHash || state.Version != 2 {
		t.Fatalf("snapshot lost graph control state: %v %+v", err, state)
	}
	oldLeader := a.leader(-1)
	a.stop(oldLeader)
	a.leader(oldLeader)
	replayed := call(http.MethodPost, "/v1/commits", "big", request, http.StatusOK)
	var result storage.CommitResult
	if err := json.Unmarshal(replayed.Body.Bytes(), &result); err != nil || !result.IdempotentReplay || result.Version != 2 || result.DataHash != committed.DataHash {
		t.Fatalf("leader replay: %s (%v)", replayed.Body.String(), err)
	}
	a.start(oldLeader)
	// The old single-group command path must remain fenced even if a caller
	// retained an earlier placement or sends directly to the home node.
	direct := httptest.NewRequest(http.MethodPost, "/v1/commits", bytes.NewBufferString(`{"idempotency_key":"second","mutations":{"upsert_entities":[{"id":"third","kind":"host"}]}}`))
	direct.Header.Set("Content-Type", "application/json")
	direct.Header.Set("X-Tenant-ID", "big")
	direct.Header.Set(sharding.EpochHeader, "1")
	w := httptest.NewRecorder()
	a.nodes[a.leader(-1)].handler.ServeHTTP(w, direct)
	if w.Code != http.StatusOK {
		t.Fatal(fmt.Sprintf("home-node mode dispatch: %d %s", w.Code, w.Body.String()))
	}
	call(http.MethodPost, "/v1/tenants", "", map[string]any{"tenant_id": "legacy"}, http.StatusOK)
	legacySchema := partition.CommitRequest{IdempotencyKey: "legacy-schema", Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}, UpsertRelationTypes: []graph.RelationType{{Name: "calls", FromKind: "host", ToKind: "host", Directed: true}}}}
	call(http.MethodPost, "/v1/commits", "legacy", legacySchema, http.StatusOK)
	expected := int64(1)
	legacyData := partition.CommitRequest{IdempotencyKey: "legacy-data", ExpectedVersion: &expected, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: left, Kind: "host"}, {ID: right, Kind: "host"}}, UpsertEdges: []graph.Edge{{Type: "calls", From: left, To: right}}}}
	legacyResponse := call(http.MethodPost, "/v1/commits", "legacy", legacyData, http.StatusOK)
	var legacyResult storage.CommitResult
	if err := json.Unmarshal(legacyResponse.Body.Bytes(), &legacyResult); err != nil {
		t.Fatal(err)
	}
	call(http.MethodPost, "/v1/indexes/rebuild", "legacy", nil, http.StatusOK)
	call(http.MethodPost, "/v1/cluster/graphs", "", map[string]any{"tenant_id": "legacy", "data_groups": []string{"a", "b"}}, http.StatusAccepted)
	copyCtx, copyCancel := context.WithTimeout(context.Background(), time.Minute)
	defer copyCancel()
	for {
		var catalogState sharding.Catalog
		if err := router.Client.JSON(copyCtx, definition(catalog, "catalog"), http.MethodGet, "/cluster/catalog", nil, &catalogState); err != nil {
			t.Fatal(err)
		}
		p := catalogState.Tenants["legacy"]
		if p.PartitionError != "" {
			t.Fatalf("populated conversion failed: %+v", p)
		}
		if p.Mode == "partitioned" && p.State == "active" {
			break
		}
		if copyCtx.Err() != nil {
			t.Fatalf("populated graph conversion did not complete: %+v", p)
		}
		time.Sleep(50 * time.Millisecond)
	}
	descriptorResponse := call(http.MethodGet, "/v1/cluster/graphs/legacy", "", nil, http.StatusOK)
	var converted partition.Graph
	if err := json.Unmarshal(descriptorResponse.Body.Bytes(), &converted); err != nil || converted.Version != legacyResult.Version || converted.DataHash != legacyResult.DataHash || converted.EntityCount != 2 || converted.EdgeCount != 1 {
		t.Fatalf("conversion changed source graph: %s (%v)", descriptorResponse.Body.String(), err)
	}
	for _, old := range []partition.CommitRequest{legacySchema, legacyData} {
		response := call(http.MethodPost, "/v1/commits", "legacy", old, http.StatusOK)
		var replay storage.CommitResult
		if err := json.Unmarshal(response.Body.Bytes(), &replay); err != nil || !replay.IdempotentReplay || !replay.Skipped {
			t.Fatalf("converted legacy identity did not replay: %s (%v)", response.Body.String(), err)
		}
	}
	call(http.MethodPost, "/v1/query", "legacy", query.Request{Op: "neighbors", ID: left, Direction: "out", Limit: 10}, http.StatusOK)
	// Moving one physical partition keeps the logical version/digest, and the
	// persistent move continues after losing the graph-control leader.
	call(http.MethodGet, "/v1/entities/"+left+"?min_version=3", "big", nil, http.StatusOK)
	call(http.MethodGet, "/v1/entities/"+left+"?min_version=999999", "big", nil, http.StatusServiceUnavailable)
	policyResponse := call(http.MethodGet, "/v1/source-policy", "big", nil, http.StatusOK)
	var sourcePolicy struct {
		Configured *bool               `json:"configured"`
		Policy     *graph.SourcePolicy `json:"policy"`
	}
	if err := json.Unmarshal(policyResponse.Body.Bytes(), &sourcePolicy); err != nil || sourcePolicy.Policy == nil || sourcePolicy.Configured == nil {
		t.Fatalf("source policy wire contract: %s", policyResponse.Body.String())
	}
	bigResponse := call(http.MethodGet, "/v1/cluster/graphs/big", "", nil, http.StatusOK)
	var beforeMove partition.Graph
	if err := json.Unmarshal(bigResponse.Body.Bytes(), &beforeMove); err != nil {
		t.Fatal(err)
	}
	bucket := partition.Bucket(left)
	targetGroup := "a"
	if beforeMove.Partitions[bucket].Group == "a" {
		targetGroup = "b"
	}
	call(http.MethodPost, fmt.Sprintf("/v1/cluster/graphs/big/partitions/%d/move", bucket), "", map[string]any{"target_group": targetGroup}, http.StatusAccepted)
	lostLeader := a.leader(-1)
	a.stop(lostLeader)
	a.leader(lostLeader)
	moveCtx, moveCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer moveCancel()
	for {
		response := call(http.MethodGet, "/v1/cluster/graphs/big", "", nil, http.StatusOK)
		var moved partition.Graph
		if err := json.Unmarshal(response.Body.Bytes(), &moved); err != nil {
			t.Fatal(err)
		}
		if moved.Move != nil && moved.Move.Phase == "cleanup" {
			if moved.Version != beforeMove.Version || moved.DataHash != beforeMove.DataHash || moved.Partitions[bucket].Group != targetGroup {
				t.Fatalf("partition move changed graph content: %+v", moved.Move)
			}
			break
		}
		if moveCtx.Err() != nil {
			t.Fatalf("partition move did not recover: %+v", moved.Move)
		}
		time.Sleep(50 * time.Millisecond)
	}
	a.start(lostLeader)
	call(http.MethodPost, "/v1/query", "big", query.Request{Op: "neighbors", ID: left, Direction: "both", Limit: 10}, http.StatusOK)
	call(http.MethodPost, "/v1/cluster/graphs/legacy/gc", "", nil, http.StatusAccepted)
	gcLeader := b.leader(-1)
	b.stop(gcLeader)
	b.leader(gcLeader)
	gcCtx, gcCancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer gcCancel()
	for {
		response := call(http.MethodGet, "/v1/cluster/graphs/legacy", "", nil, http.StatusOK)
		var collected partition.Graph
		if err := json.Unmarshal(response.Body.Bytes(), &collected); err != nil {
			t.Fatal(err)
		}
		if collected.GC == nil {
			if collected.Version != legacyResult.Version || collected.DataHash != legacyResult.DataHash {
				t.Fatal("garbage collection changed logical graph content")
			}
			break
		}
		if gcCtx.Err() != nil {
			t.Fatalf("GC did not recover: phase=%s", collected.GC.Phase)
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.start(gcLeader)
	call(http.MethodPost, "/v1/query", "legacy", query.Request{Op: "neighbors", ID: left, Direction: "in", Limit: 10}, http.StatusOK)

}
