package ha

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func newTestPartitionedGraph(t *testing.T, wal bool) (a, b, catalog *testCluster, router *sharding.Router, call func(string, string, string, any, int) *httptest.ResponseRecorder) {
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

func TestHAPartitionedGraphRoutesAndLeaderRecovery(t *testing.T) {
	a, b, catalog, router, call := newTestPartitionedGraph(t, false)
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
	for _, q := range []query.Request{{Op: "neighbors", ID: left, Direction: "out", Limit: 10}, {Op: "neighbors", ID: right, Direction: "in", Limit: 10}, {Op: "shortest_path", ID: left, TargetID: right, Direction: "out", Depth: 3}} {
		response := call(http.MethodPost, "/v1/query", "big", q, http.StatusOK)
		var result query.Response
		if json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Results) != 1 || result.Version != 2 {
			t.Fatalf("query: %s", response.Body.String())
		}
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
	remote := sharding.GraphRemote{Client: router.Client}
	descriptor, err := remote.Describe(ctx, partition.Group{ID: "a", ClusterID: a.nodes[0].cfg.Raft.ClusterID, Peers: a.nodes[0].cfg.Raft.Peers}, "big")
	if err != nil {
		t.Fatal(err)
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
	gcCtx, gcCancel := context.WithTimeout(context.Background(), 90*time.Second)
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
