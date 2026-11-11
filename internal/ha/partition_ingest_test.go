package ha

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHAPartitionIngestDurableQueueAndLeaderRecovery(t *testing.T) {
	a, b, catalog, router, call := newTestPartitionedGraph(t, true)
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}}}, http.StatusOK)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	home := partition.Group{ID: "a", ClusterID: a.nodes[0].cfg.Raft.ClusterID, Peers: a.nodes[0].cfg.Raft.Peers}
	remote := sharding.GraphRemote{Client: router.Client}
	waitGraph := func(ready func(partition.Graph) bool) partition.Graph {
		t.Helper()
		for {
			g, err := remote.Describe(ctx, home, "big")
			if err == nil && ready(g) {
				return g
			}
			select {
			case <-ctx.Done():
				t.Fatalf("graph did not reach recovery boundary: active=%+v error=%v", g.Active, err)
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	for _, group := range []*testCluster{a, b} {
		for _, node := range group.nodes {
			node.blockGraphFinalize.Store(true)
		}
	}
	for _, node := range b.nodes {
		node.blocked.Store(true)
	}
	id := "queued-first"
	for partition.Bucket(id)%2 != 1 {
		id += "x"
	}
	firstVersion, secondVersion := int64(1), int64(2)
	first := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "one", IdempotencyKey: "stable-one", ExpectedVersion: &firstVersion, Items: []storage.IngestItem{{Entity: &graph.Entity{ID: id, Kind: "host"}}}}
	first.Preconditions = []storage.IngestPrecondition{{ResourceType: "entity", ID: id, Op: "not_exists"}}
	second := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "two", ExpectedVersion: &secondVersion, Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "queued-second", Kind: "host"}}}}
	var acceptance struct {
		State      string    `json:"state"`
		Durability string    `json:"durability"`
		LSN        uint64    `json:"accepted_lsn"`
		Generation int64     `json:"generation"`
		AcceptedAt time.Time `json:"accepted_at"`
		StatusURL  string    `json:"status_url"`
	}
	response := call(http.MethodPost, "/v1/ingest/batches", "big", first, http.StatusAccepted)
	if err := json.Unmarshal(response.Body.Bytes(), &acceptance); err != nil || acceptance.Durability != "raft_majority" || acceptance.LSN == 0 || acceptance.State != "accepted" || acceptance.AcceptedAt.IsZero() || acceptance.Generation <= 1 {
		t.Fatalf("durable acceptance: %s %v", response.Body.String(), err)
	}
	firstLSN, firstURL, firstAt := acceptance.LSN, acceptance.StatusURL, acceptance.AcceptedAt
	g := waitGraph(func(g partition.Graph) bool { return g.Active != nil })
	if g.Version != 1 {
		t.Fatal("blocked data group unexpectedly published the batch")
	}
	call(http.MethodPost, "/v1/ingest/batches", "big", second, http.StatusAccepted)
	changed := first
	changed.Cursor = "different"
	call(http.MethodPost, "/v1/ingest/batches", "big", changed, http.StatusConflict)
	alias := first
	alias.BatchID = "new-batch-same-idempotency"
	response = call(http.MethodPost, "/v1/ingest/batches", "big", alias, http.StatusAccepted)
	if json.Unmarshal(response.Body.Bytes(), &acceptance) != nil || acceptance.LSN != firstLSN || acceptance.StatusURL != firstURL || !acceptance.AcceptedAt.Equal(firstAt) {
		t.Fatalf("idempotency alias created another acceptance: %s", response.Body.String())
	}
	for _, node := range a.nodes {
		node.cfg.IngestQueueMemoryBytes = 1
		node.cluster.App.mu.Lock()
		node.cluster.App.MaxPendingBytes = 1
		node.cluster.App.mu.Unlock()
	}
	third := first
	third.BatchID, third.IdempotencyKey = "three", "three"
	third.ExpectedVersion = nil
	call(http.MethodPost, "/v1/ingest/batches", "big", third, http.StatusServiceUnavailable)
	call(http.MethodPost, "/v1/ingest/batches", "big", first, http.StatusAccepted)
	// The acceptance payload and idempotency alias must also survive a snapshot
	// installed independently of the source replica and its volatile queue index.
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
	restored := &Application{Files: files, Store: storage.NewTenantStore(files, "graphdb"), ShardID: "a", MaxSnapshotBytes: 64 << 20}
	if err := restored.Restore(ctx, checkpoint.Index, snapshot); err != nil {
		t.Fatal(err)
	}
	pending, err := restored.pendingSnapshot(ctx)
	if err != nil || len(pending) != 2 {
		t.Fatalf("snapshot lost accepted work: %d %v", len(pending), err)
	}
	storedAlias, found, err := restored.partitionIngestAlias(ctx, "big", acceptance.Generation, first)
	if err != nil || !found || storedAlias.Key != restored.ingestKey("big", "agent", "collector", "one", acceptance.Generation) {
		t.Fatalf("snapshot lost idempotency alias: %+v %v", storedAlias, err)
	}
	for i := range a.nodes {
		a.stop(i)
	}
	for i := range a.nodes {
		a.start(i)
	}
	a.leader(-1)
	call(http.MethodPost, "/v1/ingest/batches", "big", third, http.StatusServiceUnavailable)
	response = call(http.MethodGet, firstURL, "big", nil, http.StatusOK)
	var status storage.IngestBatchStatus
	if json.Unmarshal(response.Body.Bytes(), &status) != nil || status.State != "accepted" || status.Result != nil || status.AcceptedLSN != firstLSN || !status.AcceptedAt.Equal(firstAt) {
		t.Fatalf("restart lost acknowledged input: %s", response.Body.String())
	}
	call(http.MethodPost, "/v1/ingest/batches", "big", alias, http.StatusAccepted)
	for _, node := range b.nodes {
		node.blocked.Store(false)
	}
	// Stop the leader in the graph-decision / queue-completion crash window.
	g = waitGraph(func(g partition.Graph) bool {
		return g.Version == 2 && g.Active != nil && g.Active.State == "committed"
	})
	if g.Active.IngestResult == nil || g.Active.IngestResult.Version != 2 {
		t.Fatal("graph publication did not durably include the ingest result")
	}
	response = call(http.MethodGet, firstURL, "big", nil, http.StatusOK)
	if json.Unmarshal(response.Body.Bytes(), &status) != nil || status.State != "accepted" {
		t.Fatalf("finalization fault did not hold the intended boundary: %s", response.Body.String())
	}
	leader = a.leader(-1)
	a.stop(leader)
	a.leader(leader)
	for _, group := range []*testCluster{a, b} {
		for _, node := range group.nodes {
			node.blockGraphFinalize.Store(false)
		}
	}
	for i, url := range []string{firstURL, "/v1/ingest/batches/agent/collector/two"} {
		for {
			response = call(http.MethodGet, url, "big", nil, http.StatusOK)
			status = storage.IngestBatchStatus{}
			if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
				t.Fatal(err)
			}
			if status.Result != nil {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("accepted request was stranded: %s", response.Body.String())
			case <-time.After(25 * time.Millisecond):
			}
		}
		if status.State != "committed" || status.Result.Version != int64(i+2) || status.Result.Applied != 1 || status.Result.Failed != 0 {
			t.Fatalf("ordered exactly-once recovery: %s", response.Body.String())
		}
	}
	a.start(leader)
	// A fresh router must resolve the same durable acceptance and result.
	fresh := sharding.NewRouter(sharding.Shard{ID: "catalog", ClusterID: catalog.nodes[0].cfg.Raft.ClusterID, Peers: catalog.nodes[0].cfg.Raft.Peers}, catalog.nodes[0].cfg.Raft.Token)
	defer fresh.Client.HTTP.CloseIdleConnections()
	data, _ := json.Marshal(first)
	r := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(data)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+catalog.nodes[0].cfg.Raft.Token)
	r.Header.Set("X-Tenant-ID", "big")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Prefer", "wait=committed")
	w := httptest.NewRecorder()
	fresh.ServeHTTP(w, r)
	var result storage.IngestResult
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Version != 2 || result.Failed != 0 {
		t.Fatalf("cross-router committed retry: %d %s", w.Code, w.Body.String())
	}
	g = waitGraph(func(g partition.Graph) bool { return g.Active == nil })
	if g.Version != 3 || g.EntityCount != 2 {
		t.Fatalf("retry republished the graph: version=%d entities=%d", g.Version, g.EntityCount)
	}
	for _, node := range a.nodes {
		node.cluster.App.mu.Lock()
		node.cluster.App.MaxPendingBytes = 1 << 20
		node.cluster.App.mu.Unlock()
	}
	for _, test := range []struct {
		name string
		code int
		edit func(*storage.IngestRequest)
	}{
		{"stale", http.StatusConflict, func(r *storage.IngestRequest) { r.ExpectedVersion = &firstVersion }},
		{"guard", http.StatusPreconditionFailed, func(r *storage.IngestRequest) {
			r.Preconditions = []storage.IngestPrecondition{{ResourceType: "entity", ID: "absent", Op: "exists"}}
		}},
		{"atomic", http.StatusUnprocessableEntity, func(r *storage.IngestRequest) {
			r.FailureMode = storage.IngestFailureModeAtomic
			r.Items = append(r.Items, storage.IngestItem{})
		}},
		{"healthy", http.StatusOK, func(r *storage.IngestRequest) {}},
	} {
		request := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: test.name, Items: []storage.IngestItem{{Entity: &graph.Entity{ID: test.name, Kind: "host"}}}}
		test.edit(&request)
		data, _ := json.Marshal(request)
		r := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(data)).WithContext(ctx)
		r.Header.Set("Authorization", "Bearer "+catalog.nodes[0].cfg.Raft.Token)
		r.Header.Set("X-Tenant-ID", "big")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Prefer", "wait=committed")
		w := httptest.NewRecorder()
		fresh.ServeHTTP(w, r)
		if w.Code != test.code {
			t.Fatalf("%s terminal HTTP result: %d %s", test.name, w.Code, w.Body.String())
		}
	}
	g = waitGraph(func(g partition.Graph) bool { return g.Active == nil })
	if g.Version != 4 || g.EntityCount != 3 {
		t.Fatalf("failed ingest changed data or blocked later work: version=%d entities=%d", g.Version, g.EntityCount)
	}
	third.FullSync = true
	call(http.MethodPost, "/v1/ingest/batches", "big", third, http.StatusNotImplemented)
	oversized := third
	oversized.FullSync = false
	oversized.Items = make([]storage.IngestItem, partition.MaxChanges+1)
	call(http.MethodPost, "/v1/ingest/batches", "big", oversized, http.StatusBadRequest)
	// A home group without a majority must never return a durable acceptance.
	leader = a.leader(-1)
	for i := range a.nodes {
		if i != leader {
			a.stop(i)
		}
	}
	requestCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	third.FullSync = false
	data, _ = json.Marshal(third)
	r = httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(data)).WithContext(requestCtx)
	r.Header.Set("Authorization", "Bearer "+catalog.nodes[0].cfg.Raft.Token)
	r.Header.Set("X-Tenant-ID", "big")
	r.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	fresh.ServeHTTP(w, r)
	if w.Code < 500 {
		t.Fatalf("home minority acknowledged a request: %d %s", w.Code, w.Body.String())
	}
}
