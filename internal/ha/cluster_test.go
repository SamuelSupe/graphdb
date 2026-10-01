package ha

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type testReplica struct {
	mu                  sync.RWMutex
	cluster             *Cluster
	handler             http.Handler
	files               *storage.FileStore
	store               *storage.TenantStore
	peer                *httptest.Server
	blocked             atomic.Bool
	loseInstallResponse atomic.Bool
	cfg                 config.Config
}

type testCluster struct {
	t     *testing.T
	nodes []*testReplica
}

func newTestCluster(t *testing.T, wal bool) *testCluster {
	return newTestClusterRole(t, wal, "", false)
}

func newTestClusterRole(t *testing.T, wal bool, shardID string, catalog bool) *testCluster {
	t.Helper()
	group := &testCluster{t: t}
	peers := make(map[uint64]string)
	for i := 0; i < 3; i++ {
		replica := &testReplica{}
		replica.peer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if replica.blocked.Load() {
				http.Error(w, "partitioned", http.StatusServiceUnavailable)
				return
			}
			replica.mu.RLock()
			cluster := replica.cluster
			replica.mu.RUnlock()
			if cluster == nil {
				http.Error(w, "stopped", http.StatusServiceUnavailable)
				return
			}
			if replica.loseInstallResponse.Load() && r.Method == http.MethodPost && r.URL.Path == "/cluster/action" {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				var action struct {
					Operation string `json:"operation"`
				}
				if json.Unmarshal(body, &action) == nil && action.Operation == "install" && replica.loseInstallResponse.Swap(false) {
					cluster.PrivateHandler().ServeHTTP(httptest.NewRecorder(), r)
					http.Error(w, "install response lost", http.StatusServiceUnavailable)
					return
				}
			}
			cluster.PrivateHandler().ServeHTTP(w, r)
		}))
		peers[uint64(i+1)] = replica.peer.URL
		group.nodes = append(group.nodes, replica)
	}
	for i, replica := range group.nodes {
		mode := "direct"
		if wal {
			mode = "wal"
		}
		replica.cfg = config.Config{Prefix: "graphdb", InstanceID: "raft-test", IngestMode: mode, IngestFlushInterval: time.Hour, IngestFlushMaxRequests: 256, IngestFlushMaxBytes: 8 << 20, Raft: config.RaftConfig{Enabled: true, ID: uint64(i + 1), ClusterID: "test", Dir: filepath.Join(t.TempDir(), "raft"), Peers: peers, Token: "01234567890123456789012345678901", Bootstrap: true, Tick: 20 * time.Millisecond, SnapshotEntries: 5, MaxSnapshotBytes: 64 << 20}}
		replica.cfg.Raft.Tick = 100 * time.Millisecond
		if catalog || shardID != "" {
			replica.cfg.Raft.ShardID, replica.cfg.Raft.Catalog = shardID, catalog
			replica.cfg.Raft.ClusterID = shardID
			if catalog {
				replica.cfg.Raft.ClusterID = "catalog"
			}
			replica.cfg.InstanceID = "raft-" + replica.cfg.Raft.ClusterID
			replica.cfg.IngestFlushInterval = 200 * time.Millisecond
		}
		replica.cfg.DataDir = t.TempDir()
		group.start(i)
	}
	t.Cleanup(func() {
		for i := range group.nodes {
			group.stop(i)
			group.nodes[i].peer.Close()
		}
	})
	return group
}

func (g *testCluster) start(i int) {
	g.t.Helper()
	replica := g.nodes[i]
	files, err := storage.OpenFileStore(replica.cfg.DataDir)
	if err != nil {
		g.t.Fatal(err)
	}
	store := storage.NewTenantStoreWithOptions(files, "graphdb", storage.TenantStoreOptions{InstanceID: replica.cfg.InstanceID, MaxWriteCacheBytes: 64 << 20})
	cluster := New(replica.cfg, store, files)
	api := &httpapi.Server{Store: store, Mode: "all", Cluster: cluster}
	handler := api.Handler()
	cluster.App.Handler = handler
	if err := cluster.Start(context.Background(), replica.cfg.Raft); err != nil {
		g.t.Fatal(err)
	}
	replica.mu.Lock()
	replica.files = files
	replica.store = store
	replica.handler = handler
	replica.cluster = cluster
	replica.mu.Unlock()
}

func (g *testCluster) stop(i int) {
	replica := g.nodes[i]
	replica.mu.Lock()
	cluster := replica.cluster
	replica.cluster = nil
	replica.mu.Unlock()
	if cluster == nil {
		return
	}
	if err := cluster.Close(); err != nil {
		g.t.Error(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := replica.store.ShutdownTasks(ctx); err != nil {
		g.t.Error(err)
	}
	if err := replica.files.Close(); err != nil {
		g.t.Error(err)
	}
}

func (g *testCluster) leader(exclude int) int {
	g.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, replica := range g.nodes {
			if i == exclude {
				continue
			}
			replica.mu.RLock()
			cluster := replica.cluster
			replica.mu.RUnlock()
			if cluster == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := cluster.Node.ReadBarrier(ctx)
			cancel()
			if err == nil {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i, replica := range g.nodes {
		if replica.cluster != nil {
			g.t.Logf("node %d: %v", i, replica.cluster.Node.Status())
		}
	}
	g.t.Fatal("cluster did not elect a ready leader")
	return -1
}

func (g *testCluster) request(i int, method, uri, body string) *httptest.ResponseRecorder {
	g.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := httptest.NewRequest(method, uri, bytes.NewBufferString(body)).WithContext(ctx)
	request.Header.Set("X-Tenant-ID", "tenant-a")
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	g.nodes[i].handler.ServeHTTP(writer, request)
	return writer
}

func (g *testCluster) mustRequest(i int, method, uri, body string, status int) *httptest.ResponseRecorder {
	g.t.Helper()
	writer := g.request(i, method, uri, body)
	if writer.Code != status {
		g.t.Fatalf("%s %s on %d: status %d, expected %d: %s", method, uri, i, writer.Code, status, writer.Body.String())
	}
	return writer
}

func (g *testCluster) manifest(i int) storage.Manifest {
	g.t.Helper()
	replica := g.nodes[i]
	replica.cluster.App.mu.RLock()
	defer replica.cluster.App.mu.RUnlock()
	manifest, err := replica.store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		g.t.Fatal(err)
	}
	return manifest
}

func (g *testCluster) waitApplied(i int, index uint64) {
	g.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		checkpoint, err := g.nodes[i].files.ReplicationCheckpoint()
		if err != nil {
			g.t.Fatal(err)
		}
		if checkpoint.Index >= index {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	g.t.Fatalf("node %d did not catch up to %d: %v", i, index, g.nodes[i].cluster.Node.Status())
}

func TestHAReplicationFailoverAndSnapshot(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	body := `{"idempotency_key":"first","mutations":{"upsert_entities":[{"id":"host:1","kind":"host","fields":{"name":"one"}}]}}`
	group.mustRequest(leader, "POST", "/v1/commits", body, http.StatusOK)
	initial := group.manifest(leader)
	follower := (leader + 1) % 3
	group.mustRequest(follower, "GET", "/v1/entities", "", http.StatusServiceUnavailable)
	group.stop(follower)
	for i := 0; i < 8; i++ {
		group.mustRequest(leader, "POST", "/v1/commits", fmt.Sprintf(`{"idempotency_key":"update-%d","mutations":{"upsert_entities":[{"id":"host:1","kind":"host","fields":{"name":"value-%d"}}]}}`, i, i), http.StatusOK)
	}
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	group.start(follower)
	group.waitApplied(follower, checkpoint.Index)
	want := group.manifest(leader)
	got := group.manifest(follower)
	if got.Version != want.Version || got.HeadCommitID != want.HeadCommitID || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("snapshot replica diverged: got %+v, want %+v", got, want)
	}
	group.stop(leader)
	replacement := group.leader(leader)
	replay := group.mustRequest(replacement, "POST", "/v1/commits", body, http.StatusOK)
	var result storage.CommitResult
	if err := json.Unmarshal(replay.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.IdempotentReplay || result.Version != initial.Version || group.manifest(replacement).Version != want.Version {
		t.Fatalf("idempotent retry changed graph: %+v", result)
	}
	conflict := fmt.Sprintf(`{"expected_version":%d,"mutations":{"upsert_entities":[{"id":"host:2","kind":"host"}]}}`, initial.Version)
	group.mustRequest(replacement, "POST", "/v1/commits", conflict, http.StatusConflict)
	group.start(leader)
	group.waitApplied(leader, checkpoint.Index)
	group.nodes[replacement].blocked.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err = group.nodes[replacement].cluster.Node.ReadBarrier(ctx)
	cancel()
	if err == nil {
		t.Fatal("isolated old leader passed a strong read barrier")
	}
	majority := group.leader(replacement)
	group.mustRequest(majority, "POST", "/v1/commits", `{"idempotency_key":"after-partition","mutations":{"upsert_entities":[{"id":"host:3","kind":"host"}]}}`, http.StatusOK)
	other := 3 - replacement - majority
	group.nodes[other].blocked.Store(true)
	ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
	err = group.nodes[majority].cluster.Node.ReadBarrier(ctx)
	cancel()
	if err == nil {
		t.Fatal("node without a majority served a strong read")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
	cmd, _ := newCommand("http")
	cmd.Method = "POST"
	cmd.URI = "/v1/commits"
	cmd.Header = http.Header{"X-Tenant-ID": []string{"tenant-a"}}
	cmd.Body = []byte(body)
	_, err = group.nodes[majority].cluster.propose(ctx, cmd)
	cancel()
	if err == nil {
		t.Fatal("node without a majority confirmed a write")
	}
}

func TestHAAcceptedWALSurvivesLeaderLoss(t *testing.T) {
	group := newTestCluster(t, true)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	body := `{"source":"agent","collector_id":"collector","batch_id":"batch-1","idempotency_key":"wal-one","items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host","fields":{"name":"accepted"}}}]}`
	accepted := group.mustRequest(leader, "POST", "/v1/ingest/batches", body, http.StatusAccepted)
	if !bytes.Contains(accepted.Body.Bytes(), []byte("raft_majority")) {
		t.Fatal(accepted.Body.String())
	}
	var acceptance struct {
		WriterID       string    `json:"writer_id"`
		EstimatedFlush time.Time `json:"estimated_flush_at"`
	}
	if err := json.Unmarshal(accepted.Body.Bytes(), &acceptance); err != nil || acceptance.WriterID == "" || acceptance.EstimatedFlush.IsZero() {
		t.Fatalf("WAL acceptance cannot be polled by an existing client: %+v, %v", acceptance, err)
	}
	group.stop(leader)
	replacement := group.leader(leader)
	group.mustRequest(replacement, "POST", "/v1/ingest/batches", body, http.StatusAccepted)
	if err := group.nodes[replacement].cluster.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := group.mustRequest(replacement, "GET", "/v1/ingest/batches/agent/collector/batch-1", "", http.StatusOK)
	var record storage.IngestBatchStatus
	if err := json.Unmarshal(status.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.State != "committed" || record.Result == nil || record.Result.Version != 1 {
		t.Fatalf("accepted WAL was not recovered: %+v", record)
	}
	group.mustRequest(replacement, "GET", "/v1/ingest/writers/"+acceptance.WriterID+"/batches/agent/collector/batch-1", "", http.StatusOK)
	if err := group.nodes[replacement].cluster.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if version := group.manifest(replacement).Version; version != 1 {
		t.Fatalf("recovered WAL committed twice: %d", version)
	}
	group.start(leader)
	checkpoint, err := group.nodes[replacement].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	group.waitApplied(leader, checkpoint.Index)
	if version := group.manifest(leader).Version; version != 1 {
		t.Fatalf("restarted replica has version %d", version)
	}
}

func TestHAPendingQueueBudgetSurvivesFlushAndRestart(t *testing.T) {
	group := newTestCluster(t, true)
	for _, replica := range group.nodes {
		replica.cfg.IngestQueueMemoryBytes = 3 << 10
		replica.cluster.App.mu.Lock()
		replica.cluster.App.MaxPendingBytes = replica.cfg.IngestQueueMemoryBytes
		replica.cluster.App.mu.Unlock()
	}
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	body := func(id string) string {
		return fmt.Sprintf(`{"source":"agent","collector_id":"collector","batch_id":%q,"idempotency_key":%q,"items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host","fields":{"payload":%q,"name":%q}}}]}`, id, id, string(bytes.Repeat([]byte("x"), 2048)), id)
	}
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("one"), http.StatusAccepted)
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("two"), http.StatusServiceUnavailable)
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("one"), http.StatusAccepted)
	if err := group.nodes[leader].cluster.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("two"), http.StatusAccepted)
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("three"), http.StatusServiceUnavailable)
	for i := range group.nodes {
		group.stop(i)
	}
	for i := range group.nodes {
		group.start(i)
	}
	leader = group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("three"), http.StatusServiceUnavailable)
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("two"), http.StatusAccepted)
	if err := group.nodes[leader].cluster.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("three"), http.StatusAccepted)
	if err := group.nodes[leader].cluster.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"one", "two", "three"} {
		status := group.mustRequest(leader, "GET", "/v1/ingest/batches/agent/collector/"+id, "", http.StatusOK)
		var record storage.IngestBatchStatus
		if err := json.Unmarshal(status.Body.Bytes(), &record); err != nil || record.State != "committed" || record.Result == nil || record.Result.Version != int64(i+1) {
			t.Fatalf("batch %s was not published exactly once: %+v, %v", id, record, err)
		}
	}
}

func BenchmarkHAWALAcceptanceHistory(b *testing.B) {
	ctx := context.Background()
	app := &Application{Store: storage.NewTenantStore(storage.NewMemoryStore(), "bench"), MaxPendingBytes: 64 << 20}
	body := fmt.Sprintf(`{"source":"agent","collector_id":"collector","batch_id":"live","items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host","fields":{"payload":%q}}}]}`, string(bytes.Repeat([]byte("x"), 1024)))
	var request storage.IngestRequest
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		b.Fatal(err)
	}
	request, err := storage.PrepareIngestRequest("tenant-a", request)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		historical := request
		historical.BatchID = fmt.Sprintf("history-%d", i)
		record := acceptedRequest{Tenant: "tenant-a", Request: historical, Index: uint64(i + 1), State: "committed"}
		key := app.ingestKey(record.Tenant, historical.Source, historical.CollectorID, historical.BatchID, 0)
		if err := app.saveAccepted(ctx, key, record); err != nil {
			b.Fatal(err)
		}
	}
	data, err := json.Marshal(request)
	if err != nil {
		b.Fatal(err)
	}
	cmd := command{Tenant: "tenant-a", At: time.Unix(1, 0), Body: data}
	key := app.ingestKey(cmd.Tenant, request.Source, request.CollectorID, request.BatchID, 0)
	completed := acceptedRequest{Tenant: cmd.Tenant, Request: request, Index: 1001, AcceptedAt: cmd.At, State: "committed"}
	b.ReportAllocs()
	for b.Loop() {
		data, err := app.accept(ctx, 1001, cmd)
		var response httpResult
		if err != nil || json.Unmarshal(data, &response) != nil || response.Status != http.StatusAccepted {
			b.Fatalf("accept status=%d err=%v", response.Status, err)
		}
		if err := app.saveAccepted(ctx, key, completed); err != nil {
			b.Fatal(err)
		}
		if err := app.Store.Objects.Delete(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

func TestHAMembershipReplacementAndTenantRestore(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:1","kind":"host","fields":{"name":"original"}}]}}`, http.StatusOK)
	backup := group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/backup", `{}`, http.StatusAccepted)
	var task storage.Task
	if err := json.Unmarshal(backup.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.nodes[leader].cluster.App.mu.RLock()
	finished, err := group.nodes[leader].store.GetTask(context.Background(), "tenant-a", task.ID)
	group.nodes[leader].cluster.App.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != storage.TaskStatusSucceeded {
		t.Fatalf("backup task: %+v", finished)
	}
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:1","kind":"host","fields":{"name":"changed"}}]}}`, http.StatusOK)
	invalidRestore := group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", `{"backup_key":"graphdb/missing-backup","overwrite":true}`, http.StatusAccepted)
	var invalidTask storage.Task
	if err := json.Unmarshal(invalidRestore.Body.Bytes(), &invalidTask); err != nil {
		t.Fatal(err)
	}
	restoreBody := fmt.Sprintf(`{"backup_key":%q,"overwrite":true}`, finished.ResultKey)
	restore := group.request(leader, "POST", "/v1/tenants/tenant-a/restore", restoreBody)
	if restore.Code == http.StatusAccepted {
		var queued storage.Task
		if err := json.Unmarshal(restore.Body.Bytes(), &queued); err != nil || queued.ID == invalidTask.ID || queued.Params["backup_key"] != finished.ResultKey {
			t.Fatalf("different queued parameters were silently coalesced: %+v, %v", queued, err)
		}
	} else if restore.Code != http.StatusConflict {
		t.Fatalf("conflicting restore: %d: %s", restore.Code, restore.Body.String())
	}
	if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed := group.mustRequest(leader, "GET", "/v1/tasks/"+invalidTask.ID, "", http.StatusOK)
	if err := json.Unmarshal(failed.Body.Bytes(), &invalidTask); err != nil || invalidTask.Status != storage.TaskStatusFailed {
		t.Fatalf("invalid restore did not fail durably: %+v, %v", invalidTask, err)
	}
	if restore.Code == http.StatusConflict {
		restore = group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", restoreBody, http.StatusAccepted)
	}
	var restoreTask storage.Task
	if err := json.Unmarshal(restore.Body.Bytes(), &restoreTask); err != nil {
		t.Fatal(err)
	}
	if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := group.manifest(leader).Version; got != 1 {
		t.Fatalf("restore did not publish backup version: %d", got)
	}
	stale := httptest.NewRequest("GET", "/v1/entities", nil)
	stale.Header.Set("X-Tenant-ID", "tenant-a")
	stale.Header.Set("X-GraphDB-Read-Generation", "1")
	staleResponse := httptest.NewRecorder()
	group.nodes[leader].handler.ServeHTTP(staleResponse, stale)
	if staleResponse.Code != http.StatusConflict {
		t.Fatalf("restore retained an old read generation: %d: %s", staleResponse.Code, staleResponse.Body.String())
	}
	replica := &testReplica{}
	replica.peer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replica.mu.RLock()
		cluster := replica.cluster
		replica.mu.RUnlock()
		if cluster == nil {
			http.Error(w, "not started", 503)
			return
		}
		cluster.Node.Handler().ServeHTTP(w, r)
	}))
	replica.cfg = group.nodes[leader].cfg
	replica.cfg.DataDir = t.TempDir()
	replica.cfg.Raft.ID = 4
	replica.cfg.Raft.Dir = filepath.Join(t.TempDir(), "raft")
	replica.cfg.Raft.Bootstrap = false
	replica.cfg.Raft.Peers = make(map[uint64]string)
	for id, address := range group.nodes[leader].cfg.Raft.Peers {
		replica.cfg.Raft.Peers[id] = address
	}
	replica.cfg.Raft.Peers[4] = replica.peer.URL
	group.nodes = append(group.nodes, replica)
	group.start(3)
	memberRequest := func(body string, status int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		request := httptest.NewRequest("POST", "/raft/members", bytes.NewBufferString(body)).WithContext(ctx)
		request.Header.Set("Authorization", "Bearer "+replica.cfg.Raft.Token)
		request.Header.Set("X-Raft-Cluster", replica.cfg.Raft.ClusterID)
		writer := httptest.NewRecorder()
		group.nodes[leader].cluster.Node.Handler().ServeHTTP(writer, request)
		if writer.Code != status {
			t.Fatalf("membership %s: %d: %s", body, writer.Code, writer.Body.String())
		}
	}
	memberRequest(fmt.Sprintf(`{"action":"add_learner","id":4,"url":%q}`, replica.peer.URL), http.StatusNoContent)
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	group.waitApplied(3, checkpoint.Index)
	memberRequest(`{"action":"promote","id":4}`, http.StatusNoContent)
	removed := (leader + 1) % 3
	memberRequest(fmt.Sprintf(`{"action":"remove","id":%d}`, removed+1), http.StatusNoContent)
	memberRequest(fmt.Sprintf(`{"action":"add_learner","id":%d,"url":%q}`, removed+1, group.nodes[removed].peer.URL), http.StatusConflict)
	group.stop(removed)
	group.mustRequest(leader, "POST", "/v1/commits", `{"idempotency_key":"replacement-write","mutations":{"upsert_entities":[{"id":"host:2","kind":"host"}]}}`, http.StatusOK)
	checkpoint, err = group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	group.waitApplied(3, checkpoint.Index)
	if got := group.manifest(3).Version; got != 2 {
		t.Fatalf("replacement node has version %d", got)
	}
	for i := range group.nodes {
		if i == removed {
			continue
		}
		group.stop(i)
		leader = group.leader(i)
		group.start(i)
		group.waitApplied(i, checkpoint.Index)
	}
	leader = group.leader(removed)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:3","kind":"host"}]}}`, http.StatusOK)
}

func TestHAAcceptedWALIsFencedByRestore(t *testing.T) {
	group := newTestCluster(t, true)
	leader := group.leader(-1)
	cluster := group.nodes[leader].cluster
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:1","kind":"host"}]}}`, http.StatusOK)
	backup := group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/backup", `{}`, http.StatusAccepted)
	var task storage.Task
	if err := json.Unmarshal(backup.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	if err := cluster.runQueuedTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	cluster.App.mu.RLock()
	finished, err := group.nodes[leader].store.GetTask(context.Background(), "tenant-a", task.ID)
	cluster.App.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	body := `{"source":"agent","collector_id":"collector","batch_id":"reused","idempotency_key":"reused","items":[{"external_id":"host:2","entity":{"id":"host:2","kind":"host"}}]}`
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body, http.StatusAccepted)
	group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", fmt.Sprintf(`{"backup_key":%q,"overwrite":true}`, finished.ResultKey), http.StatusAccepted)
	if err := cluster.runQueuedTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body, http.StatusAccepted)
	for range 2 {
		if err := cluster.flushPending(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	status := group.mustRequest(leader, "GET", "/v1/ingest/batches/agent/collector/reused", "", http.StatusOK)
	var batch storage.IngestBatchStatus
	if err := json.Unmarshal(status.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	if batch.State != "committed" || batch.Result == nil || batch.Result.Version != 2 {
		t.Fatalf("new tenant generation reused an old acceptance: %+v", batch)
	}
	cluster.App.mu.RLock()
	old, err := cluster.App.accepted(context.Background(), cluster.App.ingestKey("tenant-a", "agent", "collector", "reused", 1))
	cluster.App.mu.RUnlock()
	if err != nil || old.State != "failed" {
		t.Fatalf("old accepted WAL was not fenced: %+v, %v", old, err)
	}
}

func TestHARejectsDataWithoutRaftHistory(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.stop(leader)
	replica := group.nodes[leader]
	if err := os.RemoveAll(replica.cfg.Raft.Dir); err != nil {
		t.Fatal(err)
	}
	files, err := storage.OpenFileStore(replica.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := storage.NewTenantStoreWithOptions(files, "graphdb", storage.TenantStoreOptions{InstanceID: "raft-test"})
	cluster := New(replica.cfg, store, files)
	if err := cluster.Start(context.Background(), replica.cfg.Raft); err == nil {
		cluster.Close()
		t.Fatal("graph checkpoint without its Raft log was accepted")
	}
	standalone, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	if err := standalone.Put(context.Background(), "graphdb/old-data", []byte("existing")); err != nil {
		t.Fatal(err)
	}
	store = storage.NewTenantStore(standalone, "graphdb")
	cluster = New(replica.cfg, store, standalone)
	if err := cluster.Start(context.Background(), replica.cfg.Raft); err == nil {
		cluster.Close()
		t.Fatal("standalone data directory was accepted for bootstrap")
	}
	if err := standalone.CheckStandaloneDirectory(); err != nil {
		t.Fatalf("failed bootstrap changed the standalone directory's mode: %v", err)
	}
}

func TestHASnapshotBudgetKeepsReadsAvailable(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:1","kind":"host"}]}}`, http.StatusOK)
	for _, replica := range group.nodes {
		replica.cluster.App.mu.Lock()
		replica.cluster.App.MaxSnapshotBytes = 1
		replica.cluster.App.mu.Unlock()
	}
	for range 12 {
		response := group.request(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:1","kind":"host"}]}}`)
		if response.Code == http.StatusServiceUnavailable {
			break
		}
		if response.Code != http.StatusOK {
			t.Fatalf("snapshot budget response: %d: %s", response.Code, response.Body.String())
		}
	}
	if group.nodes[leader].cluster.Node.Status()["snapshot_error"] == nil {
		t.Fatal("oversized snapshot was not surfaced")
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:1", "", http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:2","kind":"host"}]}}`, http.StatusServiceUnavailable)
}
