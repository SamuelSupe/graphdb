package ha

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
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
	blockedMessages     atomic.Bool
	loseInstallResponse atomic.Bool
	loseStageResponse   atomic.Bool
	stageRequests       atomic.Int64
	legacyExportOnly    atomic.Bool
	cfg                 config.Config
}

type testCluster struct {
	t                   *testing.T
	nodes               []*testReplica
	decorateApplication func(*Application, http.Handler) http.Handler
}

func newTestCluster(t *testing.T, wal bool, decorate ...func(*Application, http.Handler) http.Handler) *testCluster {
	return newTestClusterRole(t, wal, "", false, decorate...)
}

func newTestClusterRole(t *testing.T, wal bool, shardID string, catalog bool, decorate ...func(*Application, http.Handler) http.Handler) *testCluster {
	t.Helper()
	group := &testCluster{t: t}
	if len(decorate) > 0 {
		group.decorateApplication = decorate[0]
	}
	peers := make(map[uint64]string)
	for i := 0; i < 3; i++ {
		replica := &testReplica{}
		replica.peer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if replica.blocked.Load() || (replica.blockedMessages.Load() && r.URL.Path == "/raft/message") {
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
			if replica.legacyExportOnly.Load() && strings.HasPrefix(r.URL.Path, "/cluster/export/") {
				http.NotFound(w, r)
				return
			}
			if r.Method == http.MethodPost && r.URL.Path == "/cluster/action" {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				var action struct {
					Operation string `json:"operation"`
				}
				if json.Unmarshal(body, &action) == nil && action.Operation == "stage" {
					replica.stageRequests.Add(1)
					if replica.loseStageResponse.Swap(false) {
						cluster.PrivateHandler().ServeHTTP(httptest.NewRecorder(), r)
						http.Error(w, "stage response lost", http.StatusServiceUnavailable)
						return
					}
				}
				if action.Operation == "install" && replica.loseInstallResponse.Swap(false) {
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
		replica.cfg = config.Config{Prefix: "graphdb", InstanceID: "raft-test", IngestMode: mode, IngestFlushInterval: time.Hour, IngestFlushMaxRequests: 256, IngestFlushMaxBytes: 8 << 20, Raft: config.RaftConfig{Enabled: true, ID: uint64(i + 1), ClusterID: "test", Dir: filepath.Join(t.TempDir(), "raft"), Peers: peers, Token: "01234567890123456789012345678901", Bootstrap: true, ElectionTicks: 10, Tick: 20 * time.Millisecond, SnapshotEntries: 5, MaxSnapshotBytes: 64 << 20}}
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
	if g.decorateApplication != nil {
		cluster.App.Handler = g.decorateApplication(cluster.App, handler)
	}
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

func (g *testCluster) request(i int, method, uri, body string, timeout ...time.Duration) *httptest.ResponseRecorder {
	g.t.Helper()
	budget := 5 * time.Second
	if len(timeout) > 0 {
		budget = timeout[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	request := httptest.NewRequest(method, uri, bytes.NewBufferString(body)).WithContext(ctx)
	request.Header.Set("X-Tenant-ID", "tenant-a")
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	g.nodes[i].handler.ServeHTTP(writer, request)
	return writer
}

func (g *testCluster) mustRequest(i int, method, uri, body string, status int, timeout ...time.Duration) *httptest.ResponseRecorder {
	g.t.Helper()
	writer := g.request(i, method, uri, body, timeout...)
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

func (g *testCluster) waitApplied(i int, index uint64, timeout ...time.Duration) {
	g.t.Helper()
	budget := 10 * time.Second
	if len(timeout) > 0 {
		budget = timeout[0]
	}
	deadline := time.Now().Add(budget)
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
	testHAReplicationFailoverAndSnapshot(t, false)
}

func TestHAStreamingReplicationFailoverAndSnapshot(t *testing.T) {
	testHAReplicationFailoverAndSnapshot(t, true)
}

func TestHAPreparedMaintenanceKeepsOtherTenantWritable(t *testing.T) {
	for _, protocol := range []int{2, 3} {
		t.Run(fmt.Sprint("protocol-", protocol), func(t *testing.T) { testHAPreparedMaintenanceKeepsOtherTenantWritable(t, protocol) })
	}
}

func testHAPreparedMaintenanceKeepsOtherTenantWritable(t *testing.T, protocol int) {
	group := newTestCluster(t, false)
	for i := range group.nodes {
		group.stop(i)
		group.nodes[i].cfg.Raft.Protocol = protocol
		group.nodes[i].cfg.Raft.StreamSnapshots = true
		group.nodes[i].cfg.Raft.SnapshotEntries = 100
		group.nodes[i].cfg.Raft.Tick = 500 * time.Millisecond
		group.start(i)
	}
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-b"}`, http.StatusOK)
	entities := []map[string]any{}
	// Contention depends on entities and concurrent writes, not filler bytes.
	// Large repeated strings amplify race instrumentation before the assertions.
	for i := 0; i < 200; i++ {
		entities = append(entities, map[string]any{"id": fmt.Sprint("host:", i), "kind": "host", "fields": map[string]any{"payload": strings.Repeat("data", 16)}})
	}
	body, _ := json.Marshal(map[string]any{"mutations": map[string]any{"upsert_entities": entities}})
	group.mustRequest(leader, "POST", "/v1/commits", string(body), http.StatusOK)
	kinds := []string{storage.TaskTypeCompact, storage.TaskTypeIndexRebuild}
	if protocol >= 3 {
		kinds = append(kinds, storage.TaskTypeGC)
	}
	for _, kind := range kinds {
		if kind == storage.TaskTypeGC {
			group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:gc-new","kind":"host"}]}}`, http.StatusOK)
			response := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"compact"}`, http.StatusAccepted)
			var task storage.Task
			if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				completed, err := group.nodes[leader].store.GetTask(context.Background(), "tenant-a", task.ID)
				if err != nil {
					t.Fatal(err)
				}
				if completed.Status == storage.TaskStatusSucceeded {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		response := group.mustRequest(leader, "POST", "/v1/tasks", fmt.Sprintf(`{"type":%q}`, kind), http.StatusAccepted)
		var task storage.Task
		if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
			t.Fatal(err)
		}
		writes := 0
		// Repeated conflicts eventually pause only this tenant's admission.
		deadline := time.Now().Add(time.Minute)
		for time.Now().Before(deadline) {
			// Keep the maintained tenant changing as well: prepared results
			// must not retry forever while another tenant still makes progress.
			hot := group.request(leader, "POST", "/v1/commits", fmt.Sprintf(`{"mutations":{"upsert_entities":[{"id":"hot:%s:%d","kind":"host"}]}}`, kind, writes), 30*time.Second)
			if hot.Code != http.StatusOK && hot.Code != http.StatusTooManyRequests {
				t.Fatalf("maintained tenant write: %d %s", hot.Code, hot.Body.String())
			}
			request := httptest.NewRequest("POST", "/v1/commits", strings.NewReader(fmt.Sprintf(`{"mutations":{"upsert_entities":[{"id":"other:%s:%d","kind":"host"}]}}`, kind, writes)))
			request.Header.Set("X-Tenant-ID", "tenant-b")
			writer := httptest.NewRecorder()
			group.nodes[leader].handler.ServeHTTP(writer, request)
			if writer.Code != http.StatusOK {
				t.Fatalf("other tenant write: %d %s", writer.Code, writer.Body.String())
			}
			writes++
			task, err := group.nodes[leader].store.GetTask(context.Background(), "tenant-a", task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if task.Status == storage.TaskStatusSucceeded {
				break
			}
			if task.Status == storage.TaskStatusFailed {
				t.Fatalf("maintenance: %+v", task)
			}
			time.Sleep(20 * time.Millisecond)
		}
		completed, err := group.nodes[leader].store.GetTask(context.Background(), "tenant-a", task.ID)
		if err != nil || completed.Status != storage.TaskStatusSucceeded || writes == 0 {
			t.Fatalf("maintenance did not complete with foreground progress: %+v writes=%d err=%v", completed, writes, err)
		}
		checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		for i := range group.nodes {
			group.waitApplied(i, checkpoint.Index)
			copy, err := group.nodes[i].store.GetTask(context.Background(), "tenant-a", task.ID)
			if err != nil || copy.Status != storage.TaskStatusSucceeded {
				t.Fatalf("replica %d task: %+v, %v", i, copy, err)
			}
			if kind == storage.TaskTypeGC {
				deleted, ok := copy.Result["deleted_keys"].([]any)
				if !ok || len(deleted) == 0 {
					t.Fatalf("GC did not exercise replicated deletions: %+v", copy.Result)
				}
				for _, key := range deleted {
					if _, err := group.nodes[i].files.Head(context.Background(), key.(string)); !errors.Is(err, storage.ErrNotFound) {
						t.Fatalf("replica %d retained GC object %s: %v", i, key, err)
					}
				}
			}
		}
	}
}

func TestHAPreparedMaintenancePipelinedChunksMatchEveryReplica(t *testing.T) {
	group := newTestCluster(t, false)
	for i := range group.nodes {
		group.stop(i)
		group.nodes[i].cfg.Raft.Protocol = 3
		group.nodes[i].cfg.Raft.SnapshotEntries = 1000
		group.nodes[i].cfg.Raft.StreamSnapshots = true
		group.nodes[i].cfg.Raft.Tick = 500 * time.Millisecond
		group.start(i)
		group.nodes[i].cluster.maintenanceMu.Lock()
	}
	t.Cleanup(func() {
		for _, node := range group.nodes {
			node.cluster.maintenanceMu.Unlock()
		}
	})
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	entities := make([]map[string]any, 128)
	for entity := range entities {
		var payload strings.Builder
		for block := range 1024 {
			digest := sha256.Sum256([]byte(fmt.Sprint(entity, ":", block)))
			payload.WriteString(hex.EncodeToString(digest[:]))
		}
		entities[entity] = map[string]any{"id": fmt.Sprint("host:", entity), "kind": "host", "fields": map[string]any{"payload": payload.String()}}
	}
	body, _ := json.Marshal(map[string]any{"mutations": map[string]any{"upsert_entities": entities}})
	group.mustRequest(leader, "POST", "/v1/commits", string(body), http.StatusOK, 2*time.Minute)
	response := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"compact"}`, http.StatusAccepted)
	var task storage.Task
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	cluster := group.nodes[leader].cluster
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cluster.App.mu.RLock()
	generation, err := cluster.App.Store.ReplicationTenantGeneration(ctx, task.TenantID)
	source, captureErr := cluster.App.Store.CaptureReplicatedMaintenance(ctx, task)
	cluster.App.mu.RUnlock()
	if err != nil || captureErr != nil {
		t.Fatalf("capture: %v, %v", err, captureErr)
	}
	defer source.Close()
	input, err := source.Build(ctx, cluster.App.MaxSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil || info.Size() <= 4*restoreChunkBytes {
		t.Fatalf("maintenance must exercise concurrent parts: %v, %v", info, err)
	}
	if err := cluster.replicateMaintenance(ctx, task, input, generation); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := cluster.App.Files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i, node := range group.nodes {
		group.waitApplied(i, checkpoint.Index, time.Minute)
		finished, err := node.store.GetTask(ctx, task.TenantID, task.ID)
		if err != nil || finished.Status != storage.TaskStatusSucceeded {
			t.Fatalf("replica %d task: %+v, %v", i, finished, err)
		}
		cold := storage.NewTenantStoreWithOptions(node.files, node.store.Prefix, storage.TenantStoreOptions{InstanceID: node.store.InstanceID})
		cold.ReplicationMode = true
		defer cold.ShutdownTasks(context.Background())
		graph, manifest, err := cold.Load(ctx, "tenant-a")
		if err != nil || manifest.SnapshotVersion != 1 || len(graph.Snapshot().Entities) != len(entities) {
			t.Fatalf("replica %d graph: %+v, %v", i, manifest, err)
		}
		for _, expected := range entities {
			entity, ok := graph.GetEntity(expected["id"].(string))
			if !ok || entity.Fields["payload"] != expected["fields"].(map[string]any)["payload"] {
				t.Fatalf("replica %d changed entity %v during pipelined transfer", i, expected["id"])
			}
		}
		parts, err := node.files.List(ctx, cluster.App.restorePrefix(task.TenantID, task.ID))
		if err != nil || len(parts) != 0 {
			t.Fatalf("replica %d retained staging: %d, %v", i, len(parts), err)
		}
	}
}

func TestHAMaintenanceWritePauseDrainsAcceptedWAL(t *testing.T) {
	group := newTestCluster(t, true)
	leader := group.leader(-1)
	cluster := group.nodes[leader].cluster
	for _, replica := range group.nodes {
		replica.cluster.maintenanceMu.Lock()
	}
	t.Cleanup(func() {
		for _, replica := range group.nodes {
			replica.cluster.maintenanceMu.Unlock()
		}
	})
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-b"}`, http.StatusOK)
	body := `{"source":"agent","collector_id":"collector","batch_id":"before-pause","items":[{"external_id":"host:accepted","entity":{"id":"host:accepted","kind":"host"}}]}`
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body, http.StatusAccepted)
	queued := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"compact"}`, http.StatusAccepted)
	var task storage.Task
	if err := json.Unmarshal(queued.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	operation, resume, err := cluster.pauseMaintenance(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	defer resume()
	status := group.mustRequest(leader, "GET", "/v1/ingest/batches/agent/collector/before-pause", "", http.StatusOK)
	var accepted storage.IngestBatchStatus
	if err := json.Unmarshal(status.Body.Bytes(), &accepted); err != nil || accepted.State != "committed" {
		t.Fatalf("accepted WAL stranded: %+v, %v", accepted, err)
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:accepted", "", http.StatusOK)
	rejected := group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:paused","kind":"host"}]}}`, http.StatusTooManyRequests)
	var pressure struct {
		Retryable bool                         `json:"retryable"`
		Reasons   []storage.BackpressureReason `json:"reasons"`
	}
	if json.Unmarshal(rejected.Body.Bytes(), &pressure) != nil || !pressure.Retryable || len(pressure.Reasons) != 1 || pressure.Reasons[0].Code != "maintenance_pending" {
		t.Fatalf("pause response: %s", rejected.Body.String())
	}
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body, http.StatusTooManyRequests)
	group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"gc"}`, http.StatusAccepted)
	group.mustRequest(leader, "POST", "/v1/indexes/rebuild?async=true", "{}", http.StatusAccepted)
	canceled := group.mustRequest(leader, "POST", "/v1/tasks/"+task.ID+"/cancel", "", http.StatusOK)
	if err := json.Unmarshal(canceled.Body.Bytes(), &task); err != nil || task.Status != storage.TaskStatusCanceled {
		t.Fatalf("paused maintenance cannot be canceled: %+v, %v", task, err)
	}
	select {
	case <-operation.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("canceled maintenance still preparing")
	}
	request := httptest.NewRequest("POST", "/v1/commits", strings.NewReader(`{"mutations":{"upsert_entities":[{"id":"host:other","kind":"host"}]}}`))
	request.Header.Set("X-Tenant-ID", "tenant-b")
	response := httptest.NewRecorder()
	group.nodes[leader].handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("other tenant blocked: %d %s", response.Code, response.Body.String())
	}
	resume()
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:paused","kind":"host"}]}}`, http.StatusOK)
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
		group.mustRequest(i, "GET", "/v1/entities/host:accepted", "", http.StatusOK)
	}
}

func TestHADiskPressureDoesNotBlockOtherTenantGC(t *testing.T) {
	for _, protocol := range []int{1, 3} {
		t.Run(fmt.Sprint("protocol-", protocol), func(t *testing.T) { testHADiskPressureDoesNotBlockOtherTenantGC(t, protocol) })
	}
}

func testHADiskPressureDoesNotBlockOtherTenantGC(t *testing.T, protocol int) {
	group := newTestCluster(t, false)
	if protocol == 3 {
		for i := range group.nodes {
			group.stop(i)
			group.nodes[i].cfg.Raft.Protocol = protocol
			group.start(i)
		}
	}
	leader := group.leader(-1)
	for _, replica := range group.nodes {
		replica.cluster.maintenanceMu.Lock()
	}
	maintenanceLocked := true
	defer func() {
		if maintenanceLocked {
			for _, replica := range group.nodes {
				replica.cluster.maintenanceMu.Unlock()
			}
		}
	}()
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-b"}`, http.StatusOK)
	var compact, gc storage.Task
	response := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"compact"}`, http.StatusAccepted)
	if err := json.Unmarshal(response.Body.Bytes(), &compact); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/tasks", strings.NewReader(`{"type":"gc"}`))
	r.Header.Set("X-Tenant-ID", "tenant-b")
	w := httptest.NewRecorder()
	group.nodes[leader].handler.ServeHTTP(w, r)
	if w.Code != http.StatusAccepted || json.Unmarshal(w.Body.Bytes(), &gc) != nil {
		t.Fatalf("queue GC: %d %s", w.Code, w.Body)
	}
	for _, replica := range group.nodes {
		if err := replica.files.ConfigureDiskSpace(storage.DiskSpacePolicy{MinFreeBytes: 1 << 60}); err != nil {
			t.Fatal(err)
		}
	}
	for _, replica := range group.nodes {
		replica.cluster.maintenanceMu.Unlock()
	}
	maintenanceLocked = false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		completed, err := group.nodes[leader].store.GetTask(context.Background(), "tenant-b", gc.ID)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Status == storage.TaskStatusSucceeded {
			waiting, err := group.nodes[leader].store.GetTask(context.Background(), "tenant-a", compact.ID)
			if err != nil || waiting.Status != storage.TaskStatusQueued {
				t.Fatalf("disk-blocked task changed: %+v, %v", waiting, err)
			}
			return
		}
		if completed.Status == storage.TaskStatusFailed {
			t.Fatalf("GC failed: %+v", completed)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("disk-blocked compact prevented another tenant's GC")
}

func TestHADiagnosticsRemainLocalWithoutQuorum(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	for i := range group.nodes {
		if i != leader {
			group.stop(i)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := group.nodes[leader].cluster.Node.ReadBarrier(ctx); err == nil {
		t.Fatal("strong read succeeded without quorum")
	}
	app := group.nodes[leader].cluster.App
	catalogGate := &Cluster{App: &Application{Catalog: true}}
	app.mu.Lock()
	defer app.mu.Unlock()
	for _, uri := range []string{"/v1/diagnostics", "/metrics"} {
		request := httptest.NewRequest("GET", uri, nil)
		request = request.WithContext(ctx)
		writer := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			catalogGate.ServeRoute(writer, request, false, true, group.nodes[leader].handler)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("local diagnostics %s blocked on the application barrier", uri)
		}
		if writer.Code != http.StatusOK {
			t.Fatalf("local observation %s: %d %s", uri, writer.Code, writer.Body.String())
		}
		if uri == "/metrics" {
			for _, want := range []string{
				`graphdb_raft_operation_seconds_count{operation="read_barrier",status="timeout"}`,
				`graphdb_raft_voters 3`,
				`graphdb_filesystem_inspection_success{role="raft"} 0`,
				`graphdb_go_goroutines`,
			} {
				if !strings.Contains(writer.Body.String(), want) {
					t.Fatalf("missing local diagnostic %s: %s", want, writer.Body)
				}
			}
		}
	}
}

func testHAReplicationFailoverAndSnapshot(t *testing.T, stream bool) {
	group := newTestCluster(t, false)
	if stream {
		for i := range group.nodes {
			group.stop(i)
			group.nodes[i].cfg.Raft.StreamSnapshots = true
			group.start(i)
		}
	}
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	body := `{"idempotency_key":"first","mutations":{"upsert_entities":[{"id":"host:1","kind":"host","fields":{"name":"one"}}]}}`
	group.mustRequest(leader, "POST", "/v1/commits", body, http.StatusOK)
	initial := group.manifest(leader)
	follower := (leader + 1) % 3
	group.mustRequest(follower, "GET", "/v1/entities", "", http.StatusOK)
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

func TestHAConcurrentPublicationBatch(t *testing.T) {
	blocked, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var once sync.Once
	var observations []uint64
	var target atomic.Pointer[Application]
	// Install the hook before starting Raft, as the real server does. Private
	// request handlers can retain the application handler without its mutex.
	group := newTestCluster(t, false, func(app *Application, next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if target.Load() == app && r.Method == "POST" && r.URL.Path == "/v1/commits" {
				position, err := app.Files.ReplicationCheckpoint()
				if err != nil {
					t.Error(err)
				}
				observations = append(observations, position.Index)
				once.Do(func() { close(blocked); <-release })
			}
			next.ServeHTTP(w, r)
		})
	})
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	replica := group.nodes[leader]
	checkpoint, err := replica.files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	target.Store(replica.cluster.App)
	const count = 8
	type publicationResult struct {
		index    int
		response []byte
		err      error
	}
	results := make(chan publicationResult, count)
	bodies := make([]string, count)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := range count {
		bodies[i] = fmt.Sprintf(`{"idempotency_key":"batch-%d","mutations":{"upsert_entities":[{"id":"host:%d","kind":"host","fields":{"name":"value-%d"}}]}}`, i, i, i)
		cmd, err := newCommand("http")
		if err != nil {
			t.Fatal(err)
		}
		cmd.Method, cmd.URI, cmd.Tenant = "POST", "/v1/commits", "tenant-a"
		cmd.Header = make(http.Header)
		cmd.Header.Set("X-Tenant-ID", "tenant-a")
		cmd.Body = []byte(bodies[i])
		payload, err := json.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		// Admission and HTTP read barriers can wait for the blocked application.
		// Submit admitted commands directly to test Raft/application isolation.
		go func(i int, payload []byte) {
			response, err := replica.cluster.Node.Propose(ctx, payload)
			results <- publicationResult{i, response, err}
		}(i, payload)
	}
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("application did not reach the publication barrier")
	}
	deadline := time.Now().Add(3 * time.Second)
	for replica.cluster.Node.Status()["commit_index"].(uint64) < checkpoint.Index+count {
		if time.Now().After(deadline) {
			t.Fatal("Raft stopped committing while application was blocked")
		}
		time.Sleep(10 * time.Millisecond)
	}
	unblock()
	versions := make(map[int64]bool)
	var first storage.CommitResult
	for range count {
		publication := <-results
		var response httpResult
		if publication.err != nil || json.Unmarshal(publication.response, &response) != nil {
			t.Fatalf("publication proposal failed: %s: %v", publication.response, publication.err)
		}
		var result storage.CommitResult
		if response.Status != 200 || json.Unmarshal(response.Body, &result) != nil {
			t.Fatalf("publication failed: %d: %s", response.Status, response.Body)
		}
		if versions[result.Version] {
			t.Fatalf("publication results shared version %d", result.Version)
		}
		versions[result.Version] = true
		if publication.index == 0 {
			first = result
		}
	}
	replica.cluster.App.mu.RLock()
	shared := false
	for i := 1; i < len(observations); i++ {
		shared = shared || observations[i] == observations[i-1]
	}
	replica.cluster.App.mu.RUnlock()
	if !shared || group.manifest(leader).Version != count {
		t.Fatalf("publications did not share a durable transaction: %v", observations)
	}
	checkpoint, err = replica.files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	want := group.manifest(leader)
	for i := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
		got := group.manifest(i)
		if got.Version != want.Version || got.HeadCommitID != want.HeadCommitID || !got.UpdatedAt.Equal(want.UpdatedAt) {
			t.Fatalf("local batch boundaries changed replica identity: got %+v, want %+v", got, want)
		}
	}
	group.stop(leader)
	replacement := group.leader(leader)
	response := group.mustRequest(replacement, "POST", "/v1/commits", bodies[0], http.StatusOK)
	var replay storage.CommitResult
	if err := json.Unmarshal(response.Body.Bytes(), &replay); err != nil || replay.Version != first.Version || replay.HeadCommitID != first.HeadCommitID {
		t.Fatalf("failover replay lost the original result: %+v, %v", replay, err)
	}
	if group.manifest(replacement).Version != count {
		t.Fatal("failover replay published a duplicate version")
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
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body("one"), http.StatusAccepted)
	changed := bytes.ReplaceAll([]byte(body("one")), []byte(`"name":"one"`), []byte(`"name":"changed"`))
	group.mustRequest(leader, "POST", "/v1/ingest/batches", string(changed), http.StatusConflict)
	generation, err := group.nodes[leader].store.ReplicationTenantGeneration(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	record, err := group.nodes[leader].cluster.App.accepted(context.Background(), group.nodes[leader].cluster.App.ingestKey("tenant-a", "agent", "collector", "one", generation))
	if err != nil || len(record.Request.Items) != 0 || record.Digest == "" {
		t.Fatalf("completed WAL payload was not compacted: %v", err)
	}
	for i, id := range []string{"one", "two", "three"} {
		status := group.mustRequest(leader, "GET", "/v1/ingest/batches/agent/collector/"+id, "", http.StatusOK)
		var record storage.IngestBatchStatus
		if err := json.Unmarshal(status.Body.Bytes(), &record); err != nil || record.State != "committed" || record.Result == nil || record.Result.Version != int64(i+1) {
			t.Fatalf("batch %s was not published exactly once: %+v, %v", id, record, err)
		}
	}
}

func TestHAWALAcceptanceUsesLeaderQueueBudget(t *testing.T) {
	group := newTestCluster(t, true)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	for i, replica := range group.nodes {
		replica.cluster.App.mu.Lock()
		replica.cluster.App.MaxPendingBytes = 1
		if i == leader {
			replica.cluster.App.MaxPendingBytes = 32 << 10
		}
		replica.cluster.App.mu.Unlock()
	}
	body := `{"source":"agent","collector_id":"collector","batch_id":"accepted-before-failover","items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host"}}]}`
	group.mustRequest(leader, "POST", "/v1/ingest/batches", body, http.StatusAccepted)
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
	}
	group.stop(leader)
	leader = group.leader(leader)
	group.mustRequest(leader, "GET", "/v1/ingest/batches/agent/collector/accepted-before-failover", "", http.StatusOK)
	if err := group.nodes[leader].cluster.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:1", "", http.StatusOK)
}

func TestHAWritesUseLeaderQuota(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(fmt.Sprintf("wal=%t", wal), func(t *testing.T) {
			group := newTestCluster(t, wal)
			leader := group.leader(-1)
			group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
			for i, replica := range group.nodes {
				replica.cluster.App.mu.Lock()
				limit := 1
				if i == leader {
					limit = 100
				}
				replica.store.Backpressure = storage.NewWritePressure(storage.BackpressureConfig{MaxEntitiesPerTenant: limit, ObjectErrorThreshold: 1})
				if i != leader {
					replica.store.Backpressure.RecordObjectOperation(time.Second, storage.ErrObjectStoreUnavailable)
				}
				if i == (leader+1)%len(group.nodes) {
					replica.store.Backpressure = nil
				}
				replica.cluster.App.mu.Unlock()
			}
			body := `{"source":"agent","collector_id":"collector","batch_id":"quota","items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host"}},{"external_id":"host:2","entity":{"id":"host:2","kind":"host"}}]}`
			status := http.StatusOK
			if wal {
				status = http.StatusAccepted
			}
			group.mustRequest(leader, "POST", "/v1/ingest/batches", body, status)
			if wal {
				if err := group.nodes[leader].cluster.flushPending(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			group.mustRequest(leader, "GET", "/v1/entities/host:2", "", http.StatusOK)
			group.mustRequest(leader, "PUT", "/v1/tenant-config", `{"quota":{"max_entities_per_tenant":2}}`, http.StatusOK)
			group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:3","kind":"host"}]}}`, http.StatusTooManyRequests)
			checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			for i := range group.nodes {
				group.waitApplied(i, checkpoint.Index)
			}
			group.stop(leader)
			leader = group.leader(leader)
			group.mustRequest(leader, "GET", "/v1/entities/host:2", "", http.StatusOK)
			group.mustRequest(leader, "GET", "/v1/entities/host:3", "", http.StatusNotFound)
		})
	}
}

func TestHAObjectBackupUsesLeaderRepository(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/ingest/batches", `{"source":"agent","collector_id":"collector","batch_id":"backup","items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host"}}]}`, http.StatusOK)
	endpoint := os.Getenv("GRAPHDB_TEST_BACKUP_S3_ENDPOINT")
	if endpoint == "" {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "backup source unavailable", http.StatusServiceUnavailable)
		}))
		defer server.Close()
		endpoint = server.URL
	}
	repositoryConfig := backupstore.Config{
		Bucket: "test-bucket", Prefix: fmt.Sprintf("replica-%d", time.Now().UnixNano()), Endpoint: endpoint, PathStyle: true,
		AccessKeyID: "test", SecretAccessKey: "test-secret",
	}
	if os.Getenv("GRAPHDB_TEST_BACKUP_S3_ENDPOINT") != "" {
		repositoryConfig.Bucket = os.Getenv("GRAPHDB_TEST_BACKUP_S3_BUCKET")
		repositoryConfig.AccessKeyID = os.Getenv("GRAPHDB_TEST_BACKUP_S3_ACCESS_KEY_ID")
		repositoryConfig.SecretAccessKey = os.Getenv("GRAPHDB_TEST_BACKUP_S3_SECRET_ACCESS_KEY")
	}
	repo, err := backupstore.New(context.Background(), repositoryConfig)
	if err != nil {
		t.Fatal(err)
	}
	other, err := backupstore.New(context.Background(), backupstore.Config{
		Bucket: "other-bucket", Prefix: "other-prefix", Endpoint: endpoint, PathStyle: true,
		AccessKeyID: "test", SecretAccessKey: "test-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	group.nodes[leader].cluster.App.mu.Lock()
	group.nodes[leader].store.Backups = repo
	group.nodes[leader].cluster.App.mu.Unlock()
	follower := group.nodes[(leader+1)%len(group.nodes)]
	follower.cluster.App.mu.Lock()
	follower.store.Backups = other
	follower.cluster.App.mu.Unlock()
	response := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"tenant_backup","params":{"destination":"object"}}`, http.StatusAccepted)
	var task storage.Task
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i, replica := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
		replica.cluster.App.mu.RLock()
		_, err := replica.store.GetTask(context.Background(), "tenant-a", task.ID)
		replica.cluster.App.mu.RUnlock()
		if err != nil {
			t.Fatalf("acknowledged object backup task missing on replica %d: %v", i, err)
		}
	}
	if os.Getenv("GRAPHDB_TEST_BACKUP_S3_ENDPOINT") != "" {
		for range 2 {
			if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		response = group.mustRequest(leader, "GET", "/v1/tasks/"+task.ID, "", http.StatusOK)
		if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
			t.Fatal(err)
		}
		if task.Status != storage.TaskStatusSucceeded {
			t.Fatalf("object backup failed: %+v", task)
		}
		checkpoint, err = group.nodes[leader].files.ReplicationCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		for i, replica := range group.nodes {
			group.waitApplied(i, checkpoint.Index)
			replica.cluster.App.mu.RLock()
			stored, err := replica.store.GetTask(context.Background(), "tenant-a", task.ID)
			replica.cluster.App.mu.RUnlock()
			if err != nil || stored.Status != storage.TaskStatusSucceeded || stored.Result["backup_key"] != task.Result["backup_key"] {
				t.Fatalf("object backup result differs on replica %d: %+v, %v", i, stored, err)
			}
		}
		group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", `{"backup_key":"s3://other-bucket/other-prefix/tenant-a/bad/manifest.json","overwrite":true}`, http.StatusBadRequest)
		response = group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", fmt.Sprintf(`{"backup_key":%q,"overwrite":true}`, task.Result["backup_key"]), http.StatusAccepted)
		var restore storage.Task
		if err := json.Unmarshal(response.Body.Bytes(), &restore); err != nil {
			t.Fatal(err)
		}
		if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
			t.Fatal(err)
		}
		checkpoint, err = group.nodes[leader].files.ReplicationCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		for i, replica := range group.nodes {
			group.waitApplied(i, checkpoint.Index)
			replica.cluster.App.mu.RLock()
			stored, err := replica.store.GetTask(context.Background(), "tenant-a", restore.ID)
			replica.cluster.App.mu.RUnlock()
			if err != nil || stored.Status != storage.TaskStatusSucceeded {
				t.Fatalf("S3 restore failed on replica %d: %+v, %v", i, stored, err)
			}
		}
	}
	group.stop(leader)
	leader = group.leader(leader)
	group.mustRequest(leader, "GET", "/v1/tasks/"+task.ID, "", http.StatusOK)
	group.mustRequest(leader, "GET", "/v1/entities/host:1", "", http.StatusOK)
}

func TestHAObjectRestoreResumesAfterLeaderLoss(t *testing.T) {
	endpoint := os.Getenv("GRAPHDB_TEST_BACKUP_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set GRAPHDB_TEST_BACKUP_S3_ENDPOINT for S3 integration")
	}
	repo, err := backupstore.New(context.Background(), backupstore.Config{
		Bucket: os.Getenv("GRAPHDB_TEST_BACKUP_S3_BUCKET"), Prefix: fmt.Sprintf("raft-resume-%d", time.Now().UnixNano()), Endpoint: endpoint, PathStyle: true,
		AccessKeyID: os.Getenv("GRAPHDB_TEST_BACKUP_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("GRAPHDB_TEST_BACKUP_S3_SECRET_ACCESS_KEY"),
	})
	if err != nil || repo == nil {
		t.Fatalf("backup repository: %v", err)
	}
	group := newTestCluster(t, false, func(app *Application, next http.Handler) http.Handler {
		app.Store.Backups = repo
		return next
	})
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	payload := strings.Repeat("x", 2<<20)
	group.mustRequest(leader, "POST", "/v1/commits", fmt.Sprintf(`{"mutations":{"upsert_entities":[{"id":"host:1","kind":"host","fields":{"payload":%q}}]}}`, payload), http.StatusOK, time.Minute)
	response := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"tenant_backup","params":{"destination":"object"}}`, http.StatusAccepted)
	var task storage.Task
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cluster := group.nodes[leader].cluster
	for range 2 {
		if err := cluster.runQueuedTask(ctx); err != nil {
			t.Fatal(err)
		}
	}
	response = group.mustRequest(leader, "GET", "/v1/tasks/"+task.ID, "", http.StatusOK)
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil || task.Status != storage.TaskStatusSucceeded {
		t.Fatalf("object backup did not succeed: %+v, %v", task, err)
	}
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"delete_entities":["host:1"]}}`, http.StatusOK, time.Minute)
	response = group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", fmt.Sprintf(`{"backup_key":%q,"overwrite":true}`, task.Result["backup_key"]), http.StatusAccepted)
	task = storage.Task{}
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	cluster.App.mu.RLock()
	input, err := cluster.App.Store.PrepareReplicatedTask(ctx, task)
	generation, generationErr := cluster.App.Store.ReplicationTenantGeneration(ctx, task.TenantID)
	cluster.App.mu.RUnlock()
	if err != nil || generationErr != nil || len(input) <= restoreChunkBytes {
		t.Fatalf("restore input: %d bytes, errors=%v/%v", len(input), err, generationErr)
	}
	digest := sha256.Sum256(input)
	manifest := restoreManifest{Bytes: int64(len(input)), SHA256: hex.EncodeToString(digest[:]), Generation: generation}
	cmd, err := newCommand("restore_part")
	if err != nil {
		t.Fatal(err)
	}
	cmd.Tenant, cmd.IDs, cmd.ExpectedGeneration = task.TenantID, []string{task.ID}, generation
	cmd.Restore = input[:restoreChunkBytes]
	cmd.Body, _ = json.Marshal(restorePart{restoreManifest: manifest, Part: 0})
	if _, err := cluster.propose(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
	}
	group.stop(leader)
	replacement := group.leader(leader)
	if err := group.nodes[replacement].cluster.runQueuedTask(ctx); err != nil {
		t.Fatal(err)
	}
	response = group.mustRequest(replacement, "GET", "/v1/tasks/"+task.ID, "", http.StatusOK)
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil || task.Status != storage.TaskStatusSucceeded {
		t.Fatalf("S3 restore did not resume after leader loss: %+v, %v", task, err)
	}
	group.start(leader)
	checkpoint, err = group.nodes[replacement].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i, replica := range group.nodes {
		group.waitApplied(i, checkpoint.Index, time.Minute)
		replica.cluster.App.mu.RLock()
		g, m, loadErr := replica.store.Load(ctx, "tenant-a")
		staging, listErr := replica.files.List(ctx, replica.cluster.App.restorePrefix("tenant-a", task.ID))
		replica.cluster.App.mu.RUnlock()
		if loadErr != nil || listErr != nil || m.Version != 1 || len(staging) != 0 {
			t.Fatalf("replica %d restore: version=%d staging=%d errors=%v/%v", i, m.Version, len(staging), loadErr, listErr)
		}
		entity, ok := g.GetEntity("host:1")
		if !ok || entity.Fields["payload"] != payload {
			t.Fatalf("replica %d did not restore the backed-up entity", i)
		}
	}
	group.mustRequest(replacement, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:2","kind":"host"}]}}`, http.StatusOK, time.Minute)
}

func TestHAWALBackpressureLeavesClusterAvailable(t *testing.T) {
	group := newTestCluster(t, true)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	for _, replica := range group.nodes {
		replica.cluster.App.mu.Lock()
		replica.store.Backpressure = storage.NewWritePressure(storage.BackpressureConfig{MaxCommitTail: 1})
		replica.cluster.App.mu.Unlock()
	}
	group.nodes[leader].cluster.FlushMaxRequests = 1
	for i := 1; i <= 3; i++ {
		body := fmt.Sprintf(`{"source":"agent","collector_id":"collector","batch_id":"tail-%d","items":[{"external_id":"host:%d","entity":{"id":"host:%d","kind":"host"}}]}`, i, i, i)
		group.mustRequest(leader, "POST", "/v1/ingest/batches", body, http.StatusAccepted)
	}
	for range 2 {
		if err := group.nodes[leader].cluster.flushPending(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := group.nodes[leader].cluster.flushPending(context.Background()); !errors.Is(err, storage.ErrBackpressure) {
		t.Fatalf("expected leader to defer the next flush for compaction: %v", err)
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:2", "", http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/compact", `{}`, http.StatusOK)
	if err := group.nodes[leader].cluster.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:3", "", http.StatusOK)
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
	type stagedInput struct{ body, input []byte }
	var completedTransfer atomic.Pointer[stagedInput]
	group := newTestCluster(t, false, func(app *Application, next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			seed := completedTransfer.Load()
			if seed == nil || r.Method != "POST" || r.URL.Path != "/v1/tenants/tenant-a/restore" {
				return
			}
			var task storage.Task
			if err := json.Unmarshal(w.(*httptest.ResponseRecorder).Body.Bytes(), &task); err != nil {
				t.Error(err)
				return
			}
			if _, err := app.stageRestore(r.Context(), command{Tenant: task.TenantID, IDs: []string{task.ID}, Body: seed.body, Restore: seed.input}); err != nil {
				t.Error(err)
			}
		})
	})
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
	for _, dryRun := range []bool{true, false} {
		response := group.mustRequest(leader, "POST", "/v1/tenants/tenant-cold/restore",
			fmt.Sprintf(`{"backup_key":%q,"dry_run":%t}`, finished.ResultKey, dryRun), http.StatusAccepted)
		var cold storage.Task
		if err := json.Unmarshal(response.Body.Bytes(), &cold); err != nil {
			t.Fatal(err)
		}
		if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
			t.Fatal(err)
		}
		group.nodes[leader].cluster.App.mu.RLock()
		cold, err = group.nodes[leader].store.GetTask(context.Background(), "tenant-cold", cold.ID)
		group.nodes[leader].cluster.App.mu.RUnlock()
		if err != nil || cold.Status != storage.TaskStatusSucceeded {
			t.Fatalf("restore into unregistered tenant did not run: dry_run=%t task=%+v error=%v", dryRun, cold, err)
		}
		if dryRun && (cold.Result["dry_run"] != true || cold.Result["target_exists"] == true) {
			t.Fatalf("dry run created a target graph: %+v", cold.Result)
		}
	}
	drillResponse := group.mustRequest(leader, "POST", "/v1/tenants/tenant-cold/restore-drill",
		fmt.Sprintf(`{"backup_key":%q,"target_tenant_id":"tenant-proof","cleanup":true}`, finished.ResultKey), http.StatusAccepted)
	var drill storage.Task
	if err := json.Unmarshal(drillResponse.Body.Bytes(), &drill); err != nil {
		t.Fatal(err)
	}
	if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	group.nodes[leader].cluster.App.mu.RLock()
	drill, err = group.nodes[leader].store.GetTask(context.Background(), "tenant-cold", drill.ID)
	group.nodes[leader].cluster.App.mu.RUnlock()
	if err != nil || drill.Status != storage.TaskStatusSucceeded {
		t.Fatalf("restore drill failed: task=%+v error=%v", drill, err)
	}
	data, err := json.Marshal(drill.Result)
	var proof storage.TenantRestoreDrillReport
	if err != nil || json.Unmarshal(data, &proof) != nil {
		t.Fatalf("decode restore drill result: %v", err)
	}
	for _, result := range proof.QueryResults {
		if !result.Skipped {
			t.Fatalf("restore drill executed an unconfigured query: %+v", result)
		}
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
	cluster := group.nodes[leader].cluster
	cluster.App.mu.RLock()
	legacyInput, err := cluster.App.Store.PrepareReplicatedTask(context.Background(), restoreTask)
	cluster.App.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := newCommand("task")
	if err != nil {
		t.Fatal(err)
	}
	legacy.Tenant, legacy.IDs, legacy.Restore = restoreTask.TenantID, []string{restoreTask.ID}, legacyInput
	if _, err := cluster.propose(context.Background(), legacy); err != nil {
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
	digest := sha256.Sum256(legacyInput)
	manifest := restoreManifest{Bytes: int64(len(legacyInput)), SHA256: hex.EncodeToString(digest[:]), Generation: 2}
	// Seed a fully persisted transfer with queue admission, before the live
	// background worker can try the deliberately unavailable backup source.
	partBody, _ := json.Marshal(restorePart{restoreManifest: manifest})
	completedTransfer.Store(&stagedInput{body: partBody, input: legacyInput})
	queuedResponse := group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", `{"backup_key":"graphdb/unavailable-backup","overwrite":true}`, http.StatusAccepted)
	var completedInputTask storage.Task
	if err := json.Unmarshal(queuedResponse.Body.Bytes(), &completedInputTask); err != nil {
		t.Fatal(err)
	}
	stagedCheckpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	previousLeader := leader
	group.stop(previousLeader)
	leader = group.leader(previousLeader)
	for i := range group.nodes {
		if i == previousLeader {
			continue
		}
		group.waitApplied(i, stagedCheckpoint.Index)
	}
	completedTransfer.Store(nil)
	if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	completedResponse := group.mustRequest(leader, "GET", "/v1/tasks/"+completedInputTask.ID, "", http.StatusOK)
	if err := json.Unmarshal(completedResponse.Body.Bytes(), &completedInputTask); err != nil || completedInputTask.Status != storage.TaskStatusSucceeded {
		t.Fatalf("fully persisted restore fetched unavailable backup after failover: %s, %v", completedResponse.Body.String(), err)
	}
	group.start(previousLeader)
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
		for {
			request := httptest.NewRequest("POST", "/raft/members", bytes.NewBufferString(body)).WithContext(ctx)
			request.Header.Set("Authorization", "Bearer "+replica.cfg.Raft.Token)
			request.Header.Set("X-Raft-Cluster", replica.cfg.Raft.ClusterID)
			writer := httptest.NewRecorder()
			group.nodes[leader].cluster.Node.Handler().ServeHTTP(writer, request)
			if writer.Code == status {
				return
			}
			// The admission barrier can observe a newer commit than waitApplied.
			// A rejected promotion has not changed membership and can be retried.
			if body == `{"action":"promote","id":4}` && status == http.StatusNoContent && writer.Code == http.StatusConflict && ctx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatalf("membership %s: %d: %s", body, writer.Code, writer.Body.String())
		}
	}
	memberRequest(fmt.Sprintf(`{"action":"add_learner","id":4,"url":%q}`, replica.peer.URL), http.StatusNoContent)
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	group.waitApplied(3, max(checkpoint.Index, group.nodes[leader].cluster.Node.Status()["commit_index"].(uint64)))
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

func TestHALargeRestoreResumesAfterLeaderLoss(t *testing.T) {
	group := newTestCluster(t, false)
	// This case targets restore-part replay. Frequent full compatibility
	// snapshots of its large setup graph dominate race-instrumented elections;
	// snapshot faults are covered by separate cases and deployment gates.
	for i := range group.nodes {
		group.stop(i)
	}
	for i, replica := range group.nodes {
		replica.cfg.Raft.SnapshotEntries = 100
		replica.cfg.Raft.ElectionTicks = 30
		group.start(i)
	}
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK, time.Minute)
	payload := string(bytes.Repeat([]byte("x"), 4<<20))
	for i := 0; i < 9; i++ {
		body := fmt.Sprintf(`{"idempotency_key":"restore-setup-%d","mutations":{"upsert_entities":[{"id":"host:%d","kind":"host","fields":{"payload":%q}}]}}`, i, i, payload)
		deadline := time.Now().Add(time.Minute)
		for {
			leader = group.leader(-1)
			response := group.request(leader, "POST", "/v1/commits", body, time.Until(deadline))
			if response.Code == http.StatusOK {
				break
			}
			var failure struct {
				Code      string `json:"code"`
				Retryable bool   `json:"retryable"`
			}
			if response.Code != http.StatusServiceUnavailable || json.Unmarshal(response.Body.Bytes(), &failure) != nil || failure.Code != "raft_unavailable" || !failure.Retryable || time.Now().After(deadline) {
				t.Fatalf("restore setup %d: %d: %s", i, response.Code, response.Body.String())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	backup := group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/backup", `{}`, http.StatusAccepted, time.Minute)
	var task storage.Task
	if err := json.Unmarshal(backup.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	backupCtx, backupCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer backupCancel()
	if err := group.nodes[leader].cluster.runQueuedTask(backupCtx); err != nil {
		t.Fatal(err)
	}
	finished := group.mustRequest(leader, "GET", "/v1/tasks/"+task.ID, "", http.StatusOK, time.Minute)
	if err := json.Unmarshal(finished.Body.Bytes(), &task); err != nil || task.Status != storage.TaskStatusSucceeded {
		t.Fatalf("backup failed: %s, %v", finished.Body.String(), err)
	}
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"delete_entities":["host:0"]}}`, http.StatusOK, time.Minute)
	restore := group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/restore", fmt.Sprintf(`{"backup_key":%q,"overwrite":true}`, task.ResultKey), http.StatusAccepted, time.Minute)
	if err := json.Unmarshal(restore.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cluster := group.nodes[leader].cluster
	cluster.App.mu.RLock()
	input, err := cluster.App.Store.PrepareReplicatedTask(ctx, task)
	cluster.App.mu.RUnlock()
	if err != nil || len(input) <= 32<<20 {
		t.Fatalf("large restore input: %d bytes, %v", len(input), err)
	}
	digest := sha256.Sum256(input)
	manifest := restoreManifest{Bytes: int64(len(input)), SHA256: hex.EncodeToString(digest[:]), Generation: 1}
	for part := int64(0); part < 2; part++ {
		cmd, err := newCommand("restore_part")
		if err != nil {
			t.Fatal(err)
		}
		cmd.Tenant, cmd.IDs = task.TenantID, []string{task.ID}
		cmd.ExpectedGeneration = 1
		cmd.Restore = input[part*restoreChunkBytes : (part+1)*restoreChunkBytes]
		cmd.Body, _ = json.Marshal(restorePart{restoreManifest: manifest, Part: part})
		if _, err := cluster.propose(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	group.stop(leader)
	replacement := group.leader(leader)
	if err := group.nodes[replacement].cluster.runQueuedTask(ctx); err != nil {
		t.Fatal(err)
	}
	result := group.mustRequest(replacement, "GET", "/v1/tasks/"+task.ID, "", http.StatusOK, time.Minute)
	if err := json.Unmarshal(result.Body.Bytes(), &task); err != nil || task.Status != storage.TaskStatusSucceeded {
		t.Fatalf("resumed restore failed: %s, %v", result.Body.String(), err)
	}
	if group.manifest(replacement).Version != 9 {
		t.Fatal("restore did not publish the backed-up version")
	}
	group.start(leader)
	checkpoint, err := group.nodes[replacement].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i := range group.nodes {
		// Rejoining replays and decodes a >32 MiB graph under race instrumentation.
		group.waitApplied(i, checkpoint.Index, 3*time.Minute)
	}
	verificationCtx, verificationCancel := context.WithTimeout(context.Background(), time.Minute)
	defer verificationCancel()
	for i, replica := range group.nodes {
		replica.cluster.App.mu.RLock()
		generation, err := replica.store.ReplicationTenantGeneration(verificationCtx, "tenant-a")
		objects, listErr := replica.files.List(verificationCtx, replica.cluster.App.restorePrefix("tenant-a", task.ID))
		replica.cluster.App.mu.RUnlock()
		if err != nil || listErr != nil || generation != 2 || len(objects) != 0 || group.manifest(i).Version != 9 {
			t.Fatalf("replica %d restore: generation=%d staging=%d errors=%v/%v", i, generation, len(objects), err, listErr)
		}
	}
}

func TestHAReadinessDuringMaintenanceStillRequiresFreshReads(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var target atomic.Pointer[Application]
	var blocked atomic.Bool
	group := newTestCluster(t, false, func(app *Application, next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if target.Load() == app && r.Method == "POST" && r.URL.Path == "/v1/commits" && blocked.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
			next.ServeHTTP(w, r)
		})
	})
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	target.Store(group.nodes[leader].cluster.App)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	completed := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		completed <- group.request(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:blocked","kind":"host"}]}}`)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("application did not start")
	}
	group.mustRequest(leader, "GET", "/v1/readiness", "", http.StatusOK)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	request := httptest.NewRequest("GET", "/v1/entities/host:blocked", nil).WithContext(ctx)
	request.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()
	group.nodes[leader].handler.ServeHTTP(response, request)
	cancel()
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("query bypassed unapplied commit: %d: %s", response.Code, response.Body.String())
	}
	arrived := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		group.nodes[leader].handler.ServeHTTP(w, r)
	}))
	server.Config.ReadTimeout = 100 * time.Millisecond
	server.Start()
	defer func() { unblock(); server.Close() }()
	server.Client().Timeout = 5 * time.Second
	written := make(chan error, 1)
	go func() {
		body := fmt.Sprintf(`{"mutations":{"upsert_entities":[{"id":"host:after","kind":"host","fields":{"payload":%q}}]}}`, bytes.Repeat([]byte("x"), 8<<10))
		request, err := http.NewRequest("POST", server.URL+"/v1/commits", bytes.NewBufferString(body))
		if err != nil {
			written <- err
			return
		}
		request.Header.Set("X-Tenant-ID", "tenant-a")
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			written <- err
			return
		}
		defer response.Body.Close()
		result, err := io.ReadAll(response.Body)
		if err == nil && response.StatusCode != http.StatusOK {
			err = fmt.Errorf("write after maintenance: %d: %s", response.StatusCode, result)
		}
		written <- err
	}()
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP write did not arrive")
	}
	// The body read deadline expires while the committed application is held.
	time.Sleep(150 * time.Millisecond)
	unblock()
	select {
	case response := <-completed:
		if response.Code != http.StatusOK {
			t.Fatalf("commit failed: %d: %s", response.Code, response.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("application did not finish")
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	group.mustRequest(leader, "GET", "/v1/entities/host:blocked", "", http.StatusOK)
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

type replicaReadFaultStore struct {
	storage.ObjectStore
	app    *Application
	target *atomic.Pointer[Application]
	err    error
	graph  bool
}

func (s *replicaReadFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	matched := strings.Contains(key, "/tasks/imports/")
	if s.graph {
		matched = strings.Contains(key, "/commits/")
	}
	if s.app == s.target.Load() && matched {
		if s.err == nil {
			if s.graph {
				return []byte("corrupt commit"), nil
			}
			return []byte(`{"entity":{"id":"host:corrupt","kind":"host"}}`), nil
		}
		return nil, s.err
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *replicaReadFaultStore) UnwrapObjectStore() storage.ObjectStore { return s.ObjectStore }

func TestHAReferencedGraphFailureStopsReplicaUntilReplay(t *testing.T) {
	for _, fault := range []struct {
		name     string
		err      error
		manifest bool
	}{{"missing_commit", storage.ErrNotFound, false}, {"corrupt_commit", nil, false}, {"missing_manifest", nil, true}} {
		t.Run(fault.name, func(t *testing.T) {
			var target atomic.Pointer[Application]
			group := newTestCluster(t, false, func(app *Application, next http.Handler) http.Handler {
				app.Store.MaxWriteCacheTenants = 0
				app.Store.Objects = storage.NewMeteredObjectStore(&replicaReadFaultStore{ObjectStore: app.Store.Objects, app: app, target: &target, err: fault.err, graph: true}, nil, nil)
				return next
			})
			leader := group.leader(-1)
			follower := (leader + 1) % len(group.nodes)
			group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
			group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:seed","kind":"host"}]}}`, http.StatusOK)
			response := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"compact"}`, http.StatusAccepted)
			var task storage.Task
			if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
				t.Fatal(err)
			}
			checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			for i := range group.nodes {
				group.waitApplied(i, checkpoint.Index)
			}
			var savedManifest []byte
			manifestPath := filepath.Join(group.nodes[follower].cfg.DataDir, "graphdb", "tenants", "tenant-a", "manifest.parquet")
			if fault.manifest {
				group.stop(follower)
				var err error
				savedManifest, err = os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(manifestPath); err != nil {
					t.Fatal(err)
				}
				group.start(follower)
			} else {
				target.Store(group.nodes[follower].cluster.App)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := group.nodes[leader].cluster.runQueuedTask(ctx); err != nil {
				t.Fatal(err)
			}
			checkpoint, err = group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			group.waitApplied((leader+2)%len(group.nodes), checkpoint.Index)
			faulted := group.nodes[follower].cluster
			for faulted.Status()["error"] == nil && ctx.Err() == nil {
				current, err := group.nodes[follower].files.ReplicationCheckpoint()
				if err != nil {
					t.Fatal(err)
				}
				if current.Index >= checkpoint.Index {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			faulted.App.mu.RLock()
			current, checkpointErr := group.nodes[follower].files.ReplicationCheckpoint()
			stored, taskErr := group.nodes[follower].store.GetTask(ctx, "tenant-a", task.ID)
			faulted.App.mu.RUnlock()
			if faulted.Status()["error"] == nil || checkpointErr != nil || taskErr != nil || current.Index >= checkpoint.Index || stored.Status != storage.TaskStatusQueued {
				t.Fatalf("graph fault became a durable task outcome: node_error=%v checkpoint=%d task_status=%s task_error=%s errors=%v/%v", faulted.Status()["error"], current.Index, stored.Status, stored.Error, checkpointErr, taskErr)
			}
			group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:quorum","kind":"host"}]}}`, http.StatusOK)
			group.stop(follower)
			target.Store(nil)
			if fault.manifest {
				if err := os.WriteFile(manifestPath, savedManifest, 0600); err != nil {
					t.Fatal(err)
				}
			}
			group.start(follower)
			checkpoint, err = group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			group.waitApplied(follower, checkpoint.Index)
			group.nodes[follower].cluster.App.mu.RLock()
			g, m, loadErr := group.nodes[follower].store.Load(ctx, "tenant-a")
			stored, taskErr = group.nodes[follower].store.GetTask(ctx, "tenant-a", task.ID)
			group.nodes[follower].cluster.App.mu.RUnlock()
			if loadErr != nil || taskErr != nil || stored.Status != storage.TaskStatusSucceeded || m.SnapshotVersion != 1 || m.Version != 2 {
				t.Fatalf("replayed graph/task: manifest=%+v task=%+v errors=%v/%v", m, stored, loadErr, taskErr)
			}
			for _, id := range []string{"host:seed", "host:quorum"} {
				if _, ok := g.GetEntity(id); !ok {
					t.Fatalf("repaired replica is missing %s", id)
				}
			}
		})
	}
}

func TestHAImportReadFailureStopsReplicaUntilReplay(t *testing.T) {
	for _, fault := range []struct {
		name string
		err  error
	}{{"io_error", syscall.EIO}, {"missing_source", storage.ErrNotFound}, {"corrupt_source", nil}, {"leader_corrupt_source", nil}} {
		t.Run(fault.name, func(t *testing.T) {
			var faultTarget atomic.Pointer[Application]
			group := newTestCluster(t, false, func(app *Application, next http.Handler) http.Handler {
				app.Store.Objects = storage.NewMeteredObjectStore(&replicaReadFaultStore{ObjectStore: app.Store.Objects, app: app, target: &faultTarget, err: fault.err}, nil, nil)
				return next
			})
			leader := group.leader(-1)
			follower := (leader + 1) % len(group.nodes)
			faultTarget.Store(group.nodes[follower].cluster.App)
			if fault.name == "leader_corrupt_source" {
				faultTarget.Store(group.nodes[leader].cluster.App)
			}
			group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
			if fault.name == "missing_source" {
				invalid := group.mustRequest(leader, "POST", "/v1/tasks", `{"type":"bulk_import","params":{"import_id":"manual","source_key":"graphdb/tenants/tenant-a/tasks/imports/missing.jsonl","format":"jsonl"}}`, http.StatusAccepted)
				var invalidTask storage.Task
				if err := json.Unmarshal(invalid.Body.Bytes(), &invalidTask); err != nil {
					t.Fatal(err)
				}
				if err := group.nodes[leader].cluster.runQueuedTask(context.Background()); err != nil {
					t.Fatalf("invalid import source stopped the cluster: %v", err)
				}
				checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
				if err != nil {
					t.Fatal(err)
				}
				for i, replica := range group.nodes {
					group.waitApplied(i, checkpoint.Index)
					failed, err := replica.store.GetTask(context.Background(), "tenant-a", invalidTask.ID)
					if err != nil || failed.Status != storage.TaskStatusFailed {
						t.Fatalf("invalid source was not a consistent task failure: %+v, %v", failed, err)
					}
				}
			}
			response := group.mustRequest(leader, "POST", "/v1/imports?format=jsonl&batch_size=1", `{"entity":{"id":"host:import","kind":"host"}}`, http.StatusAccepted)
			var task storage.Task
			if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if fault.name == "leader_corrupt_source" {
				if err := group.nodes[leader].cluster.runQueuedTask(ctx); err != nil {
					t.Fatal(err)
				}
				checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
				if err != nil {
					t.Fatal(err)
				}
				for i, replica := range group.nodes {
					group.waitApplied(i, checkpoint.Index)
					failed, err := replica.store.GetTask(ctx, "tenant-a", task.ID)
					if err != nil || failed.Status != storage.TaskStatusFailed {
						t.Fatalf("leader source corruption was accepted: %+v, %v", failed, err)
					}
					if group.manifest(i).Version != 0 {
						t.Fatal("corrupt import published graph data")
					}
				}
				faultTarget.Store(nil)
				group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:after-invalid-source","kind":"host"}]}}`, http.StatusOK)
				return
			}
			for {
				group.nodes[leader].cluster.App.mu.RLock()
				completed, err := group.nodes[leader].store.GetTask(ctx, "tenant-a", task.ID)
				group.nodes[leader].cluster.App.mu.RUnlock()
				if err != nil {
					t.Fatal(err)
				}
				if completed.Status == storage.TaskStatusSucceeded {
					break
				}
				if completed.Status == storage.TaskStatusFailed || ctx.Err() != nil {
					t.Fatalf("healthy import did not succeed: %+v, %v", completed, ctx.Err())
				}
				time.Sleep(20 * time.Millisecond)
			}
			checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			group.waitApplied((leader+2)%len(group.nodes), checkpoint.Index)
			faulted := group.nodes[follower].cluster
			for faulted.Status()["error"] == nil && ctx.Err() == nil {
				applied, err := group.nodes[follower].files.ReplicationCheckpoint()
				if err != nil {
					t.Fatal(err)
				}
				if applied.Index >= checkpoint.Index {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			faulted.App.mu.RLock()
			failedCheckpoint, err := group.nodes[follower].files.ReplicationCheckpoint()
			queued, taskErr := group.nodes[follower].store.GetTask(ctx, "tenant-a", task.ID)
			faulted.App.mu.RUnlock()
			if faulted.Status()["error"] == nil {
				t.Fatalf("faulted replica continued after skipping a committed import: checkpoint=%+v task=%+v", failedCheckpoint, queued)
			}
			t.Logf("faulted replica: %v", faulted.Status()["error"])
			if err != nil || failedCheckpoint.Index >= checkpoint.Index || taskErr != nil || queued.Status != storage.TaskStatusQueued {
				t.Fatalf("read fault became durable: checkpoint=%+v task=%+v errors=%v/%v", failedCheckpoint, queued, err, taskErr)
			}
			group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:quorum","kind":"host"}]}}`, http.StatusOK)
			group.stop(follower)
			faultTarget.Store(nil)
			group.start(follower)
			checkpoint, err = group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			group.waitApplied(follower, checkpoint.Index)
			group.nodes[follower].cluster.App.mu.RLock()
			graph, _, err := group.nodes[follower].store.Load(ctx, "tenant-a")
			group.nodes[follower].cluster.App.mu.RUnlock()
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"host:import", "host:quorum"} {
				if _, ok := graph.Entities.Get(id); !ok {
					t.Fatalf("repaired replica is missing %s", id)
				}
			}
		})
	}
}

func TestHADelayedTaskAfterPurge(t *testing.T) {
	for _, scenario := range []string{"task", "prepare_error", "capture_backup"} {
		t.Run(scenario, func(t *testing.T) {
			group := newTestCluster(t, false)
			leader := group.leader(-1)
			cluster := group.nodes[leader].cluster
			group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
			uri, body := "/v1/tenants/tenant-a/backup", `{}`
			if scenario != "task" {
				repo, err := backupstore.New(context.Background(), backupstore.Config{
					Bucket: "test-bucket", Prefix: "review", Endpoint: "http://127.0.0.1:1", PathStyle: true,
					AccessKeyID: "test", SecretAccessKey: "test-secret",
				})
				if err != nil {
					t.Fatal(err)
				}
				cluster.App.mu.Lock()
				group.nodes[leader].store.Backups = repo
				cluster.App.mu.Unlock()
				uri, body = "/v1/tasks", `{"type":"tenant_backup","params":{"destination":"object"}}`
			}
			response := group.mustRequest(leader, "POST", uri, body, http.StatusAccepted)
			var task storage.Task
			if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
				t.Fatal(err)
			}
			kind := scenario
			if scenario == "prepare_error" {
				kind = "task"
			}
			delayed, err := newCommand(kind)
			if err != nil {
				t.Fatal(err)
			}
			delayed.Tenant, delayed.IDs = "tenant-a", []string{task.ID}
			if scenario == "prepare_error" {
				delayed.Error = "backup source unavailable"
			}
			group.mustRequest(leader, "POST", "/v1/tenants/tenant-a/purge?force=true", `{}`, http.StatusOK)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			data, err := cluster.propose(ctx, delayed)
			if err != nil {
				t.Fatalf("delayed %s stopped the Raft application: %v", scenario, err)
			}
			var outcome httpResult
			if json.Unmarshal(data, &outcome) != nil || outcome.Status != http.StatusConflict {
				t.Fatalf("obsolete task was not rejected: %s", data)
			}
			checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			for i, replica := range group.nodes {
				group.waitApplied(i, checkpoint.Index)
				if _, err := replica.store.GetTask(ctx, "tenant-a", task.ID); !errors.Is(err, storage.ErrNotFound) {
					t.Fatalf("replica %d recreated a purged task: %v", i, err)
				}
			}
			group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
			group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:after-purge","kind":"host"}]}}`, http.StatusOK)
			group.stop(leader)
			leader = group.leader(leader)
			group.mustRequest(leader, "GET", "/v1/entities/host:after-purge", "", http.StatusOK)
		})
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

func TestHARejectsChangedDataPrefixOnRestart(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:seed","kind":"host"}]}}`, http.StatusOK)
	follower := (leader + 1) % len(group.nodes)
	group.waitApplied(follower, group.nodes[leader].cluster.Node.Status()["applied_index"].(uint64))
	group.stop(follower)
	replica := group.nodes[follower]
	// An upgraded directory may not have the new local identity marker yet.
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			if legacy {
				if err := os.Remove(filepath.Join(replica.cfg.DataDir, ".graphdb-replication", "prefix")); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
			}
			files, err := storage.OpenFileStore(replica.cfg.DataDir)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			before, err := files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			store := storage.NewTenantStoreWithOptions(files, "other", storage.TenantStoreOptions{InstanceID: replica.cfg.InstanceID})
			cluster := New(replica.cfg, store, files)
			if err := cluster.Start(context.Background(), replica.cfg.Raft); err == nil {
				cluster.Close()
				t.Error("changed data prefix was accepted for a checkpointed replica")
			}
			after, err := files.ReplicationCheckpoint()
			if err != nil || after.Index != before.Index {
				t.Fatalf("rejected restart changed checkpoint: %d -> %d, err=%v", before.Index, after.Index, err)
			}
			g, _, err := storage.NewTenantStore(files, "graphdb").Load(context.Background(), "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := g.GetEntity("host:seed"); !ok {
				t.Fatal("rejected restart lost the original graph")
			}
		})
	}
	group.start(follower)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:after","kind":"host"}]}}`, http.StatusOK)
	group.mustRequest(follower, "GET", "/v1/entities/host:seed", "", http.StatusOK)
	group.mustRequest(follower, "GET", "/v1/entities/host:after", "", http.StatusOK)
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

func TestHALongReadAllowsPublicationAndCancellation(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:1","kind":"host"}]}}`, http.StatusOK)
	cluster := group.nodes[leader].cluster
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		defer close(done)
		r := httptest.NewRequest("POST", "/v1/query", nil)
		r.Header.Set("X-Tenant-ID", "tenant-a")
		cluster.ServeRoute(httptest.NewRecorder(), r, false, false, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(entered)
			<-release
		}))
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("long read did not start")
	}
	// A publication must complete while the query retains its read view.
	group.mustRequest(leader, "POST", "/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:2","kind":"host"}]}}`, http.StatusOK)
	writeDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		writeDone <- group.request(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-other"}`)
	}()
	follower := (leader + 1) % 3
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := group.nodes[follower].store.GetTenantInfo(context.Background(), "tenant-other"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("structural mutation did not commit on follower")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case result := <-writeDone:
		t.Fatalf("structural mutation bypassed pinned read: %d", result.Code)
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	r := httptest.NewRequest("DELETE", "/v1/queries/running/test", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	cluster.ServeRoute(w, r, false, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unblock()
		w.WriteHeader(http.StatusNoContent)
	}))
	if w.Code != http.StatusNoContent {
		t.Fatalf("cancellation blocked behind committed mutation: %d %s", w.Code, w.Body.String())
	}
	<-done
	select {
	case result := <-writeDone:
		if result.Code != http.StatusOK {
			t.Fatalf("structural mutation failed after cancellation: %d %s", result.Code, result.Body.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("structural mutation remained blocked")
	}
}

func TestHASlowFollowerKeepsApplicationMemoryBounded(t *testing.T) {
	group := newTestCluster(t, false)
	leader := group.leader(-1)
	group.mustRequest(leader, "POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	follower := (leader + 1) % 3
	group.waitApplied(follower, checkpoint.Index)
	app := group.nodes[follower].cluster.App
	app.mu.Lock()
	unlock := sync.OnceFunc(app.mu.Unlock)
	defer unlock()
	payload := string(bytes.Repeat([]byte("x"), 256<<10))
	for i := range 20 {
		body := fmt.Sprintf(`{"idempotency_key":"slow-%d","mutations":{"upsert_entities":[{"id":"host:1","kind":"host","fields":{"payload":%q,"step":%d}}]}}`, i, payload, i)
		group.mustRequest(leader, "POST", "/v1/commits", body, http.StatusOK)
	}
	status := group.nodes[follower].cluster.Node.Status()
	if status["application_lag"].(uint64) < 10 {
		t.Fatalf("follower did not accumulate a durable backlog: %v", status)
	}
	if status["application_bytes"].(int64) > 5<<20 {
		t.Fatalf("slow follower retained unbounded payloads: %v", status)
	}
	checkpoint, err = group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	unlock()
	// Allow instrumented replay of the intentionally blocked backlog while
	// preserving the payload bound checked above.
	group.waitApplied(follower, checkpoint.Index, 30*time.Second)
	if group.manifest(follower).Version != 20 {
		t.Fatal("follower lost publications while resuming its durable backlog")
	}
}
