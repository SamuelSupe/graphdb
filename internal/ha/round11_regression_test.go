package ha

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/bootstrap"
	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestTenantReceiptColdScanPreservesConcurrentDeltas(t *testing.T) {
	dir := t.TempDir()
	files := storage.NewFileStore(dir)
	store := storage.NewTenantStore(files, "")
	app := &Application{
		Store:   store,
		Files:   files,
		ShardID: "s1",
	}

	// Simulate an active cold scan with concurrent track/untrack mutations.
	app.tenantReceiptsMu.Lock()
	app.tenantReceiptScanning = true
	wait := make(chan struct{})
	app.tenantReceiptScanWait = wait
	app.tenantReceiptsMu.Unlock()

	app.trackTenantReceipt("tenant-a", "k1")
	app.trackTenantReceipt("tenant-a", "k2")
	app.untrackTenantReceipt("k1")
	app.trackTenantReceipt("tenant-b", "k3")
	app.untrackTenantReceiptsForTenant("tenant-b")

	// Finish the simulated scan by replaying deltas onto an empty baseline.
	byTenant := map[string]map[string]struct{}{}
	owners := map[string]string{}
	app.tenantReceiptsMu.Lock()
	deltas := app.tenantReceiptDeltas
	app.tenantReceiptDeltas = nil
	app.tenantReceiptScanning = false
	app.tenantReceiptScanWait = nil
	for _, d := range deltas {
		switch d.kind {
		case tenantReceiptDeltaTrack:
			applyTenantReceiptTrack(byTenant, owners, d.tenant, d.key)
		case tenantReceiptDeltaUntrackKey:
			applyTenantReceiptUntrackKey(byTenant, owners, d.key)
		case tenantReceiptDeltaUntrackTenant:
			applyTenantReceiptUntrackTenant(byTenant, owners, d.tenant)
		}
	}
	app.tenantReceipts = byTenant
	app.tenantReceiptOwners = owners
	close(wait)
	app.tenantReceiptsMu.Unlock()

	if err := app.ensureTenantReceipts(context.Background()); err != nil {
		t.Fatalf("ensureTenantReceipts: %v", err)
	}
	if _, ok := app.tenantReceipts["tenant-a"]["k2"]; !ok {
		t.Fatalf("expected k2 tracked for tenant-a, got %+v", app.tenantReceipts)
	}
	if _, ok := app.tenantReceipts["tenant-a"]["k1"]; ok {
		t.Fatalf("expected untracked k1 removed, got %+v", app.tenantReceipts)
	}
	if _, ok := app.tenantReceipts["tenant-b"]; ok {
		t.Fatalf("expected tenant-b removed, got %+v", app.tenantReceipts)
	}
}

func TestPauseTenantWritesReferenceCounted(t *testing.T) {
	c := &Cluster{}
	c.admissionMu.Lock()
	entry := c.tenantAdmission("t1")
	entry.pauseCount = 2
	c.admissionMu.Unlock()

	if !c.maintenanceWritesPaused() {
		t.Fatal("expected writes paused when pauseCount == 2")
	}
	if _, err := c.admitTenantMutation(command{Kind: "http", Tenant: "t1", Method: "POST", URI: "/v1/commits"}); err == nil {
		t.Fatal("expected mutation deferred while paused")
	}

	c.admissionMu.Lock()
	entry.pauseCount--
	c.admissionMu.Unlock()
	if !c.maintenanceWritesPaused() {
		t.Fatal("expected writes still paused after first release when second pause is active")
	}

	c.admissionMu.Lock()
	entry.pauseCount--
	if entry.active == 0 && entry.pauseCount == 0 {
		delete(c.admissions, "t1")
	}
	c.admissionMu.Unlock()
	if c.maintenanceWritesPaused() {
		t.Fatal("expected writes unpaused after final release")
	}
	release, err := c.admitTenantMutation(command{Kind: "http", Tenant: "t1", Method: "POST", URI: "/v1/commits"})
	if err != nil {
		t.Fatalf("expected mutation admitted after unpause, got %v", err)
	}
	release()
}

// Regression test for Raft overwrite restore + ReaderCache: pre-restore lease
// renewals inside ApplyReplicated must not republish the pre-restore graph into
// ReaderCache after publishLocalTenantRestore replaces the tenant directory.
func TestReplicatedOverwriteRestoreDoesNotPoisonReaderCache(t *testing.T) {
	peers := make(map[uint64]string)
	type nodeEnv struct {
		rt      *bootstrap.StorageRuntime
		cache   *storage.ReaderCache
		cluster *Cluster
		handler http.Handler
		peer    *httptest.Server
		stopped bool
	}
	nodes := make([]*nodeEnv, 3)
	for i := range nodes {
		idx := i
		ne := &nodeEnv{}
		ne.peer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if ne.stopped || ne.cluster == nil {
				http.Error(w, "stopped", http.StatusServiceUnavailable)
				return
			}
			ne.cluster.PrivateHandler().ServeHTTP(w, r)
		}))
		t.Cleanup(ne.peer.Close)
		peers[uint64(idx+1)] = ne.peer.URL
		nodes[idx] = ne
	}
	peersJSON, err := json.Marshal(peers)
	if err != nil {
		t.Fatal(err)
	}
	for i := range nodes {
		t.Setenv("GRAPHDB_DATA_DIR", t.TempDir())
		t.Setenv("GRAPHDB_INGEST_MODE", "wal")
		t.Setenv("GRAPHDB_INGEST_FLUSH_INTERVAL", "200ms")
		t.Setenv("GRAPHDB_RAFT_NODE_ID", fmt.Sprint(i+1))
		t.Setenv("GRAPHDB_RAFT_CLUSTER_ID", "restore-cache-fence")
		t.Setenv("GRAPHDB_RAFT_ADDR", ":8081")
		t.Setenv("GRAPHDB_RAFT_TOKEN", "01234567890123456789012345678901")
		t.Setenv("GRAPHDB_RAFT_SNAPSHOT_ENTRIES", "5")
		t.Setenv("GRAPHDB_RAFT_TICK", "100ms")
		t.Setenv("GRAPHDB_RAFT_ELECTION_TICKS", "15")
		t.Setenv("GRAPHDB_RAFT_PEERS", string(peersJSON))
		cfg, err := config.Load()
		if err != nil {
			t.Fatal(err)
		}
		rt, err := bootstrap.NewStorageRuntime(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		cache := storage.NewReaderCache(rt.Store, cfg.PollInterval)
		cache.ConfigureCapacity(cfg.ReaderCacheMaxTenants, cfg.ReaderCacheMaxBytes)
		cluster := New(cfg, rt.Store, rt.Files)
		api := &httpapi.Server{Store: rt.Store, Cache: cache, Mode: cfg.Mode, Cluster: cluster}
		cluster.App.Handler = api.InternalHandler()
		if err := cluster.Start(context.Background(), cfg.Raft); err != nil {
			t.Fatal(err)
		}
		nodes[i].rt = rt
		nodes[i].cache = cache
		nodes[i].cluster = cluster
		nodes[i].handler = api.Handler()
		idx := i
		t.Cleanup(func() {
			if !nodes[idx].stopped {
				_ = nodes[idx].cluster.Close()
				_ = nodes[idx].rt.Shutdown(context.Background())
			}
		})
	}
	findLeader := func() int {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			for i, ne := range nodes {
				if ne.stopped {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				err := ne.cluster.Node.ReadBarrier(ctx)
				cancel()
				if err == nil {
					return i
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("no leader elected")
		return -1
	}
	req := func(node int, method, path, body string, want int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if !strings.HasPrefix(path, "/v1/tenants") || strings.Contains(path, "/backup") || strings.Contains(path, "/restore") {
			r.Header.Set("X-Tenant-ID", "large-restore")
		}
		w := httptest.NewRecorder()
		nodes[node].handler.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("node=%d %s %s status=%d want=%d body=%s", node, method, path, w.Code, want, w.Body.String())
		}
		return w
	}

	leader := findLeader()
	req(leader, "POST", "/v1/tenants", `{"tenant_id":"large-restore"}`, http.StatusOK)
	payload := strings.Repeat("x", 256<<10)
	for i := 0; i < 6; i++ {
		req(leader, "POST", "/v1/commits", fmt.Sprintf(`{"mutations":{"upsert_entities":[{"id":"host:%d","kind":"host","fields":{"payload":%q}}]}}`, i, payload), http.StatusOK)
	}
	backupResp := req(leader, "POST", "/v1/tenants/large-restore/backup", `{}`, http.StatusAccepted)
	var backupTask storage.Task
	if err := json.Unmarshal(backupResp.Body.Bytes(), &backupTask); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		w := req(leader, "GET", "/v1/tasks/"+backupTask.ID, "", http.StatusOK)
		if err := json.Unmarshal(w.Body.Bytes(), &backupTask); err != nil {
			t.Fatal(err)
		}
		if backupTask.Status == storage.TaskStatusSucceeded {
			break
		}
		if backupTask.Status == storage.TaskStatusFailed {
			t.Fatalf("backup failed: %s", w.Body.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	req(leader, "POST", "/v1/commits", `{"mutations":{"delete_entities":["host:0"]}}`, http.StatusOK)
	restoreResp := req(leader, "POST", "/v1/tenants/large-restore/restore", fmt.Sprintf(`{"backup_key":%q,"overwrite":true}`, backupTask.ResultKey), http.StatusAccepted)
	var restoreTask storage.Task
	if err := json.Unmarshal(restoreResp.Body.Bytes(), &restoreTask); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		w := req(leader, "GET", "/v1/tasks/"+restoreTask.ID, "", http.StatusOK)
		if err := json.Unmarshal(w.Body.Bytes(), &restoreTask); err != nil {
			t.Fatal(err)
		}
		if restoreTask.Status == storage.TaskStatusSucceeded {
			break
		}
		if restoreTask.Status == storage.TaskStatusFailed {
			t.Fatalf("restore failed: %s", w.Body.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if restoreTask.Status != storage.TaskStatusSucceeded {
		t.Fatalf("restore did not succeed: %+v", restoreTask)
	}
	for i := range nodes {
		w := req(i, "GET", "/v1/entities/host:0", "", http.StatusOK)
		var resp struct {
			Version int64 `json:"version"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Version != 6 {
			t.Fatalf("node=%d expected restored version 6, got %d", i, resp.Version)
		}
	}
}
