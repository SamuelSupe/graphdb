package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestRound5RangeCompactionPreservesSecondaryIndexCatalogKeys(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	if err := fs.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(fs, "graphdb")
	defer store.ShutdownTasks(ctx)
	store.WriteIndexRangePages = true
	store.WriteIndexAdjacencyPages = true

	tenant := "r5-range-compact"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateIndex(ctx, tenant, IndexDefinition{Kind: "host", Field: "env"}); err != nil {
		t.Fatal(err)
	}

	var entities []graph.Entity
	for i := 0; len(entities) < 400; i++ {
		id := fmt.Sprintf("host:r5-%07d", i)
		if entityShardID(id) == "00" {
			entities = append(entities, graph.Entity{
				ID:     id,
				Kind:   "host",
				Fields: graph.Fields{"env": fmt.Sprintf("env-%02d", len(entities)%5)},
			})
		}
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	// Seed two mergeable small range pages on shard 00 so compactRangeSlice actually executes a forward merge.
	pages := []EntityPageData{
		{Shard: "00", Version: catalog.Version, Entities: entities[:200], pageRole: rangeRole("")},
		{Shard: "00", Version: catalog.Version, Entities: entities[200:], pageRole: rangeRole(entities[200].ID)},
	}
	pages, err = prepareBoundedEntityPages(ctx, tenant, pages)
	if err != nil {
		t.Fatal(err)
	}
	mini := IndexCatalog{Version: catalog.Version, EntityPages: entityPageSpecs(pages)}
	store.decorateIndexCatalog(&mini, tenant)
	catalog.EntityPages = mini.EntityPages
	if err := store.writeParquetEntityPages(ctx, tenant, pages, catalog.Version); err != nil {
		t.Fatal(err)
	}
	_, meta, err := store.getIndexCatalogWithMeta(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.putIndexCatalogWithMeta(ctx, tenant, catalog, meta); err != nil {
		t.Fatal(err)
	}

	changed, _, _, err := store.compactRangeSlice(ctx, tenant, map[string]bool{})
	if err != nil {
		t.Fatalf("compactRangeSlice failed: %v", err)
	}
	if !changed {
		t.Fatalf("expected compactRangeSlice to merge the range pair")
	}
	catalog, err = store.GetIndexCatalog(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Indexes) == 0 {
		t.Fatalf("expected secondary index in catalog")
	}
	for _, idx := range catalog.Indexes {
		for _, obj := range idx.Objects {
			if strings.Contains(obj.Key, "%2F") || strings.Contains(obj.Key, ".parquet.parquet") {
				t.Fatalf("secondary index key corrupted by range compaction: %q", obj.Key)
			}
			if _, err := fs.Get(ctx, obj.Key); err != nil {
				t.Fatalf("secondary index object %q unreadable after compaction: %v", obj.Key, err)
			}
		}
	}
}

func newRound5FakeS3Server(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/")
		_, key, _ := strings.Cut(path, "/")
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			objects[key] = body
			sum := sha256.Sum256(body)
			w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:16])+`"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Query().Get("list-type") == "2" {
				prefix := r.URL.Query().Get("prefix")
				type item struct {
					Key  string `xml:"Key"`
					Size int    `xml:"Size"`
					ETag string `xml:"ETag"`
				}
				type listResp struct {
					XMLName     xml.Name `xml:"ListBucketResult"`
					IsTruncated bool     `xml:"IsTruncated"`
					Contents    []item   `xml:"Contents"`
				}
				var keys []string
				for k := range objects {
					if strings.HasPrefix(k, prefix) {
						keys = append(keys, k)
					}
				}
				sort.Strings(keys)
				resp := listResp{}
				for _, k := range keys {
					sum := sha256.Sum256(objects[k])
					resp.Contents = append(resp.Contents, item{Key: k, Size: len(objects[k]), ETag: `"` + hex.EncodeToString(sum[:16]) + `"`})
				}
				w.Header().Set("Content-Type", "application/xml")
				_ = xml.NewEncoder(w).Encode(resp)
				return
			}
			data, ok := objects[key]
			if !ok {
				http.Error(w, "<Error><Code>NoSuchKey</Code></Error>", http.StatusNotFound)
				return
			}
			sum := sha256.Sum256(data)
			w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:16])+`"`)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			_, _ = w.Write(data)
		case http.MethodHead:
			data, ok := objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			sum := sha256.Sum256(data)
			w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:16])+`"`)
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestRound5PrepareReplicatedAutomaticBackupIncludesAutomation(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	if err := fs.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(fs, "graphdb")
	defer store.ShutdownTasks(ctx)
	s3 := newRound5FakeS3Server(t)
	store.Backups, err = backupstore.New(ctx, backupstore.Config{
		Bucket:          "test-backups",
		Prefix:          "backups",
		Endpoint:        s3.URL,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		PathStyle:       true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tenant := "r5-auto-backup"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "svc:1", Kind: "service"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}

	taskID := "auto-b1"
	backupID := "scheduled-" + taskID
	_, record, _, err := store.captureTenantBackup(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.putTaskResult(ctx, tenant, backupID, taskResult(record)); err != nil {
		t.Fatal(err)
	}
	task := Task{
		ID:       taskID,
		TenantID: tenant,
		Type:     TaskTypeTenantBackup,
		Status:   TaskStatusQueued,
		Params: map[string]any{
			"destination": "object",
			"automatic":   true,
			"keep_count":  2,
		},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	prepared, err := store.PrepareReplicatedTask(ctx, task)
	if err != nil {
		t.Fatalf("PrepareReplicatedTask failed: %v", err)
	}
	var payload replicatedBackupPayload
	if err := json.Unmarshal(prepared, &payload); err != nil {
		t.Fatalf("unmarshal prepared payload: %v", err)
	}
	if payload.Automation == nil || payload.Automation["verified_at"] == nil {
		t.Fatalf("expected automation verification in prepared payload, got %+v", payload.Automation)
	}

	done, err := store.RunReplicatedTask(ctx, tenant, taskID, prepared)
	if err != nil {
		t.Fatalf("RunReplicatedTask failed: %v", err)
	}
	if done.Status != TaskStatusSucceeded {
		t.Fatalf("task status = %q, err = %q", done.Status, done.Error)
	}
	if done.Result["automation"] == nil {
		t.Fatalf("expected task.Result[\"automation\"] to be populated on replicated execution")
	}
}

func TestRound5SharedStoreTaskHeartbeatPreventsStaleReclaim(t *testing.T) {
	ctx := context.Background()
	shared := NewMemoryStore()
	nodeA := NewTenantStoreWithOptions(shared, "graphdb", TenantStoreOptions{InstanceID: "node-a"})
	defer nodeA.ShutdownTasks(ctx)
	nodeB := NewTenantStoreWithOptions(shared, "graphdb", TenantStoreOptions{InstanceID: "node-b"})
	defer nodeB.ShutdownTasks(ctx)

	prevInterval := sharedStoreTaskHeartbeatInterval
	sharedStoreTaskHeartbeatInterval = 20 * time.Millisecond
	defer func() { sharedStoreTaskHeartbeatInterval = prevInterval }()

	initialUpdatedAt := time.Now().UTC().Add(-4 * time.Minute)
	task := Task{
		ID:        "hb-task",
		TenantID:  "t1",
		Type:      TaskTypeCompact,
		Status:    TaskStatusRunning,
		OwnerID:   "node-a",
		StartedAt: initialUpdatedAt,
		UpdatedAt: initialUpdatedAt,
	}
	if err := nodeA.saveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	stop := nodeA.startTaskHeartbeat(ctx, task)
	defer stop()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, err := nodeB.GetTask(ctx, "t1", "hb-task")
		if err != nil {
			t.Fatal(err)
		}
		if current.Status != TaskStatusRunning {
			t.Fatalf("remote node killed heartbeating task: status=%q err=%q", current.Status, current.Error)
		}
		if current.UpdatedAt.After(initialUpdatedAt) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("task heartbeat did not refresh UpdatedAt")
}

func TestRound5IngestWALCleanOSShutdownReopensInSyncMode(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "wal")
	cfg := DefaultIngestWALConfig(dir)
	cfg.Durability = IngestWALDurabilityOS
	cfg.SegmentBytes = 4096
	wal, err := OpenIngestWAL(cfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	env1, _ := json.Marshal(walIngestEnvelope{RecordID: "req-1", TenantID: "t1", AcceptedLSN: 1, Request: IngestRequest{Source: "s", CollectorID: "c", BatchID: "b1"}})
	if _, err := wal.Append(ctx, IngestWALAccepted, env1); err != nil {
		t.Fatal(err)
	}
	if err := wal.prune(ctx, 0, true); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Durability = IngestWALDurabilitySync
	reopened, err := OpenIngestWAL(cfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatalf("clean os-mode WAL failed to reopen in sync mode: %v", err)
	}
	defer reopened.Close()
}

func TestRound5FileStoreReceiptDBHeadAndGetIfFileUnchangedConsistent(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	if err := fs.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(fs, "graphdb")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "t1", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := store.Ingest(ctx, "t1", IngestRequest{
			Source:      "s",
			CollectorID: "c",
			BatchID:     fmt.Sprintf("b-%d", i),
			Items: []IngestItem{{
				ExternalID: fmt.Sprintf("ext-%d", i),
				Entity:     &graph.Entity{ID: fmt.Sprintf("host:%d", i), Kind: "host"},
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	dbKey := fs.receiptPrefix + "t1/" + receiptDatabase
	data, meta, err := fs.GetWithMeta(ctx, dbKey)
	if err != nil {
		t.Fatalf("GetWithMeta(%q) failed: %v", dbKey, err)
	}
	headMeta, err := fs.Head(ctx, dbKey)
	if err != nil {
		t.Fatalf("Head(%q) failed: %v", dbKey, err)
	}
	if headMeta.ETag != meta.ETag {
		t.Fatalf("Head ETag %q != GetWithMeta ETag %q", headMeta.ETag, meta.ETag)
	}
	unchangedData, _, err := fs.GetIfFileUnchanged(ctx, dbKey, nil)
	if err != nil {
		t.Fatalf("GetIfFileUnchanged(%q) failed: %v", dbKey, err)
	}
	if !bytes.Equal(unchangedData, data) {
		t.Fatalf("GetIfFileUnchanged bytes differ from GetWithMeta snapshot")
	}
}
