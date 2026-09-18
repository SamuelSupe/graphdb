package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalRestoreDirectoryConcurrentPublication(t *testing.T) {
	ctx := context.Background()
	for range 32 {
		files, err := OpenFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			_, err := files.newRestoreDirectory()
			results <- err
		}()
		go func() {
			<-start
			results <- files.Put(ctx, "test/tenants/other/commits/new/one", []byte("durable"))
		}()
		close(start)
		for range 2 {
			if err := <-results; err != nil {
				t.Error(err)
			}
		}
		if err := files.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenFileStore(files.root)
		if err != nil {
			t.Fatal(err)
		}
		data, err := reopened.Get(ctx, "test/tenants/other/commits/new/one")
		if err != nil || string(data) != "durable" {
			t.Errorf("reopened publication = %q, %v", data, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLocalRestoreDirectoryRecoversEveryPublicationBoundary(t *testing.T) {
	for _, phase := range []string{"building", "prepared", "old_moved", "new_moved", "committed"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			files, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			key := "review/tenants/tenant-a"
			for name, value := range map[string]string{"manifest.parquet": "old", "tasks/restore.parquet": "checkpoint", "backups/backup.parquet": "backup"} {
				if err := files.Put(ctx, key+"/"+name, []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			dir, err := files.newRestoreDirectory()
			if err != nil {
				t.Fatal(err)
			}
			stage := NewFileStore(filepath.Join(dir, "build"))
			if err := stage.Put(ctx, key+"/manifest.parquet", []byte("new")); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(root, filepath.FromSlash(key))
			incoming := filepath.Join(dir, "build", filepath.FromSlash(key))
			for _, name := range []string{"tasks", "backups"} {
				if err := linkRestoreTree(ctx, filepath.Join(target, name), filepath.Join(incoming, name)); err != nil {
					t.Fatal(err)
				}
			}
			if phase != "building" {
				data, _ := json.Marshal(fileRestoreJournal{Target: key})
				if err := writeFileAtomic(filepath.Join(dir, "journal.json"), data); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "old_moved" || phase == "new_moved" || phase == "committed" {
				if err := files.restoreRename(target, filepath.Join(dir, "old")); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "new_moved" || phase == "committed" {
				if err := files.restoreRename(incoming, target); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "committed" {
				data, _ := json.Marshal(fileRestoreJournal{Target: key, Committed: true})
				if err := writeFileAtomic(filepath.Join(dir, "journal.json"), data); err != nil {
					t.Fatal(err)
				}
			}
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			want := "old"
			if phase == "committed" {
				want = "new"
			}
			for name, expected := range map[string]string{"manifest.parquet": want, "tasks/restore.parquet": "checkpoint", "backups/backup.parquet": "backup"} {
				data, err := reopened.Get(ctx, key+"/"+name)
				if err != nil || string(data) != expected {
					t.Fatalf("%s after reopen=%q err=%v want %q", name, data, err, expected)
				}
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("staging remains after recovery: %v", err)
			}
		})
	}
}

type restoreTerminalFailureStore struct {
	*FileStore
	fail atomic.Bool
}

func (s *restoreTerminalFailureStore) UnwrapObjectStore() ObjectStore { return s.FileStore }
func (s *restoreTerminalFailureStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.fail.Load() && strings.Contains(key, "/tasks/") && !strings.Contains(key, "/results/") {
		task, err := decodeParquetTask(ctx, data)
		if err == nil && task.Type == TaskTypeTenantRestore && task.Status == TaskStatusSucceeded {
			return ObjectMeta{}, fmt.Errorf("injected restore terminal persistence failure")
		}
	}
	return s.FileStore.PutConditional(ctx, key, data, condition)
}
func waitForLocalRestoreTask(t *testing.T, store *TenantStore, tenant, id string) Task {
	t.Helper()
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		task, err := store.GetTask(context.Background(), tenant, id)
		if err != nil {
			t.Fatal(err)
		}
		if taskTerminal(task.Status) && !store.taskRuntimeActive(tenant, id) {
			return task
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("task never became terminal")
	return Task{}
}
func seedLocalRestoreBackup(t *testing.T, store *TenantStore) string {
	t.Helper()
	ctx := context.Background()
	_, err := store.Commit(ctx, "source", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:original", Kind: "host"}}}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	backup, err := store.StartTask(ctx, "source", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup = waitForLocalRestoreTask(t, store, "source", backup.ID)
	if backup.Status != TaskStatusSucceeded {
		t.Fatalf("backup: %+v", backup)
	}
	return backup.Result["backup_manifest_key"].(string)
}

func TestLocalPublishedRestoreRetryPreservesLaterCommit(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(fmt.Sprintf("replaced=%v", replaced), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			files, err := OpenFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { files.Close() }()
			objects := &restoreTerminalFailureStore{FileStore: files}
			store := NewTenantStore(objects, "review")
			store.TaskMarkerTTL = time.Millisecond
			store.MaxRetries = 1
			backupKey := seedLocalRestoreBackup(t, store)
			objects.fail.Store(true)
			restore, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": backupKey, "overwrite": true})
			if err != nil {
				t.Fatal(err)
			}
			restore = waitForLocalRestoreTask(t, store, "target", restore.ID)
			if restore.Status != TaskStatusFailed || !taskCheckpointBool(restore, "local_restore_published") {
				t.Fatalf("expected terminal persistence failure after publication: %+v", restore)
			}
			objects.fail.Store(false)
			if replaced {
				replacement, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": backupKey, "overwrite": true})
				if err != nil {
					t.Fatal(err)
				}
				replacement = waitForLocalRestoreTask(t, store, "target", replacement.ID)
				if replacement.Status != TaskStatusSucceeded {
					t.Fatalf("replacement: %+v", replacement)
				}
			}
			commit, err := store.Commit(ctx, "target", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:acknowledged-later", Kind: "host"}}}, CommitOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// Finalization must work after reopening, even if the source backup is gone.
			if err := files.Delete(ctx, backupKey); err != nil {
				t.Fatal(err)
			}
			if err := store.ShutdownTasks(ctx); err != nil {
				t.Fatal(err)
			}
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			files, err = OpenFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			store = NewTenantStore(files, "review")
			defer store.ShutdownTasks(ctx)
			retry, err := store.RetryTask(ctx, "target", restore.ID)
			if err != nil {
				t.Fatal(err)
			}
			retry = waitForLocalRestoreTask(t, store, "target", retry.ID)
			want := TaskStatusSucceeded
			if replaced {
				want = TaskStatusFailed
			}
			if retry.Status != want {
				t.Fatalf("retry=%+v want status %s", retry, want)
			}
			loaded, manifest, err := store.Load(ctx, "target")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := loaded.GetEntity("host:acknowledged-later"); !ok || manifest.Version != commit.Version {
				t.Fatalf("retry lost acknowledged write: version=%d want=%d retained=%v", manifest.Version, commit.Version, ok)
			}
		})
	}
}

func TestLocalRestoreWhileRemoteStagingRemainsOpen(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "review")
	backupKey := seedLocalRestoreBackup(t, store)
	file, closeStaging, err := store.backupStagingFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	closeStaging = sync.OnceFunc(closeStaging)
	defer closeStaging()
	if _, err := file.Write([]byte("download in progress")); err != nil {
		t.Fatal(err)
	}
	restore, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": backupKey})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { store.taskWorkers.Wait(); close(done) }()
	// Durable staging can take several seconds under race instrumentation.
	// Keeping the download open still detects a retained directory IO lock.
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		closeStaging()
		<-done
		t.Fatal("restore did not finish while an unrelated download remained open")
	}
	restore, err = store.GetTask(ctx, "target", restore.ID)
	if err != nil || restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore=%+v err=%v", restore, err)
	}
	if _, err := file.Write([]byte("still downloading")); err != nil {
		t.Fatal(err)
	}
	if err := files.Put(ctx, "review/tenants/other/data", []byte("other")); err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- files.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before staging was released: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	closeStaging()
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestLocalRestoreRetainsEntityIndexLayout(t *testing.T) {
	for _, tc := range []struct {
		name      string
		records   bool
		packBytes int64
		packed    bool
	}{
		{"records", true, 1 << 20, false},
		{"packed", false, 1 << 20, true},
		{"bounded-packs", false, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			files, err := OpenFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { files.Close() }()
			store := NewTenantStore(files, "review")
			store.WriteEntityRecords = tc.records
			store.UseEntityRecordsForRead = tc.records
			store.EntityPagePackMaxBytes = tc.packBytes
			entities := make([]graph.Entity, 32)
			for i := range entities {
				id := fmt.Sprintf("host:%02d", i)
				entities[i] = graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"name": id}}
			}
			if _, err := store.Commit(ctx, "source", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			backupKey := seedLocalRestoreBackup(t, store)
			restore, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": backupKey})
			if err != nil {
				t.Fatal(err)
			}
			restore = waitForLocalRestoreTask(t, store, "target", restore.ID)
			if restore.Status != TaskStatusSucceeded {
				t.Fatalf("restore: %+v", restore)
			}
			if err := store.ShutdownTasks(ctx); err != nil {
				t.Fatal(err)
			}
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			files, err = OpenFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			store = NewTenantStore(files, "review")
			store.UseEntityRecordsForRead = tc.records
			catalog, err := store.GetIndexCatalog(ctx, "target")
			if err != nil {
				t.Fatal(err)
			}
			keys := map[string]bool{}
			for _, page := range catalog.EntityPages {
				for _, object := range page.Objects {
					keys[object.Key] = true
				}
			}
			if packed := len(keys) < len(catalog.EntityPages); packed != tc.packed {
				t.Errorf("restored page layout: %d files for %d pages, packed=%v want %v", len(keys), len(catalog.EntityPages), packed, tc.packed)
			}
			records, err := files.List(ctx, store.entityRecordPrefix("target"))
			if err != nil {
				t.Fatal(err)
			}
			wantRecords := 0
			if tc.records {
				wantRecords = len(entities) + 1
			}
			if len(records) != wantRecords {
				t.Errorf("restored entity records=%d want %d", len(records), wantRecords)
			}
			lookup := &PersistedIndexLookup{Store: store, TenantID: "target", Version: catalog.Version, Catalog: catalog}
			for _, want := range entities {
				got, ok, err := lookup.GetEntity(ctx, want.ID, nil)
				if err != nil || !ok || got.Fields["name"] != want.Fields["name"] {
					t.Fatalf("restored indexed entity %q: found=%v err=%v fields=%v", want.ID, ok, err, got.Fields)
				}
			}
		})
	}
}
