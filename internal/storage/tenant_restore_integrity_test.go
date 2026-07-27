package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestTenantRestoreFailsWhenFinalIntegrityCheckCannotRun(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	backup, err := store.StartTask(
		ctx, "tenant-a", TaskTypeTenantBackup, nil,
	)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	backupKey, _ := backup.Result["backup_manifest_key"].(string)
	input, err := store.loadTenantBackupInput(ctx, backupKey)
	if err != nil {
		t.Fatalf("load backup input: %v", err)
	}
	restore, err := store.StartTask(
		ctx,
		"tenant-b",
		TaskTypeTenantRestore,
		map[string]any{"backup_key": backupKey},
	)
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	restore = waitForTask(t, ctx, store, "tenant-b", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("initial restore = %#v", restore)
	}

	verifyStore := NewTenantStore(&failNthGetWithMetaStore{
		ObjectStore: base,
		key:         store.indexCatalogKey("tenant-b"),
		failAt:      2,
	}, "test")
	verifyStore.InstanceID = store.InstanceID
	task := Task{
		ID:       "restore-final-verification",
		TenantID: "tenant-b",
		Type:     TaskTypeTenantRestore,
		Status:   TaskStatusRunning,
		Params:   map[string]any{"backup_key": backupKey},
		Checkpoint: map[string]any{
			"backup_key":       backupKey,
			"snapshot_written": true,
			"metadata_written": true,
			"indexes_rebuilt":  true,
		},
	}

	report, err := verifyStore.restoreTenantBackupInputTask(
		ctx, task, backupKey, input,
	)
	if err == nil || !strings.Contains(err.Error(), "restore integrity") {
		t.Fatalf("restore report=%#v err=%v, want integrity failure", report, err)
	}
	if report.RestoreIntegrity.Status != "error" {
		t.Fatalf("restore integrity = %#v, want error", report.RestoreIntegrity)
	}
	persisted, taskErr := verifyStore.GetTask(
		ctx, "tenant-b", task.ID,
	)
	if taskErr != nil ||
		taskActionStatus(persisted, "verify_restore") != "failed" {
		t.Fatalf("persisted task = %#v err=%v", persisted, taskErr)
	}
}

type failNthGetWithMetaStore struct {
	ObjectStore
	key    string
	failAt int
	mu     sync.Mutex
	gets   int
}

func (s *failNthGetWithMetaStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	if key == s.key {
		s.mu.Lock()
		s.gets++
		fail := s.gets == s.failAt
		s.mu.Unlock()
		if fail {
			return nil, ObjectMeta{}, errors.New(
				"injected final index catalog read failure",
			)
		}
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}
