package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestFileStoreDeleteNonemptyParentDoesNotDrainOtherIO(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	ctx := context.Background()
	for _, key := range []string{"tenant-a/control/pending", "tenant-a/control/backup"} {
		if err := files.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	// A slow read in another tenant must not delay removing an intent whose
	// parent still holds the backup. There is no empty directory to reclaim.
	release, err := files.beginOperation(ctx, "tenant-b/large-object")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	attempt, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := files.Delete(attempt, "tenant-a/control/pending"); err != nil {
		t.Fatalf("deletion blocked behind unrelated IO: %v", err)
	}
	if _, err := files.Get(ctx, "tenant-a/control/backup"); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreReclaimsEmptyParentsWithConcurrentWrites(t *testing.T) {
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	ctx := context.Background()
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Go(func() {
			for version := range 20 {
				key := fmt.Sprintf("objects/v%d/fields/f%d/part", version, worker)
				if err := files.Put(ctx, key, []byte(key)); err != nil {
					t.Error(err)
					return
				}
				data, err := files.Get(ctx, key)
				if err != nil || string(data) != key {
					t.Errorf("concurrent publication: %q %v", data, err)
					return
				}
				if err := files.Delete(ctx, key); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	workers.Wait()
	if _, err := os.Stat(filepath.Join(root, "objects")); !os.IsNotExist(err) {
		t.Fatalf("empty object tree retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".graphdb.lock")); err != nil {
		t.Fatal(err)
	}
	if err := files.Put(ctx, "objects/v0/fields/f0/part", []byte("recreated")); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreStartupReclaimsLegacyGarbage(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	for _, dir := range []string{"objects/old/empty", "objects/current", ".graphdb-raft/empty", ".maintenance-kept/empty"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 300 {
		if err := os.MkdirAll(filepath.Join(root, fmt.Sprintf("objects/old/v%d/empty", i)), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"objects/current/part.parquet", "objects/current/.tmp-business.parquet", "objects/old/.tmp-part.parquet-123", ".tmp-probe-456"} {
		if err := os.WriteFile(filepath.Join(root, key), []byte("kept"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(outside, ".tmp-external-123"), []byte("outside"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "external")); err != nil {
		t.Fatal(err)
	}
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	for _, key := range []string{"objects/old", ".tmp-probe-456"} {
		if _, err := os.Lstat(filepath.Join(root, key)); !os.IsNotExist(err) {
			t.Fatalf("garbage retained %s: %v", key, err)
		}
	}
	for _, key := range []string{"objects/current/part.parquet", "objects/current/.tmp-business.parquet", ".graphdb-raft/empty", ".maintenance-kept/empty", "external/.tmp-external-123", ".graphdb.lock"} {
		if _, err := os.Stat(filepath.Join(root, key)); err != nil {
			t.Fatalf("retained path %s: %v", key, err)
		}
	}
}

func TestLocalGCReclaimsVersionDirectories(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	store.WriteEntityRecords = false
	defer store.ShutdownTasks(ctx)
	for version := range 8 {
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host", Fields: graph.Fields{"value": version}}}}, CommitOptions{}); err != nil {
			t.Fatal(err)
		}
		if version == 0 {
			if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := store.Compact(ctx, "tenant-a"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 2, CleanupIndexOrphans: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			children, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			if len(children) == 0 {
				t.Errorf("GC retained empty directory: %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil || manifest.Version != 8 || g.Entities.Len() != 1 {
		t.Fatalf("graph changed during cleanup: version=%d err=%v", manifest.Version, err)
	}
}

func TestRestoreCleanupPreservesRecoveryAndDrainsOnClose(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	garbage, err := files.newRestoreDirectory()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(garbage, "old-object"), []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	pending, err := files.newRestoreDirectory()
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(pending, "journal.json")
	if err := os.WriteFile(journal, []byte("pending recovery"), 0644); err != nil {
		t.Fatal(err)
	}
	files.scheduleRestoreCleanup(garbage)
	files.scheduleRestoreCleanup(pending)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(garbage); !os.IsNotExist(err) {
		t.Fatalf("cleanup did not drain: %v", err)
	}
	if _, err := os.Stat(journal); err != nil {
		t.Fatalf("cleanup removed recovery state: %v", err)
	}
}
