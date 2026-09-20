package storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestObjectStoresHonorCanceledContext(t *testing.T) {
	stores := []struct {
		name  string
		store ObjectStore
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "file", store: NewFileStore(t.TempDir())},
	}

	for _, tt := range stores {
		t.Run(tt.name, func(t *testing.T) {
			canceled, cancel := context.WithCancel(context.Background())
			cancel()

			if err := tt.store.Put(canceled, "objects/canceled-put", []byte("x")); !errors.Is(err, context.Canceled) {
				t.Fatalf("put err = %v, want context.Canceled", err)
			}
			if _, err := tt.store.Get(context.Background(), "objects/canceled-put"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("canceled put persisted object, get err = %v", err)
			}

			if _, err := tt.store.PutConditional(canceled, "objects/canceled-conditional", []byte("x"), PutCondition{}); !errors.Is(err, context.Canceled) {
				t.Fatalf("put conditional err = %v, want context.Canceled", err)
			}
			if _, err := tt.store.Get(context.Background(), "objects/canceled-conditional"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("canceled conditional put persisted object, get err = %v", err)
			}

			if err := tt.store.Put(context.Background(), "objects/seed", []byte("seed")); err != nil {
				t.Fatalf("seed put: %v", err)
			}
			if _, err := tt.store.Get(canceled, "objects/seed"); !errors.Is(err, context.Canceled) {
				t.Fatalf("get err = %v, want context.Canceled", err)
			}
			if _, _, err := tt.store.GetWithMeta(canceled, "objects/seed"); !errors.Is(err, context.Canceled) {
				t.Fatalf("get meta err = %v, want context.Canceled", err)
			}
			if _, err := tt.store.List(canceled, "objects/"); !errors.Is(err, context.Canceled) {
				t.Fatalf("list err = %v, want context.Canceled", err)
			}
			if err := tt.store.Delete(canceled, "objects/seed"); !errors.Is(err, context.Canceled) {
				t.Fatalf("delete err = %v, want context.Canceled", err)
			}
			if data, err := tt.store.Get(context.Background(), "objects/seed"); err != nil || string(data) != "seed" {
				t.Fatalf("canceled delete changed object, data=%q err=%v", data, err)
			}
		})
	}
}

func TestTenantStoreCommitHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := NewTenantStore(NewMemoryStore(), "test")
	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("commit err = %v, want context.Canceled", err)
	}

	g, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load after canceled commit: %v", err)
	}
	if manifest.Version != 0 {
		t.Fatalf("manifest version = %d, want 0", manifest.Version)
	}
	if _, ok := g.GetEntity("host:a"); ok {
		t.Fatal("canceled commit persisted entity")
	}
}

func TestObjectStoresTreatEmptyObjectAsExistingForConditions(t *testing.T) {
	stores := []struct {
		name  string
		store ObjectStore
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "file", store: NewFileStore(t.TempDir())},
	}
	for _, tt := range stores {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			meta, err := tt.store.PutConditional(ctx, "objects/empty", nil, PutCondition{IfNoneMatch: true})
			if err != nil {
				t.Fatalf("put empty: %v", err)
			}
			if !meta.Exists {
				t.Fatalf("put empty meta.Exists = false")
			}
			data, loaded, err := tt.store.GetWithMeta(ctx, "objects/empty")
			if err != nil {
				t.Fatalf("get empty: %v", err)
			}
			if len(data) != 0 || !loaded.Exists || loaded.ETag == "" {
				t.Fatalf("loaded empty data=%q meta=%#v", data, loaded)
			}
			conflictMeta, err := tt.store.PutConditional(ctx, "objects/empty", []byte("second"), PutCondition{IfNoneMatch: true})
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("if-none-match err = %v, want ErrConflict", err)
			}
			if !conflictMeta.Exists || conflictMeta.ETag != loaded.ETag {
				t.Fatalf("conflict meta = %#v, want existing etag %q", conflictMeta, loaded.ETag)
			}
			updated, err := tt.store.PutConditional(ctx, "objects/empty", []byte("second"), PutCondition{IfMatch: loaded.ETag})
			if err != nil {
				t.Fatalf("if-match update: %v", err)
			}
			if !updated.Exists || updated.ETag == loaded.ETag {
				t.Fatalf("updated meta = %#v, want new existing etag", updated)
			}
		})
	}
}

func TestObjectStoresConditionalDeleteRequiresMatchingETag(t *testing.T) {
	stores := []struct {
		name  string
		store ObjectStore
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "file", store: NewFileStore(t.TempDir())},
	}
	for _, tt := range stores {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := tt.store.Put(ctx, "objects/current.json", []byte("old")); err != nil {
				t.Fatalf("put old: %v", err)
			}
			_, oldMeta, err := tt.store.GetWithMeta(ctx, "objects/current.json")
			if err != nil {
				t.Fatalf("get old meta: %v", err)
			}
			if err := tt.store.Put(ctx, "objects/current.json", []byte("new")); err != nil {
				t.Fatalf("put new: %v", err)
			}
			if err := tt.store.DeleteConditional(ctx, "objects/current.json", PutCondition{IfMatch: oldMeta.ETag}); !errors.Is(err, ErrConflict) {
				t.Fatalf("stale delete err = %v, want ErrConflict", err)
			}
			data, err := tt.store.Get(ctx, "objects/current.json")
			if err != nil || string(data) != "new" {
				t.Fatalf("stale delete changed object, data=%q err=%v", data, err)
			}
			_, newMeta, err := tt.store.GetWithMeta(ctx, "objects/current.json")
			if err != nil {
				t.Fatalf("get new meta: %v", err)
			}
			if err := tt.store.DeleteConditional(ctx, "objects/current.json", PutCondition{IfMatch: newMeta.ETag}); err != nil {
				t.Fatalf("matching delete: %v", err)
			}
			if _, err := tt.store.Get(ctx, "objects/current.json"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("get deleted err = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestObjectStoresConditionalDeleteHonorsIfNoneMatch(t *testing.T) {
	stores := []struct {
		name  string
		store ObjectStore
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "file", store: NewFileStore(t.TempDir())},
	}
	for _, tt := range stores {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := tt.store.Put(ctx, "objects/current.json", []byte("current")); err != nil {
				t.Fatalf("put current: %v", err)
			}
			if err := tt.store.DeleteConditional(ctx, "objects/current.json", PutCondition{IfNoneMatch: true}); !errors.Is(err, ErrConflict) {
				t.Fatalf("if-none-match delete err = %v, want ErrConflict", err)
			}
			data, err := tt.store.Get(ctx, "objects/current.json")
			if err != nil || string(data) != "current" {
				t.Fatalf("if-none-match delete changed object, data=%q err=%v", data, err)
			}
			if err := tt.store.DeleteConditional(ctx, "objects/missing.json", PutCondition{IfNoneMatch: true}); err != nil {
				t.Fatalf("if-none-match delete missing: %v", err)
			}
		})
	}
}

func TestFileStoreUnconditionalPutDoesNotReadExistingObject(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod permissions are not portable on windows")
	}
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	key := "objects/current.json"
	if err := store.Put(ctx, key, []byte("old")); err != nil {
		t.Fatalf("put old: %v", err)
	}
	path := filepath.Join(root, filepath.FromSlash(key))
	if err := os.Chmod(path, 0); err != nil {
		t.Fatalf("chmod unreadable: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	if err := store.Put(ctx, key, []byte("new")); err != nil {
		t.Fatalf("unconditional put should not read old body: %v", err)
	}
	data, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("get updated: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("updated data = %q, want new", data)
	}
}

func TestFileStoreUnconditionalDeleteDoesNotReadExistingObject(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod permissions are not portable on windows")
	}
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	key := "objects/current.json"
	if err := store.Put(ctx, key, []byte("old")); err != nil {
		t.Fatalf("put old: %v", err)
	}
	path := filepath.Join(root, filepath.FromSlash(key))
	if err := os.Chmod(path, 0); err != nil {
		t.Fatalf("chmod unreadable: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("unconditional delete should not read old body: %v", err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted err = %v, want ErrNotFound", err)
	}
}

func TestObjectStoresRejectEmptyObjectKeys(t *testing.T) {
	stores := []struct {
		name  string
		store ObjectStore
	}{
		{name: "memory", store: NewMemoryStore()},
		{name: "file", store: NewFileStore(t.TempDir())},
	}
	for _, tt := range stores {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := tt.store.Put(ctx, "", []byte("x")); err == nil {
				t.Fatal("put empty key succeeded")
			}
			if _, err := tt.store.PutConditional(ctx, "", []byte("x"), PutCondition{}); err == nil {
				t.Fatal("conditional put empty key succeeded")
			}
			if _, err := tt.store.Get(ctx, ""); err == nil {
				t.Fatal("get empty key succeeded")
			}
			if _, _, err := tt.store.GetWithMeta(ctx, ""); err == nil {
				t.Fatal("get meta empty key succeeded")
			}
			if err := tt.store.Delete(ctx, ""); err == nil {
				t.Fatal("delete empty key succeeded")
			}
			if _, err := tt.store.List(ctx, ""); err != nil {
				t.Fatalf("list empty prefix should remain valid: %v", err)
			}
		})
	}
}

func TestFileStoreConditionalPutIsAtomicWithinProcess(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(t.TempDir())
	if err := store.Put(ctx, "objects/current.json", []byte("seed")); err != nil {
		t.Fatalf("seed put: %v", err)
	}
	_, meta, err := store.GetWithMeta(ctx, "objects/current.json")
	if err != nil {
		t.Fatalf("seed meta: %v", err)
	}

	const writers = 32
	var wg sync.WaitGroup
	successes := make(chan string, writers)
	conflicts := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value := []byte{byte('a' + i%26)}
			_, err := store.PutConditional(ctx, "objects/current.json", value, PutCondition{IfMatch: meta.ETag})
			if err == nil {
				successes <- string(value)
				return
			}
			conflicts <- err
		}(i)
	}
	wg.Wait()
	close(successes)
	close(conflicts)

	var written []string
	for value := range successes {
		written = append(written, value)
	}
	if len(written) != 1 {
		t.Fatalf("successful conditional writes = %d (%v), want 1", len(written), written)
	}
	for err := range conflicts {
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("conflict err = %v, want ErrConflict", err)
		}
	}
	data, err := store.Get(ctx, "objects/current.json")
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if string(data) != written[0] {
		t.Fatalf("final data = %q, want successful value %q", data, written[0])
	}
}

func TestFileStoreListSkipsAtomicWriteTemps(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	if err := store.Put(ctx, "objects/current.json", []byte("ok")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "objects", ".tmp-current.json-leftover"), []byte("partial"), 0o644); err != nil {
		t.Fatalf("write temp leftover: %v", err)
	}
	objects, err := store.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "objects/current.json" {
		t.Fatalf("listed objects = %#v, want only current object", objects)
	}
}

func TestFileStoreListWalksOnlyPrefixDirectory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	if err := store.Put(ctx, "objects/current.json", []byte("ok")); err != nil {
		t.Fatalf("put: %v", err)
	}
	unrelated := filepath.Join(root, "unrelated")
	if err := os.Mkdir(unrelated, 0o755); err != nil {
		t.Fatalf("mkdir unrelated: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unrelated, "hidden.json"), []byte("hidden"), 0o644); err != nil {
		t.Fatalf("write unrelated: %v", err)
	}
	if err := os.Chmod(unrelated, 0); err != nil {
		t.Fatalf("chmod unrelated: %v", err)
	}
	defer func() { _ = os.Chmod(unrelated, 0o755) }()

	objects, err := store.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("list prefix: %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "objects/current.json" {
		t.Fatalf("listed objects = %#v, want only current object", objects)
	}
}

func TestFileStoreListDoesNotReadObjectBodies(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	if err := store.Put(ctx, "objects/current.json", []byte("ok")); err != nil {
		t.Fatalf("put: %v", err)
	}
	path := filepath.Join(root, "objects", "current.json")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatalf("chmod unreadable: %v", err)
	}
	defer func() { _ = os.Chmod(path, 0o644) }()
	objects, err := store.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("list unreadable body: %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "objects/current.json" || objects[0].Size != 2 {
		t.Fatalf("listed objects = %#v", objects)
	}
}

func TestFileStoreRejectsPathControlObjectKeys(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	for _, key := range []string{
		"../escape.json",
		"/absolute.json",
		"objects/../escape.json",
		"./alias.json",
		"objects//alias.json",
		"objects/./alias.json",
		`objects\alias.json`,
	} {
		if err := store.Put(ctx, key, []byte("bad")); err == nil {
			t.Fatalf("put %q succeeded, want invalid key", key)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "escape.json")); !os.IsNotExist(err) {
		t.Fatalf("path-control key created alias object, stat err=%v", err)
	}
}

func TestFileStoreSkipsAndRejectsSymlinkObjects(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	if err := store.Put(ctx, "objects/current.json", []byte("ok")); err != nil {
		t.Fatalf("put: %v", err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	link := filepath.Join(root, "objects", "link.json")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := store.Get(ctx, "objects/link.json"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get symlink err = %v, want ErrNotFound", err)
	}
	objects, err := store.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(objects) != 1 || objects[0].Key != "objects/current.json" {
		t.Fatalf("listed objects = %#v, want only regular object", objects)
	}
}

func TestFileStoreDeleteIgnoresNonRegularObjectPaths(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	dir := filepath.Join(root, "objects")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir object dir: %v", err)
	}
	if err := store.Delete(ctx, "objects"); err != nil {
		t.Fatalf("delete directory object path: %v", err)
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() {
		t.Fatalf("directory object path was removed, info=%#v err=%v", info, err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	link := filepath.Join(root, "objects", "link.json")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := store.Delete(ctx, "objects/link.json"); err != nil {
		t.Fatalf("delete symlink object path: %v", err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatalf("symlink object path was removed: %v", err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "secret" {
		t.Fatalf("outside target changed, data=%q err=%v", data, err)
	}
}

func TestFileStorePutRejectsNonRegularObjectPaths(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewFileStore(root)
	dir := filepath.Join(root, "objects")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir object dir: %v", err)
	}
	if err := store.Put(ctx, "objects", []byte("replace-dir")); err == nil {
		t.Fatal("put directory object path succeeded")
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() {
		t.Fatalf("directory object path was changed, info=%#v err=%v", info, err)
	}

	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	link := filepath.Join(root, "objects", "link.json")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := store.Put(ctx, "objects/link.json", []byte("replace-link")); err == nil {
		t.Fatal("put symlink object path succeeded")
	}
	if _, err := store.PutConditional(ctx, "objects/link.json", []byte("replace-link"), PutCondition{IfNoneMatch: true}); err == nil {
		t.Fatal("conditional put symlink object path succeeded")
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatalf("symlink object path was removed: %v", err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "secret" {
		t.Fatalf("outside target changed, data=%q err=%v", data, err)
	}
}

func TestFileStoreRejectsSymlinkParentDirectories(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.json"), []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "objects")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	store := NewFileStore(root)
	if data, err := store.Get(ctx, "objects/secret.json"); err == nil || string(data) == "secret" {
		t.Fatalf("get through symlink dir data=%q err=%v, want rejection", data, err)
	}
	if err := store.Put(ctx, "objects/new.json", []byte("created outside")); err == nil {
		t.Fatal("put through symlink dir succeeded, want rejection")
	}
	if _, err := os.Stat(filepath.Join(outside, "new.json")); !os.IsNotExist(err) {
		t.Fatalf("outside object was created, stat err=%v", err)
	}
	if err := store.Delete(ctx, "objects/secret.json"); err == nil {
		t.Fatal("delete through symlink dir succeeded, want rejection")
	}
	if data, err := os.ReadFile(filepath.Join(outside, "secret.json")); err != nil || string(data) != "secret" {
		t.Fatalf("outside file was changed by delete, data=%q err=%v", data, err)
	}
}

func TestFileStoreListRejectsSymlinkRoot(t *testing.T) {
	ctx := context.Background()
	outside := t.TempDir()
	linkRoot := filepath.Join(t.TempDir(), "data")
	if err := os.Symlink(outside, linkRoot); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	store := NewFileStore(linkRoot)
	if objects, err := store.List(ctx, ""); err == nil {
		t.Fatalf("list through symlink root objects=%#v, want rejection", objects)
	}
}

func TestFileStoreExclusiveRuntime(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if _, err := OpenFileStore(root); !errors.Is(err, ErrDataDirectoryLocked) {
		t.Fatalf("second open: %v", err)
	}
	meta, err := files.PutConditional(ctx, "tenant/head", []byte("first"), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := files.Head(ctx, "tenant/head"); err != nil {
		t.Fatal(err)
	}
	next, err := files.PutConditional(ctx, "tenant/head", []byte("other"), PutCondition{IfMatch: meta.ETag})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := files.PutConditional(ctx, "tenant/head", []byte("stale"), PutCondition{IfMatch: meta.ETag}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale write: %v", err)
	}
	got, err := files.Head(ctx, "tenant/head")
	if err != nil || got.ETag != next.ETag {
		t.Fatalf("cached head after replacement: %+v %v", got, err)
	}
	files.Close()
	reopened, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	data, err := reopened.Get(ctx, "tenant/head")
	if err != nil || string(data) != "other" {
		t.Fatalf("reopen: %q %v", data, err)
	}
}

func TestFileStoreProcessDeathReleasesDirectory(t *testing.T) {
	if root := os.Getenv("GRAPHDB_TEST_DISK_CHILD"); root != "" {
		files, err := OpenFileStore(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := files.Put(context.Background(), "confirmed", []byte("durable")); err != nil {
			t.Fatal(err)
		}
		fmt.Println("ready")
		select {}
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFileStoreProcessDeathReleasesDirectory$")
	child.Env = append(os.Environ(), "GRAPHDB_TEST_DISK_CHILD="+root)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatalf("child startup: %s %v", scanner.Text(), scanner.Err())
	}
	if _, err := OpenFileStore(root); !errors.Is(err, ErrDataDirectoryLocked) {
		t.Fatalf("process lock: %v", err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	data, err := files.Get(ctx, "confirmed")
	if err != nil || string(data) != "durable" {
		t.Fatalf("confirmed write after process death: %q %v", data, err)
	}
}

func TestFileStoreLocalReadViewsAndInvalidation(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	objects := NewMeteredObjectStore(NewReadProtectedObjectStore(files, ReadProtectionConfig{MaxConcurrent: 4}), nil, nil)
	store := NewTenantStore(objects, "graphdb")
	cache := NewReaderCache(store, time.Hour)
	if _, err := store.InitTenant(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cache.Load(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	result, err := store.CommitWithReport(ctx, "a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "one", Kind: "host"}}}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, manifest, err := cache.Load(ctx, "a")
	if err != nil || manifest.Version != result.Version {
		t.Fatalf("published view: %+v %v", manifest, err)
	}
	if _, ok := got.GetEntity("one"); !ok {
		t.Fatal("new graph not visible")
	}
	release, err := store.PinReadView(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	timeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := store.lockReadViews(timeout, "a", true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("maintenance raced active view: %v", err)
	}
	other, err := store.lockReadViews(ctx, "b", true)
	if err != nil {
		t.Fatal(err)
	}
	other()
	release()
	exclusive, err := store.lockReadViews(ctx, "a", true)
	if err != nil {
		t.Fatal(err)
	}
	exclusive()
	if _, err := store.Compact(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	cache.mu.RLock()
	_, retained := cache.entries["a"]
	cache.mu.RUnlock()
	if !retained {
		t.Fatal("compaction discarded the fixed graph needed for incremental catch-up")
	}
	if _, _, err := cache.Load(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	store.deleteWriteCache("a")
	cache.Invalidate("a")
	got, _, err = cache.Load(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.GetEntity("one"); !ok {
		t.Fatal("random-read snapshot lost entity")
	}
}

func TestLocalReadViewGateAdmitsReadersBetweenMaintenanceWriters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	first, err := store.lockReadViews(ctx, "tenant-a", true)
	if err != nil {
		t.Fatal(err)
	}
	first = sync.OnceFunc(first)
	defer first()
	entered := make(chan bool, 2)
	for _, write := range []bool{true, false} {
		go func() {
			release, err := store.lockReadViews(ctx, "tenant-a", write)
			if err != nil {
				return
			}
			defer release()
			entered <- write
			<-ctx.Done()
		}()
	}
	for {
		files.runtime.mu.Lock()
		gate := files.runtime.views[store.tenantObjectPrefix("tenant-a")]
		queued := gate.refs == 3
		files.runtime.mu.Unlock()
		if queued {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(time.Millisecond)
	}
	first()
	select {
	case write := <-entered:
		if write {
			t.Fatal("another maintenance writer overtook the queued reader")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
}

func TestFileStoreRandomParquetReadAndCancellation(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	page := EntityPageData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: "a", Shard: "00", Version: 1, Entities: []graph.Entity{{ID: "one", Kind: "host", Fields: graph.Fields{"name": "example"}}}}
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		t.Fatal(err)
	}
	if err := files.Put(ctx, "page.parquet", data); err != nil {
		t.Fatal(err)
	}
	expected, err := decodeParquetEntityPage(ctx, data, "a", "00", 1)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openFileReader(ctx, files, "page.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	actual, err := decodeParquetEntityPageReader(ctx, borrowedParquetSource{reader}, "a", "00", 1)
	if err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("random read differs: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	handle, err := files.OpenReader(canceled, "page.parquet")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := handle.ReadAt(make([]byte, 4), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read: %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreFailedBatchKeepsPublishedHead(t *testing.T) {
	for _, stage := range []string{"data", "directory", "manifest"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			files, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			if err := files.Put(ctx, "head", []byte("old")); err != nil {
				t.Fatal(err)
			}
			store := NewTenantStore(files, "")
			err = store.runFileWriteJobs(ctx, 1, func(ctx context.Context, _ int) error {
				if stage == "data" {
					return files.Put(ctx, "../invalid", []byte("new"))
				}
				if err := files.Put(ctx, "new/data", []byte("new")); err != nil {
					return err
				}
				if stage == "directory" {
					// Simulate a directory becoming unavailable before its durability barrier.
					return os.Rename(filepath.Join(root, "new"), filepath.Join(root, "orphan"))
				}
				return nil
			})
			if err == nil {
				_, err = files.PutConditional(ctx, "head", []byte("new"), PutCondition{IfMatch: "wrong-generation"})
			}
			if err == nil {
				t.Fatal("failed publication reported success")
			}
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			data, err := reopened.Get(ctx, "head")
			if err != nil || string(data) != "old" {
				t.Fatalf("old head lost after %s failure: %q %v", stage, data, err)
			}
		})
	}
}

func TestFileStoreDeleteBatchReportsDirectoryBarrierFailure(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled_%t", canceled), func(t *testing.T) {
			root := t.TempDir()
			files, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := files.Put(ctx, "parts/obsolete", []byte("old")); err != nil {
				t.Fatal(err)
			}
			store := NewTenantStore(files, "")
			err = store.runFileWriteJobs(ctx, 1, func(ctx context.Context, _ int) error {
				if err := files.Delete(ctx, "parts/obsolete"); err != nil {
					return err
				}
				if err := os.Rename(filepath.Join(root, "parts"), filepath.Join(root, "unavailable")); err != nil {
					return err
				}
				if canceled {
					cancel()
					return ctx.Err()
				}
				return nil
			})
			if !errors.Is(err, os.ErrNotExist) || (canceled && !errors.Is(err, context.Canceled)) {
				t.Fatalf("delete batch lost its durability failure: %v", err)
			}
			if err := os.Rename(filepath.Join(root, "unavailable"), filepath.Join(root, "parts")); err != nil {
				t.Fatal(err)
			}
			if err := files.syncPendingDirectories(); err != nil {
				t.Fatalf("retry directory barrier: %v", err)
			}
			if _, err := files.Get(context.Background(), "parts/obsolete"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted file reappeared: %v", err)
			}
		})
	}
}

func TestFileStoreReadViewSurvivesCallerCancellation(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "graphdb")
	ctx, cancel := context.WithCancel(context.Background())
	view, releaseRequest, err := store.ReadViewContext(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, releaseLoad, err := store.ReadViewContext(context.WithoutCancel(view), "a")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	releaseRequest()
	timeout, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := store.lockReadViews(timeout, "a", true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GC entered while shared load was active: %v", err)
	}
	releaseLoad()
	release, err := store.lockReadViews(context.Background(), "a", true)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestFileStoreManifestCacheTracksPublication(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "graphdb")
	if _, err := store.InitTenant(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	first, meta, err := store.getManifest(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, _, generation, _, err := files.cachedManifest(ctx, store.manifestKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.CommitWithReport(ctx, "a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "one", Kind: "host"}}}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	files.cacheManifest(store.manifestKey("a"), first, meta, generation, false)
	// A delayed wrapping-cache read can start after the file generation changes
	// and still return the old bytes. It must not overwrite the published head.
	_, _, generation, _, err = files.cachedManifest(ctx, store.manifestKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	files.cacheManifest(store.manifestKey("a"), first, meta, generation, false)
	current, err := store.CurrentManifest(ctx, "a")
	if err != nil || current.Version != result.Version {
		t.Fatalf("stale load replaced published head: %+v %v", current, err)
	}
	data, err := files.Get(ctx, store.manifestKey("a"))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := decodeParquetManifest(ctx, data)
	if err != nil || !reflect.DeepEqual(current, persisted) {
		t.Fatalf("cached and persisted heads differ: %+v %+v %v", current, persisted, err)
	}
	if len(current.CommitKeys) > 0 {
		current.CommitKeys[0] = "caller-mutated"
	}
	if len(current.CommitSegments) > 0 {
		current.CommitSegments[0].Key = "caller-mutated"
	}
	fresh, err := store.CurrentManifest(ctx, "a")
	if err != nil || !reflect.DeepEqual(fresh, persisted) {
		t.Fatalf("caller changed cached head: %+v %v", fresh, err)
	}
	// A delayed byte-cache fill may outlive publication and eviction of the
	// bounded file metadata caches. Reload must still use the durable head.
	oldData, err := marshalParquetManifest(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	bytesCache := NewWriterObjectCache(files, WriterObjectCacheConfig{MaxBytes: 1 << 20, MaxKeys: 16})
	bytesCache.cachePositive(store.manifestKey("a"), oldData, meta, true)
	files.runtime.mu.Lock()
	clear(files.runtime.manifests)
	files.runtime.manifestBytes = 0
	clear(files.runtime.etags)
	files.runtime.mu.Unlock()
	cachedStore := NewTenantStore(bytesCache, "graphdb")
	fresh, err = cachedStore.CurrentManifest(ctx, "a")
	if err != nil || !reflect.DeepEqual(fresh, persisted) {
		t.Fatalf("evicted metadata reused a stale byte-cache head: %+v %+v %v", fresh, persisted, err)
	}
	if err := files.Put(ctx, store.manifestKey("a"), []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CurrentManifest(ctx, "a"); err == nil {
		t.Fatal("corrupt replacement was hidden by cached head")
	}
}

func TestFileStorePublicationWaitsForOtherBatchDirectories(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.Put(ctx, "head", []byte("old")); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(files, "")
	renamed, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- store.runFileWriteJobs(ctx, 1, func(ctx context.Context, _ int) error {
			if err := files.Put(ctx, "parts/data", []byte("new")); err != nil {
				close(renamed)
				return err
			}
			close(renamed)
			<-finish
			return nil
		})
	}()
	<-renamed
	// A second publisher can see and reuse the first batch's file before that
	// batch returns. An unavailable directory must still prevent head publication.
	if err := os.Rename(filepath.Join(root, "parts"), filepath.Join(root, "unavailable")); err != nil {
		close(finish)
		<-done
		t.Fatal(err)
	}
	publishErr := files.Put(ctx, "head", []byte("new"))
	close(finish)
	batchErr := <-done
	if publishErr == nil || batchErr == nil {
		t.Fatalf("publication bypassed pending barrier: publish=%v batch=%v", publishErr, batchErr)
	}
	data, err := files.Get(ctx, "head")
	if err != nil || string(data) != "old" {
		t.Fatalf("failed barrier changed head: %q %v", data, err)
	}
	if err := os.Rename(filepath.Join(root, "unavailable"), filepath.Join(root, "parts")); err != nil {
		t.Fatal(err)
	}
	if err := files.Put(ctx, "head", []byte("new")); err != nil {
		t.Fatalf("barrier retry failed: %v", err)
	}
}

func TestFileStoreDirectoryWaitHonorsCancellation(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.Put(ctx, "data", []byte("visible")); err != nil {
		t.Fatal(err)
	}
	unlock, err := files.lockDirectoryIOWeight(ctx, directoryIOCapacity)
	if err != nil {
		t.Fatal(err)
	}
	unlock = sync.OnceFunc(unlock)
	defer unlock()
	blockedCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := files.Get(blockedCtx, "data"); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked Get: %v", err)
		}
	case <-time.After(time.Second):
		unlock()
		<-done
		t.Fatal("file read ignored deadline while directory publication was blocked")
	}
	unlock()
	data, err := files.Get(ctx, "data")
	if err != nil || string(data) != "visible" {
		t.Fatalf("read after canceled waiter: %q %v", data, err)
	}
}
