package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"graphdb/internal/graph"
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
