package bootstrap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestStandaloneReopensExistingData(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir(), Prefix: "graphdb", InstanceID: "standalone"}
	ctx := context.Background()
	runtime, err := NewStorageRuntime(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Store.InitTenant(ctx, "tenant-a"); err != nil {
		runtime.Close()
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime, err = NewStorageRuntime(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err := runtime.Store.GetTenantInfo(ctx, "tenant-a"); err != nil {
		t.Fatalf("existing standalone tenant did not reopen: %v", err)
	}
}

func TestStandaloneRejectsRaftDirectoryBeforeFirstApplication(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir(), Prefix: "graphdb"}
	files, err := storage.OpenFileStore(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := files.RequireReplicatedWrites(); err != nil {
		files.Close()
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewStorageRuntime(context.Background(), cfg)
	if runtime != nil {
		runtime.Close()
	}
	if !errors.Is(err, storage.ErrRaftDataDirectory) {
		t.Fatalf("replica was opened without Raft configuration: %v", err)
	}
	cfg.Raft.Enabled = true
	runtime, err = NewStorageRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatalf("rejected standalone startup prevented Raft reopening: %v", err)
	}
	runtime.Close()
}

func TestShutdownRetainsDirectoryUntilBackgroundWorkStops(t *testing.T) {
	root := t.TempDir()
	files, err := storage.OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewTenantStore(files, "test")
	runtime := &StorageRuntime{Store: store, Files: files}
	canceled, release := make(chan struct{}), make(chan struct{})
	defer runtime.Close()
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	if !store.StartBackground(context.Background(), func(ctx context.Context) { <-ctx.Done(); close(canceled); <-release }) {
		t.Fatal("worker rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := runtime.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown returned before worker exited: %v", err)
	}
	<-canceled
	if second, err := storage.OpenFileStore(root); err == nil {
		second.Close()
		t.Fatal("directory ownership released early")
	}
	if store.StartBackground(context.Background(), func(context.Context) {}) {
		t.Fatal("shutdown accepted new work")
	}
	unblock()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenFileStore(root)
	if err != nil {
		t.Fatalf("directory did not reopen: %v", err)
	}
	reopened.Close()
}
