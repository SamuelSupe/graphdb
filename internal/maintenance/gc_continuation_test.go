package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestPressureGCStartsAndContinuesBeforeNextMaintenanceTick(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := storage.NewTenantStore(files, "test")
	defer store.ShutdownTasks(context.Background())
	const tenant = "hot"
	config := storage.DefaultTenantConfig()
	*config.Indexes.AutoRebuild = false
	*config.Indexes.RebuildOnStale = false
	if _, err := store.PutTenantConfig(ctx, tenant, config); err != nil {
		t.Fatal(err)
	}
	const commits = 130
	for i := range commits {
		if _, err := store.CommitWithReport(ctx, tenant, graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"value": i}}},
		}, storage.CommitOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	countCommits := func() int {
		t.Helper()
		files, err := store.Objects.List(ctx, "test/tenants/hot/commits/")
		if err != nil {
			t.Fatal(err)
		}
		return len(files)
	}
	disk, err := files.DiskSpace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !disk.InodesKnown || disk.AvailableInodes < 100 {
		t.Skip("filesystem does not report usable inode capacity")
	}
	gc := DefaultGCConfig()
	gc.MaxDeletes = 33
	gc.ContinuationInterval = 40 * time.Millisecond
	gc.InodePressureInterval = 100 * time.Millisecond
	gc.InodePressureMultiplier = 3
	floor := disk.AvailableInodes/3 + disk.AvailableInodes/60 + 1
	if err := files.ConfigureDiskSpace(storage.DiskSpacePolicy{MinFreeInodes: floor}); err != nil {
		t.Fatal(err)
	}
	passes := make(chan map[string]any, 16)
	runner := &Runner{Store: store, GC: gc, Audit: func(event, _ string, _ error, fields map[string]any) {
		if event == "maintenance_gc_completed" {
			select {
			case passes <- fields:
			default:
			}
		}
	}}
	before := countCommits()
	runner.Start(ctx, time.Hour)
	deadline := time.NewTimer(12 * time.Second)
	defer deadline.Stop()
	deleted := 0
	for {
		select {
		case <-deadline.C:
			t.Fatalf("reclaimable commit files were left until the next maintenance tick: %d", countCommits())
		case pass := <-passes:
			count := pass["deleted_objects"].(int)
			if count > gc.MaxDeletes || (deleted == 0 && (pass["inode_pressure"] != true || pass["paused"] != true)) {
				t.Fatalf("unexpected pressure GC batch: %+v", pass)
			}
			deleted += count
			if pass["paused"] == false {
				if remaining := countCommits(); remaining > 64 || before-remaining != deleted {
					t.Fatalf("cleanup did not finish: %d before, %d deleted, %d remaining", before, deleted, remaining)
				}
				g, manifest, err := store.Load(ctx, tenant)
				if err != nil || manifest.Version != commits || len(g.Snapshot().Entities) != 1 {
					t.Fatalf("GC changed the published graph: %+v %v", manifest, err)
				}
				return
			}
		}
	}
}

func TestInodePressureAdvancesScheduledGC(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := storage.NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	config := storage.DefaultTenantConfig()
	*config.Indexes.AutoRebuild = false
	*config.Indexes.RebuildOnStale = false
	if _, err := store.PutTenantConfig(ctx, "hot", config); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitWithReport(ctx, "hot", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	disk, err := files.DiskSpace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !disk.InodesKnown || disk.AvailableInodes < 4 {
		t.Skip("filesystem does not report usable inode capacity")
	}
	// Leave room for changes in available inodes from other CI processes.
	floor := disk.AvailableInodes - disk.AvailableInodes/4
	if err := files.ConfigureDiskSpace(storage.DiskSpacePolicy{MinFreeInodes: floor}); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Store: store}
	now := time.Now()
	if first := runner.Run(ctx, now); first.GCRuns != 1 || len(first.Errors) != 0 {
		t.Fatalf("initial cleanup: %+v", first)
	}
	if early := runner.Run(ctx, now.Add(10*time.Second)); early.GCRuns != 0 {
		t.Fatalf("inode pressure spun up another GC too early: %+v", early)
	}
	if pressure := runner.Run(ctx, now.Add(30*time.Second)); pressure.GCRuns != 1 || len(pressure.Errors) != 0 {
		t.Fatalf("inode pressure did not advance the default 30-minute GC interval: %+v", pressure)
	}
}
