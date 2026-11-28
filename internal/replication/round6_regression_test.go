package replication

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRound6SupersededSnapshotTriggersCleanup(t *testing.T) {
	dir := t.TempDir()
	disk, _, err := openStorage(dir, 1, "r6-superseded-snap")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	if err := disk.save(raft.Ready{
		Entries: []raftpb.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
			{Index: 4, Term: 1},
		},
	}, nil); err != nil {
		t.Fatal(err)
	}
	conf := raftpb.ConfState{Voters: []uint64{1}}
	if err := disk.saveConf(conf, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.CreateSnapshot(4, &conf, []byte("snap-4")); err != nil {
		t.Fatalf("CreateSnapshot(4) failed: %v", err)
	}

	snapDir := filepath.Join(dir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	obsolete := filepath.Join(snapDir, "00000000000000000002-old.snap")
	if err := os.WriteFile(obsolete, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{
		ctx:                     ctx,
		disk:                    disk,
		cfg:                     Config{Dir: dir},
		snapshotCleanupWake: make(chan struct{}, 1),
	}
	t.Cleanup(func() {
		cancel()
		node.workers.Wait()
	})
	node.workers.Add(1)
	go node.snapshotCleanupLoop(nil)
	node.applied.Store(4)
	node.scheduleSnapshotCleanup(4)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(obsolete); os.IsNotExist(err) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("expected superseded snapshot file %s to be cleaned up", obsolete)
}
