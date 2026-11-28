package replication

import (
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDiskStorageInstallSnapshotDiscardsDivergedTailEntries(t *testing.T) {
	dir := t.TempDir()
	disk, _, err := openStorage(dir, 1, "cluster-snap-tail")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	// Seed uncommitted diverged entries 1..10 at term 2 (e.g., isolated old leader).
	entries := make([]raftpb.Entry, 10)
	for i := range entries {
		entries[i] = raftpb.Entry{Index: uint64(i + 1), Term: 2, Type: raftpb.EntryNormal}
	}
	if err := disk.save(raft.Ready{Entries: entries}, nil); err != nil {
		t.Fatalf("seed entries: %v", err)
	}

	// Install an external snapshot at (Index=5, Term=3) with no trailing entries.
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     5,
			Term:      3,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}
	env := &snapshotEnvelope{
		Version:  1,
		Protocol: ProtocolVersion,
		Peers:    map[uint64]string{1: "http://127.0.0.1:1"},
	}
	if err := disk.save(raft.Ready{Snapshot: snap}, env); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	// LastIndex must be 5 (no stale term-2 tail entries 6..10 retained).
	lastIdx, err := disk.LastIndex()
	if err != nil || lastIdx != 5 {
		t.Fatalf("LastIndex after snapshot = %d (err=%v), want 5", lastIdx, err)
	}
	term, err := disk.Term(5)
	if err != nil || term != 3 {
		t.Fatalf("Term(5) = %d (err=%v), want 3", term, err)
	}
	if _, err := disk.Term(6); err != raft.ErrUnavailable {
		t.Fatalf("Term(6) = %v, want raft.ErrUnavailable", err)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen from BoltDB to confirm stale tail entries were deleted durably too.
	reopened, _, err := openStorage(dir, 1, "cluster-snap-tail")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	lastIdx, err = reopened.LastIndex()
	if err != nil || lastIdx != 5 {
		t.Fatalf("reopened LastIndex = %d (err=%v), want 5", lastIdx, err)
	}
}
