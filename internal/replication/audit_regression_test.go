package replication

import (
	"encoding/json"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestSaveSnapshotMinimumProtocolMonotonic(t *testing.T) {
	disk, _, err := openStorage(t.TempDir(), 1, "monotonic-test")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	highPayload, err := json.Marshal(proposal{ID: "p1", Data: []byte("ok"), Protocol: MaxProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.save(raft.Ready{
		Entries: []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryNormal, Data: highPayload}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	got, err := disk.minimumProtocol()
	if err != nil || got != MaxProtocolVersion {
		t.Fatalf("minimumProtocol after high entry: %d %v", got, err)
	}

	lowSnapData, err := json.Marshal(snapshotEnvelope{
		Protocol: 2,
		Version:  1,
		State:    []byte("snap"),
		Peers:    map[uint64]string{1: "http://127.0.0.1:1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := disk.CreateSnapshot(1, &raftpb.ConfState{Voters: []uint64{1}}, lowSnapData)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.saveSnapshot(snap, 1); err != nil {
		t.Fatal(err)
	}
	after, err := disk.minimumProtocol()
	if err != nil || after != MaxProtocolVersion {
		t.Fatalf("saveSnapshot regressed minimumProtocol: got %d want %d (err=%v)", after, MaxProtocolVersion, err)
	}
}

func TestClassifyProposalDroppedMapsLeadershipStepDownToNotLeader(t *testing.T) {
	n := &Node{
		cfg:          Config{ID: 1},
		ctx:          t.Context(),
		stateChanged: make(chan struct{}),
	}
	// Follower state (leader != ID) must map ErrProposalDropped to ErrNotLeader.
	n.leader.Store(2)
	if err := n.classifyProposalError(raft.ErrProposalDropped); err != ErrNotLeader {
		t.Fatalf("follower ErrProposalDropped = %v, want ErrNotLeader", err)
	}

	// Draining leader must also map ErrProposalDropped to ErrNotLeader.
	n.leader.Store(1)
	n.draining.Store(true)
	if err := n.classifyProposalError(raft.ErrProposalDropped); err != ErrNotLeader {
		t.Fatalf("draining ErrProposalDropped = %v, want ErrNotLeader", err)
	}
}

func TestDiskStorageRetainsLogTailBehindSnapshot(t *testing.T) {
	dir := t.TempDir()
	disk, _, err := openStorage(dir, 1, "tail-test")
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]raftpb.Entry, 20)
	for i := range entries {
		entries[i] = raftpb.Entry{Index: uint64(i + 1), Term: 1 + uint64(i/10), Type: raftpb.EntryNormal, Data: []byte("x")}
	}
	if err := disk.save(raft.Ready{Entries: entries}, nil); err != nil {
		t.Fatal(err)
	}
	snap, err := disk.CreateSnapshot(20, &raftpb.ConfState{Voters: []uint64{1}}, []byte("snap"))
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.saveSnapshot(snap, 15); err != nil {
		t.Fatal(err)
	}
	verify := func(s *diskStorage) {
		first, err := s.FirstIndex()
		if err != nil || first != 16 {
			t.Fatalf("FirstIndex = %d, %v; want 16", first, err)
		}
		last, err := s.LastIndex()
		if err != nil || last != 20 {
			t.Fatalf("LastIndex = %d, %v; want 20", last, err)
		}
		if term, err := s.Term(15); err != nil || term != 2 {
			t.Fatalf("Term(15) = %d, %v; want 2", term, err)
		}
		if _, err := s.Term(14); err != raft.ErrCompacted {
			t.Fatalf("Term(14) err = %v; want ErrCompacted", err)
		}
		got, err := s.Entries(16, 21, 1<<20)
		if err != nil || len(got) != 5 || got[0].Index != 16 || got[4].Index != 20 {
			t.Fatalf("Entries(16,21) = len %d err %v; want 5 entries 16..20", len(got), err)
		}
		gotSnap, err := s.Snapshot()
		if err != nil || gotSnap.Metadata.Index != 20 {
			t.Fatalf("Snapshot index = %d err %v; want 20", gotSnap.Metadata.Index, err)
		}
	}
	verify(disk)
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := openStorage(dir, 1, "tail-test")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	verify(reopened)
}
