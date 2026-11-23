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
