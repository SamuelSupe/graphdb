package replication

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestRound4CreateSnapshotRejectsEmptyConfState(t *testing.T) {
	disk, _, err := openStorage(t.TempDir(), 1, "test-cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	if _, err := disk.CreateSnapshot(1, &raftpb.ConfState{}, []byte("snap")); err == nil {
		t.Fatal("CreateSnapshot accepted empty ConfState with zero voters")
	}
	if _, err := disk.CreateSnapshot(1, nil, []byte("snap")); err == nil {
		t.Fatal("CreateSnapshot accepted nil ConfState")
	}
}
