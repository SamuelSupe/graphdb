package replication

import (
	"errors"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDurableLogTruncationCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, existing, err := openStorage(dir, 1, "test")
	if err != nil || existing {
		t.Fatalf("open fresh: %v, %v", existing, err)
	}
	entries := []raftpb.Entry{}
	for i := uint64(1); i <= 5; i++ {
		entries = append(entries, raftpb.Entry{Index: i, Term: 1, Data: []byte("old")})
	}
	if err := s.save(raft.Ready{Entries: entries, HardState: raftpb.HardState{Term: 1, Commit: 2}}); err != nil {
		t.Fatal(err)
	}
	// A new leader replaces an uncommitted suffix; it must disappear on disk.
	if err := s.save(raft.Ready{Entries: []raftpb.Entry{{Index: 3, Term: 2, Data: []byte("new")}, {Index: 4, Term: 2, Data: []byte("tail")}}, HardState: raftpb.HardState{Term: 2, Commit: 4}}); err != nil {
		t.Fatal(err)
	}
	conf := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	if err := s.saveConf(conf, 2); err != nil {
		t.Fatal(err)
	}
	s.conf, s.confIndex = conf, 2
	snapshot, err := s.CreateSnapshot(2, &conf, []byte("state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.saveSnapshot(snapshot, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s, existing, err = openStorage(dir, 1, "test")
	if err != nil || !existing {
		t.Fatalf("reopen: %v, %v", existing, err)
	}
	defer s.db.Close()
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if first != 3 || last != 4 {
		t.Fatalf("persisted range: %d..%d", first, last)
	}
	if _, err := s.Term(1); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted term: %v", err)
	}
	if term, err := s.Term(2); err != nil || term != 1 {
		t.Fatalf("snapshot term: %d, %v", term, err)
	}
	if _, err := s.Term(5); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("truncated term: %v", err)
	}
	loaded, err := s.Entries(3, 5, 0)
	if err != nil || len(loaded) != 1 || loaded[0].Term != 2 || string(loaded[0].Data) != "new" {
		t.Fatalf("bounded log read: %v, %v", loaded, err)
	}
	loaded, err = s.Entries(3, 5, ^uint64(0))
	if err != nil || len(loaded) != 2 {
		t.Fatalf("log suffix: %v, %v", loaded, err)
	}
	hard, restoredConf, err := s.InitialState()
	if err != nil || hard.Commit != 4 || len(restoredConf.Voters) != 3 {
		t.Fatalf("consensus state: %v, %v, %v", hard, restoredConf, err)
	}
}
