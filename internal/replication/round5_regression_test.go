package replication

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRound5CreateSnapshotRejectsBelowConfIndex(t *testing.T) {
	disk, _, err := openStorage(t.TempDir(), 1, "r5-snap-conf-index")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	if err := disk.save(raft.Ready{
		Entries: []raftpb.Entry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
	}, nil); err != nil {
		t.Fatal(err)
	}
	conf := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	if err := disk.saveConf(conf, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.CreateSnapshot(2, &conf, []byte("state")); !errors.Is(err, raft.ErrSnapOutOfDate) {
		t.Fatalf("CreateSnapshot below confIndex = %v, want ErrSnapOutOfDate", err)
	}
	snap, err := disk.CreateSnapshot(3, &conf, []byte("state"))
	if err != nil {
		t.Fatalf("CreateSnapshot at confIndex failed: %v", err)
	}
	if snap.Metadata.Index != 3 {
		t.Fatalf("snapshot index = %d, want 3", snap.Metadata.Index)
	}
}

func TestRound5SaveConfWithMembershipAtomicAndRaceFree(t *testing.T) {
	dir := t.TempDir()
	disk, _, err := openStorage(dir, 1, "r5-atomic-conf")
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	conf := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	changes := []pendingPeerChange{
		{change: memberChange{Action: "add_learner", ID: 4, URL: "http://four"}, index: 4},
		{change: memberChange{Action: "remove", ID: 4}, index: 5},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_, _ = disk.confState()
		}
	}()
	go func() {
		defer wg.Done()
		if err := disk.saveConfWithMembership(conf, 5, map[uint64]string{1: "http://one"}, changes); err != nil {
			t.Errorf("saveConfWithMembership: %v", err)
		}
	}()
	wg.Wait()

	gotConf, gotIdx := disk.confState()
	if gotIdx != 5 || len(gotConf.Voters) != 3 {
		t.Fatalf("confState = (%+v, %d), want 3 voters at index 5", gotConf, gotIdx)
	}
	n := &Node{disk: disk, peers: map[uint64]string{}}
	removedAt, retired, err := n.memberRetiredAt(4)
	if err != nil || !retired || removedAt != 5 {
		t.Fatalf("memberRetiredAt(4) = (%d, %v, %v), want (5, true, nil)", removedAt, retired, err)
	}
}

type round5BatchMachine struct {
	batchProtocols []int
	batchSizes     []int
}

func (m *round5BatchMachine) Applied() (uint64, error)                     { return 0, nil }
func (m *round5BatchMachine) Snapshot(context.Context) ([]byte, error)     { return []byte("s"), nil }
func (m *round5BatchMachine) Restore(context.Context, uint64, []byte) error { return nil }
func (m *round5BatchMachine) Apply(ctx context.Context, index uint64, data []byte) ([]byte, error) {
	m.batchProtocols = append(m.batchProtocols, ApplyProtocol(ctx))
	m.batchSizes = append(m.batchSizes, 1)
	return data, nil
}
func (m *round5BatchMachine) ApplyBatch(ctx context.Context, entries []ApplyEntry) ([][]byte, error) {
	m.batchProtocols = append(m.batchProtocols, ApplyProtocol(ctx))
	m.batchSizes = append(m.batchSizes, len(entries))
	out := make([][]byte, len(entries))
	for i, e := range entries {
		out[i] = e.Data
	}
	return out, nil
}

func TestRound5ApplyEntriesSplitsMixedProtocolBatches(t *testing.T) {
	machine := &round5BatchMachine{}
	n := &Node{
		ctx:       context.Background(),
		machine:   machine,
		proposals: map[string]chan result{},
		stateChanged: make(chan struct{}),
	}
	mkEntry := func(index uint64, protocol int) raftpb.Entry {
		raw, _ := json.Marshal(proposal{ID: "p", Data: []byte(`{"ok":true}`), Protocol: protocol})
		return raftpb.Entry{Index: index, Term: 1, Type: raftpb.EntryNormal, Data: raw}
	}
	entries := []raftpb.Entry{
		mkEntry(1, 12),
		mkEntry(2, 12),
		mkEntry(3, 13),
		mkEntry(4, 13),
	}
	if err := n.applyEntries(entries); err != nil {
		t.Fatalf("applyEntries failed: %v", err)
	}
	if len(machine.batchProtocols) != 2 || machine.batchProtocols[0] != 12 || machine.batchProtocols[1] != 13 {
		t.Fatalf("batchProtocols = %v, want [12 13]", machine.batchProtocols)
	}
	if len(machine.batchSizes) != 2 || machine.batchSizes[0] != 2 || machine.batchSizes[1] != 2 {
		t.Fatalf("batchSizes = %v, want [2 2]", machine.batchSizes)
	}
}
