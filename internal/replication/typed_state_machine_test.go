package replication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

type fakeTypedMachine struct {
	results []ApplyResult
	fatal   *FatalMachineError
	called  int
}

func (m *fakeTypedMachine) Applied() (uint64, error)                     { return 0, nil }
func (m *fakeTypedMachine) Apply(context.Context, uint64, []byte) ([]byte, error) {
	return nil, errors.New("untyped Apply should not be called when TypedBatchStateMachine is implemented")
}
func (m *fakeTypedMachine) Snapshot(context.Context) ([]byte, error)     { return nil, nil }
func (m *fakeTypedMachine) Restore(context.Context, uint64, []byte) error { return nil }
func (m *fakeTypedMachine) ApplyTypedBatch(_ context.Context, entries []ApplyEntry) ([]ApplyResult, *FatalMachineError) {
	m.called++
	if m.fatal != nil {
		return nil, m.fatal
	}
	return m.results[:len(entries)], nil
}

func TestTypedBatchStateMachineSeparatesRejectedConflictsFromFatalErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conflictPayload := []byte(`{"status":409,"code":"restore_stage_conflict"}`)
	machine := &fakeTypedMachine{
		results: []ApplyResult{
			AcceptResult([]byte(`{"ok":true}`)),
			RejectResult(conflictPayload, "restore_stage_conflict", "stale manifest"),
		},
	}
	n := &Node{
		ctx:            ctx,
		cancel:         cancel,
		cfg:            Config{ID: 1},
		machine:        machine,
		stateChanged:   make(chan struct{}),
		appliedChanged: make(chan struct{}),
		proposals:      map[string]chan result{},
	}

	p1, _ := json.Marshal(proposal{ID: "p1", Data: []byte("cmd1"), Protocol: ProtocolVersion})
	p2, _ := json.Marshal(proposal{ID: "p2", Data: []byte("cmd2"), Protocol: ProtocolVersion})
	ch1 := make(chan result, 1)
	ch2 := make(chan result, 1)
	n.proposals["p1"] = ch1
	n.proposals["p2"] = ch2

	entries := []raftpb.Entry{
		{Index: 1, Type: raftpb.EntryNormal, Data: p1},
		{Index: 2, Type: raftpb.EntryNormal, Data: p2},
	}
	if err := n.applyEntries(entries); err != nil {
		t.Fatalf("deterministic conflict must not fail applyEntries: %v", err)
	}
	if got := n.applied.Load(); got != 2 {
		t.Fatalf("applied = %d, want 2", got)
	}
	if r1 := <-ch1; r1.err != nil || string(r1.data) != `{"ok":true}` {
		t.Fatalf("p1 result = %+v", r1)
	}
	if r2 := <-ch2; r2.err != nil || string(r2.data) != string(conflictPayload) {
		t.Fatalf("p2 result = %+v, want conflict payload", r2)
	}

	// Now verify a FatalMachineError halts applyEntries and does NOT advance applied.
	fatalErr := errors.New("corrupt staged restore part")
	machine.fatal = FatalApplyError(fatalErr)
	p3, _ := json.Marshal(proposal{ID: "p3", Data: []byte("cmd3"), Protocol: ProtocolVersion})
	err := n.applyEntries([]raftpb.Entry{{Index: 3, Type: raftpb.EntryNormal, Data: p3}})
	if err == nil || !errors.Is(err, fatalErr) {
		t.Fatalf("expected FatalMachineError wrapping %v, got %v", fatalErr, err)
	}
	if got := n.applied.Load(); got != 2 {
		t.Fatalf("applied advanced to %d on fatal error, want 2", got)
	}
}
