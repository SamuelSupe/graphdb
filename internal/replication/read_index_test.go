package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

type recordedReadQuorums struct {
	raft.Node
	submitted chan string
}

func (r recordedReadQuorums) ReadIndex(ctx context.Context, request []byte) error {
	select {
	case r.submitted <- string(request):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestReadIndexBatchesOnlyCallersQueuedBeforeSubmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	reads := recordedReadQuorums{submitted: make(chan string, 4)}
	n := &Node{cfg: Config{ID: 1}, raft: reads, ctx: ctx, stateChanged: make(chan struct{}), readWake: make(chan struct{}, 1)}
	n.leader.Store(1)
	defer func() { cancel(); n.workers.Wait() }()
	type reply struct {
		index uint64
		err   error
	}
	call := func(ctx context.Context) <-chan reply {
		result := make(chan reply, 1)
		go func() {
			index, err := n.readIndex(ctx)
			result <- reply{index: index, err: err}
		}()
		return result
	}
	submit := func() string {
		t.Helper()
		select {
		case id := <-reads.submitted:
			return id
		case <-ctx.Done():
			t.Fatal("read quorum was not submitted")
			return ""
		}
	}
	wait := func(result <-chan reply) reply {
		t.Helper()
		select {
		case value := <-result:
			return value
		case <-ctx.Done():
			t.Fatal("read quorum did not complete")
			return reply{}
		}
	}
	results := make([]<-chan reply, 32)
	for i := range results {
		results[i] = call(ctx)
	}
	abort, cancelCaller := context.WithCancel(ctx)
	defer cancelCaller()
	cancelled := call(abort)
	for {
		n.mu.Lock()
		waiting := n.readWaiters
		n.mu.Unlock()
		if waiting == len(results)+1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("callers did not enter the collection window")
		}
		time.Sleep(time.Millisecond)
	}
	n.workers.Add(1)
	go n.readIndexLoop()
	first := submit()
	late := call(ctx)
	cancelCaller()
	if got := wait(cancelled); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("cancelled caller: %+v", got)
	}
	n.completeReadIndex(first, 10, nil)
	for _, result := range results {
		if got := wait(result); got.err != nil || got.index != 10 {
			t.Fatalf("pre-submission caller did not use the first index: %+v", got)
		}
	}
	second := submit()
	if first == second {
		t.Fatal("the late caller reused the submitted round")
	}
	n.completeReadIndex(first, 10, nil)
	n.completeReadIndex(second, 11, nil)
	if got := wait(late); got.err != nil || got.index != 11 {
		t.Fatalf("late caller did not wait for a new quorum: %+v", got)
	}
	oldLeader := call(ctx)
	lost := submit()
	n.leader.Store(2)
	n.cancelReadIndexForLeadershipLoss()
	if got := wait(oldLeader); !errors.Is(got.err, ErrNotLeader) {
		t.Fatalf("read survived leadership loss: %+v", got)
	}
	n.leader.Store(1)
	newLeader := call(ctx)
	current := submit()
	n.completeReadIndex(lost, 11, nil)
	n.completeReadIndex(current, 12, nil)
	if got := wait(newLeader); got.err != nil || got.index != 12 {
		t.Fatalf("new leadership reused an old quorum: %+v", got)
	}
}
