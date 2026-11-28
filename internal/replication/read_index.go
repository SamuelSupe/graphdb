package replication

import (
	"context"
	"time"
)

const (
	readIndexGather       = 100 * time.Microsecond
	maxInFlightReadRounds = 2
)

type readIndexRound struct {
	id      string
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	index   uint64
	err     error
}

func (n *Node) readIndex(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := n.available(true); err != nil {
		return 0, err
	}
	n.mu.Lock()
	if n.queuedReads == nil {
		n.queuedReads = &readIndexRound{done: make(chan struct{})}
		select {
		case n.readWake <- struct{}{}:
		default:
		}
	}
	round := n.queuedReads
	round.waiters++
	n.readWaiters++
	n.mu.Unlock()
	defer n.releaseReadIndex(round)
	for {
		n.mu.Lock()
		changed := n.stateChanged
		n.mu.Unlock()
		if err := n.availableState(true, false); err != nil {
			return 0, err
		}
		select {
		case <-round.done:
			return round.index, round.err
		case <-changed:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-n.ctx.Done():
			return 0, n.available(false)
		}
	}
}

func (n *Node) releaseReadIndex(round *readIndexRound) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.readWaiters--
	round.waiters--
	if round.waiters != 0 {
		return
	}
	if n.queuedReads == round {
		n.queuedReads = nil
	}
	if round.id != "" && n.activeReads[round.id] == round {
		n.completeReadIndexLocked(round, 0, context.Canceled)
	}
}

func (n *Node) readIndexLoop() {
	defer n.workers.Done()
	var lastSubmit time.Time
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.readWake:
		}
		if elapsed := time.Since(lastSubmit); elapsed < readIndexGather {
			timer := time.NewTimer(readIndexGather - elapsed)
			select {
			case <-n.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		n.submitReadIndexRound()
		lastSubmit = time.Now()
	}
}

func (n *Node) submitReadIndexRound() {
	id, err := randomID()
	if err == nil {
		err = n.available(true)
	}
	n.mu.Lock()
	if len(n.activeReads) >= maxInFlightReadRounds {
		n.mu.Unlock()
		return
	}
	round := n.queuedReads
	n.queuedReads = nil
	if round == nil {
		n.mu.Unlock()
		return
	}
	if err == nil && n.leader.Load() != n.cfg.ID {
		err = ErrNotLeader
	}
	if err != nil {
		round.err = err
		close(round.done)
		n.mu.Unlock()
		return
	}
	// Seal the membership before ReadIndex is submitted. A later caller
	// requires a new quorum observation even if this round has not replied.
	ctx, cancel := context.WithCancel(n.ctx)
	round.id, round.cancel = id, cancel
	if n.activeReads == nil {
		n.activeReads = make(map[string]*readIndexRound)
	}
	n.activeReads[id] = round
	n.mu.Unlock()
	n.metrics.Event("read_index_round")
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		n.runReadIndexRound(round, ctx, id)
	}()
}

func (n *Node) runReadIndexRound(round *readIndexRound, ctx context.Context, id string) {
	roundTimeout := 2 * time.Second
	if n.cfg.Tick > 0 {
		roundTimeout = max(time.Second, 2*n.cfg.Tick*time.Duration(max(n.cfg.ElectionTicks, 1)))
	}
	for {
		if err := n.raft.ReadIndex(ctx, []byte(id)); err != nil {
			n.completeReadIndex(id, 0, err)
			return
		}
		timer := time.NewTimer(roundTimeout)
		select {
		case <-round.done:
			timer.Stop()
			return
		case <-n.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		availErr := n.available(true)
		nextID, idErr := randomID()
		n.mu.Lock()
		if n.activeReads[id] != round {
			n.mu.Unlock()
			return
		}
		if availErr != nil {
			n.completeReadIndexLocked(round, 0, availErr)
			n.mu.Unlock()
			return
		}
		if idErr != nil {
			n.completeReadIndexLocked(round, 0, idErr)
			n.mu.Unlock()
			return
		}
		delete(n.activeReads, id)
		id = nextID
		round.id = id
		n.activeReads[id] = round
		n.mu.Unlock()
	}
}

func (n *Node) completeReadIndex(id string, index uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if round, ok := n.activeReads[id]; ok {
		n.completeReadIndexLocked(round, index, err)
	}
}

func (n *Node) cancelReadIndexForLeadershipLoss() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, round := range n.activeReads {
		n.completeReadIndexLocked(round, 0, ErrNotLeader)
	}
}

func (n *Node) completeReadIndexLocked(round *readIndexRound, index uint64, err error) {
	if n.activeReads[round.id] != round {
		return
	}
	delete(n.activeReads, round.id)
	round.index, round.err = index, err
	round.cancel()
	close(round.done)
	if n.queuedReads != nil && len(n.activeReads) < maxInFlightReadRounds {
		select {
		case n.readWake <- struct{}{}:
		default:
		}
	}
}
