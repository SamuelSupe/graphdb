package sharding

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	defaultGraphReads  = 32
	defaultGraphWrites = 8
	// Keep a process-wide backstop while giving independent home groups room
	// to proceed when one group uses all its slots.
	graphProcessLimitFactor = 4
)

type GraphAdmissionLimits struct {
	Reads  int
	Writes int
	Wait   time.Duration
}

func DefaultGraphAdmissionLimits() GraphAdmissionLimits {
	return GraphAdmissionLimits{Reads: defaultGraphReads, Writes: defaultGraphWrites, Wait: 25 * time.Millisecond}
}

func (l GraphAdmissionLimits) Validate() error {
	if l.Reads < 1 || l.Reads > 1024 {
		return fmt.Errorf("GRAPHDB_ROUTER_GRAPH_READS_PER_SHARD must be between 1 and 1024")
	}
	if l.Writes < 1 || l.Writes > 256 {
		return fmt.Errorf("GRAPHDB_ROUTER_GRAPH_WRITES_PER_SHARD must be between 1 and 256")
	}
	if l.Wait < 0 || l.Wait > time.Second {
		return fmt.Errorf("GRAPHDB_ROUTER_GRAPH_QUEUE_WAIT must be between 0s and 1s")
	}
	return nil
}

type graphScope struct {
	group, tenant string
}

type graphAdmission struct {
	mu     sync.Mutex
	limits GraphAdmissionLimits
	read   graphAdmissionLane
	write  graphAdmissionLane
}

type graphAdmissionLane struct {
	active  int
	groups  map[string]int
	tenants map[graphScope]int
	waiting map[string]int
	queue   list.List
}

type graphWaiter struct {
	scope graphScope
	ctx   context.Context
	ready chan struct{}
	entry *list.Element
}

func (a *graphAdmission) acquire(ctx context.Context, scope graphScope, write bool) func() {
	lane := &a.read
	limit, tenantLimit := a.limits.Reads, 8
	if limit == 0 {
		limit = defaultGraphReads
	}
	if write {
		lane = &a.write
		limit, tenantLimit = a.limits.Writes, 1
		if limit == 0 {
			limit = defaultGraphWrites
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	release := func() {
		a.mu.Lock()
		lane.release(scope)
		lane.grant(limit, tenantLimit)
		a.mu.Unlock()
	}
	a.mu.Lock()
	if lane.available(scope, limit, tenantLimit) {
		lane.take(scope)
		a.mu.Unlock()
		return release
	}
	if a.limits.Wait <= 0 || lane.waiting[scope.group] >= limit || lane.queue.Len() >= limit*graphProcessLimitFactor {
		a.mu.Unlock()
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, a.limits.Wait)
	defer cancel()
	waiter := &graphWaiter{scope: scope, ctx: waitCtx, ready: make(chan struct{})}
	waiter.entry = lane.queue.PushBack(waiter)
	if lane.waiting == nil {
		lane.waiting = make(map[string]int)
	}
	lane.waiting[scope.group]++
	a.mu.Unlock()

	select {
	case <-waiter.ready:
	case <-waitCtx.Done():
	}
	a.mu.Lock()
	if waiter.entry != nil {
		lane.remove(waiter)
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()
	if waitCtx.Err() != nil {
		release()
		return nil
	}
	return release
}

func (l *graphAdmissionLane) available(scope graphScope, limit, tenantLimit int) bool {
	return l.active < limit*graphProcessLimitFactor && l.groups[scope.group] < limit && l.tenants[scope] < tenantLimit
}

func (l *graphAdmissionLane) take(scope graphScope) {
	if l.groups == nil {
		l.groups = make(map[string]int)
		l.tenants = make(map[graphScope]int)
	}
	l.active++
	l.groups[scope.group]++
	l.tenants[scope]++
}

func (l *graphAdmissionLane) release(scope graphScope) {
	l.active--
	decrementGraphCount(l.groups, scope.group)
	decrementGraphCount(l.tenants, scope)
}

func (l *graphAdmissionLane) grant(limit, tenantLimit int) {
	for entry := l.queue.Front(); entry != nil; {
		waiter := entry.Value.(*graphWaiter)
		entry = entry.Next()
		if waiter.ctx.Err() == nil && l.available(waiter.scope, limit, tenantLimit) {
			l.take(waiter.scope)
			l.remove(waiter)
			close(waiter.ready)
		}
	}
}

func (l *graphAdmissionLane) remove(waiter *graphWaiter) {
	l.queue.Remove(waiter.entry)
	waiter.entry = nil
	decrementGraphCount(l.waiting, waiter.scope.group)
}

func decrementGraphCount[K comparable](counts map[K]int, key K) {
	if counts[key] == 1 {
		delete(counts, key)
	} else {
		counts[key]--
	}
}
