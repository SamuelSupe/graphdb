package storage

import (
	"container/heap"
	"time"
)

type ingestTenantFlush struct {
	tenantID string
	items    []*ingestPending
}

type ingestWorkerCompletion struct {
	tenantID string
	retry    []*ingestPending
}

type ingestForceRequest struct {
	tenantID   string
	throughLSN uint64
}

type ingestTenantQueue struct {
	tenantID string
	items    []*ingestPending
	bytes    int64
	deadline time.Time
	index    int
}

type ingestDeadlineHeap []*ingestTenantQueue

func (h ingestDeadlineHeap) Len() int { return len(h) }

func (h ingestDeadlineHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }

func (h ingestDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *ingestDeadlineHeap) Push(value any) {
	queue := value.(*ingestTenantQueue)
	queue.index = len(*h)
	*h = append(*h, queue)
}

func (h *ingestDeadlineHeap) Pop() any {
	old := *h
	last := len(old) - 1
	queue := old[last]
	queue.index = -1
	*h = old[:last]
	return queue
}

func (s *IngestService) schedule() {
	defer close(s.schedulerOK)
	defer close(s.readyCh)
	queues := map[string]*ingestTenantQueue{}
	busy := map[string]bool{}
	forceThrough := map[string]uint64{}
	forceRetry := map[string]bool{}
	awaitingAdmission := map[uint64]*ingestPending{}
	nextAcceptedSequence := uint64(1)
	deadlines := ingestDeadlineHeap{}
	heap.Init(&deadlines)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	draining := false
	shutdownCh := s.shutdownCh
	enqueue := func(pending *ingestPending) {
		tenantID := pending.envelope.TenantID
		queue := queues[tenantID]
		if queue == nil {
			queue = &ingestTenantQueue{
				tenantID: tenantID,
				deadline: pending.envelope.AcceptedAt.Add(s.config.FlushInterval),
				index:    -1,
			}
			if draining || pending.envelope.Request.FullSync {
				queue.deadline = time.Now()
			}
			queues[tenantID] = queue
			heap.Push(&deadlines, queue)
		}
		queue.items = append(queue.items, pending)
		queue.bytes += pending.bytes
		if len(queue.items) >= s.config.FlushMaxRequests ||
			queue.bytes >= s.config.FlushMaxBytes ||
			pending.envelope.Request.FullSync ||
			pending.acceptedLSN <= forceThrough[tenantID] {
			queue.deadline = time.Now()
			heap.Fix(&deadlines, queue.index)
		}
	}
	admit := func(pending *ingestPending) {
		if pending.acceptedSequence == 0 {
			// Recovered records were already sorted by their persisted LSN.
			enqueue(pending)
			return
		}
		// WAL append responses can resume their callers in a different order.
		// This process-local sequence excludes prepared and terminal records.
		awaitingAdmission[pending.acceptedSequence] = pending
		for awaitingAdmission[nextAcceptedSequence] != nil {
			next := awaitingAdmission[nextAcceptedSequence]
			delete(awaitingAdmission, nextAcceptedSequence)
			nextAcceptedSequence++
			enqueue(next)
		}
	}

	resetTimer := func() <-chan time.Time {
		if deadlines.Len() == 0 {
			return nil
		}
		delay := time.Until(deadlines[0].deadline)
		if delay < 0 {
			delay = 0
		}
		timer.Reset(delay)
		return timer.C
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	nextQueue := func(now time.Time) *ingestTenantQueue {
		for deadlines.Len() > 0 {
			queue := deadlines[0]
			if queue.deadline.After(now) {
				return nil
			}
			if !busy[queue.tenantID] {
				return queue
			}
			heap.Pop(&deadlines)
			queue.deadline = now.Add(10 * time.Millisecond)
			heap.Push(&deadlines, queue)
		}
		return nil
	}

	for {
		// Collect already accepted work before dispatching an expired queue.
		// A bounded drain keeps completion, force and shutdown handling live.
		for range min(len(s.enqueueCh), max(1, s.config.FlushMaxRequests)) {
			select {
			case pending := <-s.enqueueCh:
				admit(pending)
			case <-s.runCtx.Done():
				return
			}
		}
		queue := nextQueue(time.Now())
		var readyCh chan ingestTenantFlush
		var flush ingestTenantFlush
		if queue != nil {
			readyCh = s.readyCh
			flush = ingestTenantFlush{tenantID: queue.tenantID, items: queue.items}
		}
		if draining && len(queues) == 0 && len(busy) == 0 && len(s.enqueueCh) == 0 && len(awaitingAdmission) == 0 {
			return
		}
		stopTimer()
		var timerCh <-chan time.Time
		if queue == nil {
			timerCh = resetTimer()
		}
		select {
		case readyCh <- flush:
			// Keep completion handling live when the worker queue is full.
			heap.Pop(&deadlines)
			delete(queues, queue.tenantID)
			busy[queue.tenantID] = true
		case pending := <-s.enqueueCh:
			admit(pending)
		case force := <-s.forceCh:
			forceThrough[force.tenantID] = max(forceThrough[force.tenantID], force.throughLSN)
			if busy[force.tenantID] {
				forceRetry[force.tenantID] = true
			}
			if queue := queues[force.tenantID]; queue != nil {
				queue.deadline = time.Now()
				heap.Fix(&deadlines, queue.index)
			}
		case completion := <-s.completeCh:
			delete(busy, completion.tenantID)
			forced := forceRetry[completion.tenantID]
			delete(forceRetry, completion.tenantID)
			if len(completion.retry) > 0 {
				retryDeadline := time.Now().Add(s.ingestRetryDelay(completion.retry[0]))
				queue := queues[completion.tenantID]
				if queue == nil {
					queue = &ingestTenantQueue{
						tenantID: completion.tenantID,
						deadline: retryDeadline,
						index:    -1,
					}
					queues[completion.tenantID] = queue
					heap.Push(&deadlines, queue)
				} else if queue.deadline.Before(retryDeadline) {
					queue.deadline = retryDeadline
					heap.Fix(&deadlines, queue.index)
				}
				queue.items = append(append([]*ingestPending(nil), completion.retry...), queue.items...)
				for _, pending := range completion.retry {
					queue.bytes += pending.bytes
				}
				// A force received during this flush must survive a failed attempt,
				// but only bypass backoff once so persistent faults cannot spin.
				if forced {
					queue.deadline = time.Now()
					heap.Fix(&deadlines, queue.index)
				}
			}
		case <-timerCh:
		case <-shutdownCh:
			draining = true
			shutdownCh = nil
			for _, queue := range queues {
				queue.deadline = time.Now()
			}
			heap.Init(&deadlines)
		case <-s.runCtx.Done():
			return
		}
	}
}

func (s *IngestService) runWorker() {
	defer s.workers.Done()
	for flush := range s.readyCh {
		retry := s.flushTenant(flush.items)
		select {
		case s.completeCh <- ingestWorkerCompletion{tenantID: flush.tenantID, retry: retry}:
		case <-s.runCtx.Done():
			return
		}
	}
}
