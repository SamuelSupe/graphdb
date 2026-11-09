package storage

import (
	"context"
	"errors"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/admission"
)

const (
	maxPendingIndexChanges     = 8192
	maxBackgroundIndexUpdates  = 8
	maxBackgroundIndexAttempts = 3
)

// Coalesce the last queued batch in publication order. Each worker retains a
// read view through index publication.
func (s *TenantStore) enqueueLocalIngestIndexUpdate(ctx context.Context, tenantID string, work *commitIndexUpdate, deferOversized bool) bool {
	if IsReplicatedContext(ctx) {
		return false
	}
	s.taskMu.Lock()
	if s.taskClosing {
		s.taskMu.Unlock()
		return false
	}
	s.taskWorkers.Add(1)
	s.taskMu.Unlock()
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(s.backgroundCtx, cancel)
	finish := func() { stop(); cancel(); s.taskWorkers.Done() }
	launched := false
	defer func() {
		if !launched {
			finish()
		}
	}()
	ctx, release, err := s.ReadViewContext(runCtx, tenantID)
	if err != nil {
		return false
	}
	bound, ok := s.writerFenceFromContext(ctx, tenantID)
	if !ok {
		release()
		return false
	}
	work.retainIndexInputs()
	work.fence = bound.fence
	work.background = true
	bytes := pendingIndexBytes(work.before, work.after)
	oversized := deferOversized && bytes > s.backgroundIndexByteLimit()
	s.indexUpdateMu.Lock()
	maintenance := s.localIndexMaintenance[tenantID] > 0
	deferred := maintenance || oversized
	if pending := s.pendingIngestIndexes[tenantID]; pending != nil && pending.fence == work.fence && s.indexUpdateTails[tenantID] == pending.done {
		if pending.deferred && (deferred || pending.maintenanceDone != nil) {
			started := time.Now()
			if !s.mergeDeferredIndexUpdate(pending, work) {
				s.indexUpdateMu.Unlock()
				release()
				return true
			}
			// Seal the prefix even if the next job needs a foreground slot.
			delete(s.pendingIngestIndexes, tenantID)
			deferred = true
			defer s.recordMaintenance("index_deferred_overflow", started)
		}
		if !deferred && s.indexUnreservedTails[tenantID] == nil && pending.reservation != nil && pending.version == work.baseVersion &&
			len(pending.report.AffectedEntityIDs)+len(work.report.AffectedEntityIDs)+
				len(pending.report.AffectedEdgeIDs)+len(work.report.AffectedEdgeIDs) <= maxPendingIndexChanges &&
			pending.reservation.resize(pendingIndexBytes(pending.before, work.after)) {
			mergePendingIndexUpdate(pending, work)
			s.indexUpdateMu.Unlock()
			release()
			return true
		}
	}
	// An unreserved predecessor may need the full budget before it can finish.
	if (!deferred && s.indexUnreservedTails[tenantID] != nil) || s.activeIngestIndexUpdates >= maxBackgroundIndexUpdates {
		s.indexUpdateMu.Unlock()
		release()
		return false
	}
	if !deferred {
		// Capacity can become available after the caller releases the tenant lock.
		work.reservation = s.reserveIndexMemory(bytes)
		if work.reservation == nil {
			s.indexUpdateMu.Unlock()
			release()
			return false
		}
	}
	if deferred {
		// Retain only the delta until the predecessor and build budget are ready.
		next := commitIndexUpdate{baseVersion: work.baseVersion, version: work.baseVersion, deferred: true, background: true, fence: work.fence}
		overflow := s.mergeDeferredIndexUpdate(&next, work)
		if overflow && !maintenance {
			// The original update can still apply a single large batch incrementally.
			s.indexUpdateMu.Unlock()
			release()
			return false
		}
		if overflow {
			// Preserve online maintenance progress when even one publication
			// cannot fit the delta budget.
			next.baseVersion = -1
			s.mergeDeferredIndexUpdate(&next, work)
		}
		*work = next
		if completion := s.localMaintenanceWait[tenantID]; completion != nil {
			work.maintenanceDone = completion.done
		}
	}
	s.activeIngestIndexUpdates++
	work.done = make(chan struct{})
	work.waitFor = s.indexUpdateTails[tenantID]
	s.indexUpdateTails[tenantID] = work.done
	if work.deferred {
		s.indexUnreservedTails[tenantID] = work.done
	}
	if s.pendingIngestIndexes == nil {
		s.pendingIngestIndexes = make(map[string]*commitIndexUpdate)
	}
	s.pendingIngestIndexes[tenantID] = work
	s.indexUpdateMu.Unlock()
	launched = true
	go func() {
		defer finish()
		defer func() {
			s.indexUpdateMu.Lock()
			s.activeIngestIndexUpdates--
			s.indexUpdateMu.Unlock()
		}()
		indexErr := func() error {
			defer release()
			if work.reservation != nil {
				defer work.reservation.release()
			}
			return s.runCommitIndexUpdate(ctx, tenantID, work)
		}()
		work = nil
		// Retry recognized transient failures after releasing the old view,
		// reservation and queue position. Capture a fresh head in publication
		// order; reusing old inputs after GC or restore would be unsafe.
		for attempt := 1; attempt < maxBackgroundIndexAttempts && ctx.Err() == nil &&
			(admission.IsObjectStorePressureError(indexErr) || errors.Is(indexErr, ErrConflict)); attempt++ {
			started := time.Now()
			if err := retryDelay(ctx, attempt-1); err != nil {
				return
			}
			s.recordMaintenance("index_retry", started)
			_, indexErr = s.rebuildIndexes(runCtx, tenantID)
		}
	}()
	return true
}

func mergePendingIndexUpdate(pending, next *commitIndexUpdate) {
	pending.after = next.after
	pending.version = next.version
	if pending.rebuild || next.rebuild || !canIncrementIndexes(pending.mutations) || !canIncrementIndexes(next.mutations) {
		// Schema changes require rebuilding the catalog.
		// An absent index catalog still stays absent.
		rebuildPendingIndexUpdate(pending)
		return
	}
	pending.report.AffectedEntityIDs = uniqueStrings(append(pending.report.AffectedEntityIDs, next.report.AffectedEntityIDs...))
	pending.report.AffectedEdgeIDs = uniqueStrings(append(pending.report.AffectedEdgeIDs, next.report.AffectedEdgeIDs...))
}

func rebuildPendingIndexUpdate(work *commitIndexUpdate) {
	work.rebuild = true
	work.before = nil
	work.mutations = graph.Mutations{}
	work.report = graph.ApplyReport{}
}

func (s *TenantStore) backgroundIndexByteLimit() int64 {
	if s.MaxMaintenanceBytes > 0 {
		return s.MaxMaintenanceBytes
	}
	return defaultMaintenanceBytes
}

func pendingIndexBytes(before, after *graph.Graph) int64 {
	return addWriteCacheBytes(maintenanceGraphBytes(before), maintenanceGraphBytes(after))
}

type localMaintenanceCompletion struct {
	active int
	done   chan struct{}
}

// While immutable maintenance objects are prepared, retain coalesced index
// demand rather than letting foreground WAL completion wait for build memory.
func (s *TenantStore) beginLocalIndexMaintenance(ctx context.Context, tenantID string) func() {
	if s.localFileStore() == nil || s.ReplicationMode || IsReplicatedContext(ctx) {
		return func() {}
	}
	s.indexUpdateMu.Lock()
	if s.localIndexMaintenance == nil {
		s.localIndexMaintenance = make(map[string]int)
	}
	s.localIndexMaintenance[tenantID]++
	if s.localMaintenanceWait == nil {
		s.localMaintenanceWait = make(map[string]*localMaintenanceCompletion)
	}
	completion := s.localMaintenanceWait[tenantID]
	if completion == nil {
		completion = &localMaintenanceCompletion{done: make(chan struct{})}
		s.localMaintenanceWait[tenantID] = completion
	}
	completion.active++
	s.indexUpdateMu.Unlock()
	return func() {
		s.indexUpdateMu.Lock()
		completion.active--
		if completion.active == 0 {
			delete(s.localMaintenanceWait, tenantID)
			close(completion.done)
		}
		s.localIndexMaintenance[tenantID]--
		if s.localIndexMaintenance[tenantID] == 0 {
			delete(s.localIndexMaintenance, tenantID)
		}
		s.indexUpdateMu.Unlock()
	}
}
