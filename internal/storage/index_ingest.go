package storage

import (
	"context"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

const (
	maxPendingIndexChanges    = 8192
	maxBackgroundIndexUpdates = 8
)

// Retain bounded incremental batches in publication order. Coalesce only the
// last queued batch; a full delta starts another batch rather than a rebuild.
// Read views keep every queued version's inputs alive through publication.
func (s *TenantStore) enqueueLocalIngestIndexUpdate(ctx context.Context, tenantID string, work *commitIndexUpdate) bool {
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
	s.indexUpdateMu.Lock()
	// Do not reserve memory behind a synchronous predecessor: that predecessor
	// may need the same memory before it can complete and release this work.
	if s.indexUnreservedTails[tenantID] != nil {
		s.indexUpdateMu.Unlock()
		release()
		return false
	}
	if pending := s.pendingIngestIndexes[tenantID]; pending != nil &&
		s.indexUpdateTails[tenantID] == pending.done && pending.version == work.baseVersion && pending.fence == work.fence &&
		len(pending.report.AffectedEntityIDs)+len(work.report.AffectedEntityIDs)+
			len(pending.report.AffectedEdgeIDs)+len(work.report.AffectedEdgeIDs) <= maxPendingIndexChanges &&
		pending.reservation.resize(pendingIndexBytes(pending.before, work.after)) {
		mergePendingIndexUpdate(pending, work)
		s.indexUpdateMu.Unlock()
		release()
		return true
	}
	// Do not wait for capacity while holding the tenant lock: a queued rebuild
	// can need that lock. The caller falls back to finishing after unlocking.
	if s.activeIngestIndexUpdates >= maxBackgroundIndexUpdates {
		s.indexUpdateMu.Unlock()
		release()
		return false
	}
	work.reservation = s.reserveIndexMemory(pendingIndexBytes(work.before, work.after))
	if work.reservation == nil {
		s.indexUpdateMu.Unlock()
		release()
		return false
	}
	s.activeIngestIndexUpdates++
	work.done = make(chan struct{})
	work.waitFor = s.indexUpdateTails[tenantID]
	s.indexUpdateTails[tenantID] = work.done
	if s.pendingIngestIndexes == nil {
		s.pendingIngestIndexes = make(map[string]*commitIndexUpdate)
	}
	s.pendingIngestIndexes[tenantID] = work
	s.indexUpdateMu.Unlock()
	launched = true
	go func() {
		defer finish()
		defer work.reservation.release()
		defer release()
		defer func() {
			s.indexUpdateMu.Lock()
			s.activeIngestIndexUpdates--
			s.indexUpdateMu.Unlock()
		}()
		s.finishCommitIndexUpdate(ctx, tenantID, work, nil)
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
