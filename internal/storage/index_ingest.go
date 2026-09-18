package storage

import (
	"context"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const (
	maxPendingIndexChanges    = 8192
	maxBackgroundIndexUpdates = 8
)

// Ingest publication does not depend on index availability. Keep one active and
// one coalesced pending update between direct commits, sharing their ordering
// chain. A retained view protects files and makes FileStore.Close wait for the
// worker; later batches cannot accumulate one graph copy per accepted write.
func (s *TenantStore) enqueueLocalIngestIndexUpdate(ctx context.Context, tenantID string, work *commitIndexUpdate) bool {
	ctx, release, err := s.ReadViewContext(context.WithoutCancel(ctx), tenantID)
	if err != nil {
		return false
	}
	bound, ok := s.writerFenceFromContext(ctx, tenantID)
	if !ok {
		release()
		return false
	}
	work.fence = bound.fence
	work.background = true
	if len(work.report.AffectedEntityIDs)+len(work.report.AffectedEdgeIDs) > maxPendingIndexChanges {
		rebuildPendingIndexUpdate(work)
	}
	s.indexUpdateMu.Lock()
	if pending := s.pendingIngestIndexes[tenantID]; pending != nil &&
		s.indexUpdateTails[tenantID] == pending.done && pending.version == work.baseVersion && pending.fence == work.fence {
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
	s.activeIngestIndexUpdates++
	work.done = make(chan struct{})
	work.waitFor = s.indexUpdateTails[tenantID]
	s.indexUpdateTails[tenantID] = work.done
	if s.pendingIngestIndexes == nil {
		s.pendingIngestIndexes = make(map[string]*commitIndexUpdate)
	}
	s.pendingIngestIndexes[tenantID] = work
	s.indexUpdateMu.Unlock()
	go func() {
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
	if pending.rebuild || next.rebuild || !canIncrementIndexes(pending.mutations) || !canIncrementIndexes(next.mutations) ||
		len(pending.report.AffectedEntityIDs)+len(next.report.AffectedEntityIDs)+
			len(pending.report.AffectedEdgeIDs)+len(next.report.AffectedEdgeIDs) > maxPendingIndexChanges {
		// Force the existing gap-rebuild path once the bounded delta is full.
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
