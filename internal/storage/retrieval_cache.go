package storage

import (
	"context"
	"errors"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"
)

type cachedRetrievalSnapshot struct {
	snapshot  retrieval.Snapshot
	checkedAt time.Time
}

type retrievalSnapshotLoad struct {
	done     chan struct{}
	snapshot retrieval.Snapshot
	err      error
	canceled bool
	epoch    uint64
}

func (s *TenantStore) loadRetrievalSnapshot(
	ctx context.Context,
	tenantID string,
	minVersion int64,
) (retrieval.Snapshot, error) {
	for {
		s.lockMu.Lock()
		cached, cachedOK := s.retrievalSnapshotCache[tenantID]
		if cachedOK &&
			time.Since(cached.checkedAt) < s.lifecycleCacheTTL() &&
			(minVersion <= 0 ||
				cached.snapshot.GraphVersion >= minVersion) {
			s.lockMu.Unlock()
			return cached.snapshot, nil
		}
		if load := s.retrievalSnapshotLoads[tenantID]; load != nil {
			s.lockMu.Unlock()
			select {
			case <-load.done:
				if load.canceled && ctx.Err() == nil {
					continue
				}
				return load.snapshot, load.err
			case <-ctx.Done():
				return retrieval.Snapshot{}, ctx.Err()
			}
		}
		load := &retrievalSnapshotLoad{
			done:  make(chan struct{}),
			epoch: s.retrievalSnapshotEpoch[tenantID],
		}
		s.retrievalSnapshotLoads[tenantID] = load
		s.lockMu.Unlock()

		snapshot, err := s.resolveRetrievalSnapshotUncached(ctx, tenantID)
		s.finishRetrievalSnapshotLoad(
			tenantID,
			load,
			snapshot,
			err,
			loadCanceledByContext(ctx, err),
			cached,
			cachedOK,
		)
		if load.canceled && ctx.Err() == nil {
			continue
		}
		return load.snapshot, load.err
	}
}

func (s *TenantStore) finishRetrievalSnapshotLoad(
	tenantID string,
	load *retrievalSnapshotLoad,
	snapshot retrieval.Snapshot,
	err error,
	canceled bool,
	previous cachedRetrievalSnapshot,
	previousOK bool,
) {
	s.lockMu.Lock()
	if s.retrievalSnapshotEpoch[tenantID] != load.epoch {
		load.canceled = true
		load.snapshot = retrieval.Snapshot{}
		load.err = nil
		delete(s.retrievalSnapshotLoads, tenantID)
		close(load.done)
		s.lockMu.Unlock()
		return
	}
	current, currentOK := s.retrievalSnapshotCache[tenantID]
	if err == nil &&
		currentOK &&
		retrievalSnapshotNewer(current.snapshot, snapshot) {
		snapshot = current.snapshot
		err = nil
	} else if err == nil {
		s.retrievalSnapshotCache[tenantID] = cachedRetrievalSnapshot{
			snapshot:  snapshot,
			checkedAt: time.Now(),
		}
	} else if previousOK &&
		retrievalSnapshotCacheFallbackAllowed(err) {
		snapshot = previous.snapshot
		err = nil
	} else {
		delete(s.retrievalSnapshotCache, tenantID)
	}
	load.snapshot = snapshot
	load.err = err
	load.canceled = canceled
	delete(s.retrievalSnapshotLoads, tenantID)
	close(load.done)
	s.lockMu.Unlock()
}

func retrievalSnapshotNewer(
	left retrieval.Snapshot,
	right retrieval.Snapshot,
) bool {
	if left.TenantGeneration != right.TenantGeneration {
		return left.TenantGeneration > right.TenantGeneration
	}
	return left.Revision > right.Revision
}

func retrievalSnapshotCacheFallbackAllowed(err error) bool {
	return errors.Is(err, ErrCoordinatorUnavailable) ||
		errors.Is(err, ErrObjectStoreUnavailable) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (s *TenantStore) deleteCachedRetrievalSnapshot(tenantID string) {
	s.lockMu.Lock()
	delete(s.retrievalSnapshotCache, tenantID)
	s.retrievalSnapshotEpoch[tenantID]++
	s.lockMu.Unlock()
}

func (s *TenantStore) deleteAllCachedRetrievalSnapshots() {
	s.lockMu.Lock()
	s.retrievalSnapshotCache = map[string]cachedRetrievalSnapshot{}
	for tenantID := range s.retrievalSnapshotLoads {
		s.retrievalSnapshotEpoch[tenantID]++
	}
	s.lockMu.Unlock()
}
