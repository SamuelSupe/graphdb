package storage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// One process-wide worker, including its I/O cooldown. A slice publishes at
// most one pair. Cooldown and conflict retries retain neither views nor build
// admission; lifecycle repair is never queued behind a sleeping compactor.
var rangeCompactionSlot = make(chan struct{}, 1)

const rangeCompactionBytesPerSecond = int64(16 << 20)

type rangeCompactionIO struct {
	ObjectStore
	bytes atomic.Int64
}

func (s *rangeCompactionIO) UnwrapObjectStore() ObjectStore { return s.ObjectStore }
func (s *rangeCompactionIO) Get(ctx context.Context, key string) ([]byte, error) {
	d, e := s.ObjectStore.Get(ctx, key)
	s.bytes.Add(int64(len(d)))
	return d, e
}
func (s *rangeCompactionIO) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	d, m, e := s.ObjectStore.GetWithMeta(ctx, key)
	s.bytes.Add(int64(len(d)))
	return d, m, e
}
func (s *rangeCompactionIO) Put(ctx context.Context, key string, data []byte) error {
	s.bytes.Add(int64(len(data)))
	return s.ObjectStore.Put(ctx, key, data)
}
func (s *rangeCompactionIO) PutConditional(ctx context.Context, key string, data []byte, c PutCondition) (ObjectMeta, error) {
	s.bytes.Add(int64(len(data)))
	return s.ObjectStore.PutConditional(ctx, key, data, c)
}

func (s *TenantStore) scheduleRangeCompaction(tenant string) {
	// Raft maintenance is invoked by the replicated maintenance command. Never
	// create independent, non-replicated publications on individual replicas.
	if !s.WriteIndexAdjacencyPages || s.ReplicationMode || s.localFileStore() == nil {
		return
	}
	select {
	case rangeCompactionSlot <- struct{}{}:
	default:
		return
	}
	s.taskMu.Lock()
	if s.taskClosing {
		s.taskMu.Unlock()
		<-rangeCompactionSlot
		return
	}
	s.taskWorkers.Add(1)
	s.taskMu.Unlock()
	go func() {
		defer s.taskWorkers.Done()
		defer func() { <-rangeCompactionSlot }()
		ctx, cancel := context.WithTimeout(s.backgroundCtx, 5*time.Minute)
		defer cancel()
		started := time.Now()
		if err := s.compactRangePages(ctx, tenant); err != nil {
			s.recordMaintenance("range_compaction_failed", started)
		}
	}()
}

// Best-effort local sweep. Failed optional merges never change the graph or
// accepted WAL result; a later explicit rebuild or write may schedule a new pass.
func (s *TenantStore) compactRangePages(ctx context.Context, tenant string) error {
	skipped := map[string]bool{}
	conflicts := 0
	for {
		started := time.Now()
		changed, done, bytes, err := s.compactRangeSlice(ctx, tenant, skipped)
		stop := done
		if errors.Is(err, ErrConflict) {
			conflicts++
			if conflicts >= 3 {
				stop = true
			}
		} else if err != nil {
			stop = true
		} else {
			conflicts = 0
		}
		wait := time.Duration(bytes)*time.Second/time.Duration(rangeCompactionBytesPerSecond) - time.Since(started)
		if !changed && err != nil {
			wait = max(wait, 100*time.Millisecond)
		}
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if stop {
			return err
		}
	}
}

func (s *TenantStore) compactRangeSlice(ctx context.Context, tenant string, skipped map[string]bool) (changed, done bool, bytes int64, resultErr error) {
	io := &rangeCompactionIO{ObjectStore: s.Objects}
	worker := NewTenantStore(io, s.Prefix)
	worker.InstanceID = s.InstanceID
	worker.maintenance = s.maintenance
	worker.WriteIndexRangePages = true
	worker.WriteEntityRecords = false
	defer worker.ShutdownTasks(context.Background())
	defer func() { bytes = io.bytes.Load() }()
	ctx, releaseMemory, err := s.admitMaintenance(ctx, 64<<20)
	if err != nil {
		return false, false, 0, err
	}
	defer releaseMemory()
	ctx, releaseView, err := s.ReadViewContext(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	defer releaseView()
	catalog, meta, err := worker.getIndexCatalogWithMeta(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	manifest, err := worker.CurrentManifest(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	if manifest.Version != catalog.Version {
		return false, true, 0, nil
	}
	var entities []EntityPageData
	var edges []EdgeShardData
	var reverse ReverseIndexCatalog
	var reverseMeta ObjectMeta
	reverseChanged := false
	tryPair := func(objects []IndexObject) (int, string) {
		for i := 0; i+1 < len(objects); i++ {
			a, b := objects[i], objects[i+1]
			key := a.Key + "\x00" + b.Key
			if !skipped[key] && hasIndexRanges(objects) && rangePageMergeable(a) && rangePageMergeable(b) && a.RowCount+b.RowCount <= indexPageMergeRows && (rangePageSmall(a) || rangePageSmall(b)) {
				return i, key
			}
		}
		return -1, ""
	}
	for i := range catalog.EntityPages {
		spec := &catalog.EntityPages[i]
		n, key := tryPair(spec.Objects)
		if n < 0 {
			continue
		}
		pair := spec.Objects[n : n+2]
		next, pages, err := worker.mergeEntityRanges(ctx, tenant, catalog.Version, *spec, pair, nil, map[string]bool{pair[0].ContentHash: true, pair[1].ContentHash: true})
		if err != nil {
			return false, false, 0, err
		}
		if len(next) != 1 {
			skipped[key] = true
			return false, false, 0, nil
		}
		spec.Objects = append(append(append([]IndexObject(nil), spec.Objects[:n]...), next...), spec.Objects[n+2:]...)
		entities = pages
		changed = true
		break
	}
	if !changed {
		reverse, reverseMeta, err = worker.getReverseIndexCatalogWithMeta(ctx, tenant)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, false, 0, err
		}
		for _, specs := range [][]EdgeShard{catalog.EdgeShards, reverse.EdgeShards} {
			for i := range specs {
				spec := &specs[i]
				n, key := tryPair(spec.Objects)
				if n < 0 {
					continue
				}
				pair := spec.Objects[n : n+2]
				next, parts, err := worker.mergeEdgeRanges(ctx, tenant, catalog.Version, *spec, pair, nil, map[string]bool{pair[0].ContentHash: true, pair[1].ContentHash: true}, reverseChanged)
				if err != nil {
					return false, false, 0, err
				}
				if len(next) != 1 {
					skipped[key] = true
					return false, false, 0, nil
				}
				spec.Objects = append(append(append([]IndexObject(nil), spec.Objects[:n]...), next...), spec.Objects[n+2:]...)
				edges = parts
				changed = true
				break
			}
			if changed {
				break
			}
			reverseChanged = true
		}
	}
	if !changed {
		for direction, specs := range [][]EdgeShard{catalog.EdgeShards, reverse.EdgeShards} {
			for i := range specs {
				spec := &specs[i]
				scope := fmt.Sprintf("%t/%s/%s", direction == 1, spec.RelationType, spec.Shard)
				root, merged, done, err := worker.compactAdjacencyRoot(ctx, tenant, worker.adjacencyPrefix(tenant, catalog.Version, scope), spec.AdjacencyRoot, skipped)
				if err != nil {
					return false, false, 0, err
				}
				if !done && !merged {
					return false, false, 0, nil
				}
				if merged {
					spec.AdjacencyRoot = root
					changed = true
					reverseChanged = direction == 1
					break
				}
			}
			if changed {
				break
			}
		}
		if !changed {
			return false, true, 0, nil
		}
	}
	// Upload immutable pages before acquiring publication admission. Compare
	// the captured catalog ETag AND manifest/fence before exposing new roots.
	if len(entities) > 0 {
		s.decorateIndexCatalog(&catalog, tenant)
		err = worker.writeParquetEntityPages(ctx, tenant, entities, catalog.Version)
	} else if reverseChanged {
		mini := IndexCatalog{Version: reverse.Version, EdgeShards: reverse.EdgeShards}
		s.decorateReverseEdgeCatalog(&mini, tenant)
		reverse.EdgeShards = mini.EdgeShards
		err = worker.writeChangedReverseEdgeShards(ctx, tenant, edges, reverse.EdgeShards, reverse.Version)
	} else {
		s.decorateIndexCatalog(&catalog, tenant)
		err = worker.writeParquetEdgeShards(ctx, tenant, edges)
	}
	if err != nil {
		return false, false, 0, err
	}
	unlock, err := s.lockTenantMaintenance(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	defer unlock()
	ctx, err = worker.acquireAndBindWriterFence(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	if err = worker.EnsureTenantWritable(ctx, tenant); err != nil {
		return false, false, 0, err
	}
	if err = worker.ensureIndexRebuildCurrent(ctx, tenant, manifest); err != nil {
		return false, false, 0, err
	}
	if reverseChanged {
		if reverse.Version != catalog.Version {
			return false, false, 0, fmt.Errorf("reverse compaction version mismatch")
		}
		err = worker.putReverseIndexCatalogWithMeta(ctx, tenant, reverse, reverseMeta)
		s.lockMu.Lock()
		delete(s.reverseIndexCatalogCache, tenant)
		s.lockMu.Unlock()
	} else {
		_, err = worker.putIndexCatalogWithMetaFast(ctx, tenant, catalog, meta)
		s.deleteCachedIndexCatalog(tenant)
	}
	return err == nil, false, 0, err
}
