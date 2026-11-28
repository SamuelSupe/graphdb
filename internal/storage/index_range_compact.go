package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// One process-wide worker, including its I/O cooldown. A slice publishes at
// most one pair. Cooldown and conflict retries retain neither views nor build
// admission; lifecycle repair is never queued behind a sleeping compactor.
var rangeCompactionSlot = make(chan struct{}, 1)

const rangeCompactionBytesPerSecond = int64(16 << 20)

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
	// Execute range compaction directly on the main TenantStore `s` with a
	// lightweight ExecutionEnv I/O byte meter rather than spawning a throwaway
	// pseudo-TenantStore worker whose empty caches could thrash leases/caches.
	ctx, meter := s.withIOMeter(ctx)
	defer func() { bytes = meter.Load() }()
	// Follow the global lock hierarchy (localViewGate -> admitMaintenance -> tenantLock)
	// so concurrent exclusive lifecycle operations cannot AB-BA deadlock with builds.
	ctx, releaseView, err := s.ReadViewContext(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	defer releaseView()
	ctx, releaseMemory, err := s.admitMaintenance(ctx, 64<<20)
	if err != nil {
		return false, false, 0, err
	}
	defer releaseMemory()
	catalog, meta, err := s.getIndexCatalogWithMeta(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	manifest, err := s.CurrentManifest(ctx, tenant)
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
	mutatedEntityIdx := -1
	mutatedEdgeIdx := -1
	mutatedReverseIdx := -1
	adjacencyOnly := false
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
		next, pages, err := s.mergeEntityRanges(ctx, tenant, catalog.Version, *spec, pair, nil, map[string]bool{pair[0].ContentHash: true, pair[1].ContentHash: true})
		if err != nil {
			return false, false, 0, err
		}
		if len(next) != 1 {
			skipped[key] = true
			return false, false, 0, nil
		}
		for _, p := range pages {
			s.recordMeteredIO(ctx, int64(len(p.encodedParquet))*2+int64(len(p.Entities))*256)
		}
		spec.Objects = append(append(append([]IndexObject(nil), spec.Objects[:n]...), next...), spec.Objects[n+2:]...)
		entities = pages
		mutatedEntityIdx = i
		changed = true
		break
	}
	reverseReady := false
	if !changed {
		reverse, reverseMeta, err = s.getReverseIndexCatalogWithMeta(ctx, tenant)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, false, 0, err
		}
		reverseReady = err == nil && reverse.Version == catalog.Version
		edgeGroups := [][]EdgeShard{catalog.EdgeShards}
		if reverseReady {
			edgeGroups = append(edgeGroups, reverse.EdgeShards)
		}
		for direction, specs := range edgeGroups {
			isReverse := direction == 1
			for i := range specs {
				spec := &specs[i]
				n, key := tryPair(spec.Objects)
				if n < 0 {
					continue
				}
				pair := spec.Objects[n : n+2]
				next, parts, err := s.mergeEdgeRanges(ctx, tenant, catalog.Version, *spec, pair, nil, map[string]bool{pair[0].ContentHash: true, pair[1].ContentHash: true}, isReverse)
				if err != nil {
					return false, false, 0, err
				}
				if len(next) != 1 {
					skipped[key] = true
					return false, false, 0, nil
				}
				for _, p := range parts {
					s.recordMeteredIO(ctx, int64(len(p.encodedParquet))*2+int64(len(p.Edges))*192)
				}
				spec.Objects = append(append(append([]IndexObject(nil), spec.Objects[:n]...), next...), spec.Objects[n+2:]...)
				edges = parts
				changed = true
				reverseChanged = isReverse
				if isReverse {
					mutatedReverseIdx = i
				} else {
					mutatedEdgeIdx = i
				}
				break
			}
			if changed {
				break
			}
		}
	}
	if !changed {
		edgeGroups := [][]EdgeShard{catalog.EdgeShards}
		if reverseReady {
			edgeGroups = append(edgeGroups, reverse.EdgeShards)
		}
		for direction, specs := range edgeGroups {
			isReverse := direction == 1
			for i := range specs {
				spec := &specs[i]
				scope := fmt.Sprintf("%t/%s/%s", isReverse, spec.RelationType, spec.Shard)
				root, merged, done, err := s.compactAdjacencyRoot(ctx, tenant, s.adjacencyPrefix(tenant, catalog.Version, scope), spec.AdjacencyRoot, skipped)
				if err != nil {
					return false, false, 0, err
				}
				if !done && !merged {
					return false, false, 0, nil
				}
				if merged {
					if root != nil {
						s.recordMeteredIO(ctx, int64(root.Bytes)*2)
					}
					spec.AdjacencyRoot = root
					changed = true
					reverseChanged = isReverse
					adjacencyOnly = true
					if isReverse {
						mutatedReverseIdx = i
					} else {
						mutatedEdgeIdx = i
					}
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
	// Upload immutable pages before acquiring publication admission. Only
	// decorate the single mutated spec so existing secondary indexes and
	// reused older-version specs are never double-prefixed or rewritten.
	mutator := s.catalogMutator()
	var entityRecordJobs []entityRecordWriteJob
	if mutatedEntityIdx >= 0 {
		mini := IndexCatalog{Version: catalog.Version, EntityPages: []EntityPageSpec{catalog.EntityPages[mutatedEntityIdx]}}
		s.decorateIndexCatalog(&mini, tenant)
		catalog.EntityPages[mutatedEntityIdx] = mini.EntityPages[0]
		groups := entityPageDataPackGroups(entities, !s.WriteEntityRecords, s.EntityPagePackMaxBytes)
		var metas []ObjectMeta
		metas, err = s.writeParquetEntityPageGroups(ctx, tenant, groups, true)
		if err == nil {
			entityRecordJobs = mutator.BuildEntityRecordJobs(tenant, groups, metas)
		}
	} else if reverseChanged && !adjacencyOnly && mutatedReverseIdx >= 0 {
		mini := IndexCatalog{Version: reverse.Version, EdgeShards: []EdgeShard{reverse.EdgeShards[mutatedReverseIdx]}}
		s.decorateReverseEdgeCatalog(&mini, tenant)
		reverse.EdgeShards[mutatedReverseIdx] = mini.EdgeShards[0]
		err = s.writeChangedReverseEdgeShards(ctx, tenant, edges, []EdgeShard{reverse.EdgeShards[mutatedReverseIdx]}, reverse.Version)
	} else if !reverseChanged && !adjacencyOnly && mutatedEdgeIdx >= 0 {
		mini := IndexCatalog{Version: catalog.Version, EdgeShards: []EdgeShard{catalog.EdgeShards[mutatedEdgeIdx]}}
		s.decorateIndexCatalog(&mini, tenant)
		catalog.EdgeShards[mutatedEdgeIdx] = mini.EdgeShards[0]
		err = s.writeParquetEdgeShards(ctx, tenant, edges)
	} else {
		err = nil
	}
	if err != nil {
		return false, false, 0, err
	}
	unlock, err := s.lockTenantMaintenance(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	defer unlock()
	ctx, err = s.acquireAndBindWriterFence(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	if err = s.EnsureTenantWritable(ctx, tenant); err != nil {
		return false, false, 0, err
	}
	if err = s.ensureIndexRebuildCurrent(ctx, tenant, manifest); err != nil {
		return false, false, 0, err
	}
	currentCatalog, currentMeta, err := s.getIndexCatalogWithMeta(ctx, tenant)
	if err != nil {
		return false, false, 0, err
	}
	if currentCatalog.Version != catalog.Version || currentMeta.ETag != meta.ETag {
		return false, false, 0, ErrConflict
	}
	if reverseChanged {
		currentReverse, currentReverseMeta, revErr := s.getReverseIndexCatalogWithMeta(ctx, tenant)
		if revErr != nil {
			return false, false, 0, revErr
		}
		if currentReverse.Version != reverse.Version || currentReverseMeta.ETag != reverseMeta.ETag {
			return false, false, 0, ErrConflict
		}
	}
	if err = mutator.PublishEntityRecords(ctx, entityRecordJobs); err != nil {
		return false, false, 0, err
	}
	if reverseChanged {
		err = mutator.PublishReverseCatalog(ctx, tenant, reverse, catalog.Version, reverseMeta)
	} else {
		_, err = mutator.PublishForwardCatalog(ctx, tenant, catalog, meta, true)
	}
	return err == nil, false, 0, err
}
