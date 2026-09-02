package storage

import (
	"context"
	"fmt"
	"sync"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const commitTailLoadConcurrency = 4

type commitTailLoadResult struct {
	index  int
	commit graph.Commit
	err    error
}

func (s *TenantStore) applyCommitTail(
	ctx context.Context,
	tenantID string,
	keys []string,
	g *graph.Graph,
) ([]commitSegmentItem, int, error) {
	if len(keys) == 0 {
		return nil, 0, nil
	}
	for _, key := range keys {
		if err := s.validateTenantObjectKey(tenantID, key); err != nil {
			return nil, 0, err
		}
	}

	loadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := min(commitTailLoadConcurrency, len(keys))
	jobs := make(chan int, workers)
	results := make(chan commitTailLoadResult, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for {
				select {
				case <-loadCtx.Done():
					return
				case index, ok := <-jobs:
					if !ok {
						return
					}
					commit, err := s.getCommitObject(loadCtx, keys[index])
					select {
					case results <- commitTailLoadResult{index: index, commit: commit, err: err}:
					case <-loadCtx.Done():
						return
					}
				}
			}
		}()
	}

	dispatched := min(workers, len(keys))
	for index := 0; index < dispatched; index++ {
		jobs <- index
	}
	pending := make(map[int]commitTailLoadResult, workers)
	items := make([]commitSegmentItem, 0, len(keys))
	next := 0
	applied := 0
	for next < len(keys) {
		var result commitTailLoadResult
		select {
		case <-ctx.Done():
			cancel()
			close(jobs)
			group.Wait()
			return items, applied, ctx.Err()
		case result = <-results:
		}
		pending[result.index] = result
		advanced := 0
		for {
			result, ok := pending[next]
			if !ok {
				break
			}
			key := keys[next]
			if result.err != nil {
				cancel()
				close(jobs)
				group.Wait()
				return items, applied, fmt.Errorf("load commit %q: %w", key, result.err)
			}
			if result.commit.TenantID != tenantID {
				cancel()
				close(jobs)
				group.Wait()
				return items, applied, errTenantCommitMismatch(tenantID, key, result.commit.TenantID)
			}
			if err := applyManifestCommit(g, key, result.commit); err != nil {
				cancel()
				close(jobs)
				group.Wait()
				return items, applied, err
			}
			items = append(items, commitSegmentItem{Key: key, Commit: result.commit})
			applied++
			delete(pending, next)
			next++
			advanced++
		}
		for advanced > 0 && dispatched < len(keys) {
			jobs <- dispatched
			dispatched++
			advanced--
		}
	}
	close(jobs)
	group.Wait()
	return items, applied, nil
}
