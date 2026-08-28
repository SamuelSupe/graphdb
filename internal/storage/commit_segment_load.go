package storage

import (
	"context"
	"sync"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const commitSegmentLoadConcurrency = 4

type commitSegmentLoadResult struct {
	index int
	items []commitSegmentItem
	err   error
}

func (s *TenantStore) applyCommitSegments(
	ctx context.Context,
	tenantID string,
	refs []CommitSegmentRef,
	g *graph.Graph,
) (int, error) {
	if len(refs) == 0 {
		return 0, nil
	}

	loadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := min(commitSegmentLoadConcurrency, len(refs))
	jobs := make(chan int, workers)
	results := make(chan commitSegmentLoadResult, workers)
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
					items, err := s.loadCommitSegment(loadCtx, tenantID, refs[index])
					select {
					case results <- commitSegmentLoadResult{index: index, items: items, err: err}:
					case <-loadCtx.Done():
						return
					}
				}
			}
		}()
	}

	dispatched := min(workers, len(refs))
	for index := 0; index < dispatched; index++ {
		jobs <- index
	}
	pending := make(map[int][]commitSegmentItem, workers)
	next := 0
	total := 0
	for next < len(refs) {
		var result commitSegmentLoadResult
		select {
		case <-ctx.Done():
			cancel()
			close(jobs)
			group.Wait()
			return total, ctx.Err()
		case result = <-results:
		}
		if result.err != nil {
			cancel()
			close(jobs)
			group.Wait()
			return total, result.err
		}
		pending[result.index] = result.items
		advanced := 0
		for {
			items, ok := pending[next]
			if !ok {
				break
			}
			for _, item := range items {
				if err := applyManifestCommit(g, item.Key, item.Commit); err != nil {
					cancel()
					close(jobs)
					group.Wait()
					return total, err
				}
			}
			total += len(items)
			delete(pending, next)
			next++
			advanced++
		}
		for advanced > 0 && dispatched < len(refs) {
			jobs <- dispatched
			dispatched++
			advanced--
		}
	}
	close(jobs)
	group.Wait()
	return total, nil
}
