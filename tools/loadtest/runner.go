package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type versionTracker struct {
	value atomic.Int64
}

type runStats struct {
	scheduledBatches     atomic.Int64
	committedBatches     atomic.Int64
	backpressuredBatches atomic.Int64
	committedMutations   atomic.Int64
}

func (t *versionTracker) observe(version int64) {
	for {
		current := t.value.Load()
		if version <= current {
			return
		}
		if t.value.CompareAndSwap(current, version) {
			return
		}
	}
}

func (t *versionTracker) latest() int64 {
	return t.value.Load()
}

func runWriters(ctx context.Context, cfg config, client *apiClient, versions *versionTracker, metrics *registry, stats *runStats, wg *sync.WaitGroup) {
	jobs := make(chan int)
	for worker := 0; worker < cfg.writers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request := make(encodedJSON, 0, batchRequestJSONCapacity(cfg.batchSize))
			for batch := range jobs {
				collector := collectorName(batch % cfg.collectors)
				request = appendBatchRequestJSON(request[:0], batch, cfg.batchSize, collector, cfg.workingSet)
				for {
					outcome, err := client.ingest(ctx, metrics, request)
					if err != nil {
						break
					}
					if outcome.backpressured {
						stats.backpressuredBatches.Add(1)
						timer := time.NewTimer(outcome.retryAfter)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
						continue
					}
					versions.observe(outcome.version)
					stats.committedBatches.Add(1)
					stats.committedMutations.Add(outcome.applied)
					break
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		if cfg.duration > 0 {
			timer := time.NewTimer(cfg.duration)
			defer timer.Stop()
			for batch := 0; ; batch++ {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					return
				case jobs <- batch:
					stats.scheduledBatches.Add(1)
				}
			}
		}
		for batch := 0; batch < cfg.batches; batch++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- batch:
				stats.scheduledBatches.Add(1)
			}
		}
	}()
}

func (s *runStats) snapshot(elapsed time.Duration) loadResults {
	result := loadResults{
		ScheduledBatches:     s.scheduledBatches.Load(),
		CommittedBatches:     s.committedBatches.Load(),
		BackpressuredBatches: s.backpressuredBatches.Load(),
		CommittedMutations:   s.committedMutations.Load(),
	}
	if elapsed > 0 {
		result.MutationsPerSecond = float64(result.CommittedMutations) / elapsed.Seconds()
	}
	return result
}

func runReaders(ctx context.Context, cfg config, client *apiClient, versions *versionTracker, metrics *registry, wg *sync.WaitGroup) {
	for reader := 0; reader < cfg.readers; reader++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for batch := reader; batch < cfg.batches; batch += max(cfg.readers, 1) {
				if ctx.Err() != nil {
					return
				}
				region := regionName(batch % 8)
				minVersion := versions.latest()
				_ = client.query(ctx, metrics, "query-match", matchQuery(region))
				_ = client.query(ctx, metrics, "query-traverse", traverseQuery(batch))
				_ = client.queryStream(ctx, metrics, matchQuery(region))
				if minVersion > 0 {
					strongMatch := matchQuery(region)
					strongMatch.MinVersion = minVersion
					_ = client.queryMinVersion(ctx, metrics, "query-min-version", strongMatch)
					strongStream := matchQuery(region)
					strongStream.MinVersion = minVersion
					_ = client.queryStreamMinVersion(ctx, metrics, strongStream)
					_ = client.entityMinVersion(ctx, metrics, "host:seed", minVersion)
				}
				staleMatch := matchQuery(region)
				staleMatch.AllowStale = true
				_ = client.query(ctx, metrics, "query-allow-stale", staleMatch)
				_ = client.listEntitiesAllowStale(ctx, metrics)
			}
		}(reader)
	}
}
