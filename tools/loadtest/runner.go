package main

import (
	"context"
	"sync"
	"sync/atomic"
)

type versionTracker struct {
	value atomic.Int64
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

func runWriters(ctx context.Context, cfg config, client *apiClient, versions *versionTracker, metrics *registry, wg *sync.WaitGroup) {
	jobs := make(chan int)
	for worker := 0; worker < cfg.writers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				version, err := client.ingest(ctx, metrics, batchRequest(batch, cfg.batchSize))
				if err == nil {
					versions.observe(version)
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for batch := 0; batch < cfg.batches; batch++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- batch:
			}
		}
	}()
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
