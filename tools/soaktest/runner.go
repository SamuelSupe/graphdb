package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type soakRunner struct {
	cfg                   config
	writer                *apiClient
	reader                *apiClient
	admin                 *apiClient
	metrics               *registry
	events                *eventWriter
	maintenanceMu         sync.Mutex
	readerQueryMu         sync.RWMutex
	readerReady           atomic.Bool
	nextBatch             atomic.Int64
	queryRounds           atomic.Int64
	lastWrittenN          atomic.Int64
	latestVersion         atomic.Int64
	readerPauseUntil      atomic.Int64
	snapshotExportRunning atomic.Bool
	nextSnapshotExportAt  atomic.Int64
}

func newSoakRunner(cfg config, writer *apiClient, reader *apiClient, admin *apiClient, metrics *registry, events *eventWriter, seedVersion int64) *soakRunner {
	r := &soakRunner{cfg: cfg, writer: writer, reader: reader, admin: admin, metrics: metrics, events: events}
	r.nextBatch.Store(cfg.startBatch)
	r.lastWrittenN.Store(maxInt64(cfg.startBatch*int64(cfg.batchSize)-1, 0))
	r.observeVersion(seedVersion)
	if cfg.queryStartDelay > 0 {
		r.readerPauseUntil.Store(time.Now().Add(cfg.queryStartDelay).UnixNano())
	}
	return r
}

func (r *soakRunner) run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.writers; i++ {
		wg.Add(1)
		go r.writeLoop(ctx, &wg, i)
	}
	for i := 0; i < r.cfg.readers; i++ {
		wg.Add(1)
		go r.queryLoop(ctx, &wg, i)
	}
	r.startReaderWarmup(ctx, &wg)
	wg.Add(1)
	go r.sampleLoop(ctx, &wg)
	r.startMaintenance(ctx, &wg)
	wg.Wait()
}

func (r *soakRunner) writeLoop(ctx context.Context, wg *sync.WaitGroup, worker int) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		batch := r.nextBatch.Add(1) - 1
		version, status, err := r.writer.ingest(ctx, r.metrics, batchRequest(batch, r.cfg.batchSize))
		if err != nil {
			if ctx.Err() == nil {
				r.events.emit("write_error", map[string]any{"worker": worker, "batch": batch, "error": err.Error()})
			}
		} else if status == 200 {
			r.lastWrittenN.Store(maxInt64(batch*int64(r.cfg.batchSize)+int64(r.cfg.batchSize)-1, r.lastWrittenN.Load()))
			r.observeVersion(version)
		}
		sleepOrDone(ctx, r.cfg.writeInterval)
	}
}

func (r *soakRunner) queryLoop(ctx context.Context, wg *sync.WaitGroup, worker int) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		round := r.queryRounds.Add(1)
		if r.readerPaused() {
			sleepOrDone(ctx, r.cfg.queryInterval)
			continue
		}
		if !r.readerReady.Load() {
			sleepOrDone(ctx, r.cfg.queryInterval)
			continue
		}
		cases := queryCases(r.lastWrittenN.Load(), round, r.latestVersion.Load())
		item := cases[int(round)%len(cases)]
		r.readerQueryMu.RLock()
		if r.readerPaused() {
			r.readerQueryMu.RUnlock()
			sleepOrDone(ctx, r.cfg.queryInterval)
			continue
		}
		if err := r.runQueryCase(ctx, item); err != nil {
			if ctx.Err() == nil {
				r.events.emit("query_error", map[string]any{"worker": worker, "case": item.name, "error": err.Error()})
			}
		}
		r.readerQueryMu.RUnlock()
		sleepOrDone(ctx, r.cfg.queryInterval)
	}
}

func (r *soakRunner) runQueryCase(ctx context.Context, item queryCase) error {
	timeout := item.timeout
	if timeout <= 0 {
		timeout = r.cfg.httpTimeout
	}
	switch {
	case item.stream:
		if item.minVersion > 0 {
			item.request.MinVersion = item.minVersion
			return r.reader.queryStreamMinVersionWithTimeout(ctx, r.metrics, item.name, timeout, item.request)
		}
		return r.reader.queryStreamNamedWithTimeout(ctx, r.metrics, item.name, timeout, item.request)
	case item.saved != "":
		return r.reader.runSavedQueryNamedWithTimeout(ctx, r.metrics, item.name, timeout, item.saved)
	case item.scan == "entities-min-version":
		return r.reader.scanEntitiesMinVersionWithTimeout(ctx, r.metrics, item.name, timeout, item.minVersion)
	case item.scan == "entities-allow-stale":
		return r.reader.scanEntitiesAllowStaleWithTimeout(ctx, r.metrics, item.name, timeout)
	case item.scan == "entities":
		return r.reader.scanEntitiesNamedWithTimeout(ctx, r.metrics, item.name, timeout)
	case item.scan == "edges-allow-stale":
		return r.reader.scanEdgesAllowStaleWithTimeout(ctx, r.metrics, item.name, timeout)
	case item.scan == "edges":
		return r.reader.scanEdgesNamedWithTimeout(ctx, r.metrics, item.name, timeout)
	case item.scan == "snapshot-export":
		return r.runSnapshotExport(ctx, item, timeout)
	default:
		if item.minVersion > 0 {
			item.request.MinVersion = item.minVersion
			return r.reader.queryMinVersionWithTimeout(ctx, r.metrics, item.name, timeout, item.request)
		}
		if item.allowStale {
			item.request.AllowStale = true
		}
		resp, err := r.reader.queryWithTimeout(ctx, r.metrics, item.name, timeout, item.request)
		if err != nil {
			return err
		}
		if item.expectPlan != "" {
			if got := planStrategy(resp.json); got != item.expectPlan {
				return fmt.Errorf("plan strategy = %q, want %q", got, item.expectPlan)
			}
		}
		return nil
	}
}

func (r *soakRunner) observeVersion(version int64) {
	for {
		current := r.latestVersion.Load()
		if version <= current {
			return
		}
		if r.latestVersion.CompareAndSwap(current, version) {
			return
		}
	}
}

func (r *soakRunner) runSnapshotExport(ctx context.Context, item queryCase, timeout time.Duration) error {
	if !r.tryStartSnapshotExport(time.Now()) {
		return nil
	}
	defer r.snapshotExportRunning.Store(false)
	return r.reader.exportSnapshotStreamWithTimeout(ctx, r.metrics, item.name, timeout)
}

func (r *soakRunner) tryStartSnapshotExport(now time.Time) bool {
	next := r.nextSnapshotExportAt.Load()
	if next > 0 && now.Before(time.Unix(0, next)) {
		return false
	}
	if !r.snapshotExportRunning.CompareAndSwap(false, true) {
		return false
	}
	if r.cfg.snapshotExportInterval > 0 {
		r.nextSnapshotExportAt.Store(now.Add(r.cfg.snapshotExportInterval).UnixNano())
	}
	return true
}

func (r *soakRunner) sampleLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	r.readerReady.Store(sampleState(ctx, r.admin, r.reader, r.metrics, r.events, !r.readerPaused()).readerReady)
	ticker := time.NewTicker(r.cfg.sampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.readerReady.Store(sampleState(ctx, r.admin, r.reader, r.metrics, r.events, !r.readerPaused()).readerReady)
		}
	}
}

func sleepOrDone(ctx context.Context, duration time.Duration) {
	if duration <= 0 {
		return
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func planStrategy(body map[string]any) string {
	plan, _ := body["plan"].(map[string]any)
	strategy, _ := plan["strategy"].(string)
	return strategy
}
