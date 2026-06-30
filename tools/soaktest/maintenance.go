package main

import (
	"context"
	"os/exec"
	"sync"
	"time"
)

func (r *soakRunner) startMaintenance(ctx context.Context, wg *sync.WaitGroup) {
	if r.cfg.compactInterval > 0 {
		wg.Add(1)
		go r.periodic(ctx, wg, "compact", r.cfg.compactInterval, func(ctx context.Context, metrics *registry) error {
			return r.writer.compactWithTimeout(ctx, metrics, r.cfg.maintenanceTimeout)
		})
	}
	if r.cfg.gcInterval > 0 {
		wg.Add(1)
		go r.periodic(ctx, wg, "gc", r.cfg.gcInterval, func(ctx context.Context, metrics *registry) error {
			return r.writer.gcWithTimeout(ctx, metrics, r.cfg.maintenanceTimeout)
		})
	}
	if r.cfg.indexRebuildInterval > 0 {
		wg.Add(1)
		go r.rebuildLoop(ctx, wg)
	}
	if r.cfg.readerRestartEvery > 0 && r.cfg.readerRestartCommand != "" {
		wg.Add(1)
		go r.readerRestartLoop(ctx, wg)
	}
}

func (r *soakRunner) periodic(ctx context.Context, wg *sync.WaitGroup, name string, interval time.Duration, fn func(context.Context, *registry) error) {
	defer wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.maintenanceMu.Lock()
			if err := fn(ctx, r.metrics); err != nil {
				r.maintenanceMu.Unlock()
				r.events.emit(name+"_error", map[string]any{"error": err.Error()})
			} else {
				r.maintenanceMu.Unlock()
				r.events.emit(name+"_ok", nil)
			}
		}
	}
}

func (r *soakRunner) rebuildLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(r.cfg.indexRebuildInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.maintenanceMu.Lock()
			err := r.writer.rebuildIndexesWithTimeout(ctx, r.metrics, r.cfg.maintenanceTimeout)
			r.maintenanceMu.Unlock()
			if err != nil {
				r.events.emit("index_rebuild_error", map[string]any{"error": err.Error()})
			} else {
				r.events.emit("index_rebuild_ok", nil)
			}
		}
	}
}

func (r *soakRunner) readerRestartLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(r.cfg.readerRestartEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.pauseReaderChecks()
			r.readerQueryMu.Lock()
			start := time.Now()
			cmd := exec.CommandContext(ctx, "sh", "-c", r.cfg.readerRestartCommand)
			err := cmd.Run()
			r.metrics.add("reader-restart", time.Since(start), 0, err)
			if err != nil {
				r.events.emit("reader_restart_error", map[string]any{"error": err.Error()})
			} else {
				r.events.emit("reader_restart_ok", nil)
			}
			r.pauseReaderChecks()
			r.readerQueryMu.Unlock()
		}
	}
}

func (r *soakRunner) pauseReaderChecks() {
	if r.cfg.readerRestartGrace <= 0 {
		return
	}
	r.readerPauseUntil.Store(time.Now().Add(r.cfg.readerRestartGrace).UnixNano())
}

func (r *soakRunner) readerPaused() bool {
	until := r.readerPauseUntil.Load()
	return until > 0 && time.Now().Before(time.Unix(0, until))
}
