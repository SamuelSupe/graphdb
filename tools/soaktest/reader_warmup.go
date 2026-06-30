package main

import (
	"context"
	"net/http"
	"sync"
	"time"
)

func (r *soakRunner) startReaderWarmup(ctx context.Context, wg *sync.WaitGroup) {
	if r.cfg.queryStartDelay <= 0 || r.cfg.readerWarmupInterval <= 0 || r.cfg.readers == 0 {
		return
	}
	wg.Add(1)
	go r.readerWarmupLoop(ctx, wg)
}

func (r *soakRunner) readerWarmupLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(r.cfg.readerWarmupInterval)
	defer ticker.Stop()
	r.runReaderWarmup(ctx)
	for r.readerPaused() || !r.readerReady.Load() {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runReaderWarmup(ctx)
		}
	}
}

func (r *soakRunner) runReaderWarmup(ctx context.Context) {
	resp, err := r.reader.doWithTimeout(ctx, nil, r.cfg.readerWarmupTimeout, "reader-warmup", http.MethodPost, "/v1/query", indexedHostMatch("region-0", 10), http.StatusOK, http.StatusServiceUnavailable)
	if err != nil {
		r.events.emit("reader_warmup_error", map[string]any{"error": err.Error()})
		return
	}
	if resp.status == http.StatusServiceUnavailable {
		if err := validateReaderVersionResponse(resp); err != nil {
			r.events.emit("reader_warmup_error", map[string]any{"error": err.Error()})
			return
		}
		r.events.emit("reader_warmup_retry", map[string]any{
			"code":             resp.json["code"],
			"required_version": readerNotFreshDetailInt(resp, "required_version"),
			"visible_version":  readerNotFreshDetailInt(resp, "visible_version"),
			"reason":           readerNotFreshDetailString(resp, "reason"),
		})
		return
	}
	r.events.emit("reader_warmup_ok", nil)
}

func readerNotFreshDetailInt(resp apiResponse, key string) int64 {
	detail, _ := resp.json["detail"].(map[string]any)
	return int64Value(detail[key])
}

func readerNotFreshDetailString(resp apiResponse, key string) string {
	detail, _ := resp.json["detail"].(map[string]any)
	return stringValue(detail[key])
}
