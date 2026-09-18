package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

// Timed runs update a fixed graph so faster servers do not end up measuring a
// larger data set. Each worker owns disjoint batches in a deterministic sequence.
func runTimed(ctx context.Context, cfg config, client, reader *apiClient) error {
	groups := (cfg.entities - 2) / (cfg.batchSize * 2)
	if groups < cfg.writers || groups < 1 {
		return fmt.Errorf("entities must contain at least one batch per writer")
	}
	seedMetrics := newRegistry()
	if !cfg.skipSeed {
		if _, err := seed(ctx, client, seedMetrics); err != nil {
			return err
		}
		for batch := 0; batch < groups; batch++ {
			if _, err := client.ingest(ctx, seedMetrics, batchRequest(batch, cfg.batchSize)); err != nil {
				return err
			}
		}
		if err := client.rebuildIndexes(ctx, seedMetrics, cfg.maintenanceTimeout); err != nil {
			return err
		}
	}
	if cfg.seedOnly {
		return writeTimedReport(cfg, map[string]any{"success": true, "entities": 2 + groups*cfg.batchSize*2, "seed_metrics": seedMetrics.snapshot()})
	}
	versions := &versionTracker{}
	var sequence atomic.Int64
	if cfg.warmup > 0 {
		if _, err := timedPhase(ctx, cfg, client, reader, groups, versions, &sequence, "warmup", cfg.warmup); err != nil {
			return err
		}
	}
	initial, err := reader.request(ctx, http.MethodGet, "/v1/entities/host:seed", nil, nil)
	if err != nil || initial.status != http.StatusOK {
		return fmt.Errorf("read starting graph version: status=%d error=%v", initial.status, err)
	}
	startingVersion := responseVersion(initial)
	versions.observe(startingVersion)
	start := time.Now()
	metrics, err := timedPhase(ctx, cfg, client, reader, groups, versions, &sequence, "measure", cfg.duration)
	elapsed := time.Since(start)
	if err != nil {
		return err
	}
	published, operations := 0, 0
	for _, m := range metrics.snapshot() {
		if m.Name == "ingest" {
			published = m.Count - m.Errors
		}
		if m.Name == "ingest" || m.Name == "scan" || m.Name == "query-match" || m.Name == "query-traverse" || m.Name == "query-min-version" {
			operations += m.Count - m.Errors
		}
	}
	versionDelta := versions.latest() - startingVersion
	report := map[string]any{
		"schema_version": 2, "generated_at": time.Now().UTC(), "success": !metrics.hasErrors() && versionDelta == int64(published),
		"entities": 2 + groups*cfg.batchSize*2, "writers": cfg.writers, "readers": cfg.readers,
		"batch_size": cfg.batchSize, "run_id": cfg.runID, "warmup_seconds": cfg.warmup.Seconds(),
		"write_interval_seconds": cfg.writeInterval.Seconds(),
		"requested_seconds":      cfg.duration.Seconds(), "elapsed_seconds": elapsed.Seconds(),
		"published_batches": published, "published_batches_per_second": float64(published) / elapsed.Seconds(),
		"published_host_updates_per_second": float64(published*cfg.batchSize) / elapsed.Seconds(),
		"last_version":                      versions.latest(), "metrics": metrics.snapshot(),
		"starting_version": startingVersion, "published_version_delta": versionDelta,
		"mixed_operations_per_second": float64(operations) / elapsed.Seconds(),
	}
	metrics.print(os.Stdout)
	if err := writeTimedReport(cfg, report); err != nil {
		return err
	}
	if metrics.hasErrors() {
		return fmt.Errorf("timed workload recorded errors")
	}
	if versionDelta != int64(published) {
		return fmt.Errorf("published graph versions = %d, successful batches = %d", versionDelta, published)
	}
	return nil
}

func timedPhase(ctx context.Context, cfg config, client, reader *apiClient, groups int, versions *versionTracker, sequence *atomic.Int64, phase string, duration time.Duration) (*registry, error) {
	metrics := newRegistry()
	deadline := time.Now().Add(duration)
	var workers sync.WaitGroup
	for worker := 0; worker < cfg.writers; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			started := time.Now()
			for step := 0; time.Now().Before(deadline) && ctx.Err() == nil; step++ {
				if cfg.writeInterval > 0 {
					next := started.Add(time.Duration(step) * cfg.writeInterval)
					if !next.Before(deadline) {
						return
					}
					timer := time.NewTimer(max(0, time.Until(next)))
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					if !time.Now().Before(deadline) {
						return
					}
				}
				// Round-robin only among this worker's own groups.
				count := (groups-1-worker)/cfg.writers + 1
				batch := worker + (step%count)*cfg.writers
				body := batchRequest(batch, cfg.batchSize)
				id := fmt.Sprintf("%s-%s-%d-%d", cfg.runID, phase, worker, sequence.Add(1))
				body.BatchID, body.IdempotencyKey, body.Cursor = id, id, id
				for i := range body.Items {
					entity := body.Items[i].Entity
					if entity != nil && entity.Kind == "host" {
						entity.Fields["region"] = regionName((batch*cfg.batchSize + i/3 + step/count + 1) % 8)
						entity.Fields["loadtest_revision"] = fmt.Sprintf("%s-%s-%d-%d", cfg.updateEpoch, phase, worker, step)
					}
				}
				version, err := client.timedIngest(ctx, metrics, body, cfg.wal)
				if err == nil {
					versions.observe(version)
				}
			}
		}(worker)
	}
	for worker := 0; worker < cfg.readers; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for step := worker; time.Now().Before(deadline) && ctx.Err() == nil; step++ {
				switch step % 4 {
				case 0:
					_ = reader.timedQuery(ctx, metrics, "query-match", matchQuery(regionName(step%8)))
				case 1:
					_ = reader.timedQuery(ctx, metrics, "query-traverse", traverseQuery(step))
				case 2:
					request := matchQuery(regionName(step % 8))
					request.MinVersion = versions.latest()
					_ = reader.timedQuery(ctx, metrics, "query-min-version", request)
				case 3:
					_, _ = reader.do(ctx, metrics, "scan", http.MethodGet, "/v1/entities?kind=host&limit=100", nil, http.StatusOK)
				}
			}
		}(worker)
	}
	workers.Wait()
	return metrics, ctx.Err()
}

func (c *apiClient) timedQuery(ctx context.Context, metrics *registry, name string, body query.Request) error {
	start := time.Now()
	response, err := c.request(ctx, http.MethodPost, "/v1/query", body, nil)
	if err == nil && response.status != http.StatusOK {
		err = fmt.Errorf("query status %d: %s", response.status, response.body)
	}
	if err == nil && responseVersion(response) < body.MinVersion {
		err = fmt.Errorf("query version %d is below requested %d", responseVersion(response), body.MinVersion)
	}
	metrics.add(name, time.Since(start), response.status, err)
	return err
}

func (c *apiClient) timedIngest(ctx context.Context, metrics *registry, body storage.IngestRequest, wal bool) (int64, error) {
	started := time.Now()
	response, err := c.timedAdmission(ctx, metrics, body, wal)
	accepted := time.Now()
	if wal {
		metrics.add("wal-accept", accepted.Sub(started), response.status, err)
	}
	if err != nil {
		metrics.add("ingest", time.Since(started), response.status, err)
		return 0, err
	}
	if !wal {
		version := responseVersion(response)
		if version <= 0 || int64Value(response.json["failed"]) > 0 {
			err = fmt.Errorf("direct batch did not fully commit: %s", response.body)
		}
		metrics.add("ingest", time.Since(started), response.status, err)
		return version, err
	}
	statusURL := stringValue(response.json["status_url"])
	if statusURL == "" {
		statusURL = response.headers.Get("Location")
	}
	for response.status == http.StatusAccepted || pendingWALState(stringValue(response.json["state"])) {
		if statusURL == "" {
			err = fmt.Errorf("WAL response missing status URL")
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		if err != nil {
			break
		}
		response, err = c.request(ctx, http.MethodGet, statusURL, nil, nil)
		if err != nil {
			break
		}
		if response.status != http.StatusOK {
			err = fmt.Errorf("WAL status: %d %s", response.status, response.body)
			break
		}
	}
	version := responseVersion(response)
	if result, ok := response.json["result"].(map[string]any); ok && version == 0 {
		version = int64Value(result["version"])
	}
	if err == nil && (version <= 0 || stringValue(response.json["state"]) == storage.IngestStateFailed) {
		err = fmt.Errorf("WAL batch did not commit: %s", response.body)
	}
	if err == nil {
		path := fmt.Sprintf("/v1/entities/%s?min_version=%d", url.PathEscape("host:seed"), version)
		var visible apiResponse
		visible, err = c.request(ctx, http.MethodGet, path, nil, nil)
		if err == nil && visible.status != http.StatusOK {
			err = fmt.Errorf("WAL publication not readable: %d", visible.status)
		}
		if err == nil && responseVersion(visible) < version {
			err = fmt.Errorf("WAL readable version %d is below committed %d", responseVersion(visible), version)
		}
	}
	metrics.add("wal-readable", time.Since(accepted), response.status, err)
	metrics.add("ingest", time.Since(started), response.status, err)
	return version, err
}

func (c *apiClient) timedAdmission(ctx context.Context, metrics *registry, body storage.IngestRequest, wal bool) (apiResponse, error) {
	prefer := "wait=committed"
	if wal {
		prefer = ""
	}
	for {
		start := time.Now()
		response, err := c.request(ctx, http.MethodPost, "/v1/ingest/batches", body, map[string]string{"Prefer": prefer})
		if err != nil {
			return response, err
		}
		if response.status != http.StatusTooManyRequests || !boolValue(response.json["retryable"]) {
			if response.status != http.StatusOK && !(wal && response.status == http.StatusAccepted) {
				return response, fmt.Errorf("ingest status %d: %s", response.status, response.body)
			}
			return response, nil
		}
		metrics.add("write-backpressure", time.Since(start), response.status, nil)
		seconds, err := strconv.Atoi(response.headers.Get("Retry-After"))
		if err != nil || seconds < 1 {
			seconds = 1
		}
		timer := time.NewTimer(time.Duration(seconds) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return response, ctx.Err()
		case <-timer.C:
		}
	}
}

func pendingWALState(state string) bool {
	switch state {
	case storage.IngestStateAccepted, storage.IngestStatePrepared, storage.IngestStatePublished, storage.IngestStateRetrying:
		return true
	default:
		return false
	}
}

func writeTimedReport(cfg config, report any) error {
	if cfg.reportJSON == "" {
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	file, err := os.Create(cfg.reportJSON)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
