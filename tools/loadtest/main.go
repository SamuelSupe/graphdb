package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
)

type config struct {
	baseURL                string
	readerURL              string
	tenant                 string
	writers                int
	readers                int
	batches                int
	batchSize              int
	duration               time.Duration
	collectors             int
	workingSet             int
	startAtUnixMS          int64
	timeout                time.Duration
	httpTimeout            time.Duration
	maintenanceTimeout     time.Duration
	allowWriteBackpressure bool
	postLoadChecks         bool
	reportJSON             string
}

func main() {
	cfg := parseConfig()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	client := newClient(cfg.baseURL, cfg.tenant, cfg.httpTimeout)
	client.allowWriteBackpressure = cfg.allowWriteBackpressure
	reader := client
	if cfg.readerURL != "" {
		reader = newClient(cfg.readerURL, cfg.tenant, cfg.httpTimeout)
	}
	metrics := newRegistry()
	if err := client.health(ctx, metrics); err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
		os.Exit(1)
	}
	seedVersion, err := seed(ctx, client, metrics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed failed: %v\n", err)
		os.Exit(1)
	}
	if err := waitForStart(ctx, cfg.startAtUnixMS); err != nil {
		fmt.Fprintf(os.Stderr, "start barrier failed: %v\n", err)
		os.Exit(1)
	}
	versions := &versionTracker{}
	versions.observe(seedVersion)
	stats := &runStats{}

	start := time.Now()
	var wg sync.WaitGroup
	runWriters(ctx, cfg, client, versions, metrics, stats, &wg)
	runReaders(ctx, cfg, reader, versions, metrics, &wg)
	wg.Wait()

	if cfg.postLoadChecks {
		_ = client.saveQuery(ctx, metrics)
		_ = reader.runSavedQuery(ctx, metrics)
		_ = client.rebuildIndexes(ctx, metrics, cfg.maintenanceTimeout)
		_ = client.indexHealth(ctx, metrics)
		_ = client.collectorStatus(ctx, metrics)
	}

	elapsed := time.Since(start)
	snapshot := stats.snapshot(elapsed)
	fmt.Printf("elapsed=%s tenant=%s writer=%s reader=%s writers=%d readers=%d batches=%d batch_size=%d collectors=%d committed_mutations=%d mutations_per_second=%.2f allow_write_backpressure=%t\n",
		elapsed.Round(time.Millisecond), cfg.tenant, cfg.baseURL, reader.baseURL, cfg.writers, cfg.readers, snapshot.ScheduledBatches, cfg.batchSize, cfg.collectors, snapshot.CommittedMutations, snapshot.MutationsPerSecond, cfg.allowWriteBackpressure)
	metrics.print(os.Stdout)
	if cfg.reportJSON != "" {
		if err := writeLoadReport(cfg.reportJSON, cfg, reader.baseURL, elapsed, metrics, snapshot); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(2)
		}
	}
	if metrics.hasErrors() {
		os.Exit(2)
	}
}

func parseConfig() config {
	cfg := config{}
	flag.StringVar(&cfg.baseURL, "base", "http://localhost:8080", "GGraphDB base URL")
	flag.StringVar(&cfg.readerURL, "reader-base", "", "optional GGraphDB reader base URL for query load")
	flag.StringVar(&cfg.tenant, "tenant", "loadtest", "tenant id")
	flag.IntVar(&cfg.writers, "writers", 4, "concurrent ingestion writers")
	flag.IntVar(&cfg.readers, "readers", 8, "concurrent query readers")
	flag.IntVar(&cfg.batches, "batches", 20, "ingestion batches")
	flag.IntVar(&cfg.batchSize, "batch-size", 200, "CMDB host/service groups per batch")
	flag.DurationVar(&cfg.duration, "duration", 0, "run writers for this duration instead of a fixed batch count")
	flag.IntVar(&cfg.collectors, "collectors", 1, "collector identities distributed across writer requests")
	flag.IntVar(&cfg.workingSet, "working-set", 0, "reuse this many CMDB groups while changing their fields; zero grows unique groups")
	flag.Int64Var(&cfg.startAtUnixMS, "start-at-unix-ms", 0, "wait until this Unix millisecond before starting the measured workload")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "test timeout")
	flag.DurationVar(&cfg.httpTimeout, "http-timeout", 2*time.Minute, "per-request HTTP timeout")
	flag.DurationVar(&cfg.maintenanceTimeout, "maintenance-timeout", 10*time.Minute, "timeout for post-load maintenance calls")
	flag.BoolVar(&cfg.allowWriteBackpressure, "allow-write-backpressure", false, "treat write 429 backpressure as expected load shedding")
	flag.BoolVar(&cfg.postLoadChecks, "post-load-checks", true, "run saved-query, index rebuild, health, and collector checks after load")
	flag.StringVar(&cfg.reportJSON, "report-json", "", "optional path for a machine-readable JSON report")
	flag.Parse()
	if cfg.writers < 1 {
		cfg.writers = 1
	}
	if cfg.readers < 0 {
		cfg.readers = 0
	}
	if cfg.batchSize < 3 {
		cfg.batchSize = 3
	}
	if cfg.collectors < 1 {
		cfg.collectors = 1
	}
	if cfg.workingSet < 0 {
		cfg.workingSet = 0
	}
	if cfg.duration > 0 && cfg.timeout <= cfg.duration {
		cfg.timeout = cfg.duration + 5*time.Minute
	}
	return cfg
}

func waitForStart(ctx context.Context, startAtUnixMS int64) error {
	if startAtUnixMS <= 0 {
		return nil
	}
	delay := time.Until(time.UnixMilli(startAtUnixMS))
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func seed(ctx context.Context, client *apiClient, metrics *registry) (int64, error) {
	version, err := client.commitSchema(ctx, metrics)
	if err != nil {
		return 0, err
	}
	if err := client.rebuildIndexes(ctx, metrics, 0); err != nil {
		return 0, err
	}
	if err := client.query(ctx, metrics, "seed-query", matchQuery("region-0")); err != nil {
		return 0, err
	}
	return version, nil
}
