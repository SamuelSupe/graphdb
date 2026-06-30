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
	timeout                time.Duration
	httpTimeout            time.Duration
	maintenanceTimeout     time.Duration
	allowWriteBackpressure bool
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
	versions := &versionTracker{}
	versions.observe(seedVersion)

	start := time.Now()
	var wg sync.WaitGroup
	runWriters(ctx, cfg, client, versions, metrics, &wg)
	runReaders(ctx, cfg, reader, versions, metrics, &wg)
	wg.Wait()

	_ = client.saveQuery(ctx, metrics)
	_ = reader.runSavedQuery(ctx, metrics)
	_ = client.rebuildIndexes(ctx, metrics, cfg.maintenanceTimeout)
	_ = client.indexHealth(ctx, metrics)
	_ = client.collectorStatus(ctx, metrics)

	fmt.Printf("elapsed=%s tenant=%s writer=%s reader=%s writers=%d readers=%d batches=%d batch_size=%d allow_write_backpressure=%t\n",
		time.Since(start).Round(time.Millisecond), cfg.tenant, cfg.baseURL, reader.baseURL, cfg.writers, cfg.readers, cfg.batches, cfg.batchSize, cfg.allowWriteBackpressure)
	metrics.print(os.Stdout)
	if metrics.hasErrors() {
		os.Exit(2)
	}
}

func parseConfig() config {
	cfg := config{}
	flag.StringVar(&cfg.baseURL, "base", "http://localhost:8080", "GraphDB base URL")
	flag.StringVar(&cfg.readerURL, "reader-base", "", "optional GraphDB reader base URL for query load")
	flag.StringVar(&cfg.tenant, "tenant", "loadtest", "tenant id")
	flag.IntVar(&cfg.writers, "writers", 4, "concurrent ingestion writers")
	flag.IntVar(&cfg.readers, "readers", 8, "concurrent query readers")
	flag.IntVar(&cfg.batches, "batches", 80, "ingestion batches")
	flag.IntVar(&cfg.batchSize, "batch-size", 25, "items per batch")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "test timeout")
	flag.DurationVar(&cfg.httpTimeout, "http-timeout", 2*time.Minute, "per-request HTTP timeout")
	flag.DurationVar(&cfg.maintenanceTimeout, "maintenance-timeout", 10*time.Minute, "timeout for post-load maintenance calls")
	flag.BoolVar(&cfg.allowWriteBackpressure, "allow-write-backpressure", false, "treat write 429 backpressure as expected load shedding")
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
	return cfg
}

func seed(ctx context.Context, client *apiClient, metrics *registry) (int64, error) {
	version, err := client.commitSchema(ctx, metrics)
	if err != nil {
		return 0, err
	}
	if err := client.query(ctx, metrics, "seed-query", matchQuery("region-0")); err != nil {
		return 0, err
	}
	return version, nil
}
