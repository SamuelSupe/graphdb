package main

import (
	"flag"
	"time"
)

type config struct {
	writerURL              string
	readerURL              string
	tenant                 string
	duration               time.Duration
	httpTimeout            time.Duration
	writeInterval          time.Duration
	queryInterval          time.Duration
	queryStartDelay        time.Duration
	readerMaxStaleness     time.Duration
	readerWarmupInterval   time.Duration
	readerWarmupTimeout    time.Duration
	sampleInterval         time.Duration
	snapshotExportInterval time.Duration
	compactInterval        time.Duration
	gcInterval             time.Duration
	indexRebuildInterval   time.Duration
	maintenanceTimeout     time.Duration
	readerRestartEvery     time.Duration
	readerRestartGrace     time.Duration
	readerRestartCommand   string
	writers                int
	readers                int
	batchSize              int
	startBatch             int64
	allowBackpressure      bool
	failOnUnreadyReader    bool
	skipSetup              bool
	output                 string
}

func parseConfig() config {
	cfg := config{}
	flag.StringVar(&cfg.writerURL, "writer", "http://127.0.0.1:38080", "writer base URL")
	flag.StringVar(&cfg.readerURL, "reader", "http://127.0.0.1:38081", "reader base URL")
	flag.StringVar(&cfg.tenant, "tenant", "soaktest", "tenant id")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Minute, "soak duration, for example 24h or 72h")
	flag.DurationVar(&cfg.httpTimeout, "http-timeout", 30*time.Second, "per-request HTTP timeout")
	flag.DurationVar(&cfg.writeInterval, "write-interval", 100*time.Millisecond, "delay between writer batches per worker")
	flag.DurationVar(&cfg.queryInterval, "query-interval", 200*time.Millisecond, "delay between query rounds per worker")
	flag.DurationVar(&cfg.queryStartDelay, "query-start-delay", 0, "delay reader query workers after startup so readers can warm their cache")
	flag.DurationVar(&cfg.readerMaxStaleness, "reader-max-staleness", 30*time.Second, "maximum reader fleet staleness accepted before query workers run")
	flag.DurationVar(&cfg.readerWarmupInterval, "reader-warmup-interval", 2*time.Second, "single-reader warmup query interval while -query-start-delay is active; 0 disables")
	flag.DurationVar(&cfg.readerWarmupTimeout, "reader-warmup-timeout", 2*time.Minute, "per-request timeout for reader warmup queries")
	flag.DurationVar(&cfg.sampleInterval, "sample-interval", 30*time.Second, "usage/index/fleet sample interval")
	flag.DurationVar(&cfg.snapshotExportInterval, "snapshot-export-interval", 5*time.Minute, "minimum delay between snapshot export stream checks; 0 disables throttling")
	flag.DurationVar(&cfg.compactInterval, "compact-interval", 10*time.Minute, "compact interval; 0 disables")
	flag.DurationVar(&cfg.gcInterval, "gc-interval", 30*time.Minute, "GC interval; 0 disables")
	flag.DurationVar(&cfg.indexRebuildInterval, "index-rebuild-interval", 15*time.Minute, "index rebuild interval; 0 disables")
	flag.DurationVar(&cfg.maintenanceTimeout, "maintenance-timeout", 5*time.Minute, "per-request timeout for compact, GC, and index rebuild")
	flag.DurationVar(&cfg.readerRestartEvery, "reader-restart-interval", 0, "reader restart interval; requires -reader-restart-command")
	flag.DurationVar(&cfg.readerRestartGrace, "reader-restart-grace", 45*time.Second, "pause direct reader checks for this long around planned reader restarts")
	flag.StringVar(&cfg.readerRestartCommand, "reader-restart-command", "", "optional command used to restart reader during soak")
	flag.IntVar(&cfg.writers, "writers", 4, "concurrent writer workers")
	flag.IntVar(&cfg.readers, "readers", 8, "concurrent reader workers")
	flag.IntVar(&cfg.batchSize, "batch-size", 20, "CMDB application groups per batch")
	flag.Int64Var(&cfg.startBatch, "start-batch", 0, "first batch number, useful when resuming the same tenant")
	flag.BoolVar(&cfg.allowBackpressure, "allow-write-backpressure", true, "treat write 429 as expected load shedding")
	flag.BoolVar(&cfg.failOnUnreadyReader, "fail-on-unready-reader", false, "count unready reader fleet samples as errors")
	flag.BoolVar(&cfg.skipSetup, "skip-setup", false, "skip tenant/schema/saved-query setup when reusing an existing tenant")
	flag.StringVar(&cfg.output, "out", "", "optional NDJSON event output path; defaults to stdout")
	flag.Parse()
	normalizeConfig(&cfg)
	return cfg
}

func normalizeConfig(cfg *config) {
	if cfg.duration <= 0 {
		cfg.duration = 10 * time.Minute
	}
	if cfg.httpTimeout <= 0 {
		cfg.httpTimeout = 30 * time.Second
	}
	if cfg.writers < 1 {
		cfg.writers = 1
	}
	if cfg.readers < 0 {
		cfg.readers = 0
	}
	if cfg.batchSize < 1 {
		cfg.batchSize = 1
	}
	if cfg.sampleInterval <= 0 {
		cfg.sampleInterval = 30 * time.Second
	}
	if cfg.snapshotExportInterval < 0 {
		cfg.snapshotExportInterval = 0
	}
	if cfg.writeInterval < 0 {
		cfg.writeInterval = 0
	}
	if cfg.queryInterval < 0 {
		cfg.queryInterval = 0
	}
	if cfg.queryStartDelay < 0 {
		cfg.queryStartDelay = 0
	}
	if cfg.readerMaxStaleness < 0 {
		cfg.readerMaxStaleness = 0
	}
	if cfg.readerWarmupInterval < 0 {
		cfg.readerWarmupInterval = 0
	}
	if cfg.readerWarmupTimeout <= 0 {
		cfg.readerWarmupTimeout = cfg.httpTimeout
	}
	if cfg.maintenanceTimeout <= 0 {
		cfg.maintenanceTimeout = cfg.httpTimeout
	}
	if cfg.readerRestartGrace < 0 {
		cfg.readerRestartGrace = 0
	}
}
