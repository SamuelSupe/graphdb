package main

import (
	"flag"
	"strings"
	"time"
)

type config struct {
	input                    string
	minDuration              time.Duration
	warmup                   time.Duration
	maxReaderUnreadyRatio    float64
	maxIndexUnhealthySamples int
	maxFinalCommitTail       int
	requireCompact           bool
	requireGC                bool
	requireIndexRebuild      bool
	requireReaderRestart     bool
	readerRestartGrace       time.Duration
	shutdownGrace            time.Duration
	failOnErrorEvents        bool
	requiredOperations       []string
}

func parseConfig() config {
	cfg := config{}
	var requiredOperations string
	flag.StringVar(&cfg.input, "in", "", "soak NDJSON input path")
	flag.DurationVar(&cfg.minDuration, "min-duration", 0, "minimum observed event duration required; 0 disables")
	flag.DurationVar(&cfg.warmup, "warmup", 5*time.Minute, "ignore readiness/health failures before this duration")
	flag.Float64Var(&cfg.maxReaderUnreadyRatio, "max-reader-unready-ratio", 0.20, "maximum reader unready sample ratio after warmup")
	flag.IntVar(&cfg.maxIndexUnhealthySamples, "max-index-unhealthy-samples", 3, "maximum missing/error index health samples after warmup; stale is tracked separately")
	flag.IntVar(&cfg.maxFinalCommitTail, "max-final-commit-tail", 1000, "maximum final commit tail length")
	flag.BoolVar(&cfg.requireCompact, "require-compact", true, "require at least one compact_ok event")
	flag.BoolVar(&cfg.requireGC, "require-gc", true, "require at least one gc_ok event")
	flag.BoolVar(&cfg.requireIndexRebuild, "require-index-rebuild", true, "require at least one index_rebuild_ok event")
	flag.BoolVar(&cfg.requireReaderRestart, "require-reader-restart", false, "require at least one reader_restart_ok event")
	flag.DurationVar(&cfg.readerRestartGrace, "reader-restart-grace", 45*time.Second, "ignore reader sampling errors within this window around a planned reader_restart_ok")
	flag.DurationVar(&cfg.shutdownGrace, "shutdown-grace", 2*time.Minute, "ignore context deadline sampling errors within this window around soak_done")
	flag.BoolVar(&cfg.failOnErrorEvents, "fail-on-error-events", true, "fail when *_error or query/write error events are present")
	flag.StringVar(&requiredOperations, "require-operation", "", "comma-separated operation_metric names that must appear after warmup")
	flag.Parse()
	cfg.requiredOperations = splitList(requiredOperations)
	return cfg
}

func splitList(value string) []string {
	var result []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
