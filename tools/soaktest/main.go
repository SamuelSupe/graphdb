package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	cfg := parseConfig()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	output, closeOutput, err := eventOutput(cfg.output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open event output failed: %v\n", err)
		os.Exit(1)
	}
	defer closeOutput()

	events := newEventWriter(output)
	metrics := newRegistry()
	writer := newClient(cfg.writerURL, cfg.tenant, cfg)
	reader := newClient(cfg.readerURL, cfg.tenant, cfg)
	events.emit("soak_start", map[string]any{
		"tenant": cfg.tenant, "duration": cfg.duration.String(),
		"writer": cfg.writerURL, "reader": cfg.readerURL,
		"writers": cfg.writers, "readers": cfg.readers, "batch_size": cfg.batchSize,
		"skip_setup": cfg.skipSetup,
	})

	seedVersion, err := setup(ctx, cfg, writer, reader, metrics, events)
	if err != nil {
		events.emit("setup_failed", map[string]any{"error": err.Error()})
		metrics.print(os.Stderr)
		os.Exit(1)
	}

	start := time.Now()
	runner := newSoakRunner(cfg, writer, reader, metrics, events, seedVersion)
	runner.run(ctx)
	sampleState(context.Background(), writer, reader, metrics, events, true)
	events.emit("soak_done", map[string]any{"elapsed_ms": time.Since(start).Milliseconds()})

	fmt.Fprintf(os.Stderr, "soak finished tenant=%s elapsed=%s\n", cfg.tenant, time.Since(start).Round(time.Millisecond))
	metrics.print(os.Stderr)
	if metrics.hasErrors() {
		os.Exit(2)
	}
}

func setup(ctx context.Context, cfg config, writer *apiClient, reader *apiClient, metrics *registry, events *eventWriter) (int64, error) {
	if err := writer.health(ctx, metrics); err != nil {
		return 0, err
	}
	if err := reader.health(ctx, metrics); err != nil {
		return 0, err
	}
	if cfg.skipSetup {
		events.emit("setup_skipped", nil)
		return 0, nil
	}
	if err := writer.createTenant(ctx, metrics); err != nil {
		return 0, err
	}
	version, err := writer.commitSchema(ctx, metrics)
	if err != nil {
		return 0, err
	}
	for name, request := range savedQueries() {
		if err := writer.saveQuery(ctx, metrics, name, request); err != nil {
			return 0, err
		}
	}
	if err := writer.rebuildIndexes(ctx, metrics); err != nil {
		return 0, err
	}
	if _, err := reader.query(ctx, metrics, "warm-reader", indexedHostMatch("region-0", 10)); err != nil {
		return 0, err
	}
	events.emit("setup_ok", nil)
	return version, nil
}

func eventOutput(path string) (io.Writer, func(), error) {
	if path == "" {
		return os.Stdout, func() {}, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, func() {}, err
	}
	return file, func() { _ = file.Close() }, nil
}
