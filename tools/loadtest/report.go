package main

import (
	"encoding/json"
	"os"
	"time"
)

const loadReportSchemaVersion = 2

type loadReport struct {
	SchemaVersion int              `json:"schema_version"`
	GeneratedAt   time.Time        `json:"generated_at"`
	Success       bool             `json:"success"`
	ElapsedMS     int64            `json:"elapsed_ms"`
	Workload      loadWorkload     `json:"workload"`
	PlannedGraph  plannedGraphSize `json:"planned_graph"`
	Results       loadResults      `json:"results"`
	Metrics       []metricReport   `json:"metrics"`
}

type loadWorkload struct {
	Tenant                 string `json:"tenant"`
	WriterURL              string `json:"writer_url"`
	ReaderURL              string `json:"reader_url"`
	Writers                int    `json:"writers"`
	Readers                int    `json:"readers"`
	Batches                int    `json:"batches"`
	BatchSize              int    `json:"batch_size"`
	DurationMS             int64  `json:"duration_ms,omitempty"`
	Collectors             int    `json:"collectors"`
	WorkingSet             int    `json:"working_set"`
	StartAtUnixMS          int64  `json:"start_at_unix_ms,omitempty"`
	AllowWriteBackpressure bool   `json:"allow_write_backpressure"`
}

type loadResults struct {
	ScheduledBatches     int64   `json:"scheduled_batches"`
	CommittedBatches     int64   `json:"committed_batches"`
	BackpressuredBatches int64   `json:"backpressured_batches"`
	CommittedMutations   int64   `json:"committed_mutations"`
	MutationsPerSecond   float64 `json:"mutations_per_second"`
}

type plannedGraphSize struct {
	Entities int `json:"entities"`
	Edges    int `json:"edges"`
}

func writeLoadReport(path string, cfg config, readerURL string, elapsed time.Duration, metrics *registry, results loadResults) error {
	plannedGroups := int(results.ScheduledBatches) * cfg.batchSize
	if cfg.workingSet > 0 && plannedGroups > cfg.workingSet {
		plannedGroups = cfg.workingSet
	}
	report := loadReport{
		SchemaVersion: loadReportSchemaVersion,
		GeneratedAt:   time.Now().UTC(),
		Success:       !metrics.hasErrors(),
		ElapsedMS:     elapsed.Milliseconds(),
		Workload: loadWorkload{
			Tenant:                 cfg.tenant,
			WriterURL:              cfg.baseURL,
			ReaderURL:              readerURL,
			Writers:                cfg.writers,
			Readers:                cfg.readers,
			Batches:                cfg.batches,
			BatchSize:              cfg.batchSize,
			DurationMS:             cfg.duration.Milliseconds(),
			Collectors:             cfg.collectors,
			WorkingSet:             cfg.workingSet,
			StartAtUnixMS:          cfg.startAtUnixMS,
			AllowWriteBackpressure: cfg.allowWriteBackpressure,
		},
		PlannedGraph: plannedGraphSize{
			Entities: 2 + plannedGroups*2,
			Edges:    1 + plannedGroups,
		},
		Results: results,
		Metrics: metrics.snapshot(),
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
