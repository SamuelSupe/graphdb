package main

import (
	"encoding/json"
	"os"
	"time"
)

const loadReportSchemaVersion = 1

type loadReport struct {
	SchemaVersion int              `json:"schema_version"`
	GeneratedAt   time.Time        `json:"generated_at"`
	Success       bool             `json:"success"`
	ElapsedMS     int64            `json:"elapsed_ms"`
	Workload      loadWorkload     `json:"workload"`
	PlannedGraph  plannedGraphSize `json:"planned_graph"`
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
	AllowWriteBackpressure bool   `json:"allow_write_backpressure"`
}

type plannedGraphSize struct {
	Entities int `json:"entities"`
	Edges    int `json:"edges"`
}

func writeLoadReport(path string, cfg config, readerURL string, elapsed time.Duration, metrics *registry) error {
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
			AllowWriteBackpressure: cfg.allowWriteBackpressure,
		},
		PlannedGraph: plannedGraphSize{
			Entities: 2 + cfg.batches*cfg.batchSize*2,
			Edges:    1 + cfg.batches*cfg.batchSize,
		},
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
