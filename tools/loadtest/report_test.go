package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteLoadReport(t *testing.T) {
	metrics := newRegistry()
	metrics.add("ingest", 25*time.Millisecond, 200, nil)
	path := filepath.Join(t.TempDir(), "report.json")
	cfg := config{
		baseURL:   "http://writer",
		tenant:    "tenant-a",
		writers:   4,
		readers:   8,
		batches:   3,
		batchSize: 10,
	}
	results := loadResults{ScheduledBatches: 3, CommittedBatches: 3, CommittedMutations: 90, MutationsPerSecond: 90}
	if err := writeLoadReport(path, cfg, "http://reader", time.Second, metrics, results); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var report loadReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.SchemaVersion != 2 || !report.Success || report.PlannedGraph.Entities != 62 || report.PlannedGraph.Edges != 31 || report.Results.CommittedMutations != 90 {
		t.Fatalf("report = %#v", report)
	}
	if len(report.Metrics) != 1 || report.Metrics[0].P95MS != 25 {
		t.Fatalf("metrics = %#v", report.Metrics)
	}
}
