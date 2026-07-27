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
	if err := writeLoadReport(path, cfg, "http://reader", time.Second, metrics); err != nil {
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
	if !report.Success || report.PlannedGraph.Entities != 62 || report.PlannedGraph.Edges != 31 {
		t.Fatalf("report = %#v", report)
	}
	if len(report.Metrics) != 1 || report.Metrics[0].P95MS != 25 {
		t.Fatalf("metrics = %#v", report.Metrics)
	}
}
