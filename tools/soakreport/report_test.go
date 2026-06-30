package main

import (
	"strings"
	"testing"
	"time"
)

func TestReportAcceptsHealthySoak(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"snapshot_version":0,"commit_tail_length":1,"object_count":10,"total_bytes":1000,"categories":{"indexes_bytes":100}}`,
		`{"event":"compact_ok","ts":"2026-06-20T00:00:02Z"}`,
		`{"event":"gc_ok","ts":"2026-06-20T00:00:03Z"}`,
		`{"event":"index_rebuild_ok","ts":"2026-06-20T00:00:04Z"}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:05Z","version":1,"index_entries":10,"edge_rows":5,"entity_rows":5,"edge_shards":1,"entity_pages":1}`,
		`{"event":"index_health_sample","ts":"2026-06-20T00:00:06Z","status":"ok","manifest_version":1,"catalog_version":1}`,
		`{"event":"reader_fleet","ts":"2026-06-20T00:00:07Z","ready":true,"ready_readers":1,"total_readers":1}`,
		`{"event":"reader_freshness","ts":"2026-06-20T00:00:08Z","version_lag":0,"lag_ms":0}`,
		`{"event":"operation_metric","ts":"2026-06-20T00:00:08Z","name":"fuzzy-service-match","count":3,"errors":0,"p50_ms":10.5,"p95_ms":20.5,"p99_ms":30.5,"max_ms":40.5}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:09Z","manifest_version":2,"snapshot_version":2,"commit_tail_length":0,"object_count":12,"total_bytes":1200,"categories":{"indexes_bytes":120}}`,
	}, "\n")
	report := newReport(config{warmup: 0, maxReaderUnreadyRatio: 0.1, maxIndexUnhealthySamples: 0, maxFinalCommitTail: 10, requireCompact: true, requireGC: true, requireIndexRebuild: true, failOnErrorEvents: true})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	if violations := report.violations(); len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
	if got := report.duration(); got != 9*time.Second {
		t.Fatalf("duration = %s", got)
	}
	if got := report.operations["fuzzy-service-match"]; got.count != 3 || got.p95MS != 20.5 {
		t.Fatalf("operation metric = %#v", got)
	}
	if effect := report.maintenanceEffect("compact_ok"); effect.count != 1 || effect.tailDelta != -1 {
		t.Fatalf("compact effect = %#v", effect)
	}
}

func TestReportFlagsErrorAndTailViolations(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":99,"object_count":10,"total_bytes":1000}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:02Z","version":1}`,
		`{"event":"index_health_sample","ts":"2026-06-20T00:00:03Z","status":"missing"}`,
		`{"event":"reader_fleet","ts":"2026-06-20T00:00:04Z","ready":false}`,
		`{"event":"query_error","ts":"2026-06-20T00:00:05Z","error":"boom"}`,
	}, "\n")
	report := newReport(config{warmup: 0, maxReaderUnreadyRatio: 0.1, maxIndexUnhealthySamples: 0, maxFinalCommitTail: 10, failOnErrorEvents: true})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	violations := strings.Join(report.violations(), "\n")
	for _, want := range []string{"error events present", "final commit tail", "reader unready ratio", "index unhealthy samples"} {
		if !strings.Contains(violations, want) {
			t.Fatalf("violations %q missing %q", violations, want)
		}
	}
}

func TestReportRequiresOperationCoverage(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":0,"object_count":10,"total_bytes":1000}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:02Z","version":1}`,
		`{"event":"operation_metric","ts":"2026-06-20T00:00:03Z","name":"scan-entities","count":2,"errors":1}`,
	}, "\n")
	report := newReport(config{
		warmup:                   0,
		maxReaderUnreadyRatio:    0.1,
		maxIndexUnhealthySamples: 1,
		maxFinalCommitTail:       10,
		requiredOperations:       []string{"scan-entities", "export-snapshot-stream"},
	})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	violations := strings.Join(report.violations(), "\n")
	for _, want := range []string{"operation scan-entities errors 1", "missing operation_metric export-snapshot-stream"} {
		if !strings.Contains(violations, want) {
			t.Fatalf("violations %q missing %q", violations, want)
		}
	}
}

func TestReportIgnoresReaderSamplingErrorDuringPlannedRestartGrace(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":0,"object_count":10,"total_bytes":1000}`,
		`{"event":"reader_fleet_error","ts":"2026-06-20T00:10:00Z","error":"connection reset by peer"}`,
		`{"event":"reader_restart_ok","ts":"2026-06-20T00:10:01Z"}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:10:02Z","version":1}`,
		`{"event":"operation_metric","ts":"2026-06-20T00:10:03Z","name":"reader-fleet","count":10,"errors":1}`,
	}, "\n")
	report := newReport(config{
		warmup:                   0,
		readerRestartGrace:       45 * time.Second,
		maxReaderUnreadyRatio:    0.1,
		maxIndexUnhealthySamples: 1,
		maxFinalCommitTail:       10,
		failOnErrorEvents:        true,
	})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	classified := report.classifiedErrors()
	if len(classified.active) != 0 {
		t.Fatalf("errors = %v", classified.active)
	}
	if classified.plannedRestart["reader_fleet_error"] != 1 {
		t.Fatalf("planned restart errors = %v", classified.plannedRestart)
	}
	if errors, plannedCount, shutdownCount := effectiveOperationErrors(report.operations["reader-fleet"], classified.plannedRestart, classified.shutdown); errors != 0 || plannedCount != 1 || shutdownCount != 0 {
		t.Fatalf("effective reader-fleet errors = %d planned = %d shutdown = %d", errors, plannedCount, shutdownCount)
	}
	if violations := report.violations(); len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
}

func TestReportIgnoresSamplingDeadlineDuringShutdownGrace(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":0,"object_count":10,"total_bytes":1000}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:02Z","version":1}`,
		`{"event":"operation_metric","ts":"2026-06-20T00:59:00Z","name":"index-catalog","count":20,"errors":1}`,
		`{"event":"index_catalog_sample_error","ts":"2026-06-20T00:59:10Z","error":"Get \"http://127.0.0.1:38380/v1/indexes\": context deadline exceeded"}`,
		`{"event":"reader_fleet_error","ts":"2026-06-20T00:59:11Z","error":"Get \"http://127.0.0.1:38381/v1/control/reader-fleet-readiness\": context deadline exceeded"}`,
		`{"event":"soak_done","ts":"2026-06-20T01:00:00Z"}`,
	}, "\n")
	report := newReport(config{
		warmup:                   0,
		shutdownGrace:            2 * time.Minute,
		maxReaderUnreadyRatio:    0.1,
		maxIndexUnhealthySamples: 1,
		maxFinalCommitTail:       10,
		failOnErrorEvents:        true,
	})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	classified := report.classifiedErrors()
	if len(classified.active) != 0 {
		t.Fatalf("errors = %v", classified.active)
	}
	if classified.shutdown["index_catalog_sample_error"] != 1 || classified.shutdown["reader_fleet_error"] != 1 {
		t.Fatalf("shutdown errors = %v", classified.shutdown)
	}
	if errors, _, shutdownCount := effectiveOperationErrors(report.operations["index-catalog"], classified.plannedRestart, classified.shutdown); errors != 0 || shutdownCount != 1 {
		t.Fatalf("effective index-catalog errors = %d shutdown = %d", errors, shutdownCount)
	}
	if violations := report.violations(); len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
}

func TestReportFlagsSamplingDeadlineOutsideShutdownGrace(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":0,"object_count":10,"total_bytes":1000}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:02Z","version":1}`,
		`{"event":"index_health_sample_error","ts":"2026-06-20T00:55:00Z","error":"Get \"http://127.0.0.1:38381/v1/indexes/health\": context deadline exceeded"}`,
		`{"event":"soak_done","ts":"2026-06-20T01:00:00Z"}`,
	}, "\n")
	report := newReport(config{
		warmup:                   0,
		shutdownGrace:            2 * time.Minute,
		maxReaderUnreadyRatio:    0.1,
		maxIndexUnhealthySamples: 1,
		maxFinalCommitTail:       10,
		failOnErrorEvents:        true,
	})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	violations := strings.Join(report.violations(), "\n")
	if !strings.Contains(violations, "error events present") {
		t.Fatalf("violations = %q", violations)
	}
}

func TestReportFlagsNonDeadlineSamplingErrorDuringShutdownGrace(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":0,"object_count":10,"total_bytes":1000}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:02Z","version":1}`,
		`{"event":"reader_freshness_error","ts":"2026-06-20T00:59:10Z","error":"connection reset by peer"}`,
		`{"event":"soak_done","ts":"2026-06-20T01:00:00Z"}`,
	}, "\n")
	report := newReport(config{
		warmup:                   0,
		shutdownGrace:            2 * time.Minute,
		maxReaderUnreadyRatio:    0.1,
		maxIndexUnhealthySamples: 1,
		maxFinalCommitTail:       10,
		failOnErrorEvents:        true,
	})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	violations := strings.Join(report.violations(), "\n")
	if !strings.Contains(violations, "error events present") {
		t.Fatalf("violations = %q", violations)
	}
}

func TestReportFlagsReaderSamplingErrorOutsidePlannedRestartGrace(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":0,"object_count":10,"total_bytes":1000}`,
		`{"event":"reader_restart_ok","ts":"2026-06-20T00:10:00Z"}`,
		`{"event":"reader_fleet_error","ts":"2026-06-20T00:11:00Z","error":"connection reset by peer"}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:11:01Z","version":1}`,
	}, "\n")
	report := newReport(config{
		warmup:                   0,
		readerRestartGrace:       45 * time.Second,
		maxReaderUnreadyRatio:    0.1,
		maxIndexUnhealthySamples: 1,
		maxFinalCommitTail:       10,
		failOnErrorEvents:        true,
	})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	violations := strings.Join(report.violations(), "\n")
	if !strings.Contains(violations, "error events present") {
		t.Fatalf("violations = %q", violations)
	}
}

func TestReportRequiresDurationAndReaderRestart(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"commit_tail_length":0,"object_count":10,"total_bytes":1000}`,
		`{"event":"compact_ok","ts":"2026-06-20T00:00:02Z"}`,
		`{"event":"gc_ok","ts":"2026-06-20T00:00:03Z"}`,
		`{"event":"index_rebuild_ok","ts":"2026-06-20T00:00:04Z"}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:05Z","version":1}`,
	}, "\n")
	report := newReport(config{
		warmup:                   0,
		minDuration:              time.Minute,
		maxReaderUnreadyRatio:    0.1,
		maxIndexUnhealthySamples: 1,
		maxFinalCommitTail:       10,
		requireCompact:           true,
		requireGC:                true,
		requireIndexRebuild:      true,
		requireReaderRestart:     true,
	})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	violations := strings.Join(report.violations(), "\n")
	for _, want := range []string{"duration 5s < 1m0s", "missing reader_restart_ok"} {
		if !strings.Contains(violations, want) {
			t.Fatalf("violations %q missing %q", violations, want)
		}
	}
}

func TestReportTracksStaleIndexWithoutFailingSoak(t *testing.T) {
	input := strings.Join([]string{
		`{"event":"soak_start","ts":"2026-06-20T00:00:00Z"}`,
		`{"event":"usage_sample","ts":"2026-06-20T00:00:01Z","manifest_version":1,"snapshot_version":0,"commit_tail_length":1,"object_count":10,"total_bytes":1000}`,
		`{"event":"compact_ok","ts":"2026-06-20T00:00:02Z"}`,
		`{"event":"gc_ok","ts":"2026-06-20T00:00:03Z"}`,
		`{"event":"index_rebuild_ok","ts":"2026-06-20T00:00:04Z"}`,
		`{"event":"index_catalog_sample","ts":"2026-06-20T00:00:05Z","version":1}`,
		`{"event":"index_health_sample","ts":"2026-06-20T00:00:06Z","status":"stale","manifest_version":2,"catalog_version":1}`,
		`{"event":"reader_fleet","ts":"2026-06-20T00:00:07Z","ready":true}`,
		`{"event":"reader_freshness","ts":"2026-06-20T00:00:08Z","version_lag":1,"lag_ms":1000}`,
	}, "\n")
	report := newReport(config{warmup: 0, maxReaderUnreadyRatio: 0.1, maxIndexUnhealthySamples: 0, maxFinalCommitTail: 10, requireCompact: true, requireGC: true, requireIndexRebuild: true, failOnErrorEvents: true})
	if err := readEvents(strings.NewReader(input), func(e event) error {
		report.add(e)
		return nil
	}); err != nil {
		t.Fatalf("read events: %v", err)
	}
	if report.health.stale != 1 || report.health.unhealthy != 0 {
		t.Fatalf("health stats = %#v", report.health)
	}
	if violations := report.violations(); len(violations) != 0 {
		t.Fatalf("violations = %v", violations)
	}
}
