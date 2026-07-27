package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type rollbackDrillReport struct {
	SchemaVersion         int       `json:"schema_version"`
	Success               bool      `json:"success"`
	Commit                string    `json:"commit"`
	GeneratedAt           time.Time `json:"generated_at"`
	Namespace             string    `json:"namespace"`
	PostgresHeadVersion   int64     `json:"postgres_head_version"`
	LegacyManifestVersion int64     `json:"legacy_manifest_version"`
	LocalVersion          int64     `json:"local_version"`
	OutboxBacklog         int64     `json:"outbox_backlog"`
	LegacyMirrorLag       int64     `json:"legacy_mirror_lag"`
	MarkerRemoved         bool      `json:"marker_removed"`
	PostgresWriterFenced  bool      `json:"postgres_writer_fenced"`
	LocalWriterSucceeded  bool      `json:"local_writer_succeeded"`
}

func writeRollbackDrillReport(t *testing.T, report rollbackDrillReport) {
	t.Helper()
	path := os.Getenv("GRAPHDB_TEST_ROLLBACK_REPORT")
	if path == "" {
		return
	}
	if report.Commit == "" {
		t.Fatal("GRAPHDB_TEST_BUILD_COMMIT is required when writing a rollback report")
	}
	report.GeneratedAt = time.Now().UTC()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal rollback report: %v", err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create rollback report directory: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write rollback report: %v", err)
	}
}
