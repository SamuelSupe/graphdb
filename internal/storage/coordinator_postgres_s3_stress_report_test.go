package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type casStressReport struct {
	SchemaVersion       int       `json:"schema_version"`
	Commit              string    `json:"commit"`
	GeneratedAt         time.Time `json:"generated_at"`
	Success             bool      `json:"success"`
	Writers             int       `json:"writers"`
	TargetQPS           int       `json:"target_qps"`
	Duration            string    `json:"duration"`
	TargetCommits       int       `json:"target_commits"`
	Committed           int       `json:"committed"`
	Compactions         int64     `json:"compactions"`
	WriteConflicts      int64     `json:"write_conflicts"`
	ElapsedMS           int64     `json:"elapsed_ms"`
	Throughput          float64   `json:"throughput_commits_per_second"`
	GraphVersion        int64     `json:"graph_version"`
	HeadRevision        int64     `json:"head_revision"`
	Entities            int       `json:"entities"`
	SnapshotVersion     int64     `json:"snapshot_version"`
	CommitTailLength    int       `json:"commit_tail_length"`
	MaintenanceDrainMS  int64     `json:"maintenance_drain_ms"`
	LegacyManifest      int64     `json:"legacy_manifest_version"`
	LegacyEntities      int       `json:"legacy_entities"`
	IndexCatalogVersion int64     `json:"index_catalog_version"`
	LegacyMirrorLag     int64     `json:"legacy_mirror_lag"`
	LegacyOutboxBacklog int64     `json:"legacy_outbox_backlog"`
	DerivedTaskBacklog  int64     `json:"derived_task_backlog"`
	LegacyV1Tag         string    `json:"legacy_v1_tag,omitempty"`
	LegacyV1Validated   bool      `json:"legacy_v1_binary_validated"`
}

func writeCASStressReport(t *testing.T, report casStressReport) {
	t.Helper()
	path := os.Getenv("GRAPHDB_TEST_CAS_STRESS_REPORT")
	if path == "" {
		return
	}
	if report.Commit == "" {
		report.Commit = firstNonEmptyEnvironment(
			"GRAPHDB_TEST_BUILD_COMMIT",
			"GITHUB_SHA",
			"CI_COMMIT_SHA",
		)
	}
	if report.GeneratedAt.IsZero() {
		report.GeneratedAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal CAS stress report: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create CAS stress report directory: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write CAS stress report: %v", err)
	}
	t.Logf("PostgreSQL/S3 CAS stress report: %s", path)
}

func firstNonEmptyEnvironment(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return "unknown"
}
