package storage

import (
	"context"
	"sort"
	"strings"
	"time"
)

type TenantUsageReport struct {
	TenantID          string                `json:"tenant_id"`
	Prefix            string                `json:"prefix"`
	CheckedAt         time.Time             `json:"checked_at"`
	Cached            bool                  `json:"cached,omitempty"`
	Stale             bool                  `json:"stale,omitempty"`
	CacheAgeMS        int64                 `json:"cache_age_ms,omitempty"`
	StaleReason       string                `json:"stale_reason,omitempty"`
	ManifestVersion   int64                 `json:"manifest_version,omitempty"`
	SnapshotVersion   int64                 `json:"snapshot_version,omitempty"`
	CommitTailLength  int                   `json:"commit_tail_length"`
	ObjectCount       int                   `json:"object_count"`
	TotalBytes        int64                 `json:"total_bytes"`
	Categories        []TenantUsageCategory `json:"categories"`
	Retention         TenantRetentionStatus `json:"retention"`
	TenantConfigFound bool                  `json:"tenant_config_found"`
}

type TenantUsageCategory struct {
	Name        string `json:"name"`
	ObjectCount int    `json:"object_count"`
	Bytes       int64  `json:"bytes"`
}

type TenantRetentionStatus struct {
	KeepSnapshots           int   `json:"keep_snapshots"`
	DeadLetterMaxAgeSeconds int64 `json:"deadletter_max_age_seconds,omitempty"`
	TaskMaxAgeSeconds       int64 `json:"task_max_age_seconds,omitempty"`
	CleanupIndexOrphans     bool  `json:"cleanup_index_orphans"`
}

func (s *TenantStore) TenantUsage(ctx context.Context, tenantID string) (TenantUsageReport, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return TenantUsageReport{}, err
	}
	prefix := s.tenantObjectPrefix(tenantID)
	report := TenantUsageReport{
		TenantID:  tenantID,
		Prefix:    prefix,
		CheckedAt: time.Now().UTC(),
	}
	categories := map[string]*TenantUsageCategory{}
	err := scanObjectPrefix(ctx, s.Objects, prefix, func(objects []ObjectInfo) error {
		for _, object := range objects {
			name := tenantUsageCategory(prefix, object.Key)
			category := categories[name]
			if category == nil {
				category = &TenantUsageCategory{Name: name}
				categories[name] = category
			}
			category.ObjectCount++
			category.Bytes += object.Size
			report.ObjectCount++
			report.TotalBytes += object.Size
		}
		return nil
	})
	if err != nil {
		return TenantUsageReport{}, err
	}
	report.Categories = sortedUsageCategories(categories)
	if manifest, _, err := s.getManifest(ctx, tenantID); err == nil {
		report.ManifestVersion = manifest.Version
		report.SnapshotVersion = manifest.SnapshotVersion
		report.CommitTailLength = manifestCommitTailLength(manifest)
	}
	if config, ok, err := s.GetTenantConfig(ctx, tenantID); err != nil {
		return TenantUsageReport{}, err
	} else {
		report.TenantConfigFound = ok
		report.Retention = retentionStatusFromConfig(config.Maintenance)
	}
	if s.Backpressure != nil {
		s.Backpressure.RecordTenantUsage(tenantID, report.ObjectCount, report.TotalBytes)
	}
	return report, nil
}

func tenantUsageCategory(prefix string, key string) string {
	rest := strings.TrimPrefix(key, prefix)
	switch {
	case rest == "manifest.parquet":
		return "manifest"
	case rest == "metadata.parquet":
		return "metadata"
	case strings.HasPrefix(rest, "config/"):
		return "config"
	case strings.HasPrefix(rest, "commits/"):
		return "commits"
	case strings.HasPrefix(rest, "snapshots/"):
		return "snapshots"
	case strings.HasPrefix(rest, "indexes/tasks/"):
		return "index_tasks"
	case strings.HasPrefix(rest, "indexes/"):
		return "indexes"
	case strings.HasPrefix(rest, "ingest/") && strings.Contains(rest, "/deadletters/"):
		return "deadletters"
	case strings.HasPrefix(rest, "ingest/") && strings.Contains(rest, "/batches/"):
		return "ingest_batches"
	case strings.HasPrefix(rest, "ingest/") && strings.Contains(rest, "/idempotency/"):
		return "ingest_idempotency"
	case strings.HasPrefix(rest, "ingest/") && strings.Contains(rest, "/collectors/"):
		return "ingest_collectors"
	case strings.HasPrefix(rest, "ingest/"):
		return "ingest"
	case strings.HasPrefix(rest, "queries/"):
		return "saved_queries"
	case strings.HasPrefix(rest, "tasks/results/"):
		return "task_results"
	case strings.HasPrefix(rest, "tasks/"):
		return "tasks"
	case strings.HasPrefix(rest, "idempotency/"):
		return "commit_idempotency"
	case strings.HasPrefix(rest, "control/readers/"):
		return "reader_heartbeats"
	case strings.HasPrefix(rest, "control/"):
		return "control"
	default:
		return "other"
	}
}

func sortedUsageCategories(categories map[string]*TenantUsageCategory) []TenantUsageCategory {
	names := make([]string, 0, len(categories))
	for name := range categories {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]TenantUsageCategory, 0, len(names))
	for _, name := range names {
		result = append(result, *categories[name])
	}
	return result
}

func retentionStatusFromConfig(config TenantMaintenanceConfig) TenantRetentionStatus {
	return TenantRetentionStatus{
		KeepSnapshots:           intValue(config.KeepSnapshots, 1),
		DeadLetterMaxAgeSeconds: int64Value(config.DeadLetterMaxAgeSeconds, 0),
		TaskMaxAgeSeconds:       int64Value(config.TaskMaxAgeSeconds, 0),
		CleanupIndexOrphans:     boolPtrValue(config.CleanupIndexOrphans, true),
	}
}

func intValue(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

func int64Value(value *int64, fallback int64) int64 {
	if value == nil {
		return fallback
	}
	return *value
}

func boolPtrValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
