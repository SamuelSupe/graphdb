// Package backup defines tenant backup policy validation, backup manifests,
// and backup/restore integrity report contracts.
package backup

import (
	"fmt"
	"time"
)

const ManifestFormat = "graphdb-tenant-backup-manifest-v1"

type TenantBackupConfig struct {
	Enabled                     *bool  `json:"enabled,omitempty"`
	IntervalSeconds             *int64 `json:"interval_seconds,omitempty"`
	KeepCount                   *int   `json:"keep_count,omitempty"`
	MaxAgeSeconds               *int64 `json:"max_age_seconds,omitempty"`
	RetryInitialSeconds         *int64 `json:"retry_initial_seconds,omitempty"`
	RetryMaxSeconds             *int64 `json:"retry_max_seconds,omitempty"`
	RestoreDrillIntervalSeconds *int64 `json:"restore_drill_interval_seconds,omitempty"`
}

func DefaultPolicy(c TenantBackupConfig) TenantBackupConfig {
	enabled := false
	interval := int64(86400)
	keepCount := 30
	maxAge := int64(30 * 86400)
	retryInitial := int64(60)
	retryMax := int64(3600)
	drillInterval := int64(0)

	if c.Enabled == nil {
		c.Enabled = &enabled
	}
	if c.IntervalSeconds == nil {
		c.IntervalSeconds = &interval
	}
	if c.KeepCount == nil {
		c.KeepCount = &keepCount
	}
	if c.MaxAgeSeconds == nil {
		c.MaxAgeSeconds = &maxAge
	}
	if c.RetryInitialSeconds == nil {
		c.RetryInitialSeconds = &retryInitial
	}
	if c.RetryMaxSeconds == nil {
		c.RetryMaxSeconds = &retryMax
	}
	if c.RestoreDrillIntervalSeconds == nil {
		c.RestoreDrillIntervalSeconds = &drillInterval
	}
	return c
}

func ValidatePolicy(c TenantBackupConfig) error {
	c = DefaultPolicy(c)
	if *c.IntervalSeconds < 60 || *c.IntervalSeconds > 365*86400 {
		return fmt.Errorf("backup.interval_seconds must be between 60 and 31536000")
	}
	if *c.KeepCount < 0 || *c.KeepCount > 1000 {
		return fmt.Errorf("backup.keep_count must be between 0 and 1000")
	}
	for name, value := range map[string]int64{"max_age_seconds": *c.MaxAgeSeconds, "restore_drill_interval_seconds": *c.RestoreDrillIntervalSeconds} {
		if value < 0 || value > 10*365*86400 {
			return fmt.Errorf("backup.%s must be between 0 and 315360000", name)
		}
	}
	if *c.RetryInitialSeconds < 1 || *c.RetryMaxSeconds < *c.RetryInitialSeconds || *c.RetryMaxSeconds > 86400 {
		return fmt.Errorf("backup retry delay must satisfy 1 <= initial <= max <= 86400 seconds")
	}
	return nil
}

type TenantBackupManifest struct {
	Format          string              `json:"format"`
	TenantID        string              `json:"tenant_id"`
	BackupID        string              `json:"backup_id"`
	Version         int64               `json:"version"`
	CreatedAt       time.Time           `json:"created_at"`
	BackupRecordKey string              `json:"backup_record_key"`
	Objects         []BackupObjectRef   `json:"objects,omitempty"`
	Stats           BackupManifestStats `json:"stats"`
}

type BackupObjectRef struct {
	Role         string `json:"role"`
	Key          string `json:"key"`
	Kind         string `json:"kind,omitempty"`
	Field        string `json:"field,omitempty"`
	RelationType string `json:"relation_type,omitempty"`
	Shard        string `json:"shard,omitempty"`
	IndexType    string `json:"index_type,omitempty"`
	Bytes        int64  `json:"bytes,omitempty"`
	ETag         string `json:"etag,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
	RowCount     int    `json:"row_count,omitempty"`
	ContentHash  string `json:"content_hash,omitempty"`
	SchemaHash   string `json:"schema_hash,omitempty"`
	Required     bool   `json:"required,omitempty"`
}

type BackupManifestStats struct {
	ObjectCount int   `json:"object_count"`
	TotalBytes  int64 `json:"total_bytes"`
	Entities    int   `json:"entities"`
	Edges       int   `json:"edges"`
}

type BackupIntegrityReport struct {
	Status      string    `json:"status"`
	CheckedAt   time.Time `json:"checked_at"`
	Objects     int       `json:"objects"`
	Bytes       int64     `json:"bytes"`
	ManifestKey string    `json:"manifest_key,omitempty"`
	Issues      []string  `json:"issues,omitempty"`
}

type RestoreIntegrityReport struct {
	Status              string    `json:"status"`
	CheckedAt           time.Time `json:"checked_at"`
	ManifestVersion     int64     `json:"manifest_version,omitempty"`
	SnapshotVersion     int64     `json:"snapshot_version,omitempty"`
	IndexCatalogVersion int64     `json:"index_catalog_version,omitempty"`
	Issues              []string  `json:"issues,omitempty"`
}
