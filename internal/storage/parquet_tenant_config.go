package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const tenantConfigCodecParquet = "tenant-config-arrow-parquet-v1"

const (
	parquetTenantConfigColumnTenantID = iota
	parquetTenantConfigColumnContentHash
	parquetTenantConfigColumnSettingCount
	parquetTenantConfigColumnSetting
	parquetTenantConfigColumnValueKind
	parquetTenantConfigColumnIntValue
	parquetTenantConfigColumnBoolValue
)

const (
	tenantConfigKindInt64 = "int64"
	tenantConfigKindBool  = "bool"
)

type tenantConfigEntry struct {
	Setting   string
	ValueKind string
	IntValue  int64
	BoolValue bool
}

func marshalParquetTenantConfig(ctx context.Context, record tenantConfigRecord) ([]byte, error) {
	if err := validateTenantConfig(record.Config); err != nil {
		return nil, err
	}
	entries := tenantConfigEntries(record.Config)
	hash, err := tenantConfigContentHash(record)
	if err != nil {
		return nil, err
	}
	schema := parquetTenantConfigArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendRow := func(entry tenantConfigEntry) {
		builder.Field(parquetTenantConfigColumnTenantID).(*array.StringBuilder).Append(record.TenantID)
		builder.Field(parquetTenantConfigColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetTenantConfigColumnSettingCount).(*array.Int64Builder).Append(int64(len(entries)))
		builder.Field(parquetTenantConfigColumnSetting).(*array.StringBuilder).Append(entry.Setting)
		builder.Field(parquetTenantConfigColumnValueKind).(*array.StringBuilder).Append(entry.ValueKind)
		builder.Field(parquetTenantConfigColumnIntValue).(*array.Int64Builder).Append(entry.IntValue)
		builder.Field(parquetTenantConfigColumnBoolValue).(*array.BooleanBuilder).Append(entry.BoolValue)
	}
	if len(entries) == 0 {
		appendRow(tenantConfigEntry{})
	} else {
		for _, entry := range entries {
			appendRow(entry)
		}
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, 1, writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetTenantConfig(ctx context.Context, data []byte) (tenantConfigRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return tenantConfigRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return tenantConfigRecord{}, fmt.Errorf("parquet tenant config is empty")
	}
	if table.NumCols() < int64(parquetTenantConfigColumnBoolValue+1) {
		return tenantConfigRecord{}, fmt.Errorf("parquet tenant config has %d columns, want at least %d", table.NumCols(), parquetTenantConfigColumnBoolValue+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	var record tenantConfigRecord
	var expectedHash string
	var settingCount int64
	seen := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		tenantColumn, err := parquetStringColumn(batch, parquetTenantConfigColumnTenantID, "tenant_id")
		if err != nil {
			return tenantConfigRecord{}, err
		}
		hashColumn, err := parquetStringColumn(batch, parquetTenantConfigColumnContentHash, "content_hash")
		if err != nil {
			return tenantConfigRecord{}, err
		}
		countColumn, err := parquetInt64Column(batch, parquetTenantConfigColumnSettingCount, "setting_count")
		if err != nil {
			return tenantConfigRecord{}, err
		}
		settingColumn, err := parquetStringColumn(batch, parquetTenantConfigColumnSetting, "setting")
		if err != nil {
			return tenantConfigRecord{}, err
		}
		kindColumn, err := parquetStringColumn(batch, parquetTenantConfigColumnValueKind, "value_kind")
		if err != nil {
			return tenantConfigRecord{}, err
		}
		intColumn, err := parquetInt64Column(batch, parquetTenantConfigColumnIntValue, "int_value")
		if err != nil {
			return tenantConfigRecord{}, err
		}
		boolColumn, err := parquetBoolColumn(batch, parquetTenantConfigColumnBoolValue, "bool_value")
		if err != nil {
			return tenantConfigRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			if seen == 0 {
				record.TenantID = tenantColumn.Value(i)
				expectedHash = hashColumn.Value(i)
				settingCount = countColumn.Value(i)
			}
			if record.TenantID != tenantColumn.Value(i) || expectedHash != hashColumn.Value(i) || settingCount != countColumn.Value(i) {
				return tenantConfigRecord{}, fmt.Errorf("tenant config identity mismatch")
			}
			if settingColumn.Value(i) != "" {
				if err := applyTenantConfigEntry(&record.Config, tenantConfigEntry{
					Setting:   settingColumn.Value(i),
					ValueKind: kindColumn.Value(i),
					IntValue:  intColumn.Value(i),
					BoolValue: boolColumn.Value(i),
				}); err != nil {
					return tenantConfigRecord{}, err
				}
			}
			seen++
		}
	}
	if int64(len(tenantConfigEntries(record.Config))) != settingCount {
		return tenantConfigRecord{}, fmt.Errorf("tenant config setting count mismatch")
	}
	hash, err := tenantConfigContentHash(record)
	if err != nil {
		return tenantConfigRecord{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return tenantConfigRecord{}, fmt.Errorf("tenant config content hash mismatch")
	}
	return record, validateTenantConfig(record.Config)
}

func parquetTenantConfigArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "setting_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "setting", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "int_value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
}

func tenantConfigContentHash(record tenantConfigRecord) (string, error) {
	if err := validateTenantConfig(record.Config); err != nil {
		return "", err
	}
	entries := tenantConfigEntries(record.Config)
	parts := []string{record.TenantID, formatInt64ForHash(int64(len(entries)))}
	for _, entry := range entries {
		parts = append(parts, entry.Setting, entry.ValueKind, formatInt64ForHash(entry.IntValue), formatBoolForHash(entry.BoolValue))
	}
	return parquetScalarContentHash(parts...), nil
}

func tenantConfigEntries(config TenantConfig) []tenantConfigEntry {
	var entries []tenantConfigEntry
	appendInt64 := func(setting string, value *int64) {
		if value != nil {
			entries = append(entries, tenantConfigEntry{Setting: setting, ValueKind: tenantConfigKindInt64, IntValue: *value})
		}
	}
	appendInt := func(setting string, value *int) {
		if value != nil {
			entries = append(entries, tenantConfigEntry{Setting: setting, ValueKind: tenantConfigKindInt64, IntValue: int64(*value)})
		}
	}
	appendBool := func(setting string, value *bool) {
		if value != nil {
			entries = append(entries, tenantConfigEntry{Setting: setting, ValueKind: tenantConfigKindBool, BoolValue: *value})
		}
	}

	appendInt64("backpressure.object_latency_threshold_ms", config.Backpressure.ObjectLatencyThresholdMS)
	appendInt64("backpressure.cas_conflict_window_ms", config.Backpressure.CASConflictWindowMS)
	appendInt("backpressure.cas_conflict_threshold", config.Backpressure.CASConflictThreshold)
	appendInt("backpressure.max_commit_tail", config.Backpressure.MaxCommitTail)
	appendInt64("backpressure.retry_after_ms", config.Backpressure.RetryAfterMS)
	appendInt("quota.max_entities_per_tenant", config.Quota.MaxEntitiesPerTenant)
	appendInt("quota.max_edges_per_tenant", config.Quota.MaxEdgesPerTenant)
	appendBool("maintenance.auto_compact", config.Maintenance.AutoCompact)
	appendInt("maintenance.compact_commit_tail_threshold", config.Maintenance.CompactCommitTailThreshold)
	appendInt("maintenance.compact_object_count_threshold", config.Maintenance.CompactObjectCountThreshold)
	appendInt64("maintenance.compact_bytes_threshold", config.Maintenance.CompactBytesThreshold)
	appendInt("maintenance.small_file_object_threshold", config.Maintenance.SmallFileObjectThreshold)
	appendInt64("maintenance.small_file_bytes_threshold", config.Maintenance.SmallFileBytesThreshold)
	appendInt("maintenance.entity_page_split_threshold", config.Maintenance.EntityPageSplitThreshold)
	appendInt("maintenance.entity_page_merge_threshold", config.Maintenance.EntityPageMergeThreshold)
	appendInt("maintenance.edge_shard_split_threshold", config.Maintenance.EdgeShardSplitThreshold)
	appendInt("maintenance.edge_shard_merge_threshold", config.Maintenance.EdgeShardMergeThreshold)
	appendInt64("maintenance.gc_interval_seconds", config.Maintenance.GCIntervalSeconds)
	appendInt("maintenance.keep_snapshots", config.Maintenance.KeepSnapshots)
	appendInt64("maintenance.deadletter_max_age_seconds", config.Maintenance.DeadLetterMaxAgeSeconds)
	appendInt64("maintenance.task_max_age_seconds", config.Maintenance.TaskMaxAgeSeconds)
	appendBool("maintenance.cleanup_index_orphans", config.Maintenance.CleanupIndexOrphans)
	appendBool("indexes.auto_rebuild", config.Indexes.AutoRebuild)
	appendBool("indexes.rebuild_on_stale", config.Indexes.RebuildOnStale)
	return entries
}

func applyTenantConfigEntry(config *TenantConfig, entry tenantConfigEntry) error {
	if err := validateTenantConfigEntryKind(entry); err != nil {
		return err
	}
	intValue := int(entry.IntValue)
	int64Value := entry.IntValue
	boolValue := entry.BoolValue
	switch entry.Setting {
	case "backpressure.object_latency_threshold_ms":
		config.Backpressure.ObjectLatencyThresholdMS = &int64Value
	case "backpressure.cas_conflict_window_ms":
		config.Backpressure.CASConflictWindowMS = &int64Value
	case "backpressure.cas_conflict_threshold":
		config.Backpressure.CASConflictThreshold = &intValue
	case "backpressure.max_commit_tail":
		config.Backpressure.MaxCommitTail = &intValue
	case "backpressure.retry_after_ms":
		config.Backpressure.RetryAfterMS = &int64Value
	case "quota.max_entities_per_tenant":
		config.Quota.MaxEntitiesPerTenant = &intValue
	case "quota.max_edges_per_tenant":
		config.Quota.MaxEdgesPerTenant = &intValue
	case "maintenance.auto_compact":
		config.Maintenance.AutoCompact = &boolValue
	case "maintenance.compact_commit_tail_threshold":
		config.Maintenance.CompactCommitTailThreshold = &intValue
	case "maintenance.compact_object_count_threshold":
		config.Maintenance.CompactObjectCountThreshold = &intValue
	case "maintenance.compact_bytes_threshold":
		config.Maintenance.CompactBytesThreshold = &int64Value
	case "maintenance.small_file_object_threshold":
		config.Maintenance.SmallFileObjectThreshold = &intValue
	case "maintenance.small_file_bytes_threshold":
		config.Maintenance.SmallFileBytesThreshold = &int64Value
	case "maintenance.entity_page_split_threshold":
		config.Maintenance.EntityPageSplitThreshold = &intValue
	case "maintenance.entity_page_merge_threshold":
		config.Maintenance.EntityPageMergeThreshold = &intValue
	case "maintenance.edge_shard_split_threshold":
		config.Maintenance.EdgeShardSplitThreshold = &intValue
	case "maintenance.edge_shard_merge_threshold":
		config.Maintenance.EdgeShardMergeThreshold = &intValue
	case "maintenance.gc_interval_seconds":
		config.Maintenance.GCIntervalSeconds = &int64Value
	case "maintenance.keep_snapshots":
		config.Maintenance.KeepSnapshots = &intValue
	case "maintenance.deadletter_max_age_seconds":
		config.Maintenance.DeadLetterMaxAgeSeconds = &int64Value
	case "maintenance.task_max_age_seconds":
		config.Maintenance.TaskMaxAgeSeconds = &int64Value
	case "maintenance.cleanup_index_orphans":
		config.Maintenance.CleanupIndexOrphans = &boolValue
	case "indexes.auto_rebuild":
		config.Indexes.AutoRebuild = &boolValue
	case "indexes.rebuild_on_stale":
		config.Indexes.RebuildOnStale = &boolValue
	default:
		return fmt.Errorf("unknown tenant config setting %q", entry.Setting)
	}
	return nil
}

func validateTenantConfigEntryKind(entry tenantConfigEntry) error {
	switch entry.Setting {
	case "maintenance.auto_compact", "maintenance.cleanup_index_orphans", "indexes.auto_rebuild", "indexes.rebuild_on_stale":
		if entry.ValueKind != tenantConfigKindBool {
			return fmt.Errorf("tenant config setting %q has kind %q, want bool", entry.Setting, entry.ValueKind)
		}
	default:
		if entry.ValueKind != tenantConfigKindInt64 {
			return fmt.Errorf("tenant config setting %q has kind %q, want int64", entry.Setting, entry.ValueKind)
		}
	}
	return nil
}
