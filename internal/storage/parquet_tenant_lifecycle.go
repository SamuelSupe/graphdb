package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	tenantMetadataCodecParquet = "tenant-metadata-arrow-parquet-v1"
	tenantRegistryCodecParquet = "tenant-registry-arrow-parquet-v1"
)

const (
	parquetTenantMetadataColumnTenantID = iota
	parquetTenantMetadataColumnStatus
	parquetTenantMetadataColumnName
	parquetTenantMetadataColumnDescription
	parquetTenantMetadataColumnClonedFrom
	parquetTenantMetadataColumnCreatedAt
	parquetTenantMetadataColumnUpdatedAt
	parquetTenantMetadataColumnDisabledAt
	parquetTenantMetadataColumnDeletedAt
	parquetTenantMetadataColumnContentHash
	parquetTenantMetadataColumnLabelCount
	parquetTenantMetadataColumnMetadataCount
	parquetTenantMetadataColumnRowKind
	parquetTenantMetadataColumnOrdinal
	parquetTenantMetadataColumnEntryKey
	parquetTenantMetadataColumnValueKind
	parquetTenantMetadataColumnStringValue
	parquetTenantMetadataColumnBoolValue
	parquetTenantMetadataColumnFloatValue
)

const (
	tenantMetadataRowMetadata = "metadata"
	tenantMetadataRowLabel    = "label"
	tenantMetadataRowValue    = "value"
)

const (
	tenantMetadataKindNull   = "null"
	tenantMetadataKindString = "string"
	tenantMetadataKindBool   = "bool"
	tenantMetadataKindNumber = "number"
	tenantMetadataKindRaw    = "raw"
)

const (
	parquetTenantRegistryColumnCount = iota
	parquetTenantRegistryColumnUpdatedAt
	parquetTenantRegistryColumnContentHash
	parquetTenantRegistryColumnOrdinal
	parquetTenantRegistryColumnTenantID
)

func marshalParquetTenantMetadata(ctx context.Context, metadata TenantMetadata) ([]byte, error) {
	normalized, err := normalizeTenantMetadataForParquet(metadata)
	if err != nil {
		return nil, err
	}
	entries, err := tenantMetadataEntries(normalized)
	if err != nil {
		return nil, err
	}
	hash, err := tenantMetadataContentHash(normalized)
	if err != nil {
		return nil, err
	}
	schema := parquetTenantMetadataArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendRow := func(entry tenantMetadataEntry) {
		builder.Field(parquetTenantMetadataColumnTenantID).(*array.StringBuilder).Append(normalized.TenantID)
		builder.Field(parquetTenantMetadataColumnStatus).(*array.StringBuilder).Append(normalized.Status)
		builder.Field(parquetTenantMetadataColumnName).(*array.StringBuilder).Append(normalized.Name)
		builder.Field(parquetTenantMetadataColumnDescription).(*array.StringBuilder).Append(normalized.Description)
		builder.Field(parquetTenantMetadataColumnClonedFrom).(*array.StringBuilder).Append(normalized.ClonedFrom)
		builder.Field(parquetTenantMetadataColumnCreatedAt).(*array.StringBuilder).Append(formatParquetTime(normalized.CreatedAt))
		builder.Field(parquetTenantMetadataColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(normalized.UpdatedAt))
		builder.Field(parquetTenantMetadataColumnDisabledAt).(*array.StringBuilder).Append(formatParquetTime(normalized.DisabledAt))
		builder.Field(parquetTenantMetadataColumnDeletedAt).(*array.StringBuilder).Append(formatParquetTime(normalized.DeletedAt))
		builder.Field(parquetTenantMetadataColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetTenantMetadataColumnLabelCount).(*array.Int64Builder).Append(int64(len(normalized.Labels)))
		builder.Field(parquetTenantMetadataColumnMetadataCount).(*array.Int64Builder).Append(int64(len(normalized.Metadata)))
		builder.Field(parquetTenantMetadataColumnRowKind).(*array.StringBuilder).Append(entry.RowKind)
		builder.Field(parquetTenantMetadataColumnOrdinal).(*array.Int64Builder).Append(int64(entry.Ordinal))
		builder.Field(parquetTenantMetadataColumnEntryKey).(*array.StringBuilder).Append(entry.Key)
		builder.Field(parquetTenantMetadataColumnValueKind).(*array.StringBuilder).Append(entry.ValueKind)
		builder.Field(parquetTenantMetadataColumnStringValue).(*array.StringBuilder).Append(entry.StringValue)
		builder.Field(parquetTenantMetadataColumnBoolValue).(*array.BooleanBuilder).Append(entry.BoolValue)
		builder.Field(parquetTenantMetadataColumnFloatValue).(*array.Float64Builder).Append(entry.FloatValue)
	}
	if len(entries) == 0 {
		appendRow(tenantMetadataEntry{RowKind: tenantMetadataRowMetadata})
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

func decodeParquetTenantMetadata(ctx context.Context, data []byte) (TenantMetadata, error) {
	table, release, err := readParquetTable(ctx, data)
	if err != nil {
		return TenantMetadata{}, err
	}
	defer release()
	defer table.Release()
	if table.NumRows() < 1 {
		return TenantMetadata{}, fmt.Errorf("parquet tenant metadata is empty")
	}
	if table.NumCols() < int64(parquetTenantMetadataColumnFloatValue+1) {
		return TenantMetadata{}, fmt.Errorf("parquet tenant metadata has %d columns, want at least %d", table.NumCols(), parquetTenantMetadataColumnFloatValue+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	var metadata TenantMetadata
	var expectedHash string
	var labelCount int64
	var metadataCount int64
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetTenantMetadataColumns(batch)
		if err != nil {
			return TenantMetadata{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowMetadata := TenantMetadata{
				TenantID:    columns.tenantID.Value(i),
				Status:      normalizeTenantStatus(columns.status.Value(i)),
				Name:        columns.name.Value(i),
				Description: columns.description.Value(i),
				ClonedFrom:  columns.clonedFrom.Value(i),
				CreatedAt:   parseParquetTime(columns.createdAt.Value(i)),
				UpdatedAt:   parseParquetTime(columns.updatedAt.Value(i)),
				DisabledAt:  parseParquetTime(columns.disabledAt.Value(i)),
				DeletedAt:   parseParquetTime(columns.deletedAt.Value(i)),
			}
			if rows == 0 {
				metadata = rowMetadata
				expectedHash = columns.contentHash.Value(i)
				labelCount = columns.labelCount.Value(i)
				metadataCount = columns.metadataCount.Value(i)
			} else if !sameTenantMetadataBase(metadata, rowMetadata) ||
				expectedHash != columns.contentHash.Value(i) ||
				labelCount != columns.labelCount.Value(i) ||
				metadataCount != columns.metadataCount.Value(i) {
				return TenantMetadata{}, fmt.Errorf("tenant metadata identity mismatch")
			}
			entry := tenantMetadataEntryFromColumns(columns, i)
			if err := applyTenantMetadataEntry(&metadata, entry); err != nil {
				return TenantMetadata{}, err
			}
			rows++
		}
	}
	if int64(len(metadata.Labels)) != labelCount || int64(len(metadata.Metadata)) != metadataCount {
		return TenantMetadata{}, fmt.Errorf("tenant metadata entry count mismatch")
	}
	hash, err := tenantMetadataContentHash(metadata)
	if err != nil {
		return TenantMetadata{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return TenantMetadata{}, fmt.Errorf("tenant metadata content hash mismatch")
	}
	return metadata, nil
}

func marshalParquetTenantRegistry(ctx context.Context, registry tenantRegistry) ([]byte, error) {
	hash := tenantRegistryContentHash(registry)
	schema := parquetTenantRegistryArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendRow := func(ordinal int, tenantID string) {
		builder.Field(parquetTenantRegistryColumnCount).(*array.Int64Builder).Append(int64(len(registry.TenantIDs)))
		builder.Field(parquetTenantRegistryColumnUpdatedAt).(*array.StringBuilder).Append(registry.UpdatedAt)
		builder.Field(parquetTenantRegistryColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetTenantRegistryColumnOrdinal).(*array.Int64Builder).Append(int64(ordinal))
		builder.Field(parquetTenantRegistryColumnTenantID).(*array.StringBuilder).Append(tenantID)
	}
	if len(registry.TenantIDs) == 0 {
		appendRow(0, "")
	} else {
		for i, tenantID := range registry.TenantIDs {
			appendRow(i, tenantID)
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

func decodeParquetTenantRegistry(ctx context.Context, data []byte) (tenantRegistry, error) {
	table, release, err := readParquetTable(ctx, data)
	if err != nil {
		return tenantRegistry{}, err
	}
	defer release()
	defer table.Release()
	if table.NumRows() < 1 {
		return tenantRegistry{}, fmt.Errorf("parquet tenant registry has %d rows, want at least 1", table.NumRows())
	}
	if table.NumCols() < int64(parquetTenantRegistryColumnTenantID+1) {
		return tenantRegistry{}, fmt.Errorf("parquet tenant registry has %d columns, want at least %d", table.NumCols(), parquetTenantRegistryColumnTenantID+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()
	var registry tenantRegistry
	var count int64
	var expectedHash string
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		countColumn, err := parquetInt64Column(batch, parquetTenantRegistryColumnCount, "count")
		if err != nil {
			return tenantRegistry{}, err
		}
		updatedAtColumn, err := parquetStringColumn(batch, parquetTenantRegistryColumnUpdatedAt, "updated_at")
		if err != nil {
			return tenantRegistry{}, err
		}
		hashColumn, err := parquetStringColumn(batch, parquetTenantRegistryColumnContentHash, "content_hash")
		if err != nil {
			return tenantRegistry{}, err
		}
		ordinalColumn, err := parquetInt64Column(batch, parquetTenantRegistryColumnOrdinal, "ordinal")
		if err != nil {
			return tenantRegistry{}, err
		}
		tenantIDColumn, err := parquetStringColumn(batch, parquetTenantRegistryColumnTenantID, "tenant_id")
		if err != nil {
			return tenantRegistry{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			if rows == 0 {
				count = countColumn.Value(i)
				registry.UpdatedAt = updatedAtColumn.Value(i)
				expectedHash = hashColumn.Value(i)
			}
			if count != countColumn.Value(i) || registry.UpdatedAt != updatedAtColumn.Value(i) || expectedHash != hashColumn.Value(i) {
				return tenantRegistry{}, fmt.Errorf("tenant registry identity mismatch")
			}
			tenantID := tenantIDColumn.Value(i)
			if tenantID != "" {
				if int64(len(registry.TenantIDs)) != ordinalColumn.Value(i) {
					return tenantRegistry{}, fmt.Errorf("tenant registry ordinal mismatch")
				}
				registry.TenantIDs = append(registry.TenantIDs, tenantID)
			}
			rows++
		}
	}

	if int64(len(registry.TenantIDs)) != count {
		return tenantRegistry{}, fmt.Errorf("tenant registry identity mismatch")
	}
	if expectedHash == "" || expectedHash != tenantRegistryContentHash(registry) {
		return tenantRegistry{}, fmt.Errorf("tenant registry content hash mismatch")
	}
	return registry, nil
}

type tenantMetadataEntry struct {
	RowKind     string
	Ordinal     int
	Key         string
	ValueKind   string
	StringValue string
	BoolValue   bool
	FloatValue  float64
}

type parquetTenantMetadataColumnSet struct {
	tenantID      *array.String
	status        *array.String
	name          *array.String
	description   *array.String
	clonedFrom    *array.String
	createdAt     *array.String
	updatedAt     *array.String
	disabledAt    *array.String
	deletedAt     *array.String
	contentHash   *array.String
	labelCount    *array.Int64
	metadataCount *array.Int64
	rowKind       *array.String
	ordinal       *array.Int64
	entryKey      *array.String
	valueKind     *array.String
	stringValue   *array.String
	boolValue     *array.Boolean
	floatValue    *array.Float64
}

func parquetTenantMetadataColumns(batch arrow.RecordBatch) (parquetTenantMetadataColumnSet, error) {
	var columns parquetTenantMetadataColumnSet
	var err error
	if columns.tenantID, err = parquetStringColumn(batch, parquetTenantMetadataColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.status, err = parquetStringColumn(batch, parquetTenantMetadataColumnStatus, "status"); err != nil {
		return columns, err
	}
	if columns.name, err = parquetStringColumn(batch, parquetTenantMetadataColumnName, "name"); err != nil {
		return columns, err
	}
	if columns.description, err = parquetStringColumn(batch, parquetTenantMetadataColumnDescription, "description"); err != nil {
		return columns, err
	}
	if columns.clonedFrom, err = parquetStringColumn(batch, parquetTenantMetadataColumnClonedFrom, "cloned_from"); err != nil {
		return columns, err
	}
	if columns.createdAt, err = parquetStringColumn(batch, parquetTenantMetadataColumnCreatedAt, "created_at"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetStringColumn(batch, parquetTenantMetadataColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	if columns.disabledAt, err = parquetStringColumn(batch, parquetTenantMetadataColumnDisabledAt, "disabled_at"); err != nil {
		return columns, err
	}
	if columns.deletedAt, err = parquetStringColumn(batch, parquetTenantMetadataColumnDeletedAt, "deleted_at"); err != nil {
		return columns, err
	}
	if columns.contentHash, err = parquetStringColumn(batch, parquetTenantMetadataColumnContentHash, "content_hash"); err != nil {
		return columns, err
	}
	if columns.labelCount, err = parquetInt64Column(batch, parquetTenantMetadataColumnLabelCount, "label_count"); err != nil {
		return columns, err
	}
	if columns.metadataCount, err = parquetInt64Column(batch, parquetTenantMetadataColumnMetadataCount, "metadata_count"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetStringColumn(batch, parquetTenantMetadataColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetInt64Column(batch, parquetTenantMetadataColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.entryKey, err = parquetStringColumn(batch, parquetTenantMetadataColumnEntryKey, "entry_key"); err != nil {
		return columns, err
	}
	if columns.valueKind, err = parquetStringColumn(batch, parquetTenantMetadataColumnValueKind, "value_kind"); err != nil {
		return columns, err
	}
	if columns.stringValue, err = parquetStringColumn(batch, parquetTenantMetadataColumnStringValue, "string_value"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetBoolColumn(batch, parquetTenantMetadataColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetFloat64Column(batch, parquetTenantMetadataColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	return columns, nil
}

func tenantMetadataEntryFromColumns(columns parquetTenantMetadataColumnSet, row int) tenantMetadataEntry {
	return tenantMetadataEntry{
		RowKind:     columns.rowKind.Value(row),
		Ordinal:     int(columns.ordinal.Value(row)),
		Key:         columns.entryKey.Value(row),
		ValueKind:   columns.valueKind.Value(row),
		StringValue: columns.stringValue.Value(row),
		BoolValue:   columns.boolValue.Value(row),
		FloatValue:  columns.floatValue.Value(row),
	}
}

func parquetTenantMetadataArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "cloned_from", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "disabled_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "deleted_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "label_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "metadata_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entry_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "float_value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}, nil)
}

func parquetTenantRegistryArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func tenantMetadataContentHash(metadata TenantMetadata) (string, error) {
	metadata, err := normalizeTenantMetadataForParquet(metadata)
	if err != nil {
		return "", err
	}
	entries, err := tenantMetadataEntries(metadata)
	if err != nil {
		return "", err
	}
	parts := []string{
		metadata.TenantID,
		normalizeTenantStatus(metadata.Status),
		metadata.Name,
		metadata.Description,
		metadata.ClonedFrom,
		formatParquetTime(metadata.CreatedAt),
		formatParquetTime(metadata.UpdatedAt),
		formatParquetTime(metadata.DisabledAt),
		formatParquetTime(metadata.DeletedAt),
		formatInt64ForHash(int64(len(metadata.Labels))),
		formatInt64ForHash(int64(len(metadata.Metadata))),
	}
	for _, entry := range entries {
		parts = append(parts,
			entry.RowKind,
			formatInt64ForHash(int64(entry.Ordinal)),
			entry.Key,
			entry.ValueKind,
			entry.StringValue,
			formatBoolForHash(entry.BoolValue),
			formatFloat64ForHash(entry.FloatValue),
		)
	}
	return parquetScalarContentHash(parts...), nil
}

func normalizeTenantMetadataForParquet(metadata TenantMetadata) (TenantMetadata, error) {
	metadata.Status = normalizeTenantStatus(metadata.Status)
	payload, err := json.Marshal(metadata)
	if err != nil {
		return TenantMetadata{}, err
	}
	var normalized TenantMetadata
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return TenantMetadata{}, err
	}
	normalized.Status = normalizeTenantStatus(normalized.Status)
	return normalized, nil
}

func tenantMetadataEntries(metadata TenantMetadata) ([]tenantMetadataEntry, error) {
	entries := []tenantMetadataEntry{}
	labelKeys := make([]string, 0, len(metadata.Labels))
	for key := range metadata.Labels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)
	for i, key := range labelKeys {
		entries = append(entries, tenantMetadataEntry{
			RowKind:     tenantMetadataRowLabel,
			Ordinal:     i,
			Key:         key,
			ValueKind:   tenantMetadataKindString,
			StringValue: metadata.Labels[key],
		})
	}

	metadataKeys := make([]string, 0, len(metadata.Metadata))
	for key := range metadata.Metadata {
		metadataKeys = append(metadataKeys, key)
	}
	sort.Strings(metadataKeys)
	for i, key := range metadataKeys {
		entry, err := tenantMetadataValueEntry(i, key, metadata.Metadata[key])
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func tenantMetadataValueEntry(ordinal int, key string, value any) (tenantMetadataEntry, error) {
	entry := tenantMetadataEntry{RowKind: tenantMetadataRowValue, Ordinal: ordinal, Key: key}
	switch typed := value.(type) {
	case nil:
		entry.ValueKind = tenantMetadataKindNull
	case string:
		entry.ValueKind = tenantMetadataKindString
		entry.StringValue = typed
	case bool:
		entry.ValueKind = tenantMetadataKindBool
		entry.BoolValue = typed
	case float64:
		entry.ValueKind = tenantMetadataKindNumber
		entry.FloatValue = typed
	case int:
		entry.ValueKind = tenantMetadataKindNumber
		entry.FloatValue = float64(typed)
	case int64:
		entry.ValueKind = tenantMetadataKindNumber
		entry.FloatValue = float64(typed)
	default:
		payload, err := json.Marshal(typed)
		if err != nil {
			return tenantMetadataEntry{}, err
		}
		entry.ValueKind = tenantMetadataKindRaw
		entry.StringValue = string(payload)
	}
	return entry, nil
}

func applyTenantMetadataEntry(metadata *TenantMetadata, entry tenantMetadataEntry) error {
	switch entry.RowKind {
	case tenantMetadataRowMetadata:
		return nil
	case tenantMetadataRowLabel:
		if len(metadata.Labels) != entry.Ordinal {
			return fmt.Errorf("tenant metadata label ordinal mismatch")
		}
		if metadata.Labels == nil {
			metadata.Labels = map[string]string{}
		}
		metadata.Labels[entry.Key] = entry.StringValue
	case tenantMetadataRowValue:
		if len(metadata.Metadata) != entry.Ordinal {
			return fmt.Errorf("tenant metadata value ordinal mismatch")
		}
		value, err := tenantMetadataValueFromEntry(entry)
		if err != nil {
			return err
		}
		if metadata.Metadata == nil {
			metadata.Metadata = map[string]any{}
		}
		metadata.Metadata[entry.Key] = value
	default:
		return fmt.Errorf("unknown tenant metadata row kind %q", entry.RowKind)
	}
	return nil
}

func tenantMetadataValueFromEntry(entry tenantMetadataEntry) (any, error) {
	switch entry.ValueKind {
	case tenantMetadataKindNull:
		return nil, nil
	case tenantMetadataKindString:
		return entry.StringValue, nil
	case tenantMetadataKindBool:
		return entry.BoolValue, nil
	case tenantMetadataKindNumber:
		return entry.FloatValue, nil
	case tenantMetadataKindRaw:
		var value any
		if err := json.Unmarshal([]byte(entry.StringValue), &value); err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown tenant metadata value kind %q", entry.ValueKind)
	}
}

func sameTenantMetadataBase(left TenantMetadata, right TenantMetadata) bool {
	return left.TenantID == right.TenantID &&
		normalizeTenantStatus(left.Status) == normalizeTenantStatus(right.Status) &&
		left.Name == right.Name &&
		left.Description == right.Description &&
		left.ClonedFrom == right.ClonedFrom &&
		formatParquetTime(left.CreatedAt) == formatParquetTime(right.CreatedAt) &&
		formatParquetTime(left.UpdatedAt) == formatParquetTime(right.UpdatedAt) &&
		formatParquetTime(left.DisabledAt) == formatParquetTime(right.DisabledAt) &&
		formatParquetTime(left.DeletedAt) == formatParquetTime(right.DeletedAt)
}

func tenantRegistryContentHash(registry tenantRegistry) string {
	parts := []string{registry.UpdatedAt, formatInt64ForHash(int64(len(registry.TenantIDs)))}
	parts = append(parts, registry.TenantIDs...)
	return parquetScalarContentHash(parts...)
}
