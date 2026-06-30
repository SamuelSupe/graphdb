package storage

import (
	"bytes"
	"context"
	"fmt"

	"graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const parquetEntityRecordCodec = "entity-record-arrow-parquet-v1"

const (
	parquetEntityRecordColumnTenantID = iota
	parquetEntityRecordColumnID
	parquetEntityRecordColumnPage
	parquetEntityRecordColumnOrdinal
	parquetEntityRecordColumnPageHash
	parquetEntityRecordColumnPageETag
	parquetEntityRecordColumnContentHash
	parquetEntityRecordColumnDeleted
	parquetEntityRecordColumnVersion
	parquetEntityRecordColumnUpdatedAt
	parquetEntityRecordColumnKind
	parquetEntityRecordColumnSource
	parquetEntityRecordColumnExternalID
	parquetEntityRecordColumnEntityVersion
	parquetEntityRecordColumnEntityCreatedAt
	parquetEntityRecordColumnEntityUpdatedAt
	parquetEntityRecordColumnConfidence
	parquetEntityRecordColumnSourceRank
	parquetEntityRecordColumnSplitFrom
	parquetEntityRecordColumnRowKind
	parquetEntityRecordColumnRowOrdinal
	parquetEntityRecordColumnEntryKey
	parquetEntityRecordColumnValueKind
	parquetEntityRecordColumnStringValue
	parquetEntityRecordColumnBoolValue
	parquetEntityRecordColumnFloatValue
	parquetEntityRecordColumnFieldSourceSource
	parquetEntityRecordColumnFieldSourcePriority
	parquetEntityRecordColumnFieldSourceConfidence
	parquetEntityRecordColumnFieldSourceVersion
	parquetEntityRecordColumnFieldSourceUpdatedAt
	parquetEntityRecordColumnEntitySourceSource
	parquetEntityRecordColumnEntitySourceExternalID
	parquetEntityRecordColumnEntitySourceConfidence
	parquetEntityRecordColumnEntitySourcePriority
	parquetEntityRecordColumnEntitySourceObservedAt
	parquetEntityRecordColumnEntitySourceStale
	parquetEntityRecordColumnEntitySourceStaleAt
)

func marshalParquetEntityRecord(ctx context.Context, record EntityRecord) ([]byte, error) {
	schema := parquetEntityRecordArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	rows, err := entityPageRows(record.Entity)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		builder.Field(parquetEntityRecordColumnTenantID).(*array.StringBuilder).Append(record.TenantID)
		builder.Field(parquetEntityRecordColumnID).(*array.StringBuilder).Append(record.ID)
		builder.Field(parquetEntityRecordColumnPage).(*array.StringBuilder).Append(record.Page)
		builder.Field(parquetEntityRecordColumnOrdinal).(*array.Int64Builder).Append(int64(record.Ordinal))
		builder.Field(parquetEntityRecordColumnPageHash).(*array.StringBuilder).Append(record.PageHash)
		builder.Field(parquetEntityRecordColumnPageETag).(*array.StringBuilder).Append(record.PageETag)
		builder.Field(parquetEntityRecordColumnContentHash).(*array.StringBuilder).Append(record.ContentHash)
		builder.Field(parquetEntityRecordColumnDeleted).(*array.BooleanBuilder).Append(record.Deleted)
		builder.Field(parquetEntityRecordColumnVersion).(*array.Int64Builder).Append(record.Version)
		builder.Field(parquetEntityRecordColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(record.UpdatedAt))
		builder.Field(parquetEntityRecordColumnKind).(*array.StringBuilder).Append(record.Entity.Kind)
		builder.Field(parquetEntityRecordColumnSource).(*array.StringBuilder).Append(record.Entity.Source)
		builder.Field(parquetEntityRecordColumnExternalID).(*array.StringBuilder).Append(record.Entity.ExternalID)
		builder.Field(parquetEntityRecordColumnEntityVersion).(*array.Int64Builder).Append(record.Entity.Version)
		builder.Field(parquetEntityRecordColumnEntityCreatedAt).(*array.StringBuilder).Append(formatParquetTime(record.Entity.CreatedAt))
		builder.Field(parquetEntityRecordColumnEntityUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(record.Entity.UpdatedAt))
		builder.Field(parquetEntityRecordColumnConfidence).(*array.Float64Builder).Append(record.Entity.Confidence)
		builder.Field(parquetEntityRecordColumnSourceRank).(*array.Int64Builder).Append(int64(record.Entity.SourceRank))
		builder.Field(parquetEntityRecordColumnSplitFrom).(*array.StringBuilder).Append(record.Entity.SplitFrom)
		builder.Field(parquetEntityRecordColumnRowKind).(*array.StringBuilder).Append(row.Kind)
		builder.Field(parquetEntityRecordColumnRowOrdinal).(*array.Int64Builder).Append(int64(row.Ordinal))
		builder.Field(parquetEntityRecordColumnEntryKey).(*array.StringBuilder).Append(row.Key)
		builder.Field(parquetEntityRecordColumnValueKind).(*array.StringBuilder).Append(row.Value.Kind)
		builder.Field(parquetEntityRecordColumnStringValue).(*array.StringBuilder).Append(row.Value.StringValue)
		builder.Field(parquetEntityRecordColumnBoolValue).(*array.BooleanBuilder).Append(row.Value.BoolValue)
		builder.Field(parquetEntityRecordColumnFloatValue).(*array.Float64Builder).Append(row.Value.FloatValue)
		builder.Field(parquetEntityRecordColumnFieldSourceSource).(*array.StringBuilder).Append(row.FieldSource.Source)
		builder.Field(parquetEntityRecordColumnFieldSourcePriority).(*array.Int64Builder).Append(int64(row.FieldSource.Priority))
		builder.Field(parquetEntityRecordColumnFieldSourceConfidence).(*array.Float64Builder).Append(row.FieldSource.Confidence)
		builder.Field(parquetEntityRecordColumnFieldSourceVersion).(*array.Int64Builder).Append(row.FieldSource.Version)
		builder.Field(parquetEntityRecordColumnFieldSourceUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(row.FieldSource.UpdatedAt))
		builder.Field(parquetEntityRecordColumnEntitySourceSource).(*array.StringBuilder).Append(row.EntitySource.Source)
		builder.Field(parquetEntityRecordColumnEntitySourceExternalID).(*array.StringBuilder).Append(row.EntitySource.ExternalID)
		builder.Field(parquetEntityRecordColumnEntitySourceConfidence).(*array.Float64Builder).Append(row.EntitySource.Confidence)
		builder.Field(parquetEntityRecordColumnEntitySourcePriority).(*array.Int64Builder).Append(int64(row.EntitySource.Priority))
		builder.Field(parquetEntityRecordColumnEntitySourceObservedAt).(*array.StringBuilder).Append(formatParquetTime(row.EntitySource.ObservedAt))
		builder.Field(parquetEntityRecordColumnEntitySourceStale).(*array.BooleanBuilder).Append(row.EntitySource.Stale)
		builder.Field(parquetEntityRecordColumnEntitySourceStaleAt).(*array.StringBuilder).Append(formatParquetTime(row.EntitySource.StaleAt))
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

func decodeParquetEntityRecord(ctx context.Context, data []byte, tenantID string, id string) (EntityRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return EntityRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return EntityRecord{}, fmt.Errorf("parquet entity record is empty")
	}
	if table.NumCols() < int64(parquetEntityRecordColumnEntitySourceStaleAt+1) {
		return EntityRecord{}, fmt.Errorf("parquet entity record has %d columns, want at least %d", table.NumCols(), parquetEntityRecordColumnEntitySourceStaleAt+1)
	}

	var record EntityRecord
	var entity *graph.Entity
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetEntityRecordColumns(batch)
		if err != nil {
			return EntityRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowRecord := EntityRecord{
				LayoutVersion: CurrentObjectLayoutVersion,
				TenantID:      columns.tenantID.Value(i),
				ID:            columns.id.Value(i),
				Page:          columns.page.Value(i),
				Ordinal:       int(columns.ordinal.Value(i)),
				PageHash:      columns.pageHash.Value(i),
				PageETag:      columns.pageETag.Value(i),
				ContentHash:   columns.contentHash.Value(i),
				Deleted:       columns.deleted.Value(i),
				Version:       columns.version.Value(i),
				UpdatedAt:     parseParquetTime(columns.updatedAt.Value(i)),
			}
			if record.ID == "" {
				record = rowRecord
			} else if record.TenantID != rowRecord.TenantID || record.ID != rowRecord.ID || record.Page != rowRecord.Page || record.Version != rowRecord.Version {
				return EntityRecord{}, fmt.Errorf("parquet entity record metadata mismatch")
			}
			if entity == nil && shouldDecodeEntityRecordEntity(rowRecord, columns, i) {
				created := graph.Entity{
					ID:         rowRecord.ID,
					Kind:       columns.kind.Value(i),
					Source:     columns.source.Value(i),
					ExternalID: columns.externalID.Value(i),
					Version:    columns.entityVersion.Value(i),
					CreatedAt:  parseParquetTime(columns.entityCreatedAt.Value(i)),
					UpdatedAt:  parseParquetTime(columns.entityUpdatedAt.Value(i)),
					Confidence: columns.confidence.Value(i),
					SourceRank: int(columns.sourceRank.Value(i)),
					SplitFrom:  columns.splitFrom.Value(i),
				}
				entity = &created
			}
			if entity != nil {
				if err := applyEntityPageRow(entity, entityRecordRowFromColumns(columns, i)); err != nil {
					return EntityRecord{}, err
				}
			}
		}
	}
	if record.ID == "" {
		return EntityRecord{}, fmt.Errorf("parquet entity record is empty")
	}
	if entity != nil {
		record.Entity = decodedEntityPageCopy(*entity)
	}
	if err := normalizeObjectAfterRead(&record, "entity record"); err != nil {
		return EntityRecord{}, err
	}
	if tenantID != "" && !indexTenantMatches(record.TenantID, tenantID) {
		return EntityRecord{}, fmt.Errorf("entity record tenant mismatch: key tenant %q contains tenant %q", tenantID, record.TenantID)
	}
	if id != "" && record.ID != id {
		return EntityRecord{}, fmt.Errorf("entity record id mismatch: key id %q contains id %q", id, record.ID)
	}
	if !record.Deleted && record.Entity.ID != "" && record.Entity.ID != record.ID {
		return EntityRecord{}, fmt.Errorf("entity record entity id mismatch: record id %q entity id %q", record.ID, record.Entity.ID)
	}
	return record, nil
}

type parquetEntityRecordColumnSet struct {
	tenantID               *array.String
	id                     *array.String
	page                   *array.String
	ordinal                *array.Int64
	pageHash               *array.String
	pageETag               *array.String
	contentHash            *array.String
	deleted                *array.Boolean
	version                *array.Int64
	updatedAt              *array.String
	kind                   *array.String
	source                 *array.String
	externalID             *array.String
	entityVersion          *array.Int64
	entityCreatedAt        *array.String
	entityUpdatedAt        *array.String
	confidence             *array.Float64
	sourceRank             *array.Int64
	splitFrom              *array.String
	rowKind                *array.String
	rowOrdinal             *array.Int64
	entryKey               *array.String
	valueKind              *array.String
	stringValue            *array.String
	boolValue              *array.Boolean
	floatValue             *array.Float64
	fieldSourceSource      *array.String
	fieldSourcePriority    *array.Int64
	fieldSourceConfidence  *array.Float64
	fieldSourceVersion     *array.Int64
	fieldSourceUpdatedAt   *array.String
	entitySourceSource     *array.String
	entitySourceExternalID *array.String
	entitySourceConfidence *array.Float64
	entitySourcePriority   *array.Int64
	entitySourceObservedAt *array.String
	entitySourceStale      *array.Boolean
	entitySourceStaleAt    *array.String
}

func parquetEntityRecordColumns(batch arrow.RecordBatch) (parquetEntityRecordColumnSet, error) {
	var columns parquetEntityRecordColumnSet
	var err error
	if columns.tenantID, err = parquetStringColumn(batch, parquetEntityRecordColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.id, err = parquetStringColumn(batch, parquetEntityRecordColumnID, "id"); err != nil {
		return columns, err
	}
	if columns.page, err = parquetStringColumn(batch, parquetEntityRecordColumnPage, "page"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetInt64Column(batch, parquetEntityRecordColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.pageHash, err = parquetStringColumn(batch, parquetEntityRecordColumnPageHash, "page_hash"); err != nil {
		return columns, err
	}
	if columns.pageETag, err = parquetStringColumn(batch, parquetEntityRecordColumnPageETag, "page_etag"); err != nil {
		return columns, err
	}
	if columns.contentHash, err = parquetStringColumn(batch, parquetEntityRecordColumnContentHash, "content_hash"); err != nil {
		return columns, err
	}
	if columns.deleted, err = parquetBoolColumn(batch, parquetEntityRecordColumnDeleted, "deleted"); err != nil {
		return columns, err
	}
	if columns.version, err = parquetInt64Column(batch, parquetEntityRecordColumnVersion, "version"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetStringColumn(batch, parquetEntityRecordColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	if columns.kind, err = parquetStringColumn(batch, parquetEntityRecordColumnKind, "kind"); err != nil {
		return columns, err
	}
	if columns.source, err = parquetStringColumn(batch, parquetEntityRecordColumnSource, "source"); err != nil {
		return columns, err
	}
	if columns.externalID, err = parquetStringColumn(batch, parquetEntityRecordColumnExternalID, "external_id"); err != nil {
		return columns, err
	}
	if columns.entityVersion, err = parquetInt64Column(batch, parquetEntityRecordColumnEntityVersion, "entity_version"); err != nil {
		return columns, err
	}
	if columns.entityCreatedAt, err = parquetStringColumn(batch, parquetEntityRecordColumnEntityCreatedAt, "entity_created_at"); err != nil {
		return columns, err
	}
	if columns.entityUpdatedAt, err = parquetStringColumn(batch, parquetEntityRecordColumnEntityUpdatedAt, "entity_updated_at"); err != nil {
		return columns, err
	}
	if columns.confidence, err = parquetFloat64Column(batch, parquetEntityRecordColumnConfidence, "confidence"); err != nil {
		return columns, err
	}
	if columns.sourceRank, err = parquetInt64Column(batch, parquetEntityRecordColumnSourceRank, "source_priority"); err != nil {
		return columns, err
	}
	if columns.splitFrom, err = parquetStringColumn(batch, parquetEntityRecordColumnSplitFrom, "split_from"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetStringColumn(batch, parquetEntityRecordColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.rowOrdinal, err = parquetInt64Column(batch, parquetEntityRecordColumnRowOrdinal, "row_ordinal"); err != nil {
		return columns, err
	}
	if columns.entryKey, err = parquetStringColumn(batch, parquetEntityRecordColumnEntryKey, "entry_key"); err != nil {
		return columns, err
	}
	if columns.valueKind, err = parquetStringColumn(batch, parquetEntityRecordColumnValueKind, "value_kind"); err != nil {
		return columns, err
	}
	if columns.stringValue, err = parquetStringColumn(batch, parquetEntityRecordColumnStringValue, "string_value"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetBoolColumn(batch, parquetEntityRecordColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetFloat64Column(batch, parquetEntityRecordColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	if columns.fieldSourceSource, err = parquetStringColumn(batch, parquetEntityRecordColumnFieldSourceSource, "field_source_source"); err != nil {
		return columns, err
	}
	if columns.fieldSourcePriority, err = parquetInt64Column(batch, parquetEntityRecordColumnFieldSourcePriority, "field_source_priority"); err != nil {
		return columns, err
	}
	if columns.fieldSourceConfidence, err = parquetFloat64Column(batch, parquetEntityRecordColumnFieldSourceConfidence, "field_source_confidence"); err != nil {
		return columns, err
	}
	if columns.fieldSourceVersion, err = parquetInt64Column(batch, parquetEntityRecordColumnFieldSourceVersion, "field_source_version"); err != nil {
		return columns, err
	}
	if columns.fieldSourceUpdatedAt, err = parquetStringColumn(batch, parquetEntityRecordColumnFieldSourceUpdatedAt, "field_source_updated_at"); err != nil {
		return columns, err
	}
	if columns.entitySourceSource, err = parquetStringColumn(batch, parquetEntityRecordColumnEntitySourceSource, "entity_source_source"); err != nil {
		return columns, err
	}
	if columns.entitySourceExternalID, err = parquetStringColumn(batch, parquetEntityRecordColumnEntitySourceExternalID, "entity_source_external_id"); err != nil {
		return columns, err
	}
	if columns.entitySourceConfidence, err = parquetFloat64Column(batch, parquetEntityRecordColumnEntitySourceConfidence, "entity_source_confidence"); err != nil {
		return columns, err
	}
	if columns.entitySourcePriority, err = parquetInt64Column(batch, parquetEntityRecordColumnEntitySourcePriority, "entity_source_priority"); err != nil {
		return columns, err
	}
	if columns.entitySourceObservedAt, err = parquetStringColumn(batch, parquetEntityRecordColumnEntitySourceObservedAt, "entity_source_observed_at"); err != nil {
		return columns, err
	}
	if columns.entitySourceStale, err = parquetBoolColumn(batch, parquetEntityRecordColumnEntitySourceStale, "entity_source_stale"); err != nil {
		return columns, err
	}
	if columns.entitySourceStaleAt, err = parquetStringColumn(batch, parquetEntityRecordColumnEntitySourceStaleAt, "entity_source_stale_at"); err != nil {
		return columns, err
	}
	return columns, nil
}

func shouldDecodeEntityRecordEntity(record EntityRecord, columns parquetEntityRecordColumnSet, row int) bool {
	if !record.Deleted {
		return true
	}
	return columns.kind.Value(row) != "" ||
		columns.source.Value(row) != "" ||
		columns.externalID.Value(row) != "" ||
		columns.entityVersion.Value(row) != 0 ||
		columns.rowKind.Value(row) != entityPageRowMetadata
}

func entityRecordRowFromColumns(columns parquetEntityRecordColumnSet, row int) entityPageRow {
	return entityPageRow{
		Kind:    columns.rowKind.Value(row),
		Ordinal: int(columns.rowOrdinal.Value(row)),
		Key:     columns.entryKey.Value(row),
		Value: parquetValue{
			Kind:        columns.valueKind.Value(row),
			StringValue: columns.stringValue.Value(row),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
		FieldSource: graph.FieldSource{
			Source:     columns.fieldSourceSource.Value(row),
			Priority:   int(columns.fieldSourcePriority.Value(row)),
			Confidence: columns.fieldSourceConfidence.Value(row),
			Version:    columns.fieldSourceVersion.Value(row),
			UpdatedAt:  parseParquetTime(columns.fieldSourceUpdatedAt.Value(row)),
		},
		EntitySource: graph.EntitySource{
			Source:     columns.entitySourceSource.Value(row),
			ExternalID: columns.entitySourceExternalID.Value(row),
			Confidence: columns.entitySourceConfidence.Value(row),
			Priority:   int(columns.entitySourcePriority.Value(row)),
			ObservedAt: parseParquetTime(columns.entitySourceObservedAt.Value(row)),
			Stale:      columns.entitySourceStale.Value(row),
			StaleAt:    parseParquetTime(columns.entitySourceStaleAt.Value(row)),
		},
	}
}

func parquetEntityRecordArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "page", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "page_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "page_etag", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "deleted", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "split_from", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "row_ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entry_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "float_value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "field_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "field_source_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "entity_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_source_observed_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_stale", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "entity_source_stale_at", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetEntityRecordSchemaHash() string {
	return objectSchemaHash([]string{
		"tenant_id",
		"id",
		"page",
		"ordinal",
		"page_hash",
		"page_etag",
		"content_hash",
		"deleted",
		"version",
		"updated_at",
		"kind",
		"source",
		"external_id",
		"entity_version",
		"entity_created_at",
		"entity_updated_at",
		"confidence",
		"source_priority",
		"split_from",
		"row_kind",
		"row_ordinal",
		"entry_key",
		"value_kind",
		"string_value",
		"bool_value",
		"float_value",
		"field_source_source",
		"field_source_priority",
		"field_source_confidence",
		"field_source_version",
		"field_source_updated_at",
		"entity_source_source",
		"entity_source_external_id",
		"entity_source_confidence",
		"entity_source_priority",
		"entity_source_observed_at",
		"entity_source_stale",
		"entity_source_stale_at",
		parquetEntityRecordCodec,
	})
}
