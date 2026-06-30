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

const indexDefinitionCodecParquet = "index-definition-arrow-parquet-v1"

const (
	parquetIndexDefinitionColumnLayoutVersion = iota
	parquetIndexDefinitionColumnTenantID
	parquetIndexDefinitionColumnCount
	parquetIndexDefinitionColumnContentHash
	parquetIndexDefinitionColumnOrdinal
	parquetIndexDefinitionColumnName
	parquetIndexDefinitionColumnKind
	parquetIndexDefinitionColumnField
	parquetIndexDefinitionColumnUnique
	parquetIndexDefinitionColumnCreatedAt
	parquetIndexDefinitionColumnUpdatedAt
)

func marshalParquetIndexDefinitions(ctx context.Context, record IndexDefinitionRecord) ([]byte, error) {
	record = normalizeIndexDefinitionRecord(record)
	hash, err := indexDefinitionContentHash(record)
	if err != nil {
		return nil, err
	}
	schema := parquetIndexDefinitionArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendRow := func(ordinal int, definition IndexDefinition) {
		builder.Field(parquetIndexDefinitionColumnLayoutVersion).(*array.Int64Builder).Append(int64(record.LayoutVersion))
		builder.Field(parquetIndexDefinitionColumnTenantID).(*array.StringBuilder).Append(record.TenantID)
		builder.Field(parquetIndexDefinitionColumnCount).(*array.Int64Builder).Append(int64(len(record.Indexes)))
		builder.Field(parquetIndexDefinitionColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetIndexDefinitionColumnOrdinal).(*array.Int64Builder).Append(int64(ordinal))
		builder.Field(parquetIndexDefinitionColumnName).(*array.StringBuilder).Append(definition.Name)
		builder.Field(parquetIndexDefinitionColumnKind).(*array.StringBuilder).Append(definition.Kind)
		builder.Field(parquetIndexDefinitionColumnField).(*array.StringBuilder).Append(definition.Field)
		builder.Field(parquetIndexDefinitionColumnUnique).(*array.BooleanBuilder).Append(definition.Unique)
		builder.Field(parquetIndexDefinitionColumnCreatedAt).(*array.StringBuilder).Append(formatParquetTime(definition.CreatedAt))
		builder.Field(parquetIndexDefinitionColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(definition.UpdatedAt))
	}
	if len(record.Indexes) == 0 {
		appendRow(0, IndexDefinition{})
	} else {
		for i, definition := range record.Indexes {
			appendRow(i, definition)
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

func decodeParquetIndexDefinitions(ctx context.Context, data []byte) (IndexDefinitionRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return IndexDefinitionRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return IndexDefinitionRecord{}, fmt.Errorf("parquet index definitions is empty")
	}
	if table.NumCols() < int64(parquetIndexDefinitionColumnUpdatedAt+1) {
		return IndexDefinitionRecord{}, fmt.Errorf("parquet index definitions has %d columns, want at least %d", table.NumCols(), parquetIndexDefinitionColumnUpdatedAt+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	var record IndexDefinitionRecord
	var expectedHash string
	var count int64
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetIndexDefinitionColumns(batch)
		if err != nil {
			return IndexDefinitionRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			if rows == 0 {
				record.LayoutVersion = int(columns.layoutVersion.Value(i))
				record.TenantID = columns.tenantID.Value(i)
				count = columns.count.Value(i)
				expectedHash = columns.contentHash.Value(i)
			}
			if record.LayoutVersion != int(columns.layoutVersion.Value(i)) ||
				record.TenantID != columns.tenantID.Value(i) ||
				count != columns.count.Value(i) ||
				expectedHash != columns.contentHash.Value(i) {
				return IndexDefinitionRecord{}, fmt.Errorf("index definitions identity mismatch")
			}
			name := columns.name.Value(i)
			if name != "" {
				if int64(len(record.Indexes)) != columns.ordinal.Value(i) {
					return IndexDefinitionRecord{}, fmt.Errorf("index definitions ordinal mismatch")
				}
				record.Indexes = append(record.Indexes, IndexDefinition{
					Name:      name,
					Kind:      columns.kind.Value(i),
					Field:     columns.field.Value(i),
					Unique:    columns.unique.Value(i),
					CreatedAt: parseParquetTime(columns.createdAt.Value(i)),
					UpdatedAt: parseParquetTime(columns.updatedAt.Value(i)),
				})
			}
			rows++
		}
	}
	if int64(len(record.Indexes)) != count {
		return IndexDefinitionRecord{}, fmt.Errorf("index definitions count mismatch")
	}
	hash, err := indexDefinitionContentHash(record)
	if err != nil {
		return IndexDefinitionRecord{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return IndexDefinitionRecord{}, fmt.Errorf("index definitions content hash mismatch")
	}
	return record, nil
}

type parquetIndexDefinitionColumnSet struct {
	layoutVersion *array.Int64
	tenantID      *array.String
	count         *array.Int64
	contentHash   *array.String
	ordinal       *array.Int64
	name          *array.String
	kind          *array.String
	field         *array.String
	unique        *array.Boolean
	createdAt     *array.String
	updatedAt     *array.String
}

func parquetIndexDefinitionColumns(batch arrow.RecordBatch) (parquetIndexDefinitionColumnSet, error) {
	var columns parquetIndexDefinitionColumnSet
	var err error
	if columns.layoutVersion, err = parquetInt64Column(batch, parquetIndexDefinitionColumnLayoutVersion, "layout_version"); err != nil {
		return columns, err
	}
	if columns.tenantID, err = parquetStringColumn(batch, parquetIndexDefinitionColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.count, err = parquetInt64Column(batch, parquetIndexDefinitionColumnCount, "count"); err != nil {
		return columns, err
	}
	if columns.contentHash, err = parquetStringColumn(batch, parquetIndexDefinitionColumnContentHash, "content_hash"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetInt64Column(batch, parquetIndexDefinitionColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.name, err = parquetStringColumn(batch, parquetIndexDefinitionColumnName, "name"); err != nil {
		return columns, err
	}
	if columns.kind, err = parquetStringColumn(batch, parquetIndexDefinitionColumnKind, "kind"); err != nil {
		return columns, err
	}
	if columns.field, err = parquetStringColumn(batch, parquetIndexDefinitionColumnField, "field"); err != nil {
		return columns, err
	}
	if columns.unique, err = parquetBoolColumn(batch, parquetIndexDefinitionColumnUnique, "unique"); err != nil {
		return columns, err
	}
	if columns.createdAt, err = parquetStringColumn(batch, parquetIndexDefinitionColumnCreatedAt, "created_at"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetStringColumn(batch, parquetIndexDefinitionColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	return columns, nil
}

func parquetIndexDefinitionArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "layout_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "unique", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func normalizeIndexDefinitionRecord(record IndexDefinitionRecord) IndexDefinitionRecord {
	record.LayoutVersion = CurrentObjectLayoutVersion
	record.Indexes = append([]IndexDefinition(nil), record.Indexes...)
	return record
}

func indexDefinitionContentHash(record IndexDefinitionRecord) (string, error) {
	record = normalizeIndexDefinitionRecord(record)
	parts := []string{
		formatInt64ForHash(int64(record.LayoutVersion)),
		record.TenantID,
		formatInt64ForHash(int64(len(record.Indexes))),
	}
	for _, definition := range record.Indexes {
		parts = append(parts,
			definition.Name,
			definition.Kind,
			definition.Field,
			formatBoolForHash(definition.Unique),
			formatParquetTime(definition.CreatedAt),
			formatParquetTime(definition.UpdatedAt),
		)
	}
	return parquetScalarContentHash(parts...), nil
}
