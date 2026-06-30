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

const sourcePolicyCodecParquet = "source-policy-arrow-parquet-v1"

const (
	parquetSourcePolicyColumnTenantID = iota
	parquetSourcePolicyColumnDefaultPriority
	parquetSourcePolicyColumnSourceCount
	parquetSourcePolicyColumnContentHash
	parquetSourcePolicyColumnSourceName
	parquetSourcePolicyColumnSourcePriority
	parquetSourcePolicyColumnSourceDescription
)

func marshalParquetSourcePolicy(ctx context.Context, record sourcePolicyRecord) ([]byte, error) {
	normalized, err := normalizeSourcePolicyRecord(record)
	if err != nil {
		return nil, err
	}
	hash, err := sourcePolicyContentHash(normalized)
	if err != nil {
		return nil, err
	}

	schema := parquetSourcePolicyArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendSourcePolicyRow := func(source graph.SourcePolicyItem) {
		builder.Field(parquetSourcePolicyColumnTenantID).(*array.StringBuilder).Append(normalized.TenantID)
		builder.Field(parquetSourcePolicyColumnDefaultPriority).(*array.Int64Builder).Append(int64(normalized.DefaultPriority))
		builder.Field(parquetSourcePolicyColumnSourceCount).(*array.Int64Builder).Append(int64(len(normalized.Sources)))
		builder.Field(parquetSourcePolicyColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetSourcePolicyColumnSourceName).(*array.StringBuilder).Append(source.Name)
		builder.Field(parquetSourcePolicyColumnSourcePriority).(*array.Int64Builder).Append(int64(source.Priority))
		builder.Field(parquetSourcePolicyColumnSourceDescription).(*array.StringBuilder).Append(source.Description)
	}
	if len(normalized.Sources) == 0 {
		appendSourcePolicyRow(graph.SourcePolicyItem{})
	} else {
		for _, source := range normalized.Sources {
			appendSourcePolicyRow(source)
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

func decodeParquetSourcePolicy(ctx context.Context, data []byte) (sourcePolicyRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return sourcePolicyRecord{}, fmt.Errorf("parquet source policy is empty")
	}
	if table.NumCols() < int64(parquetSourcePolicyColumnSourceDescription+1) {
		return sourcePolicyRecord{}, fmt.Errorf("parquet source policy has %d columns, want at least %d", table.NumCols(), parquetSourcePolicyColumnSourceDescription+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()
	record := sourcePolicyRecord{}
	var sourceCount int64
	var expectedHash string
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		tenantColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnTenantID, "tenant_id")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		defaultPriorityColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnDefaultPriority, "default_priority")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceCountColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnSourceCount, "source_count")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		hashColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnContentHash, "content_hash")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceNameColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnSourceName, "source_name")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourcePriorityColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnSourcePriority, "source_priority")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceDescriptionColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnSourceDescription, "source_description")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			if rows == 0 {
				record.TenantID = tenantColumn.Value(i)
				record.DefaultPriority = int(defaultPriorityColumn.Value(i))
				sourceCount = sourceCountColumn.Value(i)
				expectedHash = hashColumn.Value(i)
			}
			if record.TenantID != tenantColumn.Value(i) || int64(record.DefaultPriority) != defaultPriorityColumn.Value(i) || sourceCount != sourceCountColumn.Value(i) || expectedHash != hashColumn.Value(i) {
				return sourcePolicyRecord{}, fmt.Errorf("source policy identity mismatch")
			}
			name := sourceNameColumn.Value(i)
			if name != "" {
				record.Sources = append(record.Sources, graph.SourcePolicyItem{
					Name:        name,
					Priority:    int(sourcePriorityColumn.Value(i)),
					Description: sourceDescriptionColumn.Value(i),
				})
			}
			rows++
		}
	}

	if int64(len(record.Sources)) != sourceCount {
		return sourcePolicyRecord{}, fmt.Errorf("source policy source count mismatch")
	}
	hash, err := sourcePolicyContentHash(record)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return sourcePolicyRecord{}, fmt.Errorf("source policy content hash mismatch")
	}
	return normalizeSourcePolicyRecord(record)
}

func parquetSourcePolicyArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "default_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "source_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "source_description", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func normalizeSourcePolicyRecord(record sourcePolicyRecord) (sourcePolicyRecord, error) {
	normalized, err := graph.NormalizeSourcePolicy(record.SourcePolicy)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	record.SourcePolicy = normalized
	return record, nil
}

func sourcePolicyContentHash(record sourcePolicyRecord) (string, error) {
	normalized, err := normalizeSourcePolicyRecord(record)
	if err != nil {
		return "", err
	}
	parts := []string{
		normalized.TenantID,
		formatInt64ForHash(int64(normalized.DefaultPriority)),
		formatInt64ForHash(int64(len(normalized.Sources))),
	}
	for _, source := range normalized.Sources {
		parts = append(parts, source.Name, formatInt64ForHash(int64(source.Priority)), source.Description)
	}
	return parquetScalarContentHash(parts...), nil
}
