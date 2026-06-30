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

const collectorStatusCodecParquet = "collector-status-arrow-parquet-v1"

const (
	parquetCollectorStatusColumnTenantID = iota
	parquetCollectorStatusColumnSource
	parquetCollectorStatusColumnCollectorID
	parquetCollectorStatusColumnLastBatchID
	parquetCollectorStatusColumnLastVersion
	parquetCollectorStatusColumnContentHash
	parquetCollectorStatusColumnLastCursor
	parquetCollectorStatusColumnLastStartedAt
	parquetCollectorStatusColumnLastFinishedAt
	parquetCollectorStatusColumnLastError
	parquetCollectorStatusColumnAppliedTotal
	parquetCollectorStatusColumnFailedTotal
)

func marshalParquetCollectorStatus(ctx context.Context, status CollectorStatus) ([]byte, error) {
	schema := parquetCollectorStatusArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(parquetCollectorStatusColumnTenantID).(*array.StringBuilder).Append(status.TenantID)
	builder.Field(parquetCollectorStatusColumnSource).(*array.StringBuilder).Append(status.Source)
	builder.Field(parquetCollectorStatusColumnCollectorID).(*array.StringBuilder).Append(status.CollectorID)
	builder.Field(parquetCollectorStatusColumnLastBatchID).(*array.StringBuilder).Append(status.LastBatchID)
	builder.Field(parquetCollectorStatusColumnLastVersion).(*array.Int64Builder).Append(status.LastVersion)
	builder.Field(parquetCollectorStatusColumnContentHash).(*array.StringBuilder).Append(collectorStatusContentHash(status))
	builder.Field(parquetCollectorStatusColumnLastCursor).(*array.StringBuilder).Append(status.LastCursor)
	builder.Field(parquetCollectorStatusColumnLastStartedAt).(*array.StringBuilder).Append(formatParquetTime(status.LastStartedAt))
	builder.Field(parquetCollectorStatusColumnLastFinishedAt).(*array.StringBuilder).Append(formatParquetTime(status.LastFinishedAt))
	builder.Field(parquetCollectorStatusColumnLastError).(*array.StringBuilder).Append(status.LastError)
	builder.Field(parquetCollectorStatusColumnAppliedTotal).(*array.Int64Builder).Append(int64(status.AppliedTotal))
	builder.Field(parquetCollectorStatusColumnFailedTotal).(*array.Int64Builder).Append(int64(status.FailedTotal))

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

func decodeParquetCollectorStatus(ctx context.Context, data []byte) (CollectorStatus, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return CollectorStatus{}, err
	}
	defer table.Release()
	if table.NumRows() != 1 {
		return CollectorStatus{}, fmt.Errorf("parquet collector status has %d rows, want 1", table.NumRows())
	}
	if table.NumCols() < int64(parquetCollectorStatusColumnFailedTotal+1) {
		return CollectorStatus{}, fmt.Errorf("parquet collector status has %d columns, want at least %d", table.NumCols(), parquetCollectorStatusColumnFailedTotal+1)
	}

	reader := array.NewTableReader(table, 1)
	defer reader.Release()
	if !reader.Next() {
		return CollectorStatus{}, fmt.Errorf("parquet collector status is empty")
	}
	batch := reader.RecordBatch()
	tenantColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnTenantID, "tenant_id")
	if err != nil {
		return CollectorStatus{}, err
	}
	sourceColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnSource, "source")
	if err != nil {
		return CollectorStatus{}, err
	}
	collectorColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnCollectorID, "collector_id")
	if err != nil {
		return CollectorStatus{}, err
	}
	lastBatchColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnLastBatchID, "last_batch_id")
	if err != nil {
		return CollectorStatus{}, err
	}
	lastVersionColumn, err := parquetInt64Column(batch, parquetCollectorStatusColumnLastVersion, "last_version")
	if err != nil {
		return CollectorStatus{}, err
	}
	hashColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnContentHash, "content_hash")
	if err != nil {
		return CollectorStatus{}, err
	}
	cursorColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnLastCursor, "last_cursor")
	if err != nil {
		return CollectorStatus{}, err
	}
	startedColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnLastStartedAt, "last_started_at")
	if err != nil {
		return CollectorStatus{}, err
	}
	finishedColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnLastFinishedAt, "last_finished_at")
	if err != nil {
		return CollectorStatus{}, err
	}
	errorColumn, err := parquetStringColumn(batch, parquetCollectorStatusColumnLastError, "last_error")
	if err != nil {
		return CollectorStatus{}, err
	}
	appliedColumn, err := parquetInt64Column(batch, parquetCollectorStatusColumnAppliedTotal, "applied_total")
	if err != nil {
		return CollectorStatus{}, err
	}
	failedColumn, err := parquetInt64Column(batch, parquetCollectorStatusColumnFailedTotal, "failed_total")
	if err != nil {
		return CollectorStatus{}, err
	}
	status := CollectorStatus{
		TenantID:       tenantColumn.Value(0),
		Source:         sourceColumn.Value(0),
		CollectorID:    collectorColumn.Value(0),
		LastBatchID:    lastBatchColumn.Value(0),
		LastVersion:    lastVersionColumn.Value(0),
		LastCursor:     cursorColumn.Value(0),
		LastStartedAt:  parseParquetTime(startedColumn.Value(0)),
		LastFinishedAt: parseParquetTime(finishedColumn.Value(0)),
		LastError:      errorColumn.Value(0),
		AppliedTotal:   int(appliedColumn.Value(0)),
		FailedTotal:    int(failedColumn.Value(0)),
	}
	if status.TenantID != tenantColumn.Value(0) ||
		status.Source != sourceColumn.Value(0) ||
		status.CollectorID != collectorColumn.Value(0) ||
		status.LastBatchID != lastBatchColumn.Value(0) ||
		status.LastVersion != lastVersionColumn.Value(0) {
		return CollectorStatus{}, fmt.Errorf("collector status identity mismatch")
	}
	hash := collectorStatusContentHash(status)
	if hashColumn.Value(0) == "" || hashColumn.Value(0) != hash {
		return CollectorStatus{}, fmt.Errorf("collector status content hash mismatch")
	}
	return status, nil
}

func parquetCollectorStatusArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "collector_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last_batch_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last_cursor", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last_started_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last_finished_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "last_error", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "applied_total", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "failed_total", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}

func collectorStatusContentHash(status CollectorStatus) string {
	return parquetScalarContentHash(
		status.TenantID,
		status.Source,
		status.CollectorID,
		status.LastBatchID,
		formatInt64ForHash(status.LastVersion),
		status.LastCursor,
		formatParquetTime(status.LastStartedAt),
		formatParquetTime(status.LastFinishedAt),
		status.LastError,
		formatInt64ForHash(int64(status.AppliedTotal)),
		formatInt64ForHash(int64(status.FailedTotal)),
	)
}
