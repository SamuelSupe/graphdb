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

const writerLeaseCodecParquet = "writer-lease-arrow-parquet-v1"

const (
	parquetWriterLeaseColumnTenantID = iota
	parquetWriterLeaseColumnOwnerID
	parquetWriterLeaseColumnExpiresAt
	parquetWriterLeaseColumnUpdatedAt
	parquetWriterLeaseColumnContentHash
)

func marshalParquetWriterLease(ctx context.Context, lease WriterLease) ([]byte, error) {
	schema := parquetWriterLeaseArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(parquetWriterLeaseColumnTenantID).(*array.StringBuilder).Append(lease.TenantID)
	builder.Field(parquetWriterLeaseColumnOwnerID).(*array.StringBuilder).Append(lease.OwnerID)
	builder.Field(parquetWriterLeaseColumnExpiresAt).(*array.StringBuilder).Append(formatParquetTime(lease.ExpiresAt))
	builder.Field(parquetWriterLeaseColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(lease.UpdatedAt))
	builder.Field(parquetWriterLeaseColumnContentHash).(*array.StringBuilder).Append(writerLeaseContentHash(lease))

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

func decodeParquetWriterLease(ctx context.Context, data []byte) (WriterLease, error) {
	table, release, err := readParquetTable(ctx, data)
	if err != nil {
		return WriterLease{}, err
	}
	defer release()
	defer table.Release()
	if table.NumRows() != 1 {
		return WriterLease{}, fmt.Errorf("parquet writer lease has %d rows, want 1", table.NumRows())
	}
	if table.NumCols() < int64(parquetWriterLeaseColumnContentHash+1) {
		return WriterLease{}, fmt.Errorf("parquet writer lease has %d columns, want at least %d", table.NumCols(), parquetWriterLeaseColumnContentHash+1)
	}

	reader := array.NewTableReader(table, 1)
	defer reader.Release()
	if !reader.Next() {
		return WriterLease{}, fmt.Errorf("parquet writer lease is empty")
	}
	batch := reader.RecordBatch()
	tenantColumn, err := parquetStringColumn(batch, parquetWriterLeaseColumnTenantID, "tenant_id")
	if err != nil {
		return WriterLease{}, err
	}
	ownerColumn, err := parquetStringColumn(batch, parquetWriterLeaseColumnOwnerID, "owner_id")
	if err != nil {
		return WriterLease{}, err
	}
	expiresColumn, err := parquetStringColumn(batch, parquetWriterLeaseColumnExpiresAt, "expires_at")
	if err != nil {
		return WriterLease{}, err
	}
	updatedColumn, err := parquetStringColumn(batch, parquetWriterLeaseColumnUpdatedAt, "updated_at")
	if err != nil {
		return WriterLease{}, err
	}
	hashColumn, err := parquetStringColumn(batch, parquetWriterLeaseColumnContentHash, "content_hash")
	if err != nil {
		return WriterLease{}, err
	}
	lease := WriterLease{
		TenantID:  tenantColumn.Value(0),
		OwnerID:   ownerColumn.Value(0),
		ExpiresAt: parseParquetTime(expiresColumn.Value(0)),
		UpdatedAt: parseParquetTime(updatedColumn.Value(0)),
	}
	if lease.TenantID != tenantColumn.Value(0) || lease.OwnerID != ownerColumn.Value(0) {
		return WriterLease{}, fmt.Errorf("writer lease identity mismatch")
	}
	hash := writerLeaseContentHash(lease)
	if hashColumn.Value(0) == "" || hashColumn.Value(0) != hash {
		return WriterLease{}, fmt.Errorf("writer lease content hash mismatch")
	}
	return lease, nil
}

func parquetWriterLeaseArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "owner_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "expires_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func writerLeaseContentHash(lease WriterLease) string {
	return parquetScalarContentHash(
		lease.TenantID,
		lease.OwnerID,
		formatParquetTime(lease.ExpiresAt),
		formatParquetTime(lease.UpdatedAt),
	)
}
