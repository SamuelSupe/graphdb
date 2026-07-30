package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	ingestMetadataSegmentCodec  = "ingest-metadata-segment-arrow-parquet-v1"
	ingestMetadataManifestCodec = "ingest-metadata-manifest-arrow-parquet-v1"
	ingestMetadataIndexCodec    = "ingest-metadata-index-arrow-parquet-v1"
)

func marshalParquetIngestMetadataDocument(ctx context.Context, codec string, value any) ([]byte, string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	contentHash := hex.EncodeToString(sum[:])
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "codec", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "payload_json", Type: arrow.BinaryTypes.Binary, Nullable: false},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).Append(codec)
	builder.Field(1).(*array.StringBuilder).Append(contentHash)
	builder.Field(2).(*array.BinaryBuilder).Append(payload)
	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, 1, writerProps, arrowProps); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), contentHash, objectContextErr(ctx)
}

func decodeParquetIngestMetadataDocument(ctx context.Context, data []byte, codec string, target any) (string, error) {
	table, release, err := readParquetTable(ctx, data)
	if err != nil {
		return "", err
	}
	defer release()
	defer table.Release()
	if table.NumRows() != 1 || table.NumCols() < 3 {
		return "", fmt.Errorf("parquet ingest metadata document has invalid shape")
	}
	reader := array.NewTableReader(table, 1)
	defer reader.Release()
	if !reader.Next() {
		return "", fmt.Errorf("parquet ingest metadata document is empty")
	}
	batch := reader.RecordBatch()
	codecColumn, err := parquetStringColumn(batch, 0, "codec")
	if err != nil {
		return "", err
	}
	hashColumn, err := parquetStringColumn(batch, 1, "content_hash")
	if err != nil {
		return "", err
	}
	payloadColumn, ok := batch.Column(2).(*array.Binary)
	if !ok {
		return "", fmt.Errorf("parquet column payload_json has type %s, want binary", batch.Column(2).DataType())
	}
	if codecColumn.Value(0) != codec {
		return "", fmt.Errorf("unsupported ingest metadata codec %q", codecColumn.Value(0))
	}
	payload := payloadColumn.Value(0)
	sum := sha256.Sum256(payload)
	contentHash := hex.EncodeToString(sum[:])
	if hashColumn.Value(0) != contentHash {
		return "", fmt.Errorf("ingest metadata content hash mismatch")
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return "", err
	}
	return contentHash, nil
}
