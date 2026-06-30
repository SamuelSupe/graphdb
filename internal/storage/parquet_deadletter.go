package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const deadLetterCodecParquet = "deadletter-arrow-parquet-v1"

const deadLetterRowMetadata = "deadletter_metadata"

func marshalParquetDeadLetter(ctx context.Context, letter DeadLetter) ([]byte, error) {
	normalized, hash, err := normalizeDeadLetterForParquet(letter)
	if err != nil {
		return nil, err
	}
	rows, err := deadLetterRows(normalized)
	if err != nil {
		return nil, err
	}
	schema := parquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	header := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      normalized.TenantID,
		ID:            normalized.ID,
		Version:       normalized.LastResult.Version,
		CreatedAt:     normalized.UpdatedAt,
	}
	for _, row := range rows {
		appendParquetCommitRow(builder, normalized.TenantID, "", header, hash, row)
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	rowGroupSize := table.NumRows()
	if rowGroupSize < 1 {
		rowGroupSize = 1
	}
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, rowGroupSize, writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetDeadLetter(ctx context.Context, data []byte) (DeadLetter, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return DeadLetter{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return DeadLetter{}, fmt.Errorf("parquet deadletter is empty")
	}
	if table.NumCols() < int64(parquetCommitColumnEdgeSourceObservedAt+1) {
		return DeadLetter{}, fmt.Errorf("parquet deadletter has %d columns, want at least %d", table.NumCols(), parquetCommitColumnEdgeSourceObservedAt+1)
	}

	var letter DeadLetter
	var batchRecord IngestBatchRecord
	var expectedHash string
	build := &commitBuild{}
	deletes := ingestDeleteRows{}
	rows := 0
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetCommitColumns(batch)
		if err != nil {
			return DeadLetter{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowTenant := columns.commitTenantID.Value(i)
			rowID := columns.commitID.Value(i)
			rowVersion := columns.version.Value(i)
			rowHash := columns.contentHash.Value(i)
			if columns.tenantID.Value(i) != rowTenant {
				return DeadLetter{}, fmt.Errorf("deadletter tenant mismatch")
			}
			if rows == 0 {
				letter.TenantID = rowTenant
				letter.ID = rowID
				letter.LastResult.Version = rowVersion
				batchRecord.TenantID = rowTenant
				expectedHash = rowHash
			} else if letter.TenantID != rowTenant || letter.ID != rowID || letter.LastResult.Version != rowVersion || expectedHash != rowHash {
				return DeadLetter{}, fmt.Errorf("deadletter identity mismatch")
			}
			row := parquetCommitRowFromColumns(columns, i)
			switch row.Kind {
			case deadLetterRowMetadata:
				letter.ID = row.ID
				letter.Source = row.Source
				letter.BatchID = row.ExternalID
				letter.Status = row.Action
				letter.Attempts = row.SourcePriority
				letter.Error = row.Reason
				letter.CreatedAt = row.CreatedAtValue
				letter.UpdatedAt = row.UpdatedAtValue
				letter.ReplayedAt = row.FieldSource.UpdatedAt
			default:
				if err := applyIngestRecordRow(&batchRecord, build, &deletes, row); err != nil {
					return DeadLetter{}, err
				}
			}
			rows++
		}
	}
	applyIngestBuildRows(&batchRecord, build, deletes)
	letter.Request = batchRecord.Request
	letter.LastResult = batchRecord.Result
	hash, err := deadLetterContentHash(letter)
	if err != nil {
		return DeadLetter{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return DeadLetter{}, fmt.Errorf("deadletter content hash mismatch")
	}
	return letter, nil
}

func deadLetterRows(letter DeadLetter) ([]parquetCommitRow, error) {
	rows := []parquetCommitRow{{
		Kind:           deadLetterRowMetadata,
		ID:             letter.ID,
		Source:         letter.Source,
		ExternalID:     letter.BatchID,
		Action:         letter.Status,
		SourcePriority: letter.Attempts,
		Reason:         letter.Error,
		CreatedAtValue: letter.CreatedAt,
		UpdatedAtValue: letter.UpdatedAt,
		FieldSource:    graph.FieldSource{UpdatedAt: letter.ReplayedAt},
	}}
	ingestRows, err := ingestRecordRows(IngestBatchRecord{
		TenantID:   letter.TenantID,
		Request:    letter.Request,
		Result:     letter.LastResult,
		StartedAt:  letter.CreatedAt,
		FinishedAt: letter.UpdatedAt,
	})
	if err != nil {
		return nil, err
	}
	rows = append(rows, ingestRows...)
	return rows, nil
}

func normalizeDeadLetterForParquet(letter DeadLetter) (DeadLetter, string, error) {
	payload, err := deadLetterPayloadJSON(letter)
	if err != nil {
		return DeadLetter{}, "", err
	}
	var normalized DeadLetter
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return DeadLetter{}, "", err
	}
	canonical, err := deadLetterPayloadJSON(normalized)
	if err != nil {
		return DeadLetter{}, "", err
	}
	return normalized, objectContentHash(canonical), nil
}

func deadLetterPayloadJSON(letter DeadLetter) ([]byte, error) {
	letter.objectKey = ""
	letter.objectMeta = ObjectMeta{}
	if letter.Request.Items == nil {
		letter.Request.Items = []IngestItem{}
	}
	return json.Marshal(letter)
}

func deadLetterContentHash(letter DeadLetter) (string, error) {
	_, hash, err := normalizeDeadLetterForParquet(letter)
	return hash, err
}
