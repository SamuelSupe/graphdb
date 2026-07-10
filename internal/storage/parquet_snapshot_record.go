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

const snapshotRecordCodecParquet = "snapshot-record-arrow-parquet-v1"

func marshalParquetSnapshotRecord(ctx context.Context, record snapshotRecord) ([]byte, error) {
	normalized, hash, err := normalizeSnapshotRecordForParquet(record)
	if err != nil {
		return nil, err
	}
	rows, err := snapshotRecordRows(normalized)
	if err != nil {
		return nil, err
	}
	schema := parquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	header := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      normalized.TenantID,
		ID:            "snapshot-record",
		Version:       normalized.Snapshot.Version,
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

func decodeParquetSnapshotRecord(ctx context.Context, data []byte) (snapshotRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return snapshotRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return snapshotRecord{}, fmt.Errorf("parquet snapshot record is empty")
	}
	if table.NumCols() < int64(parquetCommitColumnEdgeSourceObservedAt+1) {
		return snapshotRecord{}, fmt.Errorf("parquet snapshot record has %d columns, want at least %d", table.NumCols(), parquetCommitColumnEdgeSourceObservedAt+1)
	}

	var record snapshotRecord
	var expectedHash string
	build := &commitBuild{}
	rows := 0
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetCommitColumns(batch)
		if err != nil {
			return snapshotRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowRecord := snapshotRecord{
				LayoutVersion: CurrentObjectLayoutVersion,
				TenantID:      columns.commitTenantID.Value(i),
				Snapshot:      graph.Snapshot{Version: columns.version.Value(i)},
			}
			if columns.tenantID.Value(i) != rowRecord.TenantID {
				return snapshotRecord{}, fmt.Errorf("snapshot record tenant mismatch")
			}
			if rows == 0 {
				record = rowRecord
				expectedHash = columns.contentHash.Value(i)
			} else if record.TenantID != rowRecord.TenantID || record.Snapshot.Version != rowRecord.Snapshot.Version || expectedHash != columns.contentHash.Value(i) {
				return snapshotRecord{}, fmt.Errorf("snapshot record identity mismatch")
			}
			row := parquetCommitRowFromColumns(columns, i)
			switch row.Kind {
			case commitRowMetadata,
				commitRowUpsertCIType, commitRowCITypeExtends, commitRowCITypeField, commitRowCITypeFieldEnum, commitRowCITypeFieldDefault, commitRowCITypeIdentity, commitRowCITypeIdentityField,
				commitRowUpsertRelationType, commitRowRelationFromKind, commitRowRelationToKind,
				commitRowUpsertEntity, commitRowUpsertEdge:
				if err := build.apply(row); err != nil {
					return snapshotRecord{}, err
				}
			default:
				return snapshotRecord{}, fmt.Errorf("unknown snapshot record row kind %q", row.Kind)
			}
			rows++
		}
	}
	for _, ordinal := range sortedIntKeys(build.ciTypes) {
		record.Snapshot.CITypes = setCITypeAt(record.Snapshot.CITypes, ordinal, build.ciTypes[ordinal].item)
	}
	for _, ordinal := range sortedIntKeys(build.relations) {
		record.Snapshot.RelationTypes = setRelationTypeAt(record.Snapshot.RelationTypes, ordinal, build.relations[ordinal].item)
	}
	for _, ordinal := range sortedIntKeys(build.entities) {
		record.Snapshot.Entities = setEntityAt(record.Snapshot.Entities, ordinal, decodedEntityPageCopy(build.entities[ordinal].item))
	}
	for _, ordinal := range sortedIntKeys(build.edges) {
		record.Snapshot.Edges = setEdgeAt(record.Snapshot.Edges, ordinal, decodedEdgeShardCopy(build.edges[ordinal].item))
	}
	if err := normalizeObjectAfterRead(&record, "snapshot"); err != nil {
		return snapshotRecord{}, err
	}
	hash, err := snapshotRecordContentHash(record)
	if err != nil {
		return snapshotRecord{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return snapshotRecord{}, fmt.Errorf("snapshot record content hash mismatch")
	}
	return record, nil
}

func normalizeSnapshotRecordForParquet(record snapshotRecord) (snapshotRecord, string, error) {
	payload, err := snapshotRecordPayloadJSON(record)
	if err != nil {
		return snapshotRecord{}, "", err
	}
	var normalized snapshotRecord
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return snapshotRecord{}, "", err
	}
	if err := normalizeObjectAfterRead(&normalized, "snapshot"); err != nil {
		return snapshotRecord{}, "", err
	}
	return normalized, objectContentHash(payload), nil
}

func snapshotRecordRows(record snapshotRecord) ([]parquetCommitRow, error) {
	rows := []parquetCommitRow{{Kind: commitRowMetadata}}
	for i, ciType := range record.Snapshot.CITypes {
		rows = append(rows, ciTypeRows(i, ciType)...)
	}
	for i, relationType := range record.Snapshot.RelationTypes {
		rows = append(rows, relationTypeRows(i, relationType)...)
	}
	for i, entity := range record.Snapshot.Entities {
		entityRows, err := entityMutationRows(commitRowUpsertEntity, i, 0, entity)
		if err != nil {
			return nil, err
		}
		rows = append(rows, entityRows...)
	}
	for i, edge := range record.Snapshot.Edges {
		edgeRows, err := edgeMutationRows(commitRowUpsertEdge, i, edge)
		if err != nil {
			return nil, err
		}
		rows = append(rows, edgeRows...)
	}
	return rows, nil
}

func parquetSnapshotRecordSchemaHash() string {
	return parquetCommitSchemaHash(snapshotRecordCodecParquet)
}
