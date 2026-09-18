package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const snapshotRecordCodecParquet = "snapshot-record-arrow-parquet-v1"

const parquetSnapshotRecordBatchRows = 16384

func marshalParquetSnapshotRecord(ctx context.Context, record snapshotRecord) ([]byte, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	normalized, hash, err := normalizeSnapshotRecordForParquet(record)
	if err != nil {
		return nil, err
	}
	schema := parquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	writer, err := pqarrow.NewFileWriter(schema, &buf, writerProps, arrowProps)
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	header := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      normalized.TenantID,
		ID:            "snapshot-record",
		Version:       normalized.Snapshot.Version,
	}
	pending := 0
	flush := func() error {
		if err := objectContextErr(ctx); err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		batch := builder.NewRecordBatch()
		defer batch.Release()
		pending = 0
		return writer.Write(batch)
	}
	// Ordinals and the content hash span all row groups. Bound the intermediate
	// Arrow row count independently of snapshot size, including splits within an entity.
	err = visitSnapshotRecordRows(normalized, func(rows []parquetCommitRow) error {
		if err := objectContextErr(ctx); err != nil {
			return err
		}
		for _, row := range rows {
			appendParquetCommitRow(builder, normalized.TenantID, "", header, hash, row)
			pending++
			if pending == parquetSnapshotRecordBatchRows {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetSnapshotRecord(ctx context.Context, data []byte) (snapshotRecord, error) {
	return decodeParquetSnapshotRecordReader(ctx, bytes.NewReader(data))
}

func decodeParquetSnapshotRecordReader(ctx context.Context, source parquet.ReaderAtSeeker) (snapshotRecord, error) {
	if err := objectContextErr(ctx); err != nil {
		return snapshotRecord{}, err
	}
	file, err := pqfile.NewParquetReader(source)
	if err != nil {
		return snapshotRecord{}, err
	}
	defer file.Close()
	fileReader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{BatchSize: 4096}, memory.DefaultAllocator)
	if err != nil {
		return snapshotRecord{}, err
	}
	reader, release, err := readParquetRecordReader(ctx, fileReader, nil, nil)
	if err != nil {
		return snapshotRecord{}, err
	}
	defer release()
	defer reader.Release()
	if columns := reader.Schema().NumFields(); columns < parquetCommitColumnEdgeSourceObservedAt+1 {
		return snapshotRecord{}, fmt.Errorf("parquet snapshot record has %d columns, want at least %d", columns, parquetCommitColumnEdgeSourceObservedAt+1)
	}

	var record snapshotRecord
	var expectedHash string
	build := &commitBuild{}
	rows := 0
	for reader.Next() {
		if err := objectContextErr(ctx); err != nil {
			return snapshotRecord{}, err
		}
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
	if err := reader.Err(); err != nil {
		return snapshotRecord{}, err
	}
	if err := objectContextErr(ctx); err != nil {
		return snapshotRecord{}, err
	}
	if rows == 0 {
		return snapshotRecord{}, fmt.Errorf("parquet snapshot record is empty")
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
	return record, objectContextErr(ctx)
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

func visitSnapshotRecordRows(record snapshotRecord, visit func([]parquetCommitRow) error) error {
	if err := visit([]parquetCommitRow{{Kind: commitRowMetadata}}); err != nil {
		return err
	}
	for i, ciType := range record.Snapshot.CITypes {
		if err := visit(ciTypeRows(i, ciType)); err != nil {
			return err
		}
	}
	for i, relationType := range record.Snapshot.RelationTypes {
		if err := visit(relationTypeRows(i, relationType)); err != nil {
			return err
		}
	}
	for i, entity := range record.Snapshot.Entities {
		rows, err := entityMutationRows(commitRowUpsertEntity, i, 0, entity)
		if err != nil {
			return err
		}
		if err := visit(rows); err != nil {
			return err
		}
	}
	for i, edge := range record.Snapshot.Edges {
		rows, err := edgeMutationRows(commitRowUpsertEdge, i, edge)
		if err != nil {
			return err
		}
		if err := visit(rows); err != nil {
			return err
		}
	}
	return nil
}
