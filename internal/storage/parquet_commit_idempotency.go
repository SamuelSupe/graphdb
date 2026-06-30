package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const directCommitRecordCodecParquet = "direct-commit-record-arrow-parquet-v1"

const (
	directCommitRowMetadata        = "direct_commit_metadata"
	directCommitRowSegment         = "direct_commit_segment"
	directCommitRowCommitKey       = "direct_commit_key"
	directCommitRowSuppressed      = "direct_commit_suppressed"
	directCommitRowSuppressedValue = "direct_commit_suppressed_value"
	directCommitRowCanonicalEntity = "direct_commit_canonical_entity"
	directCommitRowCanonicalEdge   = "direct_commit_canonical_edge"
	directCommitRowIndexWarning    = "direct_commit_index_warning"
)

const (
	directCommitValueExisting = "existing"
	directCommitValueIncoming = "incoming"
)

func marshalParquetDirectCommitRecord(ctx context.Context, record DirectCommitRecord) ([]byte, error) {
	normalized, hash, err := normalizeDirectCommitRecordForParquet(record)
	if err != nil {
		return nil, err
	}
	rows, err := directCommitRecordRows(normalized)
	if err != nil {
		return nil, err
	}
	schema := parquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	header := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      normalized.TenantID,
		ID:            normalized.Request.IdempotencyKey,
		Version:       normalized.Result.Version,
		CreatedAt:     normalized.FinishedAt,
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

func decodeParquetDirectCommitRecord(ctx context.Context, data []byte) (DirectCommitRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return DirectCommitRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return DirectCommitRecord{}, fmt.Errorf("parquet direct commit record is empty")
	}
	if table.NumCols() < int64(parquetCommitColumnEdgeSourceObservedAt+1) {
		return DirectCommitRecord{}, fmt.Errorf("parquet direct commit record has %d columns, want at least %d", table.NumCols(), parquetCommitColumnEdgeSourceObservedAt+1)
	}

	var record DirectCommitRecord
	var expectedHash string
	build := &commitBuild{}
	rows := 0
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetCommitColumns(batch)
		if err != nil {
			return DirectCommitRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowTenant := columns.commitTenantID.Value(i)
			rowKey := columns.commitID.Value(i)
			rowVersion := columns.version.Value(i)
			rowHash := columns.contentHash.Value(i)
			if columns.tenantID.Value(i) != rowTenant {
				return DirectCommitRecord{}, fmt.Errorf("direct commit record tenant mismatch")
			}
			if rows == 0 {
				record.TenantID = rowTenant
				record.Request.IdempotencyKey = rowKey
				record.Result.TenantID = rowTenant
				record.Result.Version = rowVersion
				expectedHash = rowHash
			} else if record.TenantID != rowTenant || record.Request.IdempotencyKey != rowKey || record.Result.Version != rowVersion || expectedHash != rowHash {
				return DirectCommitRecord{}, fmt.Errorf("direct commit record identity mismatch")
			}
			row := parquetCommitRowFromColumns(columns, i)
			if err := applyDirectCommitRow(&record, build, row); err != nil {
				return DirectCommitRecord{}, err
			}
			rows++
		}
	}
	for _, ordinal := range sortedIntKeys(build.ciTypes) {
		record.Request.Mutations.UpsertCITypes = setCITypeAt(record.Request.Mutations.UpsertCITypes, ordinal, build.ciTypes[ordinal].item)
	}
	for _, ordinal := range sortedIntKeys(build.relations) {
		record.Request.Mutations.UpsertRelationTypes = setRelationTypeAt(record.Request.Mutations.UpsertRelationTypes, ordinal, build.relations[ordinal].item)
	}
	for _, ordinal := range sortedIntKeys(build.entities) {
		record.Request.Mutations.UpsertEntities = setEntityAt(record.Request.Mutations.UpsertEntities, ordinal, decodedEntityPageCopy(build.entities[ordinal].item))
	}
	for _, ordinal := range sortedIntKeys(build.edges) {
		record.Request.Mutations.UpsertEdges = setEdgeAt(record.Request.Mutations.UpsertEdges, ordinal, decodedEdgeShardCopy(build.edges[ordinal].item))
	}
	hash, err := directCommitRecordContentHash(record)
	if err != nil {
		return DirectCommitRecord{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return DirectCommitRecord{}, fmt.Errorf("direct commit record content hash mismatch")
	}
	return record, nil
}

func applyDirectCommitRow(record *DirectCommitRecord, build *commitBuild, row parquetCommitRow) error {
	switch row.Kind {
	case directCommitRowMetadata:
		record.StartedAt = row.CreatedAtValue
		record.FinishedAt = row.UpdatedAtValue
		record.Result.UpdatedAt = row.FieldSource.UpdatedAt
		record.Result.HeadCommitID = row.SourceID
		record.Result.ReadAfterCommitID = row.TargetID
		record.Result.SnapshotKey = row.From
		record.Result.SnapshotCatalogKey = row.To
		record.Result.DataMD5 = row.ExternalID
		record.Result.LayoutVersion = row.SourcePriority
		record.Result.ReadableVersion = row.VersionValue
		record.Result.SnapshotVersion = row.FieldSource.Version
		record.Result.Skipped = row.Indexed
		record.Result.IdempotentReplay = row.Unique
		if row.Required {
			value := row.EntitySource.Priority
			expected := int64(value)
			record.Request.ExpectedVersion = &expected
		}
	case directCommitRowSegment:
		hash, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		record.Result.CommitSegments = setCommitSegmentRefAt(record.Result.CommitSegments, row.ParentOrdinal, CommitSegmentRef{
			Key:          row.EntryKey,
			Codec:        row.Action,
			FirstVersion: row.VersionValue,
			LastVersion:  row.FieldSource.Version,
			Count:        row.SourcePriority,
			ContentHash:  hash,
		})
	case directCommitRowCommitKey:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		record.Result.CommitKeys = setStringAt(record.Result.CommitKeys, row.ParentOrdinal, value)
	case directCommitRowSuppressed:
		record.Result.Suppressed = setFieldConflictAt(record.Result.Suppressed, row.ParentOrdinal, graph.FieldConflict{
			ResourceType:     row.ComponentKind,
			EntityID:         row.ID,
			EdgeID:           row.TargetID,
			CanonicalID:      row.SourceID,
			IncomingID:       row.ExternalID,
			Field:            row.EntryKey,
			ExistingSource:   row.Source,
			ExistingPriority: row.SourcePriority,
			IncomingSource:   row.From,
			IncomingPriority: row.FieldSource.Priority,
			Message:          row.Reason,
		})
	case directCommitRowSuppressedValue:
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		record.Result.Suppressed = ensureFieldConflictLen(record.Result.Suppressed, row.ParentOrdinal)
		switch row.ComponentKind {
		case directCommitValueExisting:
			record.Result.Suppressed[row.ParentOrdinal].ExistingValue = value
		case directCommitValueIncoming:
			record.Result.Suppressed[row.ParentOrdinal].IncomingValue = value
		default:
			return fmt.Errorf("unknown direct commit suppressed value role %q", row.ComponentKind)
		}
	case directCommitRowCanonicalEntity:
		record.Result.CanonicalEntities = setEntityCanonicalizationAt(record.Result.CanonicalEntities, row.ParentOrdinal, graph.EntityCanonicalization{
			CanonicalID: row.ID,
			IncomingID:  row.ExternalID,
			Kind:        row.KindName,
			Source:      row.Source,
			ExternalID:  row.SourceID,
		})
	case directCommitRowCanonicalEdge:
		record.Result.CanonicalEdges = setEdgeCanonicalizationAt(record.Result.CanonicalEdges, row.ParentOrdinal, graph.EdgeCanonicalization{
			CanonicalID: row.ID,
			IncomingID:  row.ExternalID,
			Type:        row.TypeName,
			From:        row.From,
			To:          row.To,
		})
	case directCommitRowIndexWarning:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		record.Result.IndexWarnings = setStringAt(record.Result.IndexWarnings, row.ParentOrdinal, value)
	default:
		return build.apply(row)
	}
	return nil
}

func directCommitRecordRows(record DirectCommitRecord) ([]parquetCommitRow, error) {
	metadata := parquetCommitRow{
		Kind:           directCommitRowMetadata,
		ID:             record.Request.IdempotencyKey,
		SourceID:       record.Result.HeadCommitID,
		TargetID:       record.Result.ReadAfterCommitID,
		From:           record.Result.SnapshotKey,
		To:             record.Result.SnapshotCatalogKey,
		ExternalID:     record.Result.DataMD5,
		SourcePriority: record.Result.LayoutVersion,
		Required:       record.Request.ExpectedVersion != nil,
		Indexed:        record.Result.Skipped,
		Unique:         record.Result.IdempotentReplay,
		VersionValue:   record.Result.ReadableVersion,
		CreatedAtValue: record.StartedAt,
		UpdatedAtValue: record.FinishedAt,
		FieldSource: graph.FieldSource{
			Version:   record.Result.SnapshotVersion,
			UpdatedAt: record.Result.UpdatedAt,
		},
	}
	if record.Request.ExpectedVersion != nil {
		metadata.EntitySource.Priority = int(*record.Request.ExpectedVersion)
	}
	rows := []parquetCommitRow{metadata}
	mutationRows, err := commitRows(graph.Commit{Mutations: record.Request.Mutations})
	if err != nil {
		return nil, err
	}
	for _, row := range mutationRows {
		if row.Kind != commitRowMetadata {
			rows = append(rows, row)
		}
	}
	for i, ref := range record.Result.CommitSegments {
		rows = append(rows, parquetCommitRow{
			Kind:           directCommitRowSegment,
			ParentOrdinal:  i,
			EntryKey:       ref.Key,
			Action:         ref.Codec,
			VersionValue:   ref.FirstVersion,
			SourcePriority: ref.Count,
			Value:          stringCommitValue(ref.ContentHash),
			FieldSource:    graph.FieldSource{Version: ref.LastVersion},
		})
	}
	for i, key := range record.Result.CommitKeys {
		rows = append(rows, parquetCommitRow{Kind: directCommitRowCommitKey, ParentOrdinal: i, Value: stringCommitValue(key)})
	}
	for i, conflict := range record.Result.Suppressed {
		rows = append(rows, parquetCommitRow{
			Kind:           directCommitRowSuppressed,
			ComponentKind:  conflict.ResourceType,
			ParentOrdinal:  i,
			ID:             conflict.EntityID,
			TargetID:       conflict.EdgeID,
			SourceID:       conflict.CanonicalID,
			ExternalID:     conflict.IncomingID,
			EntryKey:       conflict.Field,
			Source:         conflict.ExistingSource,
			SourcePriority: conflict.ExistingPriority,
			From:           conflict.IncomingSource,
			FieldSource:    graph.FieldSource{Priority: conflict.IncomingPriority},
			Reason:         conflict.Message,
		})
		if conflict.ExistingValue != nil {
			value, err := parquetValueFromAny(conflict.ExistingValue)
			if err != nil {
				return nil, err
			}
			rows = append(rows, parquetCommitRow{Kind: directCommitRowSuppressedValue, ComponentKind: directCommitValueExisting, ParentOrdinal: i, Value: value})
		}
		if conflict.IncomingValue != nil {
			value, err := parquetValueFromAny(conflict.IncomingValue)
			if err != nil {
				return nil, err
			}
			rows = append(rows, parquetCommitRow{Kind: directCommitRowSuppressedValue, ComponentKind: directCommitValueIncoming, ParentOrdinal: i, Value: value})
		}
	}
	for i, item := range record.Result.CanonicalEntities {
		rows = append(rows, parquetCommitRow{
			Kind:          directCommitRowCanonicalEntity,
			ParentOrdinal: i,
			ID:            item.CanonicalID,
			ExternalID:    item.IncomingID,
			KindName:      item.Kind,
			Source:        item.Source,
			SourceID:      item.ExternalID,
		})
	}
	for i, item := range record.Result.CanonicalEdges {
		rows = append(rows, parquetCommitRow{
			Kind:          directCommitRowCanonicalEdge,
			ParentOrdinal: i,
			ID:            item.CanonicalID,
			ExternalID:    item.IncomingID,
			TypeName:      item.Type,
			From:          item.From,
			To:            item.To,
		})
	}
	for i, warning := range record.Result.IndexWarnings {
		rows = append(rows, parquetCommitRow{Kind: directCommitRowIndexWarning, ParentOrdinal: i, Value: stringCommitValue(warning)})
	}
	return rows, nil
}

func normalizeDirectCommitRecordForParquet(record DirectCommitRecord) (DirectCommitRecord, string, error) {
	payload, err := directCommitRecordPayloadJSON(record)
	if err != nil {
		return DirectCommitRecord{}, "", err
	}
	var normalized DirectCommitRecord
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return DirectCommitRecord{}, "", err
	}
	canonical, err := directCommitRecordPayloadJSON(normalized)
	if err != nil {
		return DirectCommitRecord{}, "", err
	}
	return normalized, objectContentHash(canonical), nil
}

func directCommitRecordPayloadJSON(record DirectCommitRecord) ([]byte, error) {
	record.Request.IdempotencyKey = strings.TrimSpace(record.Request.IdempotencyKey)
	return json.Marshal(record)
}

func directCommitRecordContentHash(record DirectCommitRecord) (string, error) {
	_, hash, err := normalizeDirectCommitRecordForParquet(record)
	return hash, err
}

func setCommitSegmentRefAt(values []CommitSegmentRef, index int, value CommitSegmentRef) []CommitSegmentRef {
	for len(values) <= index {
		values = append(values, CommitSegmentRef{})
	}
	values[index] = value
	return values
}

func ensureFieldConflictLen(values []graph.FieldConflict, index int) []graph.FieldConflict {
	for len(values) <= index {
		values = append(values, graph.FieldConflict{})
	}
	return values
}

func setFieldConflictAt(values []graph.FieldConflict, index int, value graph.FieldConflict) []graph.FieldConflict {
	values = ensureFieldConflictLen(values, index)
	values[index] = value
	return values
}

func setEntityCanonicalizationAt(values []graph.EntityCanonicalization, index int, value graph.EntityCanonicalization) []graph.EntityCanonicalization {
	for len(values) <= index {
		values = append(values, graph.EntityCanonicalization{})
	}
	values[index] = value
	return values
}

func setEdgeCanonicalizationAt(values []graph.EdgeCanonicalization, index int, value graph.EdgeCanonicalization) []graph.EdgeCanonicalization {
	for len(values) <= index {
		values = append(values, graph.EdgeCanonicalization{})
	}
	values[index] = value
	return values
}
