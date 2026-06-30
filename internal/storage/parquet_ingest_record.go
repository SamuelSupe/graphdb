package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const ingestRecordCodecParquet = "ingest-record-arrow-parquet-v1"

const (
	ingestRowMetadata      = "ingest_metadata"
	ingestRowItem          = "ingest_item"
	ingestRowDeleteEntity  = "ingest_delete_entity"
	ingestRowDeleteEdge    = "ingest_delete_edge"
	ingestRowFailure       = "ingest_failure"
	ingestRowConflict      = "ingest_conflict"
	ingestRowConflictValue = "ingest_conflict_value"
	ingestConflictValueOld = "existing"
	ingestConflictValueNew = "incoming"
)

func marshalParquetIngestRecord(ctx context.Context, record IngestBatchRecord) ([]byte, error) {
	normalized, hash, err := normalizeIngestRecordForParquet(record)
	if err != nil {
		return nil, err
	}
	rows, err := ingestRecordRows(normalized)
	if err != nil {
		return nil, err
	}
	schema := parquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	header := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      normalized.TenantID,
		ID:            normalized.Request.BatchID,
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

func decodeParquetIngestRecord(ctx context.Context, data []byte) (IngestBatchRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return IngestBatchRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return IngestBatchRecord{}, fmt.Errorf("parquet ingest record is empty")
	}
	if table.NumCols() < int64(parquetCommitColumnEdgeSourceObservedAt+1) {
		return IngestBatchRecord{}, fmt.Errorf("parquet ingest record has %d columns, want at least %d", table.NumCols(), parquetCommitColumnEdgeSourceObservedAt+1)
	}

	var record IngestBatchRecord
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
			return IngestBatchRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowTenant := columns.commitTenantID.Value(i)
			rowBatch := columns.commitID.Value(i)
			rowVersion := columns.version.Value(i)
			rowHash := columns.contentHash.Value(i)
			if columns.tenantID.Value(i) != rowTenant {
				return IngestBatchRecord{}, fmt.Errorf("ingest record tenant mismatch")
			}
			if rows == 0 {
				record.TenantID = rowTenant
				record.Request.BatchID = rowBatch
				record.Result.Version = rowVersion
				expectedHash = rowHash
			} else if record.TenantID != rowTenant || record.Request.BatchID != rowBatch || record.Result.Version != rowVersion || expectedHash != rowHash {
				return IngestBatchRecord{}, fmt.Errorf("ingest record identity mismatch")
			}
			row := parquetCommitRowFromColumns(columns, i)
			if err := applyIngestRecordRow(&record, build, &deletes, row); err != nil {
				return IngestBatchRecord{}, err
			}
			rows++
		}
	}
	applyIngestBuildRows(&record, build, deletes)
	hash, err := ingestRecordContentHash(record)
	if err != nil {
		return IngestBatchRecord{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return IngestBatchRecord{}, fmt.Errorf("ingest record content hash mismatch")
	}
	return record, nil
}

type ingestDeleteRows struct {
	entities map[int]graph.EntityDeleteRequest
	edges    map[int]graph.EdgeDeleteRequest
}

func applyIngestRecordRow(record *IngestBatchRecord, build *commitBuild, deletes *ingestDeleteRows, row parquetCommitRow) error {
	switch row.Kind {
	case ingestRowMetadata:
		record.Request.Source = row.Source
		record.Request.CollectorID = row.ExternalID
		record.Request.BatchID = row.ID
		record.Request.IdempotencyKey = row.TargetID
		record.Request.Cursor = row.SourceID
		record.Request.FullSync = row.Required
		record.Request.StaleAction = row.Action
		record.Request.StaleKind = row.KindName
		record.Result.BatchID = row.To
		record.Result.Version = row.VersionValue
		record.Result.Applied = row.SourcePriority
		record.Result.Failed = row.FieldSource.Priority
		record.Result.Suppressed = row.EdgeSource.Priority
		record.Result.Skipped = row.Indexed
		record.Result.Cursor = row.Reason
		record.StartedAt = row.CreatedAtValue
		record.FinishedAt = row.UpdatedAtValue
		if row.NestedOrdinal > 0 {
			record.Request.Items = make([]IngestItem, row.NestedOrdinal)
		}
	case ingestRowItem:
		record.Request.Items = ensureIngestItemsLen(record.Request.Items, row.ParentOrdinal)
		record.Request.Items[row.ParentOrdinal].ExternalID = row.ExternalID
	case ingestRowDeleteEntity:
		if deletes.entities == nil {
			deletes.entities = map[int]graph.EntityDeleteRequest{}
		}
		deletes.entities[row.ParentOrdinal] = graph.EntityDeleteRequest{
			ID: row.ID, Kind: row.KindName, Source: row.Source, ExternalID: row.ExternalID, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		}
	case ingestRowDeleteEdge:
		if deletes.edges == nil {
			deletes.edges = map[int]graph.EdgeDeleteRequest{}
		}
		deletes.edges[row.ParentOrdinal] = graph.EdgeDeleteRequest{
			ID: row.ID, Type: row.TypeName, From: row.From, To: row.To, Source: row.Source, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		}
	case ingestRowFailure:
		record.Result.Failures = setIngestFailureAt(record.Result.Failures, row.ParentOrdinal, IngestFailure{
			Index: row.ChildOrdinal, ExternalID: row.ExternalID, Error: row.Reason,
		})
	case ingestRowConflict:
		record.Result.Conflicts = setIngestConflictAt(record.Result.Conflicts, row.ParentOrdinal, IngestConflict{
			ResourceType:     row.ComponentKind,
			Index:            row.ChildOrdinal,
			ExternalID:       row.ExternalID,
			ExistingID:       row.Action,
			EntityID:         row.ID,
			EdgeID:           row.TargetID,
			CanonicalID:      row.SourceID,
			IncomingID:       row.SplitFrom,
			Field:            row.EntryKey,
			ExistingSource:   row.Source,
			ExistingPriority: row.SourcePriority,
			IncomingSource:   row.From,
			IncomingPriority: row.FieldSource.Priority,
			Message:          row.Reason,
		})
	case ingestRowConflictValue:
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		record.Result.Conflicts = ensureIngestConflictsLen(record.Result.Conflicts, row.ParentOrdinal)
		switch row.ComponentKind {
		case ingestConflictValueOld:
			record.Result.Conflicts[row.ParentOrdinal].ExistingValue = value
		case ingestConflictValueNew:
			record.Result.Conflicts[row.ParentOrdinal].IncomingValue = value
		default:
			return fmt.Errorf("unknown ingest conflict value role %q", row.ComponentKind)
		}
	default:
		return build.apply(row)
	}
	return nil
}

func applyIngestBuildRows(record *IngestBatchRecord, build *commitBuild, deletes ingestDeleteRows) {
	for _, ordinal := range sortedIntKeys(build.ciTypes) {
		record.Request.Items = ensureIngestItemsLen(record.Request.Items, ordinal)
		item := build.ciTypes[ordinal].item
		record.Request.Items[ordinal].CIType = &item
	}
	for _, ordinal := range sortedIntKeys(build.relations) {
		record.Request.Items = ensureIngestItemsLen(record.Request.Items, ordinal)
		item := build.relations[ordinal].item
		record.Request.Items[ordinal].Relation = &item
	}
	for _, ordinal := range sortedIntKeys(build.entities) {
		record.Request.Items = ensureIngestItemsLen(record.Request.Items, ordinal)
		item := decodedEntityPageCopy(build.entities[ordinal].item)
		record.Request.Items[ordinal].Entity = &item
	}
	for _, ordinal := range sortedIntKeys(build.edges) {
		record.Request.Items = ensureIngestItemsLen(record.Request.Items, ordinal)
		item := decodedEdgeShardCopy(build.edges[ordinal].item)
		record.Request.Items[ordinal].Edge = &item
	}
	for _, ordinal := range sortedIntKeys(deletes.entities) {
		record.Request.Items = ensureIngestItemsLen(record.Request.Items, ordinal)
		item := deletes.entities[ordinal]
		record.Request.Items[ordinal].DeleteEntity = &item
	}
	for _, ordinal := range sortedIntKeys(deletes.edges) {
		record.Request.Items = ensureIngestItemsLen(record.Request.Items, ordinal)
		item := deletes.edges[ordinal]
		record.Request.Items[ordinal].DeleteEdge = &item
	}
}

func ingestRecordRows(record IngestBatchRecord) ([]parquetCommitRow, error) {
	rows := []parquetCommitRow{{
		Kind:           ingestRowMetadata,
		Source:         record.Request.Source,
		ExternalID:     record.Request.CollectorID,
		ID:             record.Request.BatchID,
		TargetID:       record.Request.IdempotencyKey,
		SourceID:       record.Request.Cursor,
		Required:       record.Request.FullSync,
		Indexed:        record.Result.Skipped,
		Action:         record.Request.StaleAction,
		KindName:       record.Request.StaleKind,
		To:             record.Result.BatchID,
		Reason:         record.Result.Cursor,
		SourcePriority: record.Result.Applied,
		VersionValue:   record.Result.Version,
		CreatedAtValue: record.StartedAt,
		UpdatedAtValue: record.FinishedAt,
		NestedOrdinal:  len(record.Request.Items),
		FieldSource:    graph.FieldSource{Priority: record.Result.Failed},
		EdgeSource:     graph.EdgeSource{Priority: record.Result.Suppressed},
	}}
	for i, item := range record.Request.Items {
		rows = append(rows, parquetCommitRow{Kind: ingestRowItem, ParentOrdinal: i, ExternalID: item.ExternalID})
		if item.CIType != nil {
			rows = append(rows, ciTypeRows(i, *item.CIType)...)
		}
		if item.Relation != nil {
			rows = append(rows, relationTypeRows(i, *item.Relation)...)
		}
		if item.Entity != nil {
			entityRows, err := entityMutationRows(commitRowUpsertEntity, i, 0, *item.Entity)
			if err != nil {
				return nil, err
			}
			rows = append(rows, entityRows...)
		}
		if item.Edge != nil {
			edgeRows, err := edgeMutationRows(commitRowUpsertEdge, i, *item.Edge)
			if err != nil {
				return nil, err
			}
			rows = append(rows, edgeRows...)
		}
		if item.DeleteEntity != nil {
			request := item.DeleteEntity
			rows = append(rows, parquetCommitRow{
				Kind:           ingestRowDeleteEntity,
				ParentOrdinal:  i,
				ID:             request.ID,
				KindName:       request.Kind,
				Source:         request.Source,
				ExternalID:     request.ExternalID,
				SourcePriority: request.SourceRank,
				Confidence:     request.Confidence,
				Reason:         request.Reason,
			})
		}
		if item.DeleteEdge != nil {
			request := item.DeleteEdge
			rows = append(rows, parquetCommitRow{
				Kind:           ingestRowDeleteEdge,
				ParentOrdinal:  i,
				ID:             request.ID,
				TypeName:       request.Type,
				From:           request.From,
				To:             request.To,
				Source:         request.Source,
				SourcePriority: request.SourceRank,
				Confidence:     request.Confidence,
				Reason:         request.Reason,
			})
		}
	}
	for i, failure := range record.Result.Failures {
		rows = append(rows, parquetCommitRow{
			Kind:          ingestRowFailure,
			ParentOrdinal: i,
			ChildOrdinal:  failure.Index,
			ExternalID:    failure.ExternalID,
			Reason:        failure.Error,
		})
	}
	for i, conflict := range record.Result.Conflicts {
		rows = append(rows, parquetCommitRow{
			Kind:           ingestRowConflict,
			ComponentKind:  conflict.ResourceType,
			ParentOrdinal:  i,
			ChildOrdinal:   conflict.Index,
			ExternalID:     conflict.ExternalID,
			Action:         conflict.ExistingID,
			ID:             conflict.EntityID,
			TargetID:       conflict.EdgeID,
			SourceID:       conflict.CanonicalID,
			SplitFrom:      conflict.IncomingID,
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
			rows = append(rows, parquetCommitRow{Kind: ingestRowConflictValue, ComponentKind: ingestConflictValueOld, ParentOrdinal: i, Value: value})
		}
		if conflict.IncomingValue != nil {
			value, err := parquetValueFromAny(conflict.IncomingValue)
			if err != nil {
				return nil, err
			}
			rows = append(rows, parquetCommitRow{Kind: ingestRowConflictValue, ComponentKind: ingestConflictValueNew, ParentOrdinal: i, Value: value})
		}
	}
	return rows, nil
}

func normalizeIngestRecordForParquet(record IngestBatchRecord) (IngestBatchRecord, string, error) {
	payload, err := ingestRecordPayloadJSON(record)
	if err != nil {
		return IngestBatchRecord{}, "", err
	}
	var normalized IngestBatchRecord
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return IngestBatchRecord{}, "", err
	}
	canonical, err := ingestRecordPayloadJSON(normalized)
	if err != nil {
		return IngestBatchRecord{}, "", err
	}
	return normalized, objectContentHash(canonical), nil
}

func ingestRecordPayloadJSON(record IngestBatchRecord) ([]byte, error) {
	if record.Request.Items == nil {
		record.Request.Items = []IngestItem{}
	}
	return json.Marshal(record)
}

func ingestRecordContentHash(record IngestBatchRecord) (string, error) {
	_, hash, err := normalizeIngestRecordForParquet(record)
	return hash, err
}

func ensureIngestItemsLen(values []IngestItem, index int) []IngestItem {
	for len(values) <= index {
		values = append(values, IngestItem{})
	}
	return values
}

func setIngestFailureAt(values []IngestFailure, index int, value IngestFailure) []IngestFailure {
	for len(values) <= index {
		values = append(values, IngestFailure{})
	}
	values[index] = value
	return values
}

func ensureIngestConflictsLen(values []IngestConflict, index int) []IngestConflict {
	for len(values) <= index {
		values = append(values, IngestConflict{})
	}
	return values
}

func setIngestConflictAt(values []IngestConflict, index int, value IngestConflict) []IngestConflict {
	values = ensureIngestConflictsLen(values, index)
	values[index] = value
	return values
}
