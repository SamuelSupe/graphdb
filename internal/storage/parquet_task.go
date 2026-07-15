package storage

import (
	"bytes"
	"context"
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
	taskCodecParquet       = "task-arrow-parquet-v1"
	indexTaskCodecParquet  = "index-task-arrow-parquet-v1"
	taskResultCodecParquet = "task-result-arrow-parquet-v1"
)

const (
	parquetTaskColumnTenantID = iota
	parquetTaskColumnID
	parquetTaskColumnType
	parquetTaskColumnStatus
	parquetTaskColumnPhase
	parquetTaskColumnProgressCompleted
	parquetTaskColumnProgressTotal
	parquetTaskColumnOwnerID
	parquetTaskColumnCatalogVersion
	parquetTaskColumnResultKey
	parquetTaskColumnError
	parquetTaskColumnStartedAt
	parquetTaskColumnUpdatedAt
	parquetTaskColumnFinishedAt
	parquetTaskColumnContentHash
	parquetTaskColumnRowKind
	parquetTaskColumnEntryKey
	parquetTaskColumnValueKind
	parquetTaskColumnStringValue
	parquetTaskColumnBoolValue
	parquetTaskColumnFloatValue
)

const (
	taskRowMetadata   = "metadata"
	taskRowParam      = "param"
	taskRowCheckpoint = "checkpoint"
	taskRowResult     = "result"
)

type parquetTaskRow struct {
	Kind  string
	Key   string
	Value parquetValue
}

type parquetTaskIdentity struct {
	TenantID          string
	ID                string
	Type              string
	Status            string
	Phase             string
	ProgressCompleted int
	ProgressTotal     int
	OwnerID           string
	CatalogVersion    int64
	ResultKey         string
	Error             string
	StartedAt         string
	UpdatedAt         string
	FinishedAt        string
	ContentHash       string
}

type parquetTaskColumnSet struct {
	tenantID          *array.String
	id                *array.String
	taskType          *array.String
	status            *array.String
	phase             *array.String
	progressCompleted *array.Int64
	progressTotal     *array.Int64
	ownerID           *array.String
	catalogVersion    *array.Int64
	resultKey         *array.String
	taskError         *array.String
	startedAt         *array.String
	updatedAt         *array.String
	finishedAt        *array.String
	contentHash       *array.String
	rowKind           *array.String
	entryKey          *array.String
	valueKind         *array.String
	stringValue       *array.String
	boolValue         *array.Boolean
	floatValue        *array.Float64
}

func marshalParquetTask(ctx context.Context, task Task) ([]byte, error) {
	normalized, hash, err := normalizeTaskForParquet(task)
	if err != nil {
		return nil, err
	}
	identity := parquetTaskIdentity{
		TenantID:          normalized.TenantID,
		ID:                normalized.ID,
		Type:              normalized.Type,
		Status:            normalized.Status,
		Phase:             normalized.Phase,
		ProgressCompleted: normalized.ProgressCompleted,
		ProgressTotal:     normalized.ProgressTotal,
		OwnerID:           normalized.OwnerID,
		ResultKey:         normalized.ResultKey,
		Error:             normalized.Error,
		StartedAt:         formatParquetTime(normalized.StartedAt),
		UpdatedAt:         formatParquetTime(normalized.UpdatedAt),
		FinishedAt:        formatParquetTime(normalized.FinishedAt),
		ContentHash:       hash,
	}
	rows, err := taskRows(normalized.Params, normalized.Checkpoint, normalized.Result)
	if err != nil {
		return nil, err
	}
	return marshalParquetTaskRows(ctx, identity, rows)
}

func decodeParquetTask(ctx context.Context, data []byte) (Task, error) {
	identity, rows, err := decodeParquetTaskRows(ctx, data)
	if err != nil {
		return Task{}, err
	}
	task := Task{
		ID:                identity.ID,
		TenantID:          identity.TenantID,
		Type:              identity.Type,
		Status:            identity.Status,
		Phase:             identity.Phase,
		ProgressCompleted: identity.ProgressCompleted,
		ProgressTotal:     identity.ProgressTotal,
		OwnerID:           identity.OwnerID,
		ResultKey:         identity.ResultKey,
		Error:             identity.Error,
		StartedAt:         parseParquetTime(identity.StartedAt),
		UpdatedAt:         parseParquetTime(identity.UpdatedAt),
		FinishedAt:        parseParquetTime(identity.FinishedAt),
	}
	for _, row := range rows {
		switch row.Kind {
		case taskRowMetadata:
		case taskRowParam:
			value, err := anyFromParquetValue(row.Value)
			if err != nil {
				return Task{}, err
			}
			if task.Params == nil {
				task.Params = map[string]any{}
			}
			task.Params[row.Key] = value
		case taskRowCheckpoint:
			value, err := anyFromParquetValue(row.Value)
			if err != nil {
				return Task{}, err
			}
			if task.Checkpoint == nil {
				task.Checkpoint = map[string]any{}
			}
			task.Checkpoint[row.Key] = value
		case taskRowResult:
			value, err := anyFromParquetValue(row.Value)
			if err != nil {
				return Task{}, err
			}
			if task.Result == nil {
				task.Result = map[string]any{}
			}
			task.Result[row.Key] = value
		default:
			return Task{}, fmt.Errorf("unknown task row kind %q", row.Kind)
		}
	}
	hash, err := taskContentHash(task)
	if err != nil {
		return Task{}, err
	}
	if identity.ContentHash == "" || identity.ContentHash != hash {
		return Task{}, fmt.Errorf("task content hash mismatch")
	}
	return task, nil
}

func marshalParquetIndexTask(ctx context.Context, task IndexTask) ([]byte, error) {
	normalized, hash, err := normalizeIndexTaskForParquet(task)
	if err != nil {
		return nil, err
	}
	return marshalParquetTaskRows(ctx, parquetTaskIdentity{
		TenantID:          normalized.TenantID,
		ID:                normalized.ID,
		Type:              normalized.Type,
		Status:            normalized.Status,
		Phase:             normalized.Phase,
		ProgressCompleted: normalized.ProgressCompleted,
		ProgressTotal:     normalized.ProgressTotal,
		OwnerID:           normalized.OwnerID,
		CatalogVersion:    normalized.CatalogVersion,
		Error:             normalized.Error,
		StartedAt:         formatParquetTime(normalized.StartedAt),
		UpdatedAt:         formatParquetTime(normalized.UpdatedAt),
		FinishedAt:        formatParquetTime(normalized.FinishedAt),
		ContentHash:       hash,
	}, []parquetTaskRow{{Kind: taskRowMetadata}})
}

func decodeParquetIndexTask(ctx context.Context, data []byte) (IndexTask, error) {
	identity, rows, err := decodeParquetTaskRows(ctx, data)
	if err != nil {
		return IndexTask{}, err
	}
	for _, row := range rows {
		if row.Kind != taskRowMetadata {
			return IndexTask{}, fmt.Errorf("unknown index task row kind %q", row.Kind)
		}
	}
	task := IndexTask{
		ID:                identity.ID,
		TenantID:          identity.TenantID,
		Type:              identity.Type,
		Status:            identity.Status,
		Phase:             identity.Phase,
		ProgressCompleted: identity.ProgressCompleted,
		ProgressTotal:     identity.ProgressTotal,
		OwnerID:           identity.OwnerID,
		CatalogVersion:    identity.CatalogVersion,
		Error:             identity.Error,
		StartedAt:         parseParquetTime(identity.StartedAt),
		UpdatedAt:         parseParquetTime(identity.UpdatedAt),
		FinishedAt:        parseParquetTime(identity.FinishedAt),
	}
	hash, err := indexTaskContentHash(task)
	if err != nil {
		return IndexTask{}, err
	}
	if identity.ContentHash == "" || identity.ContentHash != hash {
		return IndexTask{}, fmt.Errorf("index task content hash mismatch")
	}
	return task, nil
}

func marshalParquetTaskResult(ctx context.Context, tenantID string, taskID string, result map[string]any) ([]byte, error) {
	normalized, hash, err := normalizeTaskResultForParquet(result)
	if err != nil {
		return nil, err
	}
	rows, err := taskValueRows(taskRowResult, normalized)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		rows = []parquetTaskRow{{Kind: taskRowMetadata}}
	}
	return marshalParquetTaskRows(ctx, parquetTaskIdentity{
		TenantID:    tenantID,
		ID:          taskID,
		Type:        "task_result",
		ContentHash: hash,
	}, rows)
}

func decodeParquetTaskResult(ctx context.Context, data []byte, tenantID string, taskID string) (map[string]any, error) {
	identity, rows, err := decodeParquetTaskRows(ctx, data)
	if err != nil {
		return nil, err
	}
	if identity.TenantID != tenantID || identity.ID != taskID {
		return nil, fmt.Errorf("task result identity mismatch")
	}
	result := map[string]any{}
	for _, row := range rows {
		switch row.Kind {
		case taskRowMetadata:
		case taskRowResult:
			value, err := anyFromParquetValue(row.Value)
			if err != nil {
				return nil, err
			}
			result[row.Key] = value
		default:
			return nil, fmt.Errorf("unknown task result row kind %q", row.Kind)
		}
	}
	hash, err := taskResultContentHash(result)
	if err != nil {
		return nil, err
	}
	if identity.ContentHash == "" || identity.ContentHash != hash {
		return nil, fmt.Errorf("task result content hash mismatch")
	}
	return result, nil
}

func marshalParquetTaskRows(ctx context.Context, identity parquetTaskIdentity, rows []parquetTaskRow) ([]byte, error) {
	if len(rows) == 0 {
		rows = []parquetTaskRow{{Kind: taskRowMetadata}}
	}
	schema := parquetTaskArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, row := range rows {
		appendParquetTaskRow(builder, identity, row)
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

func decodeParquetTaskRows(ctx context.Context, data []byte) (parquetTaskIdentity, []parquetTaskRow, error) {
	table, release, err := readParquetTable(ctx, data)
	if err != nil {
		return parquetTaskIdentity{}, nil, err
	}
	defer release()
	defer table.Release()
	if table.NumRows() < 1 {
		return parquetTaskIdentity{}, nil, fmt.Errorf("parquet task is empty")
	}
	if table.NumCols() < int64(parquetTaskColumnFloatValue+1) {
		return parquetTaskIdentity{}, nil, fmt.Errorf("parquet task has %d columns, want at least %d", table.NumCols(), parquetTaskColumnFloatValue+1)
	}
	var identity parquetTaskIdentity
	rows := []parquetTaskRow{}
	seen := false
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetTaskColumns(batch)
		if err != nil {
			return parquetTaskIdentity{}, nil, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowIdentity := parquetTaskIdentityFromColumns(columns, i)
			if !seen {
				identity = rowIdentity
				seen = true
			} else if identity != rowIdentity {
				return parquetTaskIdentity{}, nil, fmt.Errorf("task identity mismatch")
			}
			rows = append(rows, parquetTaskRowFromColumns(columns, i))
		}
	}
	return identity, rows, nil
}

func appendParquetTaskRow(builder *array.RecordBuilder, identity parquetTaskIdentity, row parquetTaskRow) {
	builder.Field(parquetTaskColumnTenantID).(*array.StringBuilder).Append(identity.TenantID)
	builder.Field(parquetTaskColumnID).(*array.StringBuilder).Append(identity.ID)
	builder.Field(parquetTaskColumnType).(*array.StringBuilder).Append(identity.Type)
	builder.Field(parquetTaskColumnStatus).(*array.StringBuilder).Append(identity.Status)
	builder.Field(parquetTaskColumnPhase).(*array.StringBuilder).Append(identity.Phase)
	builder.Field(parquetTaskColumnProgressCompleted).(*array.Int64Builder).Append(int64(identity.ProgressCompleted))
	builder.Field(parquetTaskColumnProgressTotal).(*array.Int64Builder).Append(int64(identity.ProgressTotal))
	builder.Field(parquetTaskColumnOwnerID).(*array.StringBuilder).Append(identity.OwnerID)
	builder.Field(parquetTaskColumnCatalogVersion).(*array.Int64Builder).Append(identity.CatalogVersion)
	builder.Field(parquetTaskColumnResultKey).(*array.StringBuilder).Append(identity.ResultKey)
	builder.Field(parquetTaskColumnError).(*array.StringBuilder).Append(identity.Error)
	builder.Field(parquetTaskColumnStartedAt).(*array.StringBuilder).Append(identity.StartedAt)
	builder.Field(parquetTaskColumnUpdatedAt).(*array.StringBuilder).Append(identity.UpdatedAt)
	builder.Field(parquetTaskColumnFinishedAt).(*array.StringBuilder).Append(identity.FinishedAt)
	builder.Field(parquetTaskColumnContentHash).(*array.StringBuilder).Append(identity.ContentHash)
	builder.Field(parquetTaskColumnRowKind).(*array.StringBuilder).Append(row.Kind)
	builder.Field(parquetTaskColumnEntryKey).(*array.StringBuilder).Append(row.Key)
	builder.Field(parquetTaskColumnValueKind).(*array.StringBuilder).Append(row.Value.Kind)
	builder.Field(parquetTaskColumnStringValue).(*array.StringBuilder).Append(row.Value.StringValue)
	builder.Field(parquetTaskColumnBoolValue).(*array.BooleanBuilder).Append(row.Value.BoolValue)
	builder.Field(parquetTaskColumnFloatValue).(*array.Float64Builder).Append(row.Value.FloatValue)
}

func parquetTaskColumns(record arrow.RecordBatch) (parquetTaskColumnSet, error) {
	var columns parquetTaskColumnSet
	var err error
	stringColumns := []struct {
		target **array.String
		index  int
		name   string
	}{
		{&columns.tenantID, parquetTaskColumnTenantID, "tenant_id"},
		{&columns.id, parquetTaskColumnID, "id"},
		{&columns.taskType, parquetTaskColumnType, "type"},
		{&columns.status, parquetTaskColumnStatus, "status"},
		{&columns.phase, parquetTaskColumnPhase, "phase"},
		{&columns.ownerID, parquetTaskColumnOwnerID, "owner_id"},
		{&columns.resultKey, parquetTaskColumnResultKey, "result_key"},
		{&columns.taskError, parquetTaskColumnError, "error"},
		{&columns.startedAt, parquetTaskColumnStartedAt, "started_at"},
		{&columns.updatedAt, parquetTaskColumnUpdatedAt, "updated_at"},
		{&columns.finishedAt, parquetTaskColumnFinishedAt, "finished_at"},
		{&columns.contentHash, parquetTaskColumnContentHash, "content_hash"},
		{&columns.rowKind, parquetTaskColumnRowKind, "row_kind"},
		{&columns.entryKey, parquetTaskColumnEntryKey, "entry_key"},
		{&columns.valueKind, parquetTaskColumnValueKind, "value_kind"},
		{&columns.stringValue, parquetTaskColumnStringValue, "string_value"},
	}
	for _, item := range stringColumns {
		if *item.target, err = parquetStringColumn(record, item.index, item.name); err != nil {
			return columns, err
		}
	}
	if columns.progressCompleted, err = parquetInt64Column(record, parquetTaskColumnProgressCompleted, "progress_completed"); err != nil {
		return columns, err
	}
	if columns.progressTotal, err = parquetInt64Column(record, parquetTaskColumnProgressTotal, "progress_total"); err != nil {
		return columns, err
	}
	if columns.catalogVersion, err = parquetInt64Column(record, parquetTaskColumnCatalogVersion, "catalog_version"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetBoolColumn(record, parquetTaskColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetFloat64Column(record, parquetTaskColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	return columns, nil
}

func parquetTaskIdentityFromColumns(columns parquetTaskColumnSet, row int) parquetTaskIdentity {
	return parquetTaskIdentity{
		TenantID:          columns.tenantID.Value(row),
		ID:                columns.id.Value(row),
		Type:              columns.taskType.Value(row),
		Status:            columns.status.Value(row),
		Phase:             columns.phase.Value(row),
		ProgressCompleted: int(columns.progressCompleted.Value(row)),
		ProgressTotal:     int(columns.progressTotal.Value(row)),
		OwnerID:           columns.ownerID.Value(row),
		CatalogVersion:    columns.catalogVersion.Value(row),
		ResultKey:         columns.resultKey.Value(row),
		Error:             columns.taskError.Value(row),
		StartedAt:         columns.startedAt.Value(row),
		UpdatedAt:         columns.updatedAt.Value(row),
		FinishedAt:        columns.finishedAt.Value(row),
		ContentHash:       columns.contentHash.Value(row),
	}
}

func parquetTaskRowFromColumns(columns parquetTaskColumnSet, row int) parquetTaskRow {
	return parquetTaskRow{
		Kind: columns.rowKind.Value(row),
		Key:  columns.entryKey.Value(row),
		Value: parquetValue{
			Kind:        columns.valueKind.Value(row),
			StringValue: columns.stringValue.Value(row),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
	}
}

func parquetTaskArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "phase", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "progress_completed", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "progress_total", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "owner_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "catalog_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "result_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "error", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "started_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "finished_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entry_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "float_value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}, nil)
}

func normalizeTaskForParquet(task Task) (Task, string, error) {
	payload, err := taskPayloadJSON(task)
	if err != nil {
		return Task{}, "", err
	}
	var normalized Task
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return Task{}, "", err
	}
	canonical, err := taskPayloadJSON(normalized)
	if err != nil {
		return Task{}, "", err
	}
	return normalized, objectContentHash(canonical), nil
}

func normalizeIndexTaskForParquet(task IndexTask) (IndexTask, string, error) {
	payload, err := indexTaskPayloadJSON(task)
	if err != nil {
		return IndexTask{}, "", err
	}
	var normalized IndexTask
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return IndexTask{}, "", err
	}
	canonical, err := indexTaskPayloadJSON(normalized)
	if err != nil {
		return IndexTask{}, "", err
	}
	return normalized, objectContentHash(canonical), nil
}

func normalizeTaskResultForParquet(result map[string]any) (map[string]any, string, error) {
	payload, err := taskResultPayloadJSON(result)
	if err != nil {
		return nil, "", err
	}
	normalized := map[string]any{}
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return nil, "", err
	}
	canonical, err := taskResultPayloadJSON(normalized)
	if err != nil {
		return nil, "", err
	}
	return normalized, objectContentHash(canonical), nil
}

func taskRows(params map[string]any, checkpoint map[string]any, result map[string]any) ([]parquetTaskRow, error) {
	rows, err := taskValueRows(taskRowParam, params)
	if err != nil {
		return nil, err
	}
	checkpointRows, err := taskValueRows(taskRowCheckpoint, checkpoint)
	if err != nil {
		return nil, err
	}
	rows = append(rows, checkpointRows...)
	resultRows, err := taskValueRows(taskRowResult, result)
	if err != nil {
		return nil, err
	}
	rows = append(rows, resultRows...)
	if len(rows) == 0 {
		rows = append(rows, parquetTaskRow{Kind: taskRowMetadata})
	}
	return rows, nil
}

func taskValueRows(kind string, values map[string]any) ([]parquetTaskRow, error) {
	keys := sortedAnyMapKeys(values)
	rows := make([]parquetTaskRow, 0, len(keys))
	for _, key := range keys {
		value, err := parquetValueFromAny(values[key])
		if err != nil {
			return nil, err
		}
		rows = append(rows, parquetTaskRow{Kind: kind, Key: key, Value: value})
	}
	return rows, nil
}

func taskContentHash(task Task) (string, error) {
	payload, err := taskPayloadJSON(task)
	if err != nil {
		return "", err
	}
	var normalized Task
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return "", err
	}
	canonical, err := taskPayloadJSON(normalized)
	if err != nil {
		return "", err
	}
	return objectContentHash(canonical), nil
}

func indexTaskContentHash(task IndexTask) (string, error) {
	payload, err := indexTaskPayloadJSON(task)
	if err != nil {
		return "", err
	}
	var normalized IndexTask
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return "", err
	}
	canonical, err := indexTaskPayloadJSON(normalized)
	if err != nil {
		return "", err
	}
	return objectContentHash(canonical), nil
}

func taskResultContentHash(result map[string]any) (string, error) {
	payload, err := taskResultPayloadJSON(result)
	if err != nil {
		return "", err
	}
	normalized := map[string]any{}
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return "", err
	}
	canonical, err := taskResultPayloadJSON(normalized)
	if err != nil {
		return "", err
	}
	return objectContentHash(canonical), nil
}

func taskPayloadJSON(task Task) ([]byte, error) {
	return json.Marshal(task)
}

func indexTaskPayloadJSON(task IndexTask) ([]byte, error) {
	return json.Marshal(task)
}

func taskResultPayloadJSON(result map[string]any) ([]byte, error) {
	return json.Marshal(result)
}
