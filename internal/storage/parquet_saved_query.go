package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const savedQueryCodecParquet = "saved-query-arrow-parquet-v1"

const (
	parquetSavedQueryColumnTenantID = iota
	parquetSavedQueryColumnName
	parquetSavedQueryColumnDescription
	parquetSavedQueryColumnCreatedAt
	parquetSavedQueryColumnUpdatedAt
	parquetSavedQueryColumnContentHash
	parquetSavedQueryColumnRequestOp
	parquetSavedQueryColumnTargetOp
	parquetSavedQueryColumnKind
	parquetSavedQueryColumnID
	parquetSavedQueryColumnTargetID
	parquetSavedQueryColumnDirection
	parquetSavedQueryColumnDirectionStrategy
	parquetSavedQueryColumnRelationType
	parquetSavedQueryColumnDepth
	parquetSavedQueryColumnLimit
	parquetSavedQueryColumnCursor
	parquetSavedQueryColumnTimeoutMS
	parquetSavedQueryColumnCostLimit
	parquetSavedQueryColumnProfile
	parquetSavedQueryColumnPathEndKind
	parquetSavedQueryColumnPathMaxPaths
	parquetSavedQueryColumnRowKind
	parquetSavedQueryColumnOrdinal
	parquetSavedQueryColumnParentOrdinal
	parquetSavedQueryColumnField
	parquetSavedQueryColumnOp
	parquetSavedQueryColumnRowName
	parquetSavedQueryColumnDesc
	parquetSavedQueryColumnValueKind
	parquetSavedQueryColumnStringValue
	parquetSavedQueryColumnBoolValue
	parquetSavedQueryColumnFloatValue
)

const (
	savedQueryRowMetadata         = "metadata"
	savedQueryRowFilter           = "filter"
	savedQueryRowWhere            = "where"
	savedQueryRowRelationType     = "relation_type"
	savedQueryRowPathNodeKind     = "path_node_kind"
	savedQueryRowPathRelationType = "path_relation_type"
	savedQueryRowPathEndWhere     = "path_end_where"
	savedQueryRowSort             = "sort"
	savedQueryRowProject          = "project"
	savedQueryRowAggregate        = "aggregate"
)

type savedQueryParquetRow struct {
	Kind          string
	Ordinal       int
	ParentOrdinal int
	Field         string
	Op            string
	Name          string
	Desc          bool
	Value         parquetValue
}

type savedQueryColumnSet struct {
	tenantID          *array.String
	name              *array.String
	description       *array.String
	createdAt         *array.String
	updatedAt         *array.String
	contentHash       *array.String
	requestOp         *array.String
	targetOp          *array.String
	kind              *array.String
	id                *array.String
	targetID          *array.String
	direction         *array.String
	directionStrategy *array.String
	relationType      *array.String
	depth             *array.Int64
	limit             *array.Int64
	cursor            *array.String
	timeoutMS         *array.Int64
	costLimit         *array.Int64
	profile           *array.Boolean
	pathEndKind       *array.String
	pathMaxPaths      *array.Int64
	rowKind           *array.String
	ordinal           *array.Int64
	parentOrdinal     *array.Int64
	field             *array.String
	op                *array.String
	rowName           *array.String
	desc              *array.Boolean
	valueKind         *array.String
	stringValue       *array.String
	boolValue         *array.Boolean
	floatValue        *array.Float64
}

func marshalParquetSavedQuery(ctx context.Context, saved SavedQuery) ([]byte, error) {
	normalized, hash, err := normalizeSavedQueryForParquet(saved)
	if err != nil {
		return nil, err
	}
	rows, err := savedQueryRows(normalized.Request)
	if err != nil {
		return nil, err
	}
	schema := parquetSavedQueryArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, row := range rows {
		appendSavedQueryRow(builder, normalized, hash, row)
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

func decodeParquetSavedQuery(ctx context.Context, data []byte) (SavedQuery, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return SavedQuery{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return SavedQuery{}, fmt.Errorf("parquet saved query is empty")
	}
	if table.NumCols() < int64(parquetSavedQueryColumnFloatValue+1) {
		return SavedQuery{}, fmt.Errorf("parquet saved query has %d columns, want at least %d", table.NumCols(), parquetSavedQueryColumnFloatValue+1)
	}

	var saved SavedQuery
	var contentHash string
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := savedQueryColumns(batch)
		if err != nil {
			return SavedQuery{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowSaved := savedQueryFromColumns(columns, i)
			if saved.Name == "" {
				saved = rowSaved
				contentHash = columns.contentHash.Value(i)
			} else if saved.TenantID != rowSaved.TenantID || saved.Name != rowSaved.Name {
				return SavedQuery{}, fmt.Errorf("saved query identity mismatch")
			}
			if err := applySavedQueryRow(&saved.Request, savedQueryRowFromColumns(columns, i)); err != nil {
				return SavedQuery{}, err
			}
		}
	}
	if strings.TrimSpace(saved.Name) == "" {
		return SavedQuery{}, fmt.Errorf("parquet saved query is empty")
	}
	hash, err := savedQueryContentHash(saved)
	if err != nil {
		return SavedQuery{}, err
	}
	if contentHash == "" || contentHash != hash {
		return SavedQuery{}, fmt.Errorf("saved query content hash mismatch")
	}
	return saved, nil
}

func normalizeSavedQueryForParquet(saved SavedQuery) (SavedQuery, string, error) {
	payload, err := savedQueryPayloadJSON(saved)
	if err != nil {
		return SavedQuery{}, "", err
	}
	var normalized SavedQuery
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return SavedQuery{}, "", err
	}
	return normalized, objectContentHash(payload), nil
}

func savedQueryRows(request query.Request) ([]savedQueryParquetRow, error) {
	rows := []savedQueryParquetRow{{Kind: savedQueryRowMetadata}}
	filterFields := sortedAnyMapKeys(request.Filters)
	for i, field := range filterFields {
		value, err := parquetValueFromAny(request.Filters[field])
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowFilter, Ordinal: i, Field: field, Value: value})
	}
	for i, filter := range request.Where {
		value, err := parquetValueFromAny(filter.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowWhere, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
	}
	for i, relationType := range request.RelationTypes {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowRelationType, Ordinal: i, Value: stringCommitValue(relationType)})
	}
	for i, kind := range request.Path.NodeKinds {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathNodeKind, Ordinal: i, Value: stringCommitValue(kind)})
	}
	for i, relationType := range request.Path.RelationTypes {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathRelationType, Ordinal: i, Value: stringCommitValue(relationType)})
	}
	for i, filter := range request.Path.EndWhere {
		value, err := parquetValueFromAny(filter.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathEndWhere, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
	}
	for i, sortSpec := range request.Sort {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowSort, Ordinal: i, Field: sortSpec.Field, Desc: sortSpec.Desc})
	}
	for i, field := range request.Project {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowProject, Ordinal: i, Value: stringCommitValue(field)})
	}
	for i, aggregate := range request.Aggregate {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowAggregate, Ordinal: i, Name: aggregate.Name, Op: aggregate.Op, Field: aggregate.Field})
	}
	return rows, nil
}

func appendSavedQueryRow(builder *array.RecordBuilder, saved SavedQuery, hash string, row savedQueryParquetRow) {
	builder.Field(parquetSavedQueryColumnTenantID).(*array.StringBuilder).Append(saved.TenantID)
	builder.Field(parquetSavedQueryColumnName).(*array.StringBuilder).Append(saved.Name)
	builder.Field(parquetSavedQueryColumnDescription).(*array.StringBuilder).Append(saved.Description)
	builder.Field(parquetSavedQueryColumnCreatedAt).(*array.StringBuilder).Append(formatParquetTime(saved.CreatedAt))
	builder.Field(parquetSavedQueryColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(saved.UpdatedAt))
	builder.Field(parquetSavedQueryColumnContentHash).(*array.StringBuilder).Append(hash)
	builder.Field(parquetSavedQueryColumnRequestOp).(*array.StringBuilder).Append(saved.Request.Op)
	builder.Field(parquetSavedQueryColumnTargetOp).(*array.StringBuilder).Append(saved.Request.TargetOp)
	builder.Field(parquetSavedQueryColumnKind).(*array.StringBuilder).Append(saved.Request.Kind)
	builder.Field(parquetSavedQueryColumnID).(*array.StringBuilder).Append(saved.Request.ID)
	builder.Field(parquetSavedQueryColumnTargetID).(*array.StringBuilder).Append(saved.Request.TargetID)
	builder.Field(parquetSavedQueryColumnDirection).(*array.StringBuilder).Append(saved.Request.Direction)
	builder.Field(parquetSavedQueryColumnDirectionStrategy).(*array.StringBuilder).Append(saved.Request.DirectionStrategy)
	builder.Field(parquetSavedQueryColumnRelationType).(*array.StringBuilder).Append(saved.Request.RelationType)
	builder.Field(parquetSavedQueryColumnDepth).(*array.Int64Builder).Append(int64(saved.Request.Depth))
	builder.Field(parquetSavedQueryColumnLimit).(*array.Int64Builder).Append(int64(saved.Request.Limit))
	builder.Field(parquetSavedQueryColumnCursor).(*array.StringBuilder).Append(saved.Request.Cursor)
	builder.Field(parquetSavedQueryColumnTimeoutMS).(*array.Int64Builder).Append(int64(saved.Request.TimeoutMS))
	builder.Field(parquetSavedQueryColumnCostLimit).(*array.Int64Builder).Append(int64(saved.Request.CostLimit))
	builder.Field(parquetSavedQueryColumnProfile).(*array.BooleanBuilder).Append(saved.Request.Profile)
	builder.Field(parquetSavedQueryColumnPathEndKind).(*array.StringBuilder).Append(saved.Request.Path.EndKind)
	builder.Field(parquetSavedQueryColumnPathMaxPaths).(*array.Int64Builder).Append(int64(saved.Request.Path.MaxPaths))
	builder.Field(parquetSavedQueryColumnRowKind).(*array.StringBuilder).Append(row.Kind)
	builder.Field(parquetSavedQueryColumnOrdinal).(*array.Int64Builder).Append(int64(row.Ordinal))
	builder.Field(parquetSavedQueryColumnParentOrdinal).(*array.Int64Builder).Append(int64(row.ParentOrdinal))
	builder.Field(parquetSavedQueryColumnField).(*array.StringBuilder).Append(row.Field)
	builder.Field(parquetSavedQueryColumnOp).(*array.StringBuilder).Append(row.Op)
	builder.Field(parquetSavedQueryColumnRowName).(*array.StringBuilder).Append(row.Name)
	builder.Field(parquetSavedQueryColumnDesc).(*array.BooleanBuilder).Append(row.Desc)
	builder.Field(parquetSavedQueryColumnValueKind).(*array.StringBuilder).Append(row.Value.Kind)
	builder.Field(parquetSavedQueryColumnStringValue).(*array.StringBuilder).Append(row.Value.StringValue)
	builder.Field(parquetSavedQueryColumnBoolValue).(*array.BooleanBuilder).Append(row.Value.BoolValue)
	builder.Field(parquetSavedQueryColumnFloatValue).(*array.Float64Builder).Append(row.Value.FloatValue)
}

func savedQueryFromColumns(columns savedQueryColumnSet, row int) SavedQuery {
	return SavedQuery{
		TenantID:    columns.tenantID.Value(row),
		Name:        columns.name.Value(row),
		Description: columns.description.Value(row),
		CreatedAt:   parseParquetTime(columns.createdAt.Value(row)),
		UpdatedAt:   parseParquetTime(columns.updatedAt.Value(row)),
		Request: query.Request{
			Op:                columns.requestOp.Value(row),
			TargetOp:          columns.targetOp.Value(row),
			Kind:              columns.kind.Value(row),
			ID:                columns.id.Value(row),
			TargetID:          columns.targetID.Value(row),
			Direction:         columns.direction.Value(row),
			DirectionStrategy: columns.directionStrategy.Value(row),
			RelationType:      columns.relationType.Value(row),
			Depth:             int(columns.depth.Value(row)),
			Limit:             int(columns.limit.Value(row)),
			Cursor:            columns.cursor.Value(row),
			TimeoutMS:         int(columns.timeoutMS.Value(row)),
			CostLimit:         int(columns.costLimit.Value(row)),
			Profile:           columns.profile.Value(row),
			Path: query.PathFilter{
				EndKind:  columns.pathEndKind.Value(row),
				MaxPaths: int(columns.pathMaxPaths.Value(row)),
			},
		},
	}
}

func savedQueryRowFromColumns(columns savedQueryColumnSet, row int) savedQueryParquetRow {
	return savedQueryParquetRow{
		Kind:          columns.rowKind.Value(row),
		Ordinal:       int(columns.ordinal.Value(row)),
		ParentOrdinal: int(columns.parentOrdinal.Value(row)),
		Field:         columns.field.Value(row),
		Op:            columns.op.Value(row),
		Name:          columns.rowName.Value(row),
		Desc:          columns.desc.Value(row),
		Value: parquetValue{
			Kind:        columns.valueKind.Value(row),
			StringValue: columns.stringValue.Value(row),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
	}
}

func applySavedQueryRow(request *query.Request, row savedQueryParquetRow) error {
	switch row.Kind {
	case savedQueryRowMetadata:
		return nil
	case savedQueryRowFilter:
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		if request.Filters == nil {
			request.Filters = graph.Fields{}
		}
		request.Filters[row.Field] = value
	case savedQueryRowWhere:
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		request.Where = setQueryFilterAt(request.Where, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	case savedQueryRowRelationType:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.RelationTypes = setStringAt(request.RelationTypes, row.Ordinal, value)
	case savedQueryRowPathNodeKind:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.NodeKinds = setStringAt(request.Path.NodeKinds, row.Ordinal, value)
	case savedQueryRowPathRelationType:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.RelationTypes = setStringAt(request.Path.RelationTypes, row.Ordinal, value)
	case savedQueryRowPathEndWhere:
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.EndWhere = setQueryFilterAt(request.Path.EndWhere, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	case savedQueryRowSort:
		request.Sort = setQuerySortAt(request.Sort, row.Ordinal, query.SortSpec{Field: row.Field, Desc: row.Desc})
	case savedQueryRowProject:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Project = setStringAt(request.Project, row.Ordinal, value)
	case savedQueryRowAggregate:
		request.Aggregate = setQueryAggregateAt(request.Aggregate, row.Ordinal, query.Aggregation{Name: row.Name, Op: row.Op, Field: row.Field})
	default:
		return fmt.Errorf("unknown saved query row kind %q", row.Kind)
	}
	return nil
}

func savedQueryColumns(batch arrow.RecordBatch) (savedQueryColumnSet, error) {
	var columns savedQueryColumnSet
	var err error
	stringColumns := []struct {
		target **array.String
		index  int
		name   string
	}{
		{&columns.tenantID, parquetSavedQueryColumnTenantID, "tenant_id"},
		{&columns.name, parquetSavedQueryColumnName, "name"},
		{&columns.description, parquetSavedQueryColumnDescription, "description"},
		{&columns.createdAt, parquetSavedQueryColumnCreatedAt, "created_at"},
		{&columns.updatedAt, parquetSavedQueryColumnUpdatedAt, "updated_at"},
		{&columns.contentHash, parquetSavedQueryColumnContentHash, "content_hash"},
		{&columns.requestOp, parquetSavedQueryColumnRequestOp, "request_op"},
		{&columns.targetOp, parquetSavedQueryColumnTargetOp, "target_op"},
		{&columns.kind, parquetSavedQueryColumnKind, "kind"},
		{&columns.id, parquetSavedQueryColumnID, "id"},
		{&columns.targetID, parquetSavedQueryColumnTargetID, "target_id"},
		{&columns.direction, parquetSavedQueryColumnDirection, "direction"},
		{&columns.directionStrategy, parquetSavedQueryColumnDirectionStrategy, "direction_strategy"},
		{&columns.relationType, parquetSavedQueryColumnRelationType, "relation_type"},
		{&columns.cursor, parquetSavedQueryColumnCursor, "cursor"},
		{&columns.pathEndKind, parquetSavedQueryColumnPathEndKind, "path_end_kind"},
		{&columns.rowKind, parquetSavedQueryColumnRowKind, "row_kind"},
		{&columns.field, parquetSavedQueryColumnField, "field"},
		{&columns.op, parquetSavedQueryColumnOp, "op"},
		{&columns.rowName, parquetSavedQueryColumnRowName, "row_name"},
		{&columns.valueKind, parquetSavedQueryColumnValueKind, "value_kind"},
		{&columns.stringValue, parquetSavedQueryColumnStringValue, "string_value"},
	}
	for _, item := range stringColumns {
		if *item.target, err = parquetStringColumn(batch, item.index, item.name); err != nil {
			return columns, err
		}
	}
	intColumns := []struct {
		target **array.Int64
		index  int
		name   string
	}{
		{&columns.depth, parquetSavedQueryColumnDepth, "depth"},
		{&columns.limit, parquetSavedQueryColumnLimit, "limit"},
		{&columns.timeoutMS, parquetSavedQueryColumnTimeoutMS, "timeout_ms"},
		{&columns.costLimit, parquetSavedQueryColumnCostLimit, "cost_limit"},
		{&columns.pathMaxPaths, parquetSavedQueryColumnPathMaxPaths, "path_max_paths"},
		{&columns.ordinal, parquetSavedQueryColumnOrdinal, "ordinal"},
		{&columns.parentOrdinal, parquetSavedQueryColumnParentOrdinal, "parent_ordinal"},
	}
	for _, item := range intColumns {
		if *item.target, err = parquetInt64Column(batch, item.index, item.name); err != nil {
			return columns, err
		}
	}
	if columns.profile, err = parquetBoolColumn(batch, parquetSavedQueryColumnProfile, "profile"); err != nil {
		return columns, err
	}
	if columns.desc, err = parquetBoolColumn(batch, parquetSavedQueryColumnDesc, "desc"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetBoolColumn(batch, parquetSavedQueryColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetFloat64Column(batch, parquetSavedQueryColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	return columns, nil
}

func parquetSavedQueryArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "request_op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "target_op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "target_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "direction", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "direction_strategy", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "relation_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "depth", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "limit", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cursor", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timeout_ms", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "cost_limit", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "profile", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "path_end_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "path_max_paths", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "parent_ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "row_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "desc", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "float_value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}, nil)
}

func savedQueryPayloadJSON(saved SavedQuery) ([]byte, error) {
	saved.Name = strings.TrimSpace(saved.Name)
	return json.Marshal(saved)
}

func savedQueryContentHash(saved SavedQuery) (string, error) {
	payload, err := savedQueryPayloadJSON(saved)
	if err != nil {
		return "", err
	}
	return objectContentHash(payload), nil
}

func setQueryFilterAt(values []query.Filter, index int, value query.Filter) []query.Filter {
	for len(values) <= index {
		values = append(values, query.Filter{})
	}
	values[index] = value
	return values
}

func setQuerySortAt(values []query.SortSpec, index int, value query.SortSpec) []query.SortSpec {
	for len(values) <= index {
		values = append(values, query.SortSpec{})
	}
	values[index] = value
	return values
}

func setQueryAggregateAt(values []query.Aggregation, index int, value query.Aggregation) []query.Aggregation {
	for len(values) <= index {
		values = append(values, query.Aggregation{})
	}
	values[index] = value
	return values
}
