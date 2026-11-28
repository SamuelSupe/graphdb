package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
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
	parquetSavedQueryColumnMinVersion
	parquetSavedQueryColumnAllowStale
)

const (
	savedQueryRowMetadata              = "metadata"
	savedQueryRowFilter                = "filter"
	savedQueryRowWhere                 = "where"
	savedQueryRowWhereExpr             = "where_expr"
	savedQueryRowEdgeWhere             = "edge_where"
	savedQueryRowEdgeWhereExpr         = "edge_where_expr"
	savedQueryRowRelationType          = "relation_type"
	savedQueryRowPathNodeKind          = "path_node_kind"
	savedQueryRowPathRelationType      = "path_relation_type"
	savedQueryRowPathEndWhere          = "path_end_where"
	savedQueryRowPathEndWhereExpr      = "path_end_where_expr"
	savedQueryRowPathStep              = "path_step"
	savedQueryRowPathStepNodeKind      = "path_step_node_kind"
	savedQueryRowPathStepRelationType  = "path_step_relation_type"
	savedQueryRowPathStepWhere         = "path_step_where"
	savedQueryRowPathStepWhereExpr     = "path_step_where_expr"
	savedQueryRowPathStepEdgeWhere     = "path_step_edge_where"
	savedQueryRowPathStepEdgeWhereExpr = "path_step_edge_where_expr"
	savedQueryRowSort                  = "sort"
	savedQueryRowProject               = "project"
	savedQueryRowAggregate             = "aggregate"
	savedQueryRowGroupBy               = "group_by"
	savedQueryRowHaving                = "having"
	savedQueryRowHavingExpr            = "having_expr"
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
	minVersion        *array.Int64
	allowStale        *array.Boolean
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
	table, release, err := parquetio.ReadTable(ctx, data)
	if err != nil {
		return SavedQuery{}, err
	}
	defer release()
	defer table.Release()
	if table.NumRows() < 1 {
		return SavedQuery{}, fmt.Errorf("parquet saved query is empty")
	}
	if table.NumCols() < int64(parquetSavedQueryColumnFloatValue+1) {
		return SavedQuery{}, fmt.Errorf("parquet saved query has %d columns, want at least %d", table.NumCols(), parquetSavedQueryColumnFloatValue+1)
	}

	var saved SavedQuery
	var contentHash string
	exprs := newSavedQueryExprBuilder()
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
				contentHash = strings.Clone(columns.contentHash.Value(i))
			} else if saved.TenantID != rowSaved.TenantID || saved.Name != rowSaved.Name {
				return SavedQuery{}, fmt.Errorf("saved query identity mismatch")
			}
			ordinal := columns.ordinal.Value(i)
			if ordinal < 0 || ordinal >= table.NumRows() {
				return SavedQuery{}, fmt.Errorf("invalid saved query ordinal %d for %d rows", ordinal, table.NumRows())
			}
			if err := applySavedQueryRow(&saved.Request, exprs, savedQueryRowFromColumns(columns, i), table.NumRows()); err != nil {
				return SavedQuery{}, err
			}
		}
	}
	if err := exprs.apply(&saved.Request); err != nil {
		return SavedQuery{}, err
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
	return normalized, codec.ContentHash(payload), nil
}

func savedQueryRows(request query.Request) ([]savedQueryParquetRow, error) {
	rows := []savedQueryParquetRow{{Kind: savedQueryRowMetadata}}
	filterFields := codec.SortedAnyMapKeys(request.Filters)
	for i, field := range filterFields {
		value, err := parquetio.ValueFromAny(request.Filters[field])
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowFilter, Ordinal: i, Field: field, Value: value})
	}
	for i, filter := range request.Where {
		value, err := parquetio.ValueFromAny(filter.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowWhere, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
	}
	var err error
	if rows, err = appendSavedQueryExprRows(rows, savedQueryRowWhereExpr, "", request.WhereExpr); err != nil {
		return nil, err
	}
	for i, filter := range request.EdgeWhere {
		value, err := parquetio.ValueFromAny(filter.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowEdgeWhere, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
	}
	if rows, err = appendSavedQueryExprRows(rows, savedQueryRowEdgeWhereExpr, "", request.EdgeWhereExpr); err != nil {
		return nil, err
	}
	for i, relationType := range request.RelationTypes {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowRelationType, Ordinal: i, Value: codec.StringCommitValue(relationType)})
	}
	for i, kind := range request.Path.NodeKinds {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathNodeKind, Ordinal: i, Value: codec.StringCommitValue(kind)})
	}
	for i, relationType := range request.Path.RelationTypes {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathRelationType, Ordinal: i, Value: codec.StringCommitValue(relationType)})
	}
	for i, filter := range request.Path.EndWhere {
		value, err := parquetio.ValueFromAny(filter.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathEndWhere, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
	}
	if rows, err = appendSavedQueryExprRows(rows, savedQueryRowPathEndWhereExpr, "", request.Path.EndWhereExpr); err != nil {
		return nil, err
	}
	for stepIdx, step := range request.Path.Steps {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathStep, Ordinal: stepIdx, Op: step.Direction})
		for i, kind := range step.NodeKinds {
			rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathStepNodeKind, ParentOrdinal: stepIdx, Ordinal: i, Value: codec.StringCommitValue(kind)})
		}
		for i, relationType := range step.RelationTypes {
			rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathStepRelationType, ParentOrdinal: stepIdx, Ordinal: i, Value: codec.StringCommitValue(relationType)})
		}
		for i, filter := range step.Where {
			value, err := parquetio.ValueFromAny(filter.Value)
			if err != nil {
				return nil, err
			}
			rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathStepWhere, ParentOrdinal: stepIdx, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
		}
		if rows, err = appendSavedQueryExprRows(rows, savedQueryRowPathStepWhereExpr, strconv.Itoa(stepIdx), step.WhereExpr); err != nil {
			return nil, err
		}
		for i, filter := range step.EdgeWhere {
			value, err := parquetio.ValueFromAny(filter.Value)
			if err != nil {
				return nil, err
			}
			rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowPathStepEdgeWhere, ParentOrdinal: stepIdx, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
		}
		if rows, err = appendSavedQueryExprRows(rows, savedQueryRowPathStepEdgeWhereExpr, strconv.Itoa(stepIdx), step.EdgeWhereExpr); err != nil {
			return nil, err
		}
	}
	for i, sortSpec := range request.Sort {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowSort, Ordinal: i, Field: sortSpec.Field, Desc: sortSpec.Desc})
	}
	for i, field := range request.Project {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowProject, Ordinal: i, Value: codec.StringCommitValue(field)})
	}
	for i, aggregate := range request.Aggregate {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowAggregate, Ordinal: i, Name: aggregate.Name, Op: aggregate.Op, Field: aggregate.Field})
	}
	for i, field := range request.GroupBy {
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowGroupBy, Ordinal: i, Value: codec.StringCommitValue(field)})
	}
	for i, filter := range request.Having {
		value, err := parquetio.ValueFromAny(filter.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, savedQueryParquetRow{Kind: savedQueryRowHaving, Ordinal: i, Field: filter.Field, Op: filter.Op, Value: value})
	}
	if rows, err = appendSavedQueryExprRows(rows, savedQueryRowHavingExpr, "", request.HavingExpr); err != nil {
		return nil, err
	}
	return rows, nil
}

func appendSavedQueryRow(builder *array.RecordBuilder, saved SavedQuery, hash string, row savedQueryParquetRow) {
	builder.Field(parquetSavedQueryColumnTenantID).(*array.StringBuilder).Append(saved.TenantID)
	builder.Field(parquetSavedQueryColumnName).(*array.StringBuilder).Append(saved.Name)
	builder.Field(parquetSavedQueryColumnDescription).(*array.StringBuilder).Append(saved.Description)
	builder.Field(parquetSavedQueryColumnCreatedAt).(*array.StringBuilder).Append(parquetio.FormatTime(saved.CreatedAt))
	builder.Field(parquetSavedQueryColumnUpdatedAt).(*array.StringBuilder).Append(parquetio.FormatTime(saved.UpdatedAt))
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
	builder.Field(parquetSavedQueryColumnMinVersion).(*array.Int64Builder).Append(saved.Request.MinVersion)
	builder.Field(parquetSavedQueryColumnAllowStale).(*array.BooleanBuilder).Append(saved.Request.AllowStale)
}

func savedQueryFromColumns(columns savedQueryColumnSet, row int) SavedQuery {
	saved := SavedQuery{
		TenantID:    strings.Clone(columns.tenantID.Value(row)),
		Name:        strings.Clone(columns.name.Value(row)),
		Description: strings.Clone(columns.description.Value(row)),
		CreatedAt:   parquetio.ParseTime(columns.createdAt.Value(row)),
		UpdatedAt:   parquetio.ParseTime(columns.updatedAt.Value(row)),
		Request: query.Request{
			Op:                strings.Clone(columns.requestOp.Value(row)),
			TargetOp:          strings.Clone(columns.targetOp.Value(row)),
			Kind:              strings.Clone(columns.kind.Value(row)),
			ID:                strings.Clone(columns.id.Value(row)),
			TargetID:          strings.Clone(columns.targetID.Value(row)),
			Direction:         strings.Clone(columns.direction.Value(row)),
			DirectionStrategy: strings.Clone(columns.directionStrategy.Value(row)),
			RelationType:      strings.Clone(columns.relationType.Value(row)),
			Depth:             int(columns.depth.Value(row)),
			Limit:             int(columns.limit.Value(row)),
			Cursor:            strings.Clone(columns.cursor.Value(row)),
			TimeoutMS:         int(columns.timeoutMS.Value(row)),
			CostLimit:         int(columns.costLimit.Value(row)),
			Profile:           columns.profile.Value(row),
			Path: query.PathFilter{
				EndKind:  strings.Clone(columns.pathEndKind.Value(row)),
				MaxPaths: int(columns.pathMaxPaths.Value(row)),
			},
		},
	}
	if columns.minVersion != nil {
		saved.Request.MinVersion = columns.minVersion.Value(row)
	}
	if columns.allowStale != nil {
		saved.Request.AllowStale = columns.allowStale.Value(row)
	}
	return saved
}

func savedQueryRowFromColumns(columns savedQueryColumnSet, row int) savedQueryParquetRow {
	return savedQueryParquetRow{
		Kind:          strings.Clone(columns.rowKind.Value(row)),
		Ordinal:       int(columns.ordinal.Value(row)),
		ParentOrdinal: int(columns.parentOrdinal.Value(row)),
		Field:         strings.Clone(columns.field.Value(row)),
		Op:            strings.Clone(columns.op.Value(row)),
		Name:          strings.Clone(columns.rowName.Value(row)),
		Desc:          columns.desc.Value(row),
		Value: parquetValue{
			Kind:        strings.Clone(columns.valueKind.Value(row)),
			StringValue: strings.Clone(columns.stringValue.Value(row)),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
	}
}

func applySavedQueryRow(request *query.Request, exprs *savedQueryExprBuilder, row savedQueryParquetRow, totalRows int64) error {
	switch row.Kind {
	case savedQueryRowMetadata:
		return nil
	case savedQueryRowFilter:
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		if request.Filters == nil {
			request.Filters = graph.Fields{}
		}
		request.Filters[row.Field] = value
	case savedQueryRowWhere:
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		request.Where = setQueryFilterAt(request.Where, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	case savedQueryRowWhereExpr, savedQueryRowEdgeWhereExpr, savedQueryRowHavingExpr, savedQueryRowPathEndWhereExpr, savedQueryRowPathStepWhereExpr, savedQueryRowPathStepEdgeWhereExpr:
		return exprs.add(row)
	case savedQueryRowEdgeWhere:
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		request.EdgeWhere = setQueryFilterAt(request.EdgeWhere, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	case savedQueryRowRelationType:
		value, err := codec.StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.RelationTypes = codec.SetStringAt(request.RelationTypes, row.Ordinal, value)
	case savedQueryRowPathNodeKind:
		value, err := codec.StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.NodeKinds = codec.SetStringAt(request.Path.NodeKinds, row.Ordinal, value)
	case savedQueryRowPathRelationType:
		value, err := codec.StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.RelationTypes = codec.SetStringAt(request.Path.RelationTypes, row.Ordinal, value)
	case savedQueryRowPathEndWhere:
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.EndWhere = setQueryFilterAt(request.Path.EndWhere, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	case savedQueryRowPathStep:
		request.Path.Steps = ensureQueryPathStepsLen(request.Path.Steps, row.Ordinal)
		request.Path.Steps[row.Ordinal].Direction = row.Op
	case savedQueryRowPathStepNodeKind:
		if row.ParentOrdinal < 0 || int64(row.ParentOrdinal) >= totalRows {
			return fmt.Errorf("invalid saved query path step ordinal %d", row.ParentOrdinal)
		}
		value, err := codec.StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.Steps = ensureQueryPathStepsLen(request.Path.Steps, row.ParentOrdinal)
		request.Path.Steps[row.ParentOrdinal].NodeKinds = codec.SetStringAt(request.Path.Steps[row.ParentOrdinal].NodeKinds, row.Ordinal, value)
	case savedQueryRowPathStepRelationType:
		if row.ParentOrdinal < 0 || int64(row.ParentOrdinal) >= totalRows {
			return fmt.Errorf("invalid saved query path step ordinal %d", row.ParentOrdinal)
		}
		value, err := codec.StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.Steps = ensureQueryPathStepsLen(request.Path.Steps, row.ParentOrdinal)
		request.Path.Steps[row.ParentOrdinal].RelationTypes = codec.SetStringAt(request.Path.Steps[row.ParentOrdinal].RelationTypes, row.Ordinal, value)
	case savedQueryRowPathStepWhere:
		if row.ParentOrdinal < 0 || int64(row.ParentOrdinal) >= totalRows {
			return fmt.Errorf("invalid saved query path step ordinal %d", row.ParentOrdinal)
		}
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.Steps = ensureQueryPathStepsLen(request.Path.Steps, row.ParentOrdinal)
		request.Path.Steps[row.ParentOrdinal].Where = setQueryFilterAt(request.Path.Steps[row.ParentOrdinal].Where, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	case savedQueryRowPathStepEdgeWhere:
		if row.ParentOrdinal < 0 || int64(row.ParentOrdinal) >= totalRows {
			return fmt.Errorf("invalid saved query path step ordinal %d", row.ParentOrdinal)
		}
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		request.Path.Steps = ensureQueryPathStepsLen(request.Path.Steps, row.ParentOrdinal)
		request.Path.Steps[row.ParentOrdinal].EdgeWhere = setQueryFilterAt(request.Path.Steps[row.ParentOrdinal].EdgeWhere, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	case savedQueryRowSort:
		request.Sort = setQuerySortAt(request.Sort, row.Ordinal, query.SortSpec{Field: row.Field, Desc: row.Desc})
	case savedQueryRowProject:
		value, err := codec.StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.Project = codec.SetStringAt(request.Project, row.Ordinal, value)
	case savedQueryRowAggregate:
		request.Aggregate = setQueryAggregateAt(request.Aggregate, row.Ordinal, query.Aggregation{Name: row.Name, Op: row.Op, Field: row.Field})
	case savedQueryRowGroupBy:
		value, err := codec.StringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		request.GroupBy = codec.SetStringAt(request.GroupBy, row.Ordinal, value)
	case savedQueryRowHaving:
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		request.Having = setQueryFilterAt(request.Having, row.Ordinal, query.Filter{Field: row.Field, Op: row.Op, Value: value})
	default:
		return fmt.Errorf("unknown saved query row kind %q", row.Kind)
	}
	return nil
}

type savedQueryExprEntry struct {
	ordinal       int
	parentOrdinal int
	node          query.FilterExpr
}

type savedQueryExprKey struct {
	kind string
	name string
}

type savedQueryExprBuilder struct {
	entries map[savedQueryExprKey][]savedQueryExprEntry
}

func newSavedQueryExprBuilder() *savedQueryExprBuilder {
	return &savedQueryExprBuilder{entries: map[savedQueryExprKey][]savedQueryExprEntry{}}
}

func (b *savedQueryExprBuilder) add(row savedQueryParquetRow) error {
	var value any
	if row.Desc {
		var err error
		value, err = parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
	}
	key := savedQueryExprKey{kind: row.Kind, name: row.Name}
	b.entries[key] = append(b.entries[key], savedQueryExprEntry{
		ordinal:       row.Ordinal,
		parentOrdinal: row.ParentOrdinal,
		node: query.FilterExpr{
			Op:    row.Op,
			Field: row.Field,
			Value: value,
		},
	})
	return nil
}

func (b *savedQueryExprBuilder) apply(request *query.Request) error {
	for key, entries := range b.entries {
		expr, err := buildSavedQueryFilterExpr(entries)
		if err != nil {
			return err
		}
		switch key.kind {
		case savedQueryRowWhereExpr:
			request.WhereExpr = expr
		case savedQueryRowEdgeWhereExpr:
			request.EdgeWhereExpr = expr
		case savedQueryRowHavingExpr:
			request.HavingExpr = expr
		case savedQueryRowPathEndWhereExpr:
			request.Path.EndWhereExpr = expr
		case savedQueryRowPathStepWhereExpr:
			stepIdx, err := strconv.Atoi(key.name)
			if err != nil || stepIdx < 0 || stepIdx >= len(request.Path.Steps) {
				return fmt.Errorf("invalid saved query path step expr index %q", key.name)
			}
			request.Path.Steps[stepIdx].WhereExpr = expr
		case savedQueryRowPathStepEdgeWhereExpr:
			stepIdx, err := strconv.Atoi(key.name)
			if err != nil || stepIdx < 0 || stepIdx >= len(request.Path.Steps) {
				return fmt.Errorf("invalid saved query path step edge expr index %q", key.name)
			}
			request.Path.Steps[stepIdx].EdgeWhereExpr = expr
		}
	}
	return nil
}

func appendSavedQueryExprRows(rows []savedQueryParquetRow, kind string, name string, expr *query.FilterExpr) ([]savedQueryParquetRow, error) {
	if expr == nil {
		return rows, nil
	}
	nextID := 0
	var walk func(node query.FilterExpr, parent int) error
	walk = func(node query.FilterExpr, parent int) error {
		id := nextID
		nextID++
		var value parquetValue
		hasValue := node.Value != nil
		if hasValue {
			var err error
			value, err = parquetio.ValueFromAny(node.Value)
			if err != nil {
				return err
			}
		}
		rows = append(rows, savedQueryParquetRow{
			Kind:          kind,
			Ordinal:       id,
			ParentOrdinal: parent,
			Name:          name,
			Field:         node.Field,
			Op:            node.Op,
			Desc:          hasValue,
			Value:         value,
		})
		for _, child := range node.Children {
			if err := walk(child, id); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(*expr, -1); err != nil {
		return nil, err
	}
	return rows, nil
}

func buildSavedQueryFilterExpr(entries []savedQueryExprEntry) (*query.FilterExpr, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	nodes := make([]query.FilterExpr, len(entries))
	parents := make([]int, len(entries))
	seen := make([]bool, len(entries))
	for _, entry := range entries {
		if entry.ordinal < 0 || entry.ordinal >= len(entries) || seen[entry.ordinal] {
			return nil, fmt.Errorf("invalid saved query filter expression ordinal %d", entry.ordinal)
		}
		nodes[entry.ordinal] = entry.node
		parents[entry.ordinal] = entry.parentOrdinal
		seen[entry.ordinal] = true
	}
	rootIndex := -1
	for i := range nodes {
		parent := parents[i]
		if parent == -1 {
			if rootIndex != -1 {
				return nil, fmt.Errorf("saved query filter expression has multiple roots")
			}
			rootIndex = i
			continue
		}
		if parent < 0 || parent >= i {
			return nil, fmt.Errorf("invalid saved query filter expression parent %d for node %d", parent, i)
		}
	}
	if rootIndex != 0 {
		return nil, fmt.Errorf("saved query filter expression missing root")
	}
	childrenByParent := make([][]int, len(nodes))
	for i := 1; i < len(nodes); i++ {
		childrenByParent[parents[i]] = append(childrenByParent[parents[i]], i)
	}
	var assemble func(idx int) query.FilterExpr
	assemble = func(idx int) query.FilterExpr {
		node := nodes[idx]
		if childIDs := childrenByParent[idx]; len(childIDs) > 0 {
			node.Children = make([]query.FilterExpr, len(childIDs))
			for j, childID := range childIDs {
				node.Children[j] = assemble(childID)
			}
		}
		return node
	}
	root := assemble(0)
	return &root, nil
}

func ensureQueryPathStepsLen(values []query.PathStep, index int) []query.PathStep {
	for len(values) <= index {
		values = append(values, query.PathStep{})
	}
	return values
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
		if *item.target, err = parquetio.StringColumn(batch, item.index, item.name); err != nil {
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
		if *item.target, err = parquetio.Int64Column(batch, item.index, item.name); err != nil {
			return columns, err
		}
	}
	if columns.profile, err = parquetio.BoolColumn(batch, parquetSavedQueryColumnProfile, "profile"); err != nil {
		return columns, err
	}
	if columns.desc, err = parquetio.BoolColumn(batch, parquetSavedQueryColumnDesc, "desc"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetio.BoolColumn(batch, parquetSavedQueryColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetio.Float64Column(batch, parquetSavedQueryColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	if batch.NumCols() > int64(parquetSavedQueryColumnMinVersion) {
		if columns.minVersion, err = parquetio.Int64Column(batch, parquetSavedQueryColumnMinVersion, "min_version"); err != nil {
			return columns, err
		}
	}
	if batch.NumCols() > int64(parquetSavedQueryColumnAllowStale) {
		if columns.allowStale, err = parquetio.BoolColumn(batch, parquetSavedQueryColumnAllowStale, "allow_stale"); err != nil {
			return columns, err
		}
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
		{Name: "min_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "allow_stale", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
}

func savedQueryPayloadJSON(saved SavedQuery) ([]byte, error) {
	saved.Name = strings.TrimSpace(saved.Name)
	saved.CreatedAt = saved.CreatedAt.UTC()
	saved.UpdatedAt = saved.UpdatedAt.UTC()
	return json.Marshal(saved)
}

func savedQueryContentHash(saved SavedQuery) (string, error) {
	payload, err := savedQueryPayloadJSON(saved)
	if err != nil {
		return "", err
	}
	return codec.ContentHash(payload), nil
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
