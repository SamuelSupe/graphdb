package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	parquetEdgeColumnTenantID = iota
	parquetEdgeColumnRelationType
	parquetEdgeColumnShard
	parquetEdgeColumnVersion
	parquetEdgeColumnUpdatedAt
	parquetEdgeColumnEdgeID
	parquetEdgeColumnEdgeType
	parquetEdgeColumnFrom
	parquetEdgeColumnTo
	parquetEdgeColumnSource
	parquetEdgeColumnExternalID
	parquetEdgeColumnEdgeVersion
	parquetEdgeColumnEdgeCreatedAt
	parquetEdgeColumnEdgeUpdatedAt
	parquetEdgeColumnConfidence
	parquetEdgeColumnSourceRank
	parquetEdgeColumnRowKind
	parquetEdgeColumnOrdinal
	parquetEdgeColumnEntryKey
	parquetEdgeColumnValueKind
	parquetEdgeColumnStringValue
	parquetEdgeColumnBoolValue
	parquetEdgeColumnFloatValue
	parquetEdgeColumnFieldSourceSource
	parquetEdgeColumnFieldSourcePriority
	parquetEdgeColumnFieldSourceConfidence
	parquetEdgeColumnFieldSourceVersion
	parquetEdgeColumnFieldSourceUpdatedAt
	parquetEdgeColumnEdgeSourceSource
	parquetEdgeColumnEdgeSourceExternalID
	parquetEdgeColumnEdgeSourceEdgeID
	parquetEdgeColumnEdgeSourceConfidence
	parquetEdgeColumnEdgeSourcePriority
	parquetEdgeColumnEdgeSourceObservedAt
)

const (
	edgeShardRowMetadata        = "metadata"
	edgeShardRowField           = "field"
	edgeShardRowFieldSource     = "field_source"
	edgeShardRowSource          = "source"
	edgeShardRowExistenceSource = "existence_source"
)

func (s *TenantStore) writeParquetEdgeShards(ctx context.Context, tenantID string, shards []EdgeShardData) error {
	return s.writeParquetEdgeShardsWithOptions(ctx, tenantID, shards, true)
}

func (s *TenantStore) writeParquetEdgeShardsFast(ctx context.Context, tenantID string, shards []EdgeShardData) error {
	return s.writeParquetEdgeShardsWithOptions(ctx, tenantID, shards, false)
}

func (s *TenantStore) writeParquetEdgeShardsWithOptions(ctx context.Context, tenantID string, shards []EdgeShardData, checkExisting bool) error {
	for _, group := range edgeShardDataPackGroups(shards) {
		for i := range group.Shards {
			group.Shards[i].TenantID = tenantID
		}
		pack := mergeEdgeShardPack(group)
		pack.TenantID = tenantID
		key := s.parquetEdgeShardVersionKey(tenantID, pack.Version, pack.RelationType, pack.Shard)
		if len(group.Shards) > 1 {
			key = s.parquetEdgeShardPackVersionKey(tenantID, pack.Version, pack.RelationType, group.ID)
			if checkExisting {
				if _, ok, err := s.existingParquetEdgeShardPackMeta(ctx, key, tenantID, group.Shards); err != nil || ok {
					if err != nil {
						return err
					}
					continue
				}
			}
		}
		if err := s.putParquetEdgeShardObject(ctx, key, tenantID, pack, checkExisting); err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) putParquetEdgeShard(ctx context.Context, tenantID string, shard EdgeShardData) error {
	key := s.parquetEdgeShardVersionKey(tenantID, shard.Version, shard.RelationType, shard.Shard)
	return s.putParquetEdgeShardObject(ctx, key, tenantID, shard, true)
}

func (s *TenantStore) putParquetEdgeShardObject(ctx context.Context, key string, tenantID string, shard EdgeShardData, checkExisting bool) error {
	if checkExisting {
		if ok, err := s.parquetEdgeShardUnchanged(ctx, key, tenantID, shard); err != nil || ok {
			return err
		}
	}
	data, err := marshalParquetEdgeShard(ctx, shard)
	if err != nil {
		return err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		s.markObjectKeyCached(key)
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	return s.putConflictingParquetEdgeShardObject(ctx, key, tenantID, shard, data)
}

func (s *TenantStore) putConflictingParquetEdgeShardObject(ctx context.Context, key string, tenantID string, shard EdgeShardData, data []byte) error {
	existing, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return err
	}
	decoded, decodeErr := decodeParquetEdgeShard(ctx, existing, tenantID, shard.RelationType, shard.Shard, shard.Version)
	if decodeErr == nil && edgeShardContentHash(decoded) == edgeShardContentHash(shard) {
		s.markObjectKeyCached(key)
		return nil
	}
	_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag})
	if err == nil {
		s.markObjectKeyCached(key)
	}
	return err
}

func (s *TenantStore) parquetEdgeShardUnchanged(ctx context.Context, key string, tenantID string, shard EdgeShardData) (bool, error) {
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil || !mayExist {
		return false, err
	}
	data, _, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	existing, err := decodeParquetEdgeShard(ctx, data, tenantID, shard.RelationType, shard.Shard, shard.Version)
	if err != nil {
		return false, nil
	}
	return edgeShardContentHash(existing) == edgeShardContentHash(shard), nil
}

func (s *TenantStore) existingParquetEdgeShardPackMeta(ctx context.Context, key string, tenantID string, shards []EdgeShardData) (ObjectMeta, bool, error) {
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil || !mayExist {
		return ObjectMeta{}, false, err
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return ObjectMeta{}, false, nil
	}
	if err != nil {
		return ObjectMeta{}, false, err
	}
	for _, shard := range shards {
		existing, err := decodeParquetEdgeShard(ctx, data, tenantID, shard.RelationType, shard.Shard, shard.Version)
		if err != nil {
			return meta, false, nil
		}
		if edgeShardContentHash(existing) != edgeShardContentHash(shard) {
			return meta, false, nil
		}
	}
	return meta, true, nil
}

func marshalParquetEdgeShard(ctx context.Context, shard EdgeShardData) ([]byte, error) {
	schema := parquetEdgeShardArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	edges := append([]graph.Edge(nil), shard.Edges...)
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	for _, edge := range edges {
		rowShard := shard.Shard
		if isIndexPackID(shard.Shard) {
			rowShard = edgeShardID(edge.From)
		}
		rows, err := edgeShardRows(edge)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			builder.Field(parquetEdgeColumnTenantID).(*array.StringBuilder).Append(shard.TenantID)
			builder.Field(parquetEdgeColumnRelationType).(*array.StringBuilder).Append(shard.RelationType)
			builder.Field(parquetEdgeColumnShard).(*array.StringBuilder).Append(rowShard)
			builder.Field(parquetEdgeColumnVersion).(*array.Int64Builder).Append(shard.Version)
			builder.Field(parquetEdgeColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(shard.UpdatedAt))
			builder.Field(parquetEdgeColumnEdgeID).(*array.StringBuilder).Append(edge.ID)
			builder.Field(parquetEdgeColumnEdgeType).(*array.StringBuilder).Append(edge.Type)
			builder.Field(parquetEdgeColumnFrom).(*array.StringBuilder).Append(edge.From)
			builder.Field(parquetEdgeColumnTo).(*array.StringBuilder).Append(edge.To)
			builder.Field(parquetEdgeColumnSource).(*array.StringBuilder).Append(edge.Source)
			builder.Field(parquetEdgeColumnExternalID).(*array.StringBuilder).Append(edge.ExternalID)
			builder.Field(parquetEdgeColumnEdgeVersion).(*array.Int64Builder).Append(edge.Version)
			builder.Field(parquetEdgeColumnEdgeCreatedAt).(*array.StringBuilder).Append(formatParquetTime(edge.CreatedAt))
			builder.Field(parquetEdgeColumnEdgeUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(edge.UpdatedAt))
			builder.Field(parquetEdgeColumnConfidence).(*array.Float64Builder).Append(edge.Confidence)
			builder.Field(parquetEdgeColumnSourceRank).(*array.Int64Builder).Append(int64(edge.SourceRank))
			builder.Field(parquetEdgeColumnRowKind).(*array.StringBuilder).Append(row.Kind)
			builder.Field(parquetEdgeColumnOrdinal).(*array.Int64Builder).Append(int64(row.Ordinal))
			builder.Field(parquetEdgeColumnEntryKey).(*array.StringBuilder).Append(row.Key)
			builder.Field(parquetEdgeColumnValueKind).(*array.StringBuilder).Append(row.Value.Kind)
			builder.Field(parquetEdgeColumnStringValue).(*array.StringBuilder).Append(row.Value.StringValue)
			builder.Field(parquetEdgeColumnBoolValue).(*array.BooleanBuilder).Append(row.Value.BoolValue)
			builder.Field(parquetEdgeColumnFloatValue).(*array.Float64Builder).Append(row.Value.FloatValue)
			builder.Field(parquetEdgeColumnFieldSourceSource).(*array.StringBuilder).Append(row.FieldSource.Source)
			builder.Field(parquetEdgeColumnFieldSourcePriority).(*array.Int64Builder).Append(int64(row.FieldSource.Priority))
			builder.Field(parquetEdgeColumnFieldSourceConfidence).(*array.Float64Builder).Append(row.FieldSource.Confidence)
			builder.Field(parquetEdgeColumnFieldSourceVersion).(*array.Int64Builder).Append(row.FieldSource.Version)
			builder.Field(parquetEdgeColumnFieldSourceUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(row.FieldSource.UpdatedAt))
			builder.Field(parquetEdgeColumnEdgeSourceSource).(*array.StringBuilder).Append(row.EdgeSource.Source)
			builder.Field(parquetEdgeColumnEdgeSourceExternalID).(*array.StringBuilder).Append(row.EdgeSource.ExternalID)
			builder.Field(parquetEdgeColumnEdgeSourceEdgeID).(*array.StringBuilder).Append(row.EdgeSource.EdgeID)
			builder.Field(parquetEdgeColumnEdgeSourceConfidence).(*array.Float64Builder).Append(row.EdgeSource.Confidence)
			builder.Field(parquetEdgeColumnEdgeSourcePriority).(*array.Int64Builder).Append(int64(row.EdgeSource.Priority))
			builder.Field(parquetEdgeColumnEdgeSourceObservedAt).(*array.StringBuilder).Append(formatParquetTime(row.EdgeSource.ObservedAt))
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
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

func decodeParquetEdgeShard(ctx context.Context, data []byte, tenantID string, relationType string, shardID string, version int64) (EdgeShardData, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return EdgeShardData{}, err
	}
	defer table.Release()
	if table.NumCols() < int64(parquetEdgeColumnEdgeSourceObservedAt+1) {
		return EdgeShardData{}, fmt.Errorf("parquet edge shard has %d columns, want at least %d", table.NumCols(), parquetEdgeColumnEdgeSourceObservedAt+1)
	}
	shard := EdgeShardData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, RelationType: relationType, Shard: shardID, Version: version}
	byID := map[string]*graph.Edge{}
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		record := reader.RecordBatch()
		if record.NumCols() < int64(parquetEdgeColumnEdgeSourceObservedAt+1) {
			return EdgeShardData{}, fmt.Errorf("parquet edge record has %d columns, want at least %d", record.NumCols(), parquetEdgeColumnEdgeSourceObservedAt+1)
		}
		columns, err := parquetEdgeShardColumns(record)
		if err != nil {
			return EdgeShardData{}, err
		}
		for i := 0; i < int(record.NumRows()); i++ {
			rowTenant := columns.tenantID.Value(i)
			if rowTenant != "" {
				if shard.TenantID == "" || shard.TenantID == tenantID {
					shard.TenantID = rowTenant
				} else if shard.TenantID != rowTenant {
					return EdgeShardData{}, fmt.Errorf("parquet edge shard tenant mismatch")
				}
			}
			rowRelationType := columns.relationType.Value(i)
			if relationType != "" && rowRelationType != "" && rowRelationType != relationType {
				continue
			}
			if rowRelationType != "" {
				if shard.RelationType == "" || shard.RelationType == relationType {
					shard.RelationType = rowRelationType
				} else if shard.RelationType != rowRelationType {
					return EdgeShardData{}, fmt.Errorf("parquet edge shard relation type mismatch")
				}
			}
			rowShard := columns.shard.Value(i)
			if shardID != "" && rowShard != "" && rowShard != shardID {
				continue
			}
			if rowShard != "" {
				if shardID == "" {
					if shard.Shard == "" {
						shard.Shard = rowShard
					}
				} else if shard.Shard == "" || shard.Shard == shardID {
					shard.Shard = rowShard
				} else if shard.Shard != rowShard {
					return EdgeShardData{}, fmt.Errorf("parquet edge shard id mismatch")
				}
			}
			rowVersion := columns.version.Value(i)
			if rowVersion != 0 {
				if shard.Version == 0 || version == 0 || shard.Version == version {
					shard.Version = rowVersion
				} else if shard.Version != rowVersion {
					return EdgeShardData{}, fmt.Errorf("parquet edge shard version mismatch")
				}
			}
			if shard.UpdatedAt.IsZero() {
				shard.UpdatedAt = parseParquetTime(columns.updatedAt.Value(i))
			}
			edge := byID[columns.edgeID.Value(i)]
			if edge == nil {
				edgeType := columns.edgeType.Value(i)
				if edgeType == "" {
					edgeType = rowRelationType
				}
				created := graph.Edge{
					ID:         columns.edgeID.Value(i),
					Type:       edgeType,
					From:       columns.from.Value(i),
					To:         columns.to.Value(i),
					Source:     columns.source.Value(i),
					ExternalID: columns.externalID.Value(i),
					Version:    columns.edgeVersion.Value(i),
					CreatedAt:  parseParquetTime(columns.edgeCreatedAt.Value(i)),
					UpdatedAt:  parseParquetTime(columns.edgeUpdatedAt.Value(i)),
					Confidence: columns.confidence.Value(i),
					SourceRank: int(columns.sourceRank.Value(i)),
				}
				byID[created.ID] = &created
				edge = &created
			}
			if err := applyEdgeShardRow(edge, edgeShardRowFromColumns(columns, i)); err != nil {
				return EdgeShardData{}, err
			}
		}
	}
	for _, edge := range byID {
		shard.Edges = append(shard.Edges, decodedEdgeShardCopy(*edge))
	}
	sort.Slice(shard.Edges, func(i, j int) bool { return shard.Edges[i].ID < shard.Edges[j].ID })
	return shard, nil
}

type edgeShardRow struct {
	Kind        string
	Ordinal     int
	Key         string
	Value       parquetValue
	FieldSource graph.FieldSource
	EdgeSource  graph.EdgeSource
}

type parquetEdgeShardColumnSet struct {
	tenantID              *array.String
	relationType          *array.String
	shard                 *array.String
	version               *array.Int64
	updatedAt             *array.String
	edgeID                *array.String
	edgeType              *array.String
	from                  *array.String
	to                    *array.String
	source                *array.String
	externalID            *array.String
	edgeVersion           *array.Int64
	edgeCreatedAt         *array.String
	edgeUpdatedAt         *array.String
	confidence            *array.Float64
	sourceRank            *array.Int64
	rowKind               *array.String
	ordinal               *array.Int64
	entryKey              *array.String
	valueKind             *array.String
	stringValue           *array.String
	boolValue             *array.Boolean
	floatValue            *array.Float64
	fieldSourceSource     *array.String
	fieldSourcePriority   *array.Int64
	fieldSourceConfidence *array.Float64
	fieldSourceVersion    *array.Int64
	fieldSourceUpdatedAt  *array.String
	edgeSourceSource      *array.String
	edgeSourceExternalID  *array.String
	edgeSourceEdgeID      *array.String
	edgeSourceConfidence  *array.Float64
	edgeSourcePriority    *array.Int64
	edgeSourceObservedAt  *array.String
}

func edgeShardRows(edge graph.Edge) ([]edgeShardRow, error) {
	edge = graph.CopyEdge(edge)
	rows := []edgeShardRow{}
	fieldNames := sortedAnyMapKeys(edge.Fields)
	for i, field := range fieldNames {
		value, err := parquetValueFromAny(edge.Fields[field])
		if err != nil {
			return nil, err
		}
		rows = append(rows, edgeShardRow{Kind: edgeShardRowField, Ordinal: i, Key: field, Value: value})
	}
	sourceFields := sortedFieldSourceKeys(edge.FieldSources)
	for i, field := range sourceFields {
		rows = append(rows, edgeShardRow{Kind: edgeShardRowFieldSource, Ordinal: i, Key: field, FieldSource: edge.FieldSources[field]})
	}
	for i, source := range edge.Sources {
		rows = append(rows, edgeShardRow{Kind: edgeShardRowSource, Ordinal: i, EdgeSource: source})
	}
	if edge.ExistenceSource != nil {
		rows = append(rows, edgeShardRow{Kind: edgeShardRowExistenceSource, FieldSource: *edge.ExistenceSource})
	}
	if len(rows) == 0 {
		rows = append(rows, edgeShardRow{Kind: edgeShardRowMetadata})
	}
	return rows, nil
}

func applyEdgeShardRow(edge *graph.Edge, row edgeShardRow) error {
	switch row.Kind {
	case edgeShardRowMetadata:
		return nil
	case edgeShardRowField:
		if len(edge.Fields) != row.Ordinal {
			return fmt.Errorf("edge shard field ordinal mismatch")
		}
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		if edge.Fields == nil {
			edge.Fields = graph.Fields{}
		}
		edge.Fields[row.Key] = value
	case edgeShardRowFieldSource:
		if len(edge.FieldSources) != row.Ordinal {
			return fmt.Errorf("edge shard field source ordinal mismatch")
		}
		if edge.FieldSources == nil {
			edge.FieldSources = map[string]graph.FieldSource{}
		}
		edge.FieldSources[row.Key] = row.FieldSource
	case edgeShardRowSource:
		if len(edge.Sources) != row.Ordinal {
			return fmt.Errorf("edge shard source ordinal mismatch")
		}
		edge.Sources = append(edge.Sources, row.EdgeSource)
	case edgeShardRowExistenceSource:
		source := row.FieldSource
		edge.ExistenceSource = &source
	default:
		return fmt.Errorf("unknown edge shard row kind %q", row.Kind)
	}
	return nil
}

func decodedEdgeShardCopy(edge graph.Edge) graph.Edge {
	out := graph.CopyEdge(edge)
	if len(out.Fields) == 0 {
		out.Fields = nil
	}
	if len(out.FieldSources) == 0 {
		out.FieldSources = nil
	}
	if len(out.Sources) == 0 {
		out.Sources = nil
	}
	return out
}

func parquetEdgeShardColumns(record arrow.RecordBatch) (parquetEdgeShardColumnSet, error) {
	var columns parquetEdgeShardColumnSet
	var err error
	if columns.tenantID, err = parquetStringColumn(record, parquetEdgeColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.relationType, err = parquetStringColumn(record, parquetEdgeColumnRelationType, "relation_type"); err != nil {
		return columns, err
	}
	if columns.shard, err = parquetStringColumn(record, parquetEdgeColumnShard, "shard"); err != nil {
		return columns, err
	}
	if columns.version, err = parquetInt64Column(record, parquetEdgeColumnVersion, "version"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetStringColumn(record, parquetEdgeColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	if columns.edgeID, err = parquetStringColumn(record, parquetEdgeColumnEdgeID, "edge_id"); err != nil {
		return columns, err
	}
	if columns.edgeType, err = parquetStringColumn(record, parquetEdgeColumnEdgeType, "edge_type"); err != nil {
		return columns, err
	}
	if columns.from, err = parquetStringColumn(record, parquetEdgeColumnFrom, "from"); err != nil {
		return columns, err
	}
	if columns.to, err = parquetStringColumn(record, parquetEdgeColumnTo, "to"); err != nil {
		return columns, err
	}
	if columns.source, err = parquetStringColumn(record, parquetEdgeColumnSource, "source"); err != nil {
		return columns, err
	}
	if columns.externalID, err = parquetStringColumn(record, parquetEdgeColumnExternalID, "external_id"); err != nil {
		return columns, err
	}
	if columns.edgeVersion, err = parquetInt64Column(record, parquetEdgeColumnEdgeVersion, "edge_version"); err != nil {
		return columns, err
	}
	if columns.edgeCreatedAt, err = parquetStringColumn(record, parquetEdgeColumnEdgeCreatedAt, "edge_created_at"); err != nil {
		return columns, err
	}
	if columns.edgeUpdatedAt, err = parquetStringColumn(record, parquetEdgeColumnEdgeUpdatedAt, "edge_updated_at"); err != nil {
		return columns, err
	}
	if columns.confidence, err = parquetFloat64Column(record, parquetEdgeColumnConfidence, "confidence"); err != nil {
		return columns, err
	}
	if columns.sourceRank, err = parquetInt64Column(record, parquetEdgeColumnSourceRank, "source_priority"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetStringColumn(record, parquetEdgeColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetInt64Column(record, parquetEdgeColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.entryKey, err = parquetStringColumn(record, parquetEdgeColumnEntryKey, "entry_key"); err != nil {
		return columns, err
	}
	if columns.valueKind, err = parquetStringColumn(record, parquetEdgeColumnValueKind, "value_kind"); err != nil {
		return columns, err
	}
	if columns.stringValue, err = parquetStringColumn(record, parquetEdgeColumnStringValue, "string_value"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetBoolColumn(record, parquetEdgeColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetFloat64Column(record, parquetEdgeColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	if columns.fieldSourceSource, err = parquetStringColumn(record, parquetEdgeColumnFieldSourceSource, "field_source_source"); err != nil {
		return columns, err
	}
	if columns.fieldSourcePriority, err = parquetInt64Column(record, parquetEdgeColumnFieldSourcePriority, "field_source_priority"); err != nil {
		return columns, err
	}
	if columns.fieldSourceConfidence, err = parquetFloat64Column(record, parquetEdgeColumnFieldSourceConfidence, "field_source_confidence"); err != nil {
		return columns, err
	}
	if columns.fieldSourceVersion, err = parquetInt64Column(record, parquetEdgeColumnFieldSourceVersion, "field_source_version"); err != nil {
		return columns, err
	}
	if columns.fieldSourceUpdatedAt, err = parquetStringColumn(record, parquetEdgeColumnFieldSourceUpdatedAt, "field_source_updated_at"); err != nil {
		return columns, err
	}
	if columns.edgeSourceSource, err = parquetStringColumn(record, parquetEdgeColumnEdgeSourceSource, "edge_source_source"); err != nil {
		return columns, err
	}
	if columns.edgeSourceExternalID, err = parquetStringColumn(record, parquetEdgeColumnEdgeSourceExternalID, "edge_source_external_id"); err != nil {
		return columns, err
	}
	if columns.edgeSourceEdgeID, err = parquetStringColumn(record, parquetEdgeColumnEdgeSourceEdgeID, "edge_source_edge_id"); err != nil {
		return columns, err
	}
	if columns.edgeSourceConfidence, err = parquetFloat64Column(record, parquetEdgeColumnEdgeSourceConfidence, "edge_source_confidence"); err != nil {
		return columns, err
	}
	if columns.edgeSourcePriority, err = parquetInt64Column(record, parquetEdgeColumnEdgeSourcePriority, "edge_source_priority"); err != nil {
		return columns, err
	}
	if columns.edgeSourceObservedAt, err = parquetStringColumn(record, parquetEdgeColumnEdgeSourceObservedAt, "edge_source_observed_at"); err != nil {
		return columns, err
	}
	return columns, nil
}

func edgeShardRowFromColumns(columns parquetEdgeShardColumnSet, row int) edgeShardRow {
	return edgeShardRow{
		Kind:    columns.rowKind.Value(row),
		Ordinal: int(columns.ordinal.Value(row)),
		Key:     columns.entryKey.Value(row),
		Value: parquetValue{
			Kind:        columns.valueKind.Value(row),
			StringValue: columns.stringValue.Value(row),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
		FieldSource: graph.FieldSource{
			Source:     columns.fieldSourceSource.Value(row),
			Priority:   int(columns.fieldSourcePriority.Value(row)),
			Confidence: columns.fieldSourceConfidence.Value(row),
			Version:    columns.fieldSourceVersion.Value(row),
			UpdatedAt:  parseParquetTime(columns.fieldSourceUpdatedAt.Value(row)),
		},
		EdgeSource: graph.EdgeSource{
			Source:     columns.edgeSourceSource.Value(row),
			ExternalID: columns.edgeSourceExternalID.Value(row),
			EdgeID:     columns.edgeSourceEdgeID.Value(row),
			Confidence: columns.edgeSourceConfidence.Value(row),
			Priority:   int(columns.edgeSourcePriority.Value(row)),
			ObservedAt: parseParquetTime(columns.edgeSourceObservedAt.Value(row)),
		},
	}
}

func (l *PersistedIndexLookup) outEdgesFromParquetShard(ctx context.Context, spec EdgeShard, from string, allowed map[string]struct{}) ([]graph.Edge, bool, error) {
	shard, ok, err := l.Store.loadParquetEdgeShardObject(ctx, l.TenantID, l.Version, spec)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if !ok {
		return nil, false, nil
	}
	if !indexTenantMatches(shard.TenantID, l.TenantID) {
		return nil, false, nil
	}
	if !edgeShardMatchesCatalog(shard, spec, l.Version) {
		return nil, false, nil
	}
	edges := make([]graph.Edge, 0)
	for _, edge := range shard.Edges {
		if edge.From == from && relationAllowedForLookup(edge.Type, allowed) {
			edges = append(edges, edge)
		}
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	return edges, true, nil
}

func (s *TenantStore) loadParquetEdgeShardObject(ctx context.Context, tenantID string, version int64, spec EdgeShard) (EdgeShardData, bool, error) {
	key := firstIndexObjectKey(spec.Objects, "shard", s.parquetEdgeShardVersionKey(tenantID, version, spec.RelationType, spec.Shard))
	if data, _, ok, err := s.cachedIndexObject(ctx, "edge_shard", tenantID, version, key, spec.ContentHash, spec.SchemaHash); err != nil {
		return EdgeShardData{}, false, err
	} else if ok {
		shard, err := decodeParquetEdgeShard(ctx, data, tenantID, spec.RelationType, spec.Shard, 0)
		if err == nil && edgeShardObjectMatchesCatalog(shard, spec, version) {
			return shard, true, nil
		}
		s.dropCachedIndexObject("edge_shard", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return EdgeShardData{}, false, nil
	}
	if err != nil {
		return EdgeShardData{}, false, err
	}
	shard, err := decodeParquetEdgeShard(ctx, data, tenantID, spec.RelationType, spec.Shard, 0)
	if err != nil {
		return EdgeShardData{}, false, err
	}
	if edgeShardObjectMatchesCatalog(shard, spec, version) {
		s.putCachedIndexObject("edge_shard", tenantID, version, key, spec.ContentHash, spec.SchemaHash, data, meta)
	}
	return shard, true, nil
}

func edgeShardObjectMatchesCatalog(shard EdgeShardData, spec EdgeShard, version int64) bool {
	if shard.RelationType != spec.RelationType || shard.Shard != spec.Shard || len(shard.Edges) != spec.EdgeCount {
		return false
	}
	if version > 0 && shard.Version > version {
		return false
	}
	return spec.ContentHash != "" && edgeShardContentHash(shard) == spec.ContentHash
}

func parquetEdgeShardArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "relation_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "shard", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "from", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "to", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "edge_created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entry_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "float_value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "field_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "field_source_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_edge_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "edge_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "edge_source_observed_at", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetEdgeShardSchemaHash() string {
	return objectSchemaHash([]string{
		"tenant_id",
		"relation_type",
		"shard",
		"version",
		"updated_at",
		"edge_id",
		"edge_type",
		"from",
		"to",
		"source",
		"external_id",
		"edge_version",
		"edge_created_at",
		"edge_updated_at",
		"confidence",
		"source_priority",
		"row_kind",
		"ordinal",
		"entry_key",
		"value_kind",
		"string_value",
		"bool_value",
		"float_value",
		"field_source_source",
		"field_source_priority",
		"field_source_confidence",
		"field_source_version",
		"field_source_updated_at",
		"edge_source_source",
		"edge_source_external_id",
		"edge_source_edge_id",
		"edge_source_confidence",
		"edge_source_priority",
		"edge_source_observed_at",
		parquetEdgeShardCodec,
	})
}
