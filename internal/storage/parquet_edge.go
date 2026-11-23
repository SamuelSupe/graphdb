package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
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

func (s *TenantStore) writeParquetEdgeShards(ctx context.Context, tenantID string, shards []EdgeShardData) error {
	return s.writeParquetEdgeShardsWithOptions(ctx, tenantID, shards, true)
}

func (s *TenantStore) writeParquetEdgeShardsWithOptions(ctx context.Context, tenantID string, shards []EdgeShardData, checkExisting bool) error {
	return s.writeParquetEdgeShardGroups(ctx, tenantID, edgeShardDataPackGroups(shards), checkExisting)
}

func (s *TenantStore) writeParquetEdgeShardGroups(ctx context.Context, tenantID string, groups []edgeShardDataPackGroup, checkExisting bool) error {
	return s.runFileWriteJobs(ctx, len(groups), func(workCtx context.Context, index int) error {
		group := groups[index]
		for i := range group.Shards {
			group.Shards[i].TenantID = tenantID
		}
		pack := mergeEdgeShardPack(group)
		pack.TenantID = tenantID
		key := s.parquetEdgeShardVersionKey(tenantID, pack.Version, pack.RelationType, pack.Shard)
		if pack.pageRole != "" {
			key = s.parquetEdgeShardVersionKey(tenantID, pack.Version, pack.RelationType, group.ID)
		}
		if len(group.Shards) > 1 {
			key = s.parquetEdgeShardPackVersionKey(tenantID, pack.Version, pack.RelationType, group.ID)
			if checkExisting {
				if _, ok, err := s.existingParquetEdgeShardPackMeta(workCtx, key, tenantID, group.Shards); err != nil || ok {
					return err
				}
			}
		}
		return s.putParquetEdgeShardObject(workCtx, key, tenantID, pack, checkExisting)
	})
}

func (s *TenantStore) putParquetEdgeShardObject(ctx context.Context, key string, tenantID string, shard EdgeShardData, checkExisting bool) error {
	if checkExisting {
		if ok, err := s.parquetEdgeShardUnchanged(ctx, key, tenantID, shard); err != nil || ok {
			return err
		}
	}
	var data []byte
	var err error
	if len(shard.encodedParquet) > 0 {
		if err = objectContextErr(ctx); err != nil {
			return err
		}
		data = shard.encodedParquet
	} else {
		data, err = marshalParquetEdgeShard(ctx, shard)
		if err != nil {
			return err
		}
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
	if len(shards) == 0 {
		return ObjectMeta{}, false, nil
	}
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
	decoded, err := decodeParquetEdgeShard(ctx, data, tenantID, shards[0].RelationType, "", shards[0].Version)
	if err != nil {
		return meta, false, nil
	}
	byShard := make(map[string][]graph.Edge, len(shards))
	for _, edge := range decoded.Edges {
		sid := edgeShardID(edge.From)
		if shards[0].reverse {
			sid = edgeShardID(edge.To)
		}
		byShard[sid] = append(byShard[sid], edge)
	}
	for _, shard := range shards {
		existing := EdgeShardData{
			LayoutVersion: decoded.LayoutVersion,
			TenantID:      decoded.TenantID,
			RelationType:  shard.RelationType,
			Shard:         shard.Shard,
			Edges:         byShard[shard.Shard],
			Version:       decoded.Version,
			UpdatedAt:     decoded.UpdatedAt,
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
	shardUpdatedAt := parquetio.FormatTime(shard.UpdatedAt)

	type rowPosition struct {
		index int
		shard string
	}
	edges := append([]graph.Edge(nil), shard.Edges...)
	var order []rowPosition
	if isIndexPackID(shard.Shard) {
		order = make([]rowPosition, len(edges))
		for i, edge := range edges {
			rs := edgeShardID(edge.From)
			if shard.reverse {
				rs = edgeShardID(edge.To)
			}
			order[i] = rowPosition{index: i, shard: rs}
		}
		sort.Slice(order, func(i, j int) bool {
			if order[i].shard != order[j].shard {
				return order[i].shard < order[j].shard
			}
			return edges[order[i].index].ID < edges[order[j].index].ID
		})
	} else {
		sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	}
	var shardRowEnds []int64
	lastShard := ""
	for i := range edges {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		edge := edges[i]
		rowShard := shard.Shard
		if order != nil {
			edge = edges[order[i].index]
			rowShard = order[i].shard
		}
		if i > 0 && rowShard != lastShard {
			shardRowEnds = append(shardRowEnds, int64(builder.Field(0).Len()))
		}
		lastShard = rowShard
		rows, err := codec.EdgeRows(edge)
		if err != nil {
			return nil, err
		}
		createdAt := parquetio.FormatTime(edge.CreatedAt)
		updatedAt := parquetio.FormatTime(edge.UpdatedAt)
		for _, row := range rows {
			builder.Field(parquetEdgeColumnTenantID).(*array.StringBuilder).Append(shard.TenantID)
			builder.Field(parquetEdgeColumnRelationType).(*array.StringBuilder).Append(shard.RelationType)
			builder.Field(parquetEdgeColumnShard).(*array.StringBuilder).Append(rowShard)
			builder.Field(parquetEdgeColumnVersion).(*array.Int64Builder).Append(shard.Version)
			builder.Field(parquetEdgeColumnUpdatedAt).(*array.StringBuilder).Append(shardUpdatedAt)
			builder.Field(parquetEdgeColumnEdgeID).(*array.StringBuilder).Append(edge.ID)
			builder.Field(parquetEdgeColumnEdgeType).(*array.StringBuilder).Append(edge.Type)
			builder.Field(parquetEdgeColumnFrom).(*array.StringBuilder).Append(edge.From)
			builder.Field(parquetEdgeColumnTo).(*array.StringBuilder).Append(edge.To)
			builder.Field(parquetEdgeColumnSource).(*array.StringBuilder).Append(edge.Source)
			builder.Field(parquetEdgeColumnExternalID).(*array.StringBuilder).Append(edge.ExternalID)
			builder.Field(parquetEdgeColumnEdgeVersion).(*array.Int64Builder).Append(edge.Version)
			builder.Field(parquetEdgeColumnEdgeCreatedAt).(*array.StringBuilder).Append(createdAt)
			builder.Field(parquetEdgeColumnEdgeUpdatedAt).(*array.StringBuilder).Append(updatedAt)
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
			builder.Field(parquetEdgeColumnFieldSourceUpdatedAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.FieldSource.UpdatedAt))
			builder.Field(parquetEdgeColumnEdgeSourceSource).(*array.StringBuilder).Append(row.EdgeSource.Source)
			builder.Field(parquetEdgeColumnEdgeSourceExternalID).(*array.StringBuilder).Append(row.EdgeSource.ExternalID)
			builder.Field(parquetEdgeColumnEdgeSourceEdgeID).(*array.StringBuilder).Append(row.EdgeSource.EdgeID)
			builder.Field(parquetEdgeColumnEdgeSourceConfidence).(*array.Float64Builder).Append(row.EdgeSource.Confidence)
			builder.Field(parquetEdgeColumnEdgeSourcePriority).(*array.Int64Builder).Append(int64(row.EdgeSource.Priority))
			builder.Field(parquetEdgeColumnEdgeSourceObservedAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.EdgeSource.ObservedAt))
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()

	var buf bytes.Buffer
	writerProps := parquetio.DataWriterProperties(schema)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	writer, err := pqarrow.NewFileWriter(schema, &buf, writerProps, arrowProps)
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	if order != nil && record.NumRows() > 0 {
		shardRowEnds = append(shardRowEnds, record.NumRows())
		rowGroupSize := int64(parquetEntityPageRowGroupSize)
		previousEnd := int64(0)
		for _, end := range shardRowEnds {
			if end-previousEnd < 512 {
				shardRowEnds = []int64{record.NumRows()}
				rowGroupSize = 512
				break
			}
			previousEnd = end
		}
		start := int64(0)
		for _, end := range shardRowEnds {
			for start < end {
				stop := min(start+rowGroupSize, end)
				slice := record.NewSlice(start, stop)
				err := writer.Write(slice)
				slice.Release()
				if err != nil {
					return nil, err
				}
				start = stop
			}
		}
	} else {
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetEdgeShard(ctx context.Context, data []byte, tenantID string, relationType string, shardID string, version int64) (EdgeShardData, error) {
	return decodeParquetEdgeShardReader(ctx, bytes.NewReader(data), tenantID, relationType, shardID, version)
}

func decodeParquetEdgeShardReader(ctx context.Context, source parquet.ReaderAtSeeker, tenantID string, relationType string, shardID string, version int64) (EdgeShardData, error) {
	file, err := pqfile.NewParquetReader(source)
	if err != nil {
		return EdgeShardData{}, err
	}
	defer file.Close()
	if file.MetaData().Schema.NumColumns() < parquetEdgeColumnEdgeSourceObservedAt+1 {
		return EdgeShardData{}, fmt.Errorf("parquet edge shard has %d columns, want at least %d", file.MetaData().Schema.NumColumns(), parquetEdgeColumnEdgeSourceObservedAt+1)
	}
	shard := EdgeShardData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, RelationType: relationType, Shard: shardID, Version: version}
	filterShard := shardID != "" && !isIndexPackID(shardID)
	rowGroups := make([]int, 0, file.NumRowGroups())
	for i := 0; i < file.NumRowGroups(); i++ {
		if !filterShard || parquetRowGroupStringMayContain(file, i, parquetEdgeColumnShard, shardID) ||
			parquetRowGroupStringMayContain(file, i, parquetEdgeColumnShard, "") {
			rowGroups = append(rowGroups, i)
		}
	}
	if len(rowGroups) == 0 {
		return shard, objectContextErr(ctx)
	}
	fileReader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{BatchSize: 128}, memory.DefaultAllocator)
	if err != nil {
		return EdgeShardData{}, err
	}
	reader, release, err := parquetio.ReadRecordReader(ctx, fileReader, nil, rowGroups)
	if err != nil {
		return EdgeShardData{}, err
	}
	defer release()
	defer reader.Release()
	byID := map[string]*graph.Edge{}
	for reader.Next() {
		if err := objectContextErr(ctx); err != nil {
			return EdgeShardData{}, err
		}
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
					shard.TenantID = strings.Clone(rowTenant)
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
					shard.RelationType = strings.Clone(rowRelationType)
				} else if shard.RelationType != rowRelationType {
					return EdgeShardData{}, fmt.Errorf("parquet edge shard relation type mismatch")
				}
			}
			rowShard := columns.shard.Value(i)
			if filterShard && rowShard != "" && rowShard != shardID {
				continue
			}
			if rowShard != "" && !isIndexPackID(shardID) {
				if shardID == "" {
					if shard.Shard == "" {
						shard.Shard = strings.Clone(rowShard)
					}
				} else if shard.Shard == "" || shard.Shard == shardID {
					shard.Shard = strings.Clone(rowShard)
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
				shard.UpdatedAt = parquetio.ParseTime(columns.updatedAt.Value(i))
			}
			edge := byID[columns.edgeID.Value(i)]
			if edge == nil {
				edgeType := columns.edgeType.Value(i)
				if edgeType == "" {
					edgeType = rowRelationType
				}
				created := graph.Edge{
					ID:         strings.Clone(columns.edgeID.Value(i)),
					Type:       strings.Clone(edgeType),
					From:       strings.Clone(columns.from.Value(i)),
					To:         strings.Clone(columns.to.Value(i)),
					Source:     strings.Clone(columns.source.Value(i)),
					ExternalID: strings.Clone(columns.externalID.Value(i)),
					Version:    columns.edgeVersion.Value(i),
					CreatedAt:  parquetio.ParseTime(columns.edgeCreatedAt.Value(i)),
					UpdatedAt:  parquetio.ParseTime(columns.edgeUpdatedAt.Value(i)),
					Confidence: columns.confidence.Value(i),
					SourceRank: int(columns.sourceRank.Value(i)),
				}
				byID[created.ID] = &created
				edge = &created
			}
			if err := codec.ApplyEdgeRow(edge, edgeShardRowFromColumns(columns, i)); err != nil {
				return EdgeShardData{}, err
			}
		}
	}
	if err := reader.Err(); err != nil {
		return EdgeShardData{}, err
	}
	for _, edge := range byID {
		shard.Edges = append(shard.Edges, codec.DecodedEdgeCopy(*edge))
	}
	sort.Slice(shard.Edges, func(i, j int) bool { return shard.Edges[i].ID < shard.Edges[j].ID })
	return shard, nil
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

func parquetEdgeShardColumns(record arrow.RecordBatch) (parquetEdgeShardColumnSet, error) {
	var columns parquetEdgeShardColumnSet
	var err error
	if columns.tenantID, err = parquetio.StringColumn(record, parquetEdgeColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.relationType, err = parquetio.StringColumn(record, parquetEdgeColumnRelationType, "relation_type"); err != nil {
		return columns, err
	}
	if columns.shard, err = parquetio.StringColumn(record, parquetEdgeColumnShard, "shard"); err != nil {
		return columns, err
	}
	if columns.version, err = parquetio.Int64Column(record, parquetEdgeColumnVersion, "version"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetio.StringColumn(record, parquetEdgeColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	if columns.edgeID, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeID, "edge_id"); err != nil {
		return columns, err
	}
	if columns.edgeType, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeType, "edge_type"); err != nil {
		return columns, err
	}
	if columns.from, err = parquetio.StringColumn(record, parquetEdgeColumnFrom, "from"); err != nil {
		return columns, err
	}
	if columns.to, err = parquetio.StringColumn(record, parquetEdgeColumnTo, "to"); err != nil {
		return columns, err
	}
	if columns.source, err = parquetio.StringColumn(record, parquetEdgeColumnSource, "source"); err != nil {
		return columns, err
	}
	if columns.externalID, err = parquetio.StringColumn(record, parquetEdgeColumnExternalID, "external_id"); err != nil {
		return columns, err
	}
	if columns.edgeVersion, err = parquetio.Int64Column(record, parquetEdgeColumnEdgeVersion, "edge_version"); err != nil {
		return columns, err
	}
	if columns.edgeCreatedAt, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeCreatedAt, "edge_created_at"); err != nil {
		return columns, err
	}
	if columns.edgeUpdatedAt, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeUpdatedAt, "edge_updated_at"); err != nil {
		return columns, err
	}
	if columns.confidence, err = parquetio.Float64Column(record, parquetEdgeColumnConfidence, "confidence"); err != nil {
		return columns, err
	}
	if columns.sourceRank, err = parquetio.Int64Column(record, parquetEdgeColumnSourceRank, "source_priority"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetio.StringColumn(record, parquetEdgeColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetio.Int64Column(record, parquetEdgeColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.entryKey, err = parquetio.StringColumn(record, parquetEdgeColumnEntryKey, "entry_key"); err != nil {
		return columns, err
	}
	if columns.valueKind, err = parquetio.StringColumn(record, parquetEdgeColumnValueKind, "value_kind"); err != nil {
		return columns, err
	}
	if columns.stringValue, err = parquetio.StringColumn(record, parquetEdgeColumnStringValue, "string_value"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetio.BoolColumn(record, parquetEdgeColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetio.Float64Column(record, parquetEdgeColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	if columns.fieldSourceSource, err = parquetio.StringColumn(record, parquetEdgeColumnFieldSourceSource, "field_source_source"); err != nil {
		return columns, err
	}
	if columns.fieldSourcePriority, err = parquetio.Int64Column(record, parquetEdgeColumnFieldSourcePriority, "field_source_priority"); err != nil {
		return columns, err
	}
	if columns.fieldSourceConfidence, err = parquetio.Float64Column(record, parquetEdgeColumnFieldSourceConfidence, "field_source_confidence"); err != nil {
		return columns, err
	}
	if columns.fieldSourceVersion, err = parquetio.Int64Column(record, parquetEdgeColumnFieldSourceVersion, "field_source_version"); err != nil {
		return columns, err
	}
	if columns.fieldSourceUpdatedAt, err = parquetio.StringColumn(record, parquetEdgeColumnFieldSourceUpdatedAt, "field_source_updated_at"); err != nil {
		return columns, err
	}
	if columns.edgeSourceSource, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeSourceSource, "edge_source_source"); err != nil {
		return columns, err
	}
	if columns.edgeSourceExternalID, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeSourceExternalID, "edge_source_external_id"); err != nil {
		return columns, err
	}
	if columns.edgeSourceEdgeID, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeSourceEdgeID, "edge_source_edge_id"); err != nil {
		return columns, err
	}
	if columns.edgeSourceConfidence, err = parquetio.Float64Column(record, parquetEdgeColumnEdgeSourceConfidence, "edge_source_confidence"); err != nil {
		return columns, err
	}
	if columns.edgeSourcePriority, err = parquetio.Int64Column(record, parquetEdgeColumnEdgeSourcePriority, "edge_source_priority"); err != nil {
		return columns, err
	}
	if columns.edgeSourceObservedAt, err = parquetio.StringColumn(record, parquetEdgeColumnEdgeSourceObservedAt, "edge_source_observed_at"); err != nil {
		return columns, err
	}
	return columns, nil
}

func edgeShardRowFromColumns(columns parquetEdgeShardColumnSet, row int) edgeShardRow {
	return edgeShardRow{
		Kind:    strings.Clone(columns.rowKind.Value(row)),
		Ordinal: int(columns.ordinal.Value(row)),
		Key:     strings.Clone(columns.entryKey.Value(row)),
		Value: parquetValue{
			Kind:        strings.Clone(columns.valueKind.Value(row)),
			StringValue: strings.Clone(columns.stringValue.Value(row)),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
		FieldSource: graph.FieldSource{
			Source:     strings.Clone(columns.fieldSourceSource.Value(row)),
			Priority:   int(columns.fieldSourcePriority.Value(row)),
			Confidence: columns.fieldSourceConfidence.Value(row),
			Version:    columns.fieldSourceVersion.Value(row),
			UpdatedAt:  parquetio.ParseTime(columns.fieldSourceUpdatedAt.Value(row)),
		},
		EdgeSource: graph.EdgeSource{
			Source:     strings.Clone(columns.edgeSourceSource.Value(row)),
			ExternalID: strings.Clone(columns.edgeSourceExternalID.Value(row)),
			EdgeID:     strings.Clone(columns.edgeSourceEdgeID.Value(row)),
			Confidence: columns.edgeSourceConfidence.Value(row),
			Priority:   int(columns.edgeSourcePriority.Value(row)),
			ObservedAt: parquetio.ParseTime(columns.edgeSourceObservedAt.Value(row)),
		},
	}
}

func (l *PersistedIndexLookup) outEdgesFromParquetShard(ctx context.Context, spec EdgeShard, from string, allowed map[string]struct{}) ([]graph.Edge, bool, error) {
	edges, ok, err := l.cachedParquetOutEdges(ctx, spec, from)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if !ok {
		return nil, false, nil
	}
	if len(allowed) > 0 {
		filtered := edges[:0]
		for _, edge := range edges {
			if relationAllowedForLookup(edge.Type, allowed) {
				filtered = append(filtered, edge)
			}
		}
		edges = filtered
	}
	return edges, true, nil
}

func (l *PersistedIndexLookup) inEdgesFromParquetShard(ctx context.Context, spec EdgeShard, to string, allowed map[string]struct{}) ([]graph.Edge, bool, error) {
	edges, ok, err := l.cachedParquetInEdges(ctx, spec, to)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if !ok {
		return nil, false, nil
	}
	if len(allowed) > 0 {
		filtered := edges[:0]
		for _, edge := range edges {
			if relationAllowedForLookup(edge.Type, allowed) {
				filtered = append(filtered, edge)
			}
		}
		edges = filtered
	}
	return edges, true, nil
}

func (s *TenantStore) loadParquetEdgeShardObject(ctx context.Context, tenantID string, version int64, spec EdgeShard) (EdgeShardData, bool, error) {
	if hasIndexRanges(spec.Objects) {
		return s.loadEdgeRangePage(ctx, tenantID, version, spec)
	}
	key := firstIndexObjectKey(spec.Objects, "shard", s.parquetEdgeShardVersionKey(tenantID, version, spec.RelationType, spec.Shard))
	if exclusiveFileStore(s.Objects) != nil {
		releaseDecode, err := parquetio.AcquireDecode(ctx)
		if err != nil {
			return EdgeShardData{}, false, err
		}
		defer releaseDecode()
		ctx = parquetio.WithDecodeAdmission(ctx)
		reader, err := openFileReader(ctx, s.Objects, key)
		if errors.Is(err, ErrNotFound) {
			return EdgeShardData{}, false, nil
		}
		if err != nil {
			return EdgeShardData{}, false, err
		}
		defer reader.Close()
		shard, err := decodeParquetEdgeShardReader(ctx, reader, tenantID, spec.RelationType, spec.Shard, 0)
		if err == nil {
			shard.cacheVerified = edgeShardObjectMatchesCatalog(shard, spec, version)
		}
		return shard, err == nil, err
	}
	if data, _, verified, ok, err := s.cachedIndexObjectWithVerification(ctx, "edge_shard", tenantID, version, key, spec.ContentHash, spec.SchemaHash); err != nil {
		return EdgeShardData{}, false, err
	} else if ok {
		shard, err := decodeParquetEdgeShard(ctx, data, tenantID, spec.RelationType, spec.Shard, 0)
		shard.cacheVerified = verified
		if err == nil && edgeShardObjectMatchesCatalog(shard, spec, version) {
			if !verified {
				s.markCachedIndexObjectVerified("edge_shard", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
			}
			shard.cacheVerified = true
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
		shard.cacheVerified = true
		s.putVerifiedCachedIndexObject("edge_shard", tenantID, version, key, spec.ContentHash, spec.SchemaHash, data, meta)
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
	return shard.cacheVerified || (spec.ContentHash != "" && edgeShardContentHash(shard) == spec.ContentHash)
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
