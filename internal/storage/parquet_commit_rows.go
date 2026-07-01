package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	parquetCommitColumnTenantID = iota
	parquetCommitColumnSegmentKey
	parquetCommitColumnCommitTenantID
	parquetCommitColumnCommitID
	parquetCommitColumnVersion
	parquetCommitColumnCreatedAt
	parquetCommitColumnContentHash
	parquetCommitColumnRowKind
	parquetCommitColumnComponentKind
	parquetCommitColumnParentOrdinal
	parquetCommitColumnChildOrdinal
	parquetCommitColumnNestedOrdinal
	parquetCommitColumnEntryKey
	parquetCommitColumnValueKind
	parquetCommitColumnStringValue
	parquetCommitColumnBoolValue
	parquetCommitColumnFloatValue
	parquetCommitColumnID
	parquetCommitColumnName
	parquetCommitColumnDisplayName
	parquetCommitColumnKindName
	parquetCommitColumnTypeName
	parquetCommitColumnFieldType
	parquetCommitColumnSource
	parquetCommitColumnExternalID
	parquetCommitColumnFrom
	parquetCommitColumnTo
	parquetCommitColumnTargetID
	parquetCommitColumnSourceID
	parquetCommitColumnReason
	parquetCommitColumnAction
	parquetCommitColumnStrategy
	parquetCommitColumnReverseName
	parquetCommitColumnFromKind
	parquetCommitColumnToKind
	parquetCommitColumnCardinality
	parquetCommitColumnImpactDirection
	parquetCommitColumnSplitFrom
	parquetCommitColumnRequired
	parquetCommitColumnIndexed
	parquetCommitColumnUnique
	parquetCommitColumnDirected
	parquetCommitColumnAllowCrossKind
	parquetCommitColumnStandard
	parquetCommitColumnConfidence
	parquetCommitColumnSourcePriority
	parquetCommitColumnVersionValue
	parquetCommitColumnCreatedAtValue
	parquetCommitColumnUpdatedAtValue
	parquetCommitColumnFieldSourceSource
	parquetCommitColumnFieldSourcePriority
	parquetCommitColumnFieldSourceConfidence
	parquetCommitColumnFieldSourceVersion
	parquetCommitColumnFieldSourceUpdatedAt
	parquetCommitColumnEntitySourceSource
	parquetCommitColumnEntitySourceExternalID
	parquetCommitColumnEntitySourceConfidence
	parquetCommitColumnEntitySourcePriority
	parquetCommitColumnEntitySourceObservedAt
	parquetCommitColumnEntitySourceStale
	parquetCommitColumnEntitySourceStaleAt
	parquetCommitColumnEdgeSourceSource
	parquetCommitColumnEdgeSourceExternalID
	parquetCommitColumnEdgeSourceEdgeID
	parquetCommitColumnEdgeSourceConfidence
	parquetCommitColumnEdgeSourcePriority
	parquetCommitColumnEdgeSourceObservedAt
)

const (
	commitRowMetadata              = "metadata"
	commitRowUpsertCIType          = "upsert_ci_type"
	commitRowCITypeExtends         = "ci_type_extends"
	commitRowCITypeField           = "ci_type_field"
	commitRowCITypeFieldEnum       = "ci_type_field_enum"
	commitRowCITypeFieldDefault    = "ci_type_field_default"
	commitRowCITypeIdentity        = "ci_type_identity"
	commitRowCITypeIdentityField   = "ci_type_identity_field"
	commitRowDeleteCIType          = "delete_ci_type"
	commitRowUpsertRelationType    = "upsert_relation_type"
	commitRowRelationFromKind      = "relation_from_kind"
	commitRowRelationToKind        = "relation_to_kind"
	commitRowDeleteRelationType    = "delete_relation_type"
	commitRowUpsertEntity          = "upsert_entity"
	commitRowDeleteEntity          = "delete_entity"
	commitRowDeleteEntityRequest   = "delete_entity_request"
	commitRowMarkSourceStale       = "mark_source_stale"
	commitRowStaleObservedExternal = "stale_observed_external_id"
	commitRowUpsertEdge            = "upsert_edge"
	commitRowDeleteEdge            = "delete_edge"
	commitRowDeleteEdgeRequest     = "delete_edge_request"
	commitRowMergeEntity           = "merge_entity"
	commitRowMergeSource           = "merge_source"
	commitRowSplitRequest          = "split_request"
	commitRowSplitEntity           = "split_entity"
)

type parquetCommitTableItem struct {
	Key         string
	Commit      graph.Commit
	ContentHash string
}

type parquetCommitRow struct {
	Kind            string
	ComponentKind   string
	ParentOrdinal   int
	ChildOrdinal    int
	NestedOrdinal   int
	EntryKey        string
	Value           parquetValue
	ID              string
	Name            string
	DisplayName     string
	KindName        string
	TypeName        string
	FieldType       string
	Source          string
	ExternalID      string
	From            string
	To              string
	TargetID        string
	SourceID        string
	Reason          string
	Action          string
	Strategy        string
	ReverseName     string
	FromKind        string
	ToKind          string
	Cardinality     string
	ImpactDirection string
	SplitFrom       string
	Required        bool
	Indexed         bool
	Unique          bool
	Directed        bool
	AllowCrossKind  bool
	Standard        bool
	Confidence      float64
	SourcePriority  int
	VersionValue    int64
	CreatedAtValue  time.Time
	UpdatedAtValue  time.Time
	FieldSource     graph.FieldSource
	EntitySource    graph.EntitySource
	EdgeSource      graph.EdgeSource
}

type parquetCommitColumnSet struct {
	tenantID               *array.String
	segmentKey             *array.String
	commitTenantID         *array.String
	commitID               *array.String
	version                *array.Int64
	createdAt              *array.String
	contentHash            *array.String
	rowKind                *array.String
	componentKind          *array.String
	parentOrdinal          *array.Int64
	childOrdinal           *array.Int64
	nestedOrdinal          *array.Int64
	entryKey               *array.String
	valueKind              *array.String
	stringValue            *array.String
	boolValue              *array.Boolean
	floatValue             *array.Float64
	id                     *array.String
	name                   *array.String
	displayName            *array.String
	kindName               *array.String
	typeName               *array.String
	fieldType              *array.String
	source                 *array.String
	externalID             *array.String
	from                   *array.String
	to                     *array.String
	targetID               *array.String
	sourceID               *array.String
	reason                 *array.String
	action                 *array.String
	strategy               *array.String
	reverseName            *array.String
	fromKind               *array.String
	toKind                 *array.String
	cardinality            *array.String
	impactDirection        *array.String
	splitFrom              *array.String
	required               *array.Boolean
	indexed                *array.Boolean
	unique                 *array.Boolean
	directed               *array.Boolean
	allowCrossKind         *array.Boolean
	standard               *array.Boolean
	confidence             *array.Float64
	sourcePriority         *array.Int64
	versionValue           *array.Int64
	createdAtValue         *array.String
	updatedAtValue         *array.String
	fieldSourceSource      *array.String
	fieldSourcePriority    *array.Int64
	fieldSourceConfidence  *array.Float64
	fieldSourceVersion     *array.Int64
	fieldSourceUpdatedAt   *array.String
	entitySourceSource     *array.String
	entitySourceExternalID *array.String
	entitySourceConfidence *array.Float64
	entitySourcePriority   *array.Int64
	entitySourceObservedAt *array.String
	entitySourceStale      *array.Boolean
	entitySourceStaleAt    *array.String
	edgeSourceSource       *array.String
	edgeSourceExternalID   *array.String
	edgeSourceEdgeID       *array.String
	edgeSourceConfidence   *array.Float64
	edgeSourcePriority     *array.Int64
	edgeSourceObservedAt   *array.String
}

func marshalParquetCommitItems(ctx context.Context, tenantID string, items []parquetCommitTableItem) ([]byte, error) {
	schema := parquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, item := range items {
		normalized, hash, err := normalizeCommitForParquet(item.Commit)
		if err != nil {
			return nil, err
		}
		if item.ContentHash == "" {
			item.ContentHash = hash
		}
		rows, err := commitRows(normalized)
		if err != nil {
			return nil, err
		}
		rowTenant := tenantID
		if rowTenant == "" {
			rowTenant = normalized.TenantID
		}
		for _, row := range rows {
			appendParquetCommitRow(builder, rowTenant, item.Key, normalized, item.ContentHash, row)
		}
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

func decodeParquetCommitItems(ctx context.Context, data []byte) ([]parquetCommitTableItem, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return nil, fmt.Errorf("parquet commit table is empty")
	}
	if table.NumCols() < int64(parquetCommitColumnEdgeSourceObservedAt+1) {
		return nil, fmt.Errorf("parquet commit table has %d columns, want at least %d", table.NumCols(), parquetCommitColumnEdgeSourceObservedAt+1)
	}

	builders := map[string]*commitBuild{}
	order := []string{}
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetCommitColumns(batch)
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			key := commitBuildKey(columns.segmentKey.Value(i), columns.commitID.Value(i), columns.version.Value(i))
			build := builders[key]
			if build == nil {
				build = &commitBuild{
					key:         columns.segmentKey.Value(i),
					contentHash: columns.contentHash.Value(i),
					commit: graph.Commit{
						LayoutVersion: CurrentObjectLayoutVersion,
						TenantID:      columns.commitTenantID.Value(i),
						ID:            columns.commitID.Value(i),
						Version:       columns.version.Value(i),
						CreatedAt:     parseParquetTime(columns.createdAt.Value(i)),
					},
				}
				builders[key] = build
				order = append(order, key)
			}
			if build.contentHash != columns.contentHash.Value(i) ||
				build.commit.TenantID != columns.commitTenantID.Value(i) ||
				build.commit.ID != columns.commitID.Value(i) ||
				build.commit.Version != columns.version.Value(i) {
				return nil, fmt.Errorf("parquet commit row identity mismatch")
			}
			if err := build.apply(parquetCommitRowFromColumns(columns, i)); err != nil {
				return nil, err
			}
		}
	}

	items := make([]parquetCommitTableItem, 0, len(order))
	for _, key := range order {
		item, err := builders[key].finish()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func normalizeCommitForParquet(commit graph.Commit) (graph.Commit, string, error) {
	payload, err := commitPayloadJSON(commit)
	if err != nil {
		return graph.Commit{}, "", err
	}
	var normalized graph.Commit
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return graph.Commit{}, "", err
	}
	return normalized, objectContentHash(payload), nil
}

func commitRows(commit graph.Commit) ([]parquetCommitRow, error) {
	rows := []parquetCommitRow{{Kind: commitRowMetadata}}
	for i, ciType := range commit.Mutations.UpsertCITypes {
		rows = append(rows, ciTypeRows(i, ciType)...)
	}
	for i, name := range commit.Mutations.DeleteCITypes {
		rows = append(rows, parquetCommitRow{Kind: commitRowDeleteCIType, ParentOrdinal: i, Name: name})
	}
	for i, relationType := range commit.Mutations.UpsertRelationTypes {
		rows = append(rows, relationTypeRows(i, relationType)...)
	}
	for i, name := range commit.Mutations.DeleteRelationTypes {
		rows = append(rows, parquetCommitRow{Kind: commitRowDeleteRelationType, ParentOrdinal: i, Name: name})
	}
	for i, entity := range commit.Mutations.UpsertEntities {
		entityRows, err := entityMutationRows(commitRowUpsertEntity, i, 0, entity)
		if err != nil {
			return nil, err
		}
		rows = append(rows, entityRows...)
	}
	for i, id := range commit.Mutations.DeleteEntities {
		rows = append(rows, parquetCommitRow{Kind: commitRowDeleteEntity, ParentOrdinal: i, ID: id})
	}
	for i, request := range commit.Mutations.DeleteEntityRequests {
		rows = append(rows, parquetCommitRow{
			Kind:           commitRowDeleteEntityRequest,
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
	for i, request := range commit.Mutations.MarkSourceStale {
		rows = append(rows, parquetCommitRow{
			Kind:           commitRowMarkSourceStale,
			ParentOrdinal:  i,
			Source:         request.Source,
			KindName:       request.Kind,
			Action:         request.Action,
			SourcePriority: request.SourceRank,
			Confidence:     request.Confidence,
			Reason:         request.Reason,
		})
		for j, externalID := range request.ObservedExternalIDs {
			rows = append(rows, parquetCommitRow{Kind: commitRowStaleObservedExternal, ParentOrdinal: i, ChildOrdinal: j, Value: stringCommitValue(externalID)})
		}
	}
	for i, edge := range commit.Mutations.UpsertEdges {
		edgeRows, err := edgeMutationRows(commitRowUpsertEdge, i, edge)
		if err != nil {
			return nil, err
		}
		rows = append(rows, edgeRows...)
	}
	for i, id := range commit.Mutations.DeleteEdges {
		rows = append(rows, parquetCommitRow{Kind: commitRowDeleteEdge, ParentOrdinal: i, ID: id})
	}
	for i, request := range commit.Mutations.DeleteEdgeRequests {
		rows = append(rows, parquetCommitRow{
			Kind:           commitRowDeleteEdgeRequest,
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
	for i, merge := range commit.Mutations.MergeEntities {
		rows = append(rows, parquetCommitRow{Kind: commitRowMergeEntity, ParentOrdinal: i, TargetID: merge.TargetID})
		for j, sourceID := range merge.SourceIDs {
			rows = append(rows, parquetCommitRow{Kind: commitRowMergeSource, ParentOrdinal: i, ChildOrdinal: j, SourceID: sourceID})
		}
	}
	for i, split := range commit.Mutations.SplitEntities {
		rows = append(rows, parquetCommitRow{Kind: commitRowSplitRequest, ParentOrdinal: i, SourceID: split.SourceID})
		for j, entity := range split.Entities {
			entityRows, err := entityMutationRows(commitRowSplitEntity, i, j, entity)
			if err != nil {
				return nil, err
			}
			rows = append(rows, entityRows...)
		}
	}
	return rows, nil
}

func ciTypeRows(ordinal int, ciType graph.CIType) []parquetCommitRow {
	rows := []parquetCommitRow{{
		Kind:          commitRowUpsertCIType,
		ParentOrdinal: ordinal,
		Name:          ciType.Name,
		DisplayName:   ciType.DisplayName,
	}}
	for i, extend := range ciType.Extends {
		rows = append(rows, parquetCommitRow{Kind: commitRowCITypeExtends, ParentOrdinal: ordinal, ChildOrdinal: i, Value: stringCommitValue(extend)})
	}
	fieldNames := sortedFieldSpecKeys(ciType.Fields)
	for i, fieldName := range fieldNames {
		spec := ciType.Fields[fieldName]
		rows = append(rows, parquetCommitRow{
			Kind:          commitRowCITypeField,
			ParentOrdinal: ordinal,
			ChildOrdinal:  i,
			EntryKey:      fieldName,
			FieldType:     spec.Type,
			Strategy:      spec.MergeStrategy,
			Required:      spec.Required,
			Indexed:       spec.Indexed,
			Unique:        spec.Unique,
		})
		for j, enumValue := range spec.Enum {
			value, err := parquetValueFromAny(enumValue)
			if err == nil {
				rows = append(rows, parquetCommitRow{Kind: commitRowCITypeFieldEnum, ParentOrdinal: ordinal, ChildOrdinal: i, NestedOrdinal: j, EntryKey: fieldName, Value: value})
			}
		}
		if spec.Default != nil {
			if value, err := parquetValueFromAny(spec.Default); err == nil {
				rows = append(rows, parquetCommitRow{Kind: commitRowCITypeFieldDefault, ParentOrdinal: ordinal, ChildOrdinal: i, EntryKey: fieldName, Value: value})
			}
		}
	}
	for i, identity := range ciType.IdentityKeys {
		rows = append(rows, parquetCommitRow{
			Kind:          commitRowCITypeIdentity,
			ParentOrdinal: ordinal,
			ChildOrdinal:  i,
			Name:          identity.Name,
			Confidence:    identity.ConfidenceThreshold,
			Strategy:      identity.Strategy,
		})
		for j, field := range identity.Fields {
			rows = append(rows, parquetCommitRow{Kind: commitRowCITypeIdentityField, ParentOrdinal: ordinal, ChildOrdinal: i, NestedOrdinal: j, Value: stringCommitValue(field)})
		}
	}
	return rows
}

func relationTypeRows(ordinal int, relationType graph.RelationType) []parquetCommitRow {
	rows := []parquetCommitRow{{
		Kind:            commitRowUpsertRelationType,
		ParentOrdinal:   ordinal,
		Name:            relationType.Name,
		DisplayName:     relationType.DisplayName,
		ReverseName:     relationType.ReverseName,
		FromKind:        relationType.FromKind,
		ToKind:          relationType.ToKind,
		Directed:        relationType.Directed,
		Cardinality:     relationType.Cardinality,
		ImpactDirection: relationType.ImpactDirection,
		AllowCrossKind:  relationType.AllowCrossKind,
		Standard:        relationType.Standard,
	}}
	for i, kind := range relationType.FromKinds {
		rows = append(rows, parquetCommitRow{Kind: commitRowRelationFromKind, ParentOrdinal: ordinal, ChildOrdinal: i, Value: stringCommitValue(kind)})
	}
	for i, kind := range relationType.ToKinds {
		rows = append(rows, parquetCommitRow{Kind: commitRowRelationToKind, ParentOrdinal: ordinal, ChildOrdinal: i, Value: stringCommitValue(kind)})
	}
	return rows
}

func entityMutationRows(kind string, parent int, child int, entity graph.Entity) ([]parquetCommitRow, error) {
	entity = graph.CopyEntity(entity)
	components, err := entityPageRows(entity)
	if err != nil {
		return nil, err
	}
	rows := make([]parquetCommitRow, 0, len(components))
	for _, component := range components {
		row := parquetCommitRow{
			Kind:           kind,
			ComponentKind:  component.Kind,
			ParentOrdinal:  parent,
			ChildOrdinal:   child,
			NestedOrdinal:  component.Ordinal,
			EntryKey:       component.Key,
			Value:          component.Value,
			ID:             entity.ID,
			KindName:       entity.Kind,
			Source:         entity.Source,
			ExternalID:     entity.ExternalID,
			Confidence:     entity.Confidence,
			SourcePriority: entity.SourceRank,
			VersionValue:   entity.Version,
			CreatedAtValue: entity.CreatedAt,
			UpdatedAtValue: entity.UpdatedAt,
			SplitFrom:      entity.SplitFrom,
			Strategy:       entity.FieldWriteModes[component.Key],
			FieldSource:    component.FieldSource,
			EntitySource:   component.EntitySource,
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func edgeMutationRows(kind string, parent int, edge graph.Edge) ([]parquetCommitRow, error) {
	edge = graph.CopyEdge(edge)
	components, err := edgeShardRows(edge)
	if err != nil {
		return nil, err
	}
	rows := make([]parquetCommitRow, 0, len(components))
	for _, component := range components {
		row := parquetCommitRow{
			Kind:           kind,
			ComponentKind:  component.Kind,
			ParentOrdinal:  parent,
			ChildOrdinal:   component.Ordinal,
			EntryKey:       component.Key,
			Value:          component.Value,
			ID:             edge.ID,
			TypeName:       edge.Type,
			From:           edge.From,
			To:             edge.To,
			Source:         edge.Source,
			ExternalID:     edge.ExternalID,
			Confidence:     edge.Confidence,
			SourcePriority: edge.SourceRank,
			VersionValue:   edge.Version,
			CreatedAtValue: edge.CreatedAt,
			UpdatedAtValue: edge.UpdatedAt,
			FieldSource:    component.FieldSource,
			EdgeSource:     component.EdgeSource,
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func appendParquetCommitRow(builder *array.RecordBuilder, tenantID string, key string, commit graph.Commit, hash string, row parquetCommitRow) {
	builder.Field(parquetCommitColumnTenantID).(*array.StringBuilder).Append(tenantID)
	builder.Field(parquetCommitColumnSegmentKey).(*array.StringBuilder).Append(key)
	builder.Field(parquetCommitColumnCommitTenantID).(*array.StringBuilder).Append(commit.TenantID)
	builder.Field(parquetCommitColumnCommitID).(*array.StringBuilder).Append(commit.ID)
	builder.Field(parquetCommitColumnVersion).(*array.Int64Builder).Append(commit.Version)
	builder.Field(parquetCommitColumnCreatedAt).(*array.StringBuilder).Append(formatParquetTime(commit.CreatedAt))
	builder.Field(parquetCommitColumnContentHash).(*array.StringBuilder).Append(hash)
	builder.Field(parquetCommitColumnRowKind).(*array.StringBuilder).Append(row.Kind)
	builder.Field(parquetCommitColumnComponentKind).(*array.StringBuilder).Append(row.ComponentKind)
	builder.Field(parquetCommitColumnParentOrdinal).(*array.Int64Builder).Append(int64(row.ParentOrdinal))
	builder.Field(parquetCommitColumnChildOrdinal).(*array.Int64Builder).Append(int64(row.ChildOrdinal))
	builder.Field(parquetCommitColumnNestedOrdinal).(*array.Int64Builder).Append(int64(row.NestedOrdinal))
	builder.Field(parquetCommitColumnEntryKey).(*array.StringBuilder).Append(row.EntryKey)
	builder.Field(parquetCommitColumnValueKind).(*array.StringBuilder).Append(row.Value.Kind)
	builder.Field(parquetCommitColumnStringValue).(*array.StringBuilder).Append(row.Value.StringValue)
	builder.Field(parquetCommitColumnBoolValue).(*array.BooleanBuilder).Append(row.Value.BoolValue)
	builder.Field(parquetCommitColumnFloatValue).(*array.Float64Builder).Append(row.Value.FloatValue)
	builder.Field(parquetCommitColumnID).(*array.StringBuilder).Append(row.ID)
	builder.Field(parquetCommitColumnName).(*array.StringBuilder).Append(row.Name)
	builder.Field(parquetCommitColumnDisplayName).(*array.StringBuilder).Append(row.DisplayName)
	builder.Field(parquetCommitColumnKindName).(*array.StringBuilder).Append(row.KindName)
	builder.Field(parquetCommitColumnTypeName).(*array.StringBuilder).Append(row.TypeName)
	builder.Field(parquetCommitColumnFieldType).(*array.StringBuilder).Append(row.FieldType)
	builder.Field(parquetCommitColumnSource).(*array.StringBuilder).Append(row.Source)
	builder.Field(parquetCommitColumnExternalID).(*array.StringBuilder).Append(row.ExternalID)
	builder.Field(parquetCommitColumnFrom).(*array.StringBuilder).Append(row.From)
	builder.Field(parquetCommitColumnTo).(*array.StringBuilder).Append(row.To)
	builder.Field(parquetCommitColumnTargetID).(*array.StringBuilder).Append(row.TargetID)
	builder.Field(parquetCommitColumnSourceID).(*array.StringBuilder).Append(row.SourceID)
	builder.Field(parquetCommitColumnReason).(*array.StringBuilder).Append(row.Reason)
	builder.Field(parquetCommitColumnAction).(*array.StringBuilder).Append(row.Action)
	builder.Field(parquetCommitColumnStrategy).(*array.StringBuilder).Append(row.Strategy)
	builder.Field(parquetCommitColumnReverseName).(*array.StringBuilder).Append(row.ReverseName)
	builder.Field(parquetCommitColumnFromKind).(*array.StringBuilder).Append(row.FromKind)
	builder.Field(parquetCommitColumnToKind).(*array.StringBuilder).Append(row.ToKind)
	builder.Field(parquetCommitColumnCardinality).(*array.StringBuilder).Append(row.Cardinality)
	builder.Field(parquetCommitColumnImpactDirection).(*array.StringBuilder).Append(row.ImpactDirection)
	builder.Field(parquetCommitColumnSplitFrom).(*array.StringBuilder).Append(row.SplitFrom)
	builder.Field(parquetCommitColumnRequired).(*array.BooleanBuilder).Append(row.Required)
	builder.Field(parquetCommitColumnIndexed).(*array.BooleanBuilder).Append(row.Indexed)
	builder.Field(parquetCommitColumnUnique).(*array.BooleanBuilder).Append(row.Unique)
	builder.Field(parquetCommitColumnDirected).(*array.BooleanBuilder).Append(row.Directed)
	builder.Field(parquetCommitColumnAllowCrossKind).(*array.BooleanBuilder).Append(row.AllowCrossKind)
	builder.Field(parquetCommitColumnStandard).(*array.BooleanBuilder).Append(row.Standard)
	builder.Field(parquetCommitColumnConfidence).(*array.Float64Builder).Append(row.Confidence)
	builder.Field(parquetCommitColumnSourcePriority).(*array.Int64Builder).Append(int64(row.SourcePriority))
	builder.Field(parquetCommitColumnVersionValue).(*array.Int64Builder).Append(row.VersionValue)
	builder.Field(parquetCommitColumnCreatedAtValue).(*array.StringBuilder).Append(formatParquetTime(row.CreatedAtValue))
	builder.Field(parquetCommitColumnUpdatedAtValue).(*array.StringBuilder).Append(formatParquetTime(row.UpdatedAtValue))
	builder.Field(parquetCommitColumnFieldSourceSource).(*array.StringBuilder).Append(row.FieldSource.Source)
	builder.Field(parquetCommitColumnFieldSourcePriority).(*array.Int64Builder).Append(int64(row.FieldSource.Priority))
	builder.Field(parquetCommitColumnFieldSourceConfidence).(*array.Float64Builder).Append(row.FieldSource.Confidence)
	builder.Field(parquetCommitColumnFieldSourceVersion).(*array.Int64Builder).Append(row.FieldSource.Version)
	builder.Field(parquetCommitColumnFieldSourceUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(row.FieldSource.UpdatedAt))
	builder.Field(parquetCommitColumnEntitySourceSource).(*array.StringBuilder).Append(row.EntitySource.Source)
	builder.Field(parquetCommitColumnEntitySourceExternalID).(*array.StringBuilder).Append(row.EntitySource.ExternalID)
	builder.Field(parquetCommitColumnEntitySourceConfidence).(*array.Float64Builder).Append(row.EntitySource.Confidence)
	builder.Field(parquetCommitColumnEntitySourcePriority).(*array.Int64Builder).Append(int64(row.EntitySource.Priority))
	builder.Field(parquetCommitColumnEntitySourceObservedAt).(*array.StringBuilder).Append(formatParquetTime(row.EntitySource.ObservedAt))
	builder.Field(parquetCommitColumnEntitySourceStale).(*array.BooleanBuilder).Append(row.EntitySource.Stale)
	builder.Field(parquetCommitColumnEntitySourceStaleAt).(*array.StringBuilder).Append(formatParquetTime(row.EntitySource.StaleAt))
	builder.Field(parquetCommitColumnEdgeSourceSource).(*array.StringBuilder).Append(row.EdgeSource.Source)
	builder.Field(parquetCommitColumnEdgeSourceExternalID).(*array.StringBuilder).Append(row.EdgeSource.ExternalID)
	builder.Field(parquetCommitColumnEdgeSourceEdgeID).(*array.StringBuilder).Append(row.EdgeSource.EdgeID)
	builder.Field(parquetCommitColumnEdgeSourceConfidence).(*array.Float64Builder).Append(row.EdgeSource.Confidence)
	builder.Field(parquetCommitColumnEdgeSourcePriority).(*array.Int64Builder).Append(int64(row.EdgeSource.Priority))
	builder.Field(parquetCommitColumnEdgeSourceObservedAt).(*array.StringBuilder).Append(formatParquetTime(row.EdgeSource.ObservedAt))
}

func parquetCommitArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "segment_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "commit_tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "commit_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "component_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "parent_ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "child_ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "nested_ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entry_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "float_value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "display_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "from", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "to", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "target_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "reason", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "action", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "strategy", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "reverse_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "from_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "to_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "cardinality", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "impact_direction", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "split_from", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "required", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "indexed", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "unique", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "directed", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "allow_cross_kind", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "standard", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "version_value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "created_at_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "updated_at_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "field_source_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "entity_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_source_observed_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_stale", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "entity_source_stale_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_edge_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "edge_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "edge_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "edge_source_observed_at", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetCommitColumns(batch arrow.RecordBatch) (parquetCommitColumnSet, error) {
	var columns parquetCommitColumnSet
	var err error
	stringColumns := []struct {
		target **array.String
		index  int
		name   string
	}{
		{&columns.tenantID, parquetCommitColumnTenantID, "tenant_id"},
		{&columns.segmentKey, parquetCommitColumnSegmentKey, "segment_key"},
		{&columns.commitTenantID, parquetCommitColumnCommitTenantID, "commit_tenant_id"},
		{&columns.commitID, parquetCommitColumnCommitID, "commit_id"},
		{&columns.createdAt, parquetCommitColumnCreatedAt, "created_at"},
		{&columns.contentHash, parquetCommitColumnContentHash, "content_hash"},
		{&columns.rowKind, parquetCommitColumnRowKind, "row_kind"},
		{&columns.componentKind, parquetCommitColumnComponentKind, "component_kind"},
		{&columns.entryKey, parquetCommitColumnEntryKey, "entry_key"},
		{&columns.valueKind, parquetCommitColumnValueKind, "value_kind"},
		{&columns.stringValue, parquetCommitColumnStringValue, "string_value"},
		{&columns.id, parquetCommitColumnID, "id"},
		{&columns.name, parquetCommitColumnName, "name"},
		{&columns.displayName, parquetCommitColumnDisplayName, "display_name"},
		{&columns.kindName, parquetCommitColumnKindName, "kind"},
		{&columns.typeName, parquetCommitColumnTypeName, "type"},
		{&columns.fieldType, parquetCommitColumnFieldType, "field_type"},
		{&columns.source, parquetCommitColumnSource, "source"},
		{&columns.externalID, parquetCommitColumnExternalID, "external_id"},
		{&columns.from, parquetCommitColumnFrom, "from"},
		{&columns.to, parquetCommitColumnTo, "to"},
		{&columns.targetID, parquetCommitColumnTargetID, "target_id"},
		{&columns.sourceID, parquetCommitColumnSourceID, "source_id"},
		{&columns.reason, parquetCommitColumnReason, "reason"},
		{&columns.action, parquetCommitColumnAction, "action"},
		{&columns.strategy, parquetCommitColumnStrategy, "strategy"},
		{&columns.reverseName, parquetCommitColumnReverseName, "reverse_name"},
		{&columns.fromKind, parquetCommitColumnFromKind, "from_kind"},
		{&columns.toKind, parquetCommitColumnToKind, "to_kind"},
		{&columns.cardinality, parquetCommitColumnCardinality, "cardinality"},
		{&columns.impactDirection, parquetCommitColumnImpactDirection, "impact_direction"},
		{&columns.splitFrom, parquetCommitColumnSplitFrom, "split_from"},
		{&columns.createdAtValue, parquetCommitColumnCreatedAtValue, "created_at_value"},
		{&columns.updatedAtValue, parquetCommitColumnUpdatedAtValue, "updated_at_value"},
		{&columns.fieldSourceSource, parquetCommitColumnFieldSourceSource, "field_source_source"},
		{&columns.fieldSourceUpdatedAt, parquetCommitColumnFieldSourceUpdatedAt, "field_source_updated_at"},
		{&columns.entitySourceSource, parquetCommitColumnEntitySourceSource, "entity_source_source"},
		{&columns.entitySourceExternalID, parquetCommitColumnEntitySourceExternalID, "entity_source_external_id"},
		{&columns.entitySourceObservedAt, parquetCommitColumnEntitySourceObservedAt, "entity_source_observed_at"},
		{&columns.entitySourceStaleAt, parquetCommitColumnEntitySourceStaleAt, "entity_source_stale_at"},
		{&columns.edgeSourceSource, parquetCommitColumnEdgeSourceSource, "edge_source_source"},
		{&columns.edgeSourceExternalID, parquetCommitColumnEdgeSourceExternalID, "edge_source_external_id"},
		{&columns.edgeSourceEdgeID, parquetCommitColumnEdgeSourceEdgeID, "edge_source_edge_id"},
		{&columns.edgeSourceObservedAt, parquetCommitColumnEdgeSourceObservedAt, "edge_source_observed_at"},
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
		{&columns.version, parquetCommitColumnVersion, "version"},
		{&columns.parentOrdinal, parquetCommitColumnParentOrdinal, "parent_ordinal"},
		{&columns.childOrdinal, parquetCommitColumnChildOrdinal, "child_ordinal"},
		{&columns.nestedOrdinal, parquetCommitColumnNestedOrdinal, "nested_ordinal"},
		{&columns.sourcePriority, parquetCommitColumnSourcePriority, "source_priority"},
		{&columns.versionValue, parquetCommitColumnVersionValue, "version_value"},
		{&columns.fieldSourcePriority, parquetCommitColumnFieldSourcePriority, "field_source_priority"},
		{&columns.fieldSourceVersion, parquetCommitColumnFieldSourceVersion, "field_source_version"},
		{&columns.entitySourcePriority, parquetCommitColumnEntitySourcePriority, "entity_source_priority"},
		{&columns.edgeSourcePriority, parquetCommitColumnEdgeSourcePriority, "edge_source_priority"},
	}
	for _, item := range intColumns {
		if *item.target, err = parquetInt64Column(batch, item.index, item.name); err != nil {
			return columns, err
		}
	}
	boolColumns := []struct {
		target **array.Boolean
		index  int
		name   string
	}{
		{&columns.boolValue, parquetCommitColumnBoolValue, "bool_value"},
		{&columns.required, parquetCommitColumnRequired, "required"},
		{&columns.indexed, parquetCommitColumnIndexed, "indexed"},
		{&columns.unique, parquetCommitColumnUnique, "unique"},
		{&columns.directed, parquetCommitColumnDirected, "directed"},
		{&columns.allowCrossKind, parquetCommitColumnAllowCrossKind, "allow_cross_kind"},
		{&columns.standard, parquetCommitColumnStandard, "standard"},
		{&columns.entitySourceStale, parquetCommitColumnEntitySourceStale, "entity_source_stale"},
	}
	for _, item := range boolColumns {
		if *item.target, err = parquetBoolColumn(batch, item.index, item.name); err != nil {
			return columns, err
		}
	}
	floatColumns := []struct {
		target **array.Float64
		index  int
		name   string
	}{
		{&columns.floatValue, parquetCommitColumnFloatValue, "float_value"},
		{&columns.confidence, parquetCommitColumnConfidence, "confidence"},
		{&columns.fieldSourceConfidence, parquetCommitColumnFieldSourceConfidence, "field_source_confidence"},
		{&columns.entitySourceConfidence, parquetCommitColumnEntitySourceConfidence, "entity_source_confidence"},
		{&columns.edgeSourceConfidence, parquetCommitColumnEdgeSourceConfidence, "edge_source_confidence"},
	}
	for _, item := range floatColumns {
		if *item.target, err = parquetFloat64Column(batch, item.index, item.name); err != nil {
			return columns, err
		}
	}
	return columns, nil
}

func parquetCommitRowFromColumns(columns parquetCommitColumnSet, row int) parquetCommitRow {
	return parquetCommitRow{
		Kind:          columns.rowKind.Value(row),
		ComponentKind: columns.componentKind.Value(row),
		ParentOrdinal: int(columns.parentOrdinal.Value(row)),
		ChildOrdinal:  int(columns.childOrdinal.Value(row)),
		NestedOrdinal: int(columns.nestedOrdinal.Value(row)),
		EntryKey:      columns.entryKey.Value(row),
		Value: parquetValue{
			Kind:        columns.valueKind.Value(row),
			StringValue: columns.stringValue.Value(row),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
		ID:              columns.id.Value(row),
		Name:            columns.name.Value(row),
		DisplayName:     columns.displayName.Value(row),
		KindName:        columns.kindName.Value(row),
		TypeName:        columns.typeName.Value(row),
		FieldType:       columns.fieldType.Value(row),
		Source:          columns.source.Value(row),
		ExternalID:      columns.externalID.Value(row),
		From:            columns.from.Value(row),
		To:              columns.to.Value(row),
		TargetID:        columns.targetID.Value(row),
		SourceID:        columns.sourceID.Value(row),
		Reason:          columns.reason.Value(row),
		Action:          columns.action.Value(row),
		Strategy:        columns.strategy.Value(row),
		ReverseName:     columns.reverseName.Value(row),
		FromKind:        columns.fromKind.Value(row),
		ToKind:          columns.toKind.Value(row),
		Cardinality:     columns.cardinality.Value(row),
		ImpactDirection: columns.impactDirection.Value(row),
		SplitFrom:       columns.splitFrom.Value(row),
		Required:        columns.required.Value(row),
		Indexed:         columns.indexed.Value(row),
		Unique:          columns.unique.Value(row),
		Directed:        columns.directed.Value(row),
		AllowCrossKind:  columns.allowCrossKind.Value(row),
		Standard:        columns.standard.Value(row),
		Confidence:      columns.confidence.Value(row),
		SourcePriority:  int(columns.sourcePriority.Value(row)),
		VersionValue:    columns.versionValue.Value(row),
		CreatedAtValue:  parseParquetTime(columns.createdAtValue.Value(row)),
		UpdatedAtValue:  parseParquetTime(columns.updatedAtValue.Value(row)),
		FieldSource: graph.FieldSource{
			Source:     columns.fieldSourceSource.Value(row),
			Priority:   int(columns.fieldSourcePriority.Value(row)),
			Confidence: columns.fieldSourceConfidence.Value(row),
			Version:    columns.fieldSourceVersion.Value(row),
			UpdatedAt:  parseParquetTime(columns.fieldSourceUpdatedAt.Value(row)),
		},
		EntitySource: graph.EntitySource{
			Source:     columns.entitySourceSource.Value(row),
			ExternalID: columns.entitySourceExternalID.Value(row),
			Confidence: columns.entitySourceConfidence.Value(row),
			Priority:   int(columns.entitySourcePriority.Value(row)),
			ObservedAt: parseParquetTime(columns.entitySourceObservedAt.Value(row)),
			Stale:      columns.entitySourceStale.Value(row),
			StaleAt:    parseParquetTime(columns.entitySourceStaleAt.Value(row)),
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

type commitBuild struct {
	key           string
	contentHash   string
	commit        graph.Commit
	ciTypes       map[int]*ciTypeBuild
	relations     map[int]*relationBuild
	entities      map[int]*entityBuild
	splitEntities map[int]map[int]*entityBuild
	edges         map[int]*edgeBuild
}

type ciTypeBuild struct {
	item       graph.CIType
	fields     map[int]string
	identities map[int]*identityBuild
}

type identityBuild struct {
	item graph.IdentityKey
}

type relationBuild struct {
	item graph.RelationType
}

type entityBuild struct {
	item        graph.Entity
	initialized bool
}

type edgeBuild struct {
	item        graph.Edge
	initialized bool
}

func (b *commitBuild) apply(row parquetCommitRow) error {
	switch row.Kind {
	case commitRowMetadata:
		return nil
	case commitRowUpsertCIType, commitRowCITypeExtends, commitRowCITypeField, commitRowCITypeFieldEnum, commitRowCITypeFieldDefault, commitRowCITypeIdentity, commitRowCITypeIdentityField:
		return b.applyCITypeRow(row)
	case commitRowDeleteCIType:
		b.commit.Mutations.DeleteCITypes = setStringAt(b.commit.Mutations.DeleteCITypes, row.ParentOrdinal, row.Name)
	case commitRowUpsertRelationType, commitRowRelationFromKind, commitRowRelationToKind:
		return b.applyRelationRow(row)
	case commitRowDeleteRelationType:
		b.commit.Mutations.DeleteRelationTypes = setStringAt(b.commit.Mutations.DeleteRelationTypes, row.ParentOrdinal, row.Name)
	case commitRowUpsertEntity:
		entity, err := b.entity(row.ParentOrdinal)
		if err != nil {
			return err
		}
		return entity.apply(row)
	case commitRowDeleteEntity:
		b.commit.Mutations.DeleteEntities = setStringAt(b.commit.Mutations.DeleteEntities, row.ParentOrdinal, row.ID)
	case commitRowDeleteEntityRequest:
		b.commit.Mutations.DeleteEntityRequests = setEntityDeleteRequestAt(b.commit.Mutations.DeleteEntityRequests, row.ParentOrdinal, graph.EntityDeleteRequest{
			ID: row.ID, Kind: row.KindName, Source: row.Source, ExternalID: row.ExternalID, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		})
	case commitRowMarkSourceStale:
		b.commit.Mutations.MarkSourceStale = setSourceStaleAt(b.commit.Mutations.MarkSourceStale, row.ParentOrdinal, graph.SourceStaleRequest{
			Source: row.Source, Kind: row.KindName, Action: row.Action, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		})
	case commitRowStaleObservedExternal:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		b.commit.Mutations.MarkSourceStale = ensureSourceStaleLen(b.commit.Mutations.MarkSourceStale, row.ParentOrdinal)
		b.commit.Mutations.MarkSourceStale[row.ParentOrdinal].ObservedExternalIDs = setStringAt(b.commit.Mutations.MarkSourceStale[row.ParentOrdinal].ObservedExternalIDs, row.ChildOrdinal, value)
	case commitRowUpsertEdge:
		edge, err := b.edge(row.ParentOrdinal)
		if err != nil {
			return err
		}
		return edge.apply(row)
	case commitRowDeleteEdge:
		b.commit.Mutations.DeleteEdges = setStringAt(b.commit.Mutations.DeleteEdges, row.ParentOrdinal, row.ID)
	case commitRowDeleteEdgeRequest:
		b.commit.Mutations.DeleteEdgeRequests = setEdgeDeleteRequestAt(b.commit.Mutations.DeleteEdgeRequests, row.ParentOrdinal, graph.EdgeDeleteRequest{
			ID: row.ID, Type: row.TypeName, From: row.From, To: row.To, Source: row.Source, SourceRank: row.SourcePriority, Confidence: row.Confidence, Reason: row.Reason,
		})
	case commitRowMergeEntity:
		b.commit.Mutations.MergeEntities = setMergeAt(b.commit.Mutations.MergeEntities, row.ParentOrdinal, graph.MergeRequest{TargetID: row.TargetID})
	case commitRowMergeSource:
		b.commit.Mutations.MergeEntities = ensureMergeLen(b.commit.Mutations.MergeEntities, row.ParentOrdinal)
		b.commit.Mutations.MergeEntities[row.ParentOrdinal].SourceIDs = setStringAt(b.commit.Mutations.MergeEntities[row.ParentOrdinal].SourceIDs, row.ChildOrdinal, row.SourceID)
	case commitRowSplitRequest:
		b.commit.Mutations.SplitEntities = setSplitAt(b.commit.Mutations.SplitEntities, row.ParentOrdinal, graph.SplitRequest{SourceID: row.SourceID})
	case commitRowSplitEntity:
		entity, err := b.splitEntity(row.ParentOrdinal, row.ChildOrdinal)
		if err != nil {
			return err
		}
		return entity.apply(row)
	default:
		return fmt.Errorf("unknown commit row kind %q", row.Kind)
	}
	return nil
}

func (b *commitBuild) finish() (parquetCommitTableItem, error) {
	for _, ordinal := range sortedIntKeys(b.ciTypes) {
		b.commit.Mutations.UpsertCITypes = setCITypeAt(b.commit.Mutations.UpsertCITypes, ordinal, b.ciTypes[ordinal].item)
	}
	for _, ordinal := range sortedIntKeys(b.relations) {
		b.commit.Mutations.UpsertRelationTypes = setRelationTypeAt(b.commit.Mutations.UpsertRelationTypes, ordinal, b.relations[ordinal].item)
	}
	for _, ordinal := range sortedIntKeys(b.entities) {
		b.commit.Mutations.UpsertEntities = setEntityAt(b.commit.Mutations.UpsertEntities, ordinal, decodedEntityPageCopy(b.entities[ordinal].item))
	}
	for _, ordinal := range sortedIntKeys(b.edges) {
		b.commit.Mutations.UpsertEdges = setEdgeAt(b.commit.Mutations.UpsertEdges, ordinal, decodedEdgeShardCopy(b.edges[ordinal].item))
	}
	for _, parent := range sortedIntKeys(b.splitEntities) {
		b.commit.Mutations.SplitEntities = ensureSplitLen(b.commit.Mutations.SplitEntities, parent)
		for _, child := range sortedIntKeys(b.splitEntities[parent]) {
			b.commit.Mutations.SplitEntities[parent].Entities = setEntityAt(b.commit.Mutations.SplitEntities[parent].Entities, child, decodedEntityPageCopy(b.splitEntities[parent][child].item))
		}
	}
	if err := normalizeObjectAfterRead(&b.commit, "commit"); err != nil {
		return parquetCommitTableItem{}, err
	}
	hash, err := commitPayloadHash(b.commit)
	if err != nil {
		return parquetCommitTableItem{}, err
	}
	if b.contentHash == "" || b.contentHash != hash {
		return parquetCommitTableItem{}, fmt.Errorf("commit object content hash mismatch")
	}
	return parquetCommitTableItem{Key: b.key, Commit: b.commit, ContentHash: b.contentHash}, nil
}

func (b *commitBuild) ciType(ordinal int) *ciTypeBuild {
	if b.ciTypes == nil {
		b.ciTypes = map[int]*ciTypeBuild{}
	}
	item := b.ciTypes[ordinal]
	if item == nil {
		item = &ciTypeBuild{fields: map[int]string{}, identities: map[int]*identityBuild{}}
		b.ciTypes[ordinal] = item
	}
	return item
}

func (b *commitBuild) relation(ordinal int) *relationBuild {
	if b.relations == nil {
		b.relations = map[int]*relationBuild{}
	}
	item := b.relations[ordinal]
	if item == nil {
		item = &relationBuild{}
		b.relations[ordinal] = item
	}
	return item
}

func (b *commitBuild) entity(ordinal int) (*entityBuild, error) {
	if b.entities == nil {
		b.entities = map[int]*entityBuild{}
	}
	item := b.entities[ordinal]
	if item == nil {
		item = &entityBuild{}
		b.entities[ordinal] = item
	}
	return item, nil
}

func (b *commitBuild) edge(ordinal int) (*edgeBuild, error) {
	if b.edges == nil {
		b.edges = map[int]*edgeBuild{}
	}
	item := b.edges[ordinal]
	if item == nil {
		item = &edgeBuild{}
		b.edges[ordinal] = item
	}
	return item, nil
}

func (b *commitBuild) splitEntity(parent int, child int) (*entityBuild, error) {
	if b.splitEntities == nil {
		b.splitEntities = map[int]map[int]*entityBuild{}
	}
	byChild := b.splitEntities[parent]
	if byChild == nil {
		byChild = map[int]*entityBuild{}
		b.splitEntities[parent] = byChild
	}
	item := byChild[child]
	if item == nil {
		item = &entityBuild{}
		byChild[child] = item
	}
	return item, nil
}

func (b *commitBuild) applyCITypeRow(row parquetCommitRow) error {
	item := b.ciType(row.ParentOrdinal)
	switch row.Kind {
	case commitRowUpsertCIType:
		item.item.Name = row.Name
		item.item.DisplayName = row.DisplayName
	case commitRowCITypeExtends:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		item.item.Extends = setStringAt(item.item.Extends, row.ChildOrdinal, value)
	case commitRowCITypeField:
		if item.item.Fields == nil {
			item.item.Fields = map[string]graph.FieldSpec{}
		}
		item.fields[row.ChildOrdinal] = row.EntryKey
		item.item.Fields[row.EntryKey] = graph.FieldSpec{Type: row.FieldType, MergeStrategy: row.Strategy, Required: row.Required, Indexed: row.Indexed, Unique: row.Unique}
	case commitRowCITypeFieldEnum:
		field := item.fields[row.ChildOrdinal]
		spec := item.item.Fields[field]
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		spec.Enum = setAnyAt(spec.Enum, row.NestedOrdinal, value)
		item.item.Fields[field] = spec
	case commitRowCITypeFieldDefault:
		field := item.fields[row.ChildOrdinal]
		spec := item.item.Fields[field]
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		spec.Default = value
		item.item.Fields[field] = spec
	case commitRowCITypeIdentity:
		identity := graph.IdentityKey{Name: row.Name, ConfidenceThreshold: row.Confidence, Strategy: row.Strategy}
		item.item.IdentityKeys = setIdentityAt(item.item.IdentityKeys, row.ChildOrdinal, identity)
	case commitRowCITypeIdentityField:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		item.item.IdentityKeys = ensureIdentityLen(item.item.IdentityKeys, row.ChildOrdinal)
		item.item.IdentityKeys[row.ChildOrdinal].Fields = setStringAt(item.item.IdentityKeys[row.ChildOrdinal].Fields, row.NestedOrdinal, value)
	}
	return nil
}

func (b *commitBuild) applyRelationRow(row parquetCommitRow) error {
	item := b.relation(row.ParentOrdinal)
	switch row.Kind {
	case commitRowUpsertRelationType:
		item.item = graph.RelationType{
			Name: row.Name, DisplayName: row.DisplayName, ReverseName: row.ReverseName, FromKind: row.FromKind, ToKind: row.ToKind,
			Directed: row.Directed, Cardinality: row.Cardinality, ImpactDirection: row.ImpactDirection, AllowCrossKind: row.AllowCrossKind, Standard: row.Standard,
		}
	case commitRowRelationFromKind:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		item.item.FromKinds = setStringAt(item.item.FromKinds, row.ChildOrdinal, value)
	case commitRowRelationToKind:
		value, err := stringFromCommitValue(row.Value)
		if err != nil {
			return err
		}
		item.item.ToKinds = setStringAt(item.item.ToKinds, row.ChildOrdinal, value)
	}
	return nil
}

func (b *entityBuild) apply(row parquetCommitRow) error {
	if !b.initialized {
		b.item = graph.Entity{
			ID: row.ID, Kind: row.KindName, Source: row.Source, ExternalID: row.ExternalID, Confidence: row.Confidence, SourceRank: row.SourcePriority,
			Version: row.VersionValue, CreatedAt: row.CreatedAtValue, UpdatedAt: row.UpdatedAtValue, SplitFrom: row.SplitFrom,
		}
		b.initialized = true
	}
	return applyEntityPageRow(&b.item, entityPageRow{
		Kind: row.ComponentKind, Ordinal: row.NestedOrdinal, Key: row.EntryKey, Value: row.Value, Strategy: row.Strategy, FieldSource: row.FieldSource, EntitySource: row.EntitySource,
	})
}

func (b *edgeBuild) apply(row parquetCommitRow) error {
	if !b.initialized {
		b.item = graph.Edge{
			ID: row.ID, Type: row.TypeName, From: row.From, To: row.To, Source: row.Source, ExternalID: row.ExternalID, Confidence: row.Confidence, SourceRank: row.SourcePriority,
			Version: row.VersionValue, CreatedAt: row.CreatedAtValue, UpdatedAt: row.UpdatedAtValue,
		}
		b.initialized = true
	}
	return applyEdgeShardRow(&b.item, edgeShardRow{
		Kind: row.ComponentKind, Ordinal: row.ChildOrdinal, Key: row.EntryKey, Value: row.Value, FieldSource: row.FieldSource, EdgeSource: row.EdgeSource,
	})
}

func commitBuildKey(key string, id string, version int64) string {
	return key + "\x00" + id + "\x00" + formatInt64ForHash(version)
}

func stringCommitValue(value string) parquetValue {
	return parquetValue{Kind: parquetValueKindString, StringValue: value}
}

func stringFromCommitValue(value parquetValue) (string, error) {
	out, err := anyFromParquetValue(value)
	if err != nil {
		return "", err
	}
	if out == nil {
		return "", nil
	}
	text, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("commit value is %T, want string", out)
	}
	return text, nil
}

func sortedFieldSpecKeys(values map[string]graph.FieldSpec) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedIntKeys[T any](values map[int]T) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func setStringAt(values []string, index int, value string) []string {
	for len(values) <= index {
		values = append(values, "")
	}
	values[index] = value
	return values
}

func setAnyAt(values []any, index int, value any) []any {
	for len(values) <= index {
		values = append(values, nil)
	}
	values[index] = value
	return values
}

func setCITypeAt(values []graph.CIType, index int, value graph.CIType) []graph.CIType {
	for len(values) <= index {
		values = append(values, graph.CIType{})
	}
	values[index] = value
	return values
}

func setRelationTypeAt(values []graph.RelationType, index int, value graph.RelationType) []graph.RelationType {
	for len(values) <= index {
		values = append(values, graph.RelationType{})
	}
	values[index] = value
	return values
}

func setEntityAt(values []graph.Entity, index int, value graph.Entity) []graph.Entity {
	for len(values) <= index {
		values = append(values, graph.Entity{})
	}
	values[index] = value
	return values
}

func setEdgeAt(values []graph.Edge, index int, value graph.Edge) []graph.Edge {
	for len(values) <= index {
		values = append(values, graph.Edge{})
	}
	values[index] = value
	return values
}

func ensureIdentityLen(values []graph.IdentityKey, index int) []graph.IdentityKey {
	for len(values) <= index {
		values = append(values, graph.IdentityKey{})
	}
	return values
}

func setIdentityAt(values []graph.IdentityKey, index int, value graph.IdentityKey) []graph.IdentityKey {
	values = ensureIdentityLen(values, index)
	values[index] = value
	return values
}

func ensureSourceStaleLen(values []graph.SourceStaleRequest, index int) []graph.SourceStaleRequest {
	for len(values) <= index {
		values = append(values, graph.SourceStaleRequest{})
	}
	return values
}

func setSourceStaleAt(values []graph.SourceStaleRequest, index int, value graph.SourceStaleRequest) []graph.SourceStaleRequest {
	values = ensureSourceStaleLen(values, index)
	values[index] = value
	return values
}

func setEntityDeleteRequestAt(values []graph.EntityDeleteRequest, index int, value graph.EntityDeleteRequest) []graph.EntityDeleteRequest {
	for len(values) <= index {
		values = append(values, graph.EntityDeleteRequest{})
	}
	values[index] = value
	return values
}

func setEdgeDeleteRequestAt(values []graph.EdgeDeleteRequest, index int, value graph.EdgeDeleteRequest) []graph.EdgeDeleteRequest {
	for len(values) <= index {
		values = append(values, graph.EdgeDeleteRequest{})
	}
	values[index] = value
	return values
}

func ensureMergeLen(values []graph.MergeRequest, index int) []graph.MergeRequest {
	for len(values) <= index {
		values = append(values, graph.MergeRequest{})
	}
	return values
}

func setMergeAt(values []graph.MergeRequest, index int, value graph.MergeRequest) []graph.MergeRequest {
	values = ensureMergeLen(values, index)
	values[index] = value
	return values
}

func ensureSplitLen(values []graph.SplitRequest, index int) []graph.SplitRequest {
	for len(values) <= index {
		values = append(values, graph.SplitRequest{})
	}
	return values
}

func setSplitAt(values []graph.SplitRequest, index int, value graph.SplitRequest) []graph.SplitRequest {
	values = ensureSplitLen(values, index)
	values[index] = value
	return values
}

func parquetCommitSchemaHash(codec string) string {
	fields := make([]string, 0, len(parquetCommitArrowSchema().Fields())+1)
	for _, field := range parquetCommitArrowSchema().Fields() {
		fields = append(fields, field.Name)
	}
	fields = append(fields, codec)
	return objectSchemaHash(fields)
}
