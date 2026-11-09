package codec

import (
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	ParquetCommitColumnEdgeSourceObservedAt
)

const (
	CommitRowMetadata              = "metadata"
	CommitRowUpsertCIType          = "upsert_ci_type"
	CommitRowCITypeExtends         = "ci_type_extends"
	CommitRowCITypeField           = "ci_type_field"
	CommitRowCITypeFieldEnum       = "ci_type_field_enum"
	CommitRowCITypeFieldDefault    = "ci_type_field_default"
	CommitRowCITypeIdentity        = "ci_type_identity"
	CommitRowCITypeIdentityField   = "ci_type_identity_field"
	commitRowDeleteCIType          = "delete_ci_type"
	CommitRowUpsertRelationType    = "upsert_relation_type"
	CommitRowRelationFromKind      = "relation_from_kind"
	CommitRowRelationToKind        = "relation_to_kind"
	commitRowDeleteRelationType    = "delete_relation_type"
	CommitRowUpsertEntity          = "upsert_entity"
	CommitRowDeleteEntity          = "delete_entity"
	commitRowDeleteEntityRequest   = "delete_entity_request"
	commitRowMarkSourceStale       = "mark_source_stale"
	commitRowStaleObservedExternal = "stale_observed_external_id"
	CommitRowUpsertEdge            = "upsert_edge"
	commitRowDeleteEdge            = "delete_edge"
	commitRowDeleteEdgeRequest     = "delete_edge_request"
	commitRowMergeEntity           = "merge_entity"
	commitRowMergeSource           = "merge_source"
	commitRowSplitRequest          = "split_request"
	commitRowSplitEntity           = "split_entity"
)

type CommitTableItem struct {
	Key         string
	Commit      graph.Commit
	ContentHash string
}

type ParquetCommitRow struct {
	Kind            string
	ComponentKind   string
	ParentOrdinal   int
	ChildOrdinal    int
	NestedOrdinal   int
	EntryKey        string
	Value           parquetio.Value
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

func AppendParquetCommitRow(builder *array.RecordBuilder, tenantID string, key string, commit graph.Commit, hash string, row ParquetCommitRow) {
	builder.Field(parquetCommitColumnTenantID).(*array.StringBuilder).Append(tenantID)
	builder.Field(parquetCommitColumnSegmentKey).(*array.StringBuilder).Append(key)
	builder.Field(parquetCommitColumnCommitTenantID).(*array.StringBuilder).Append(commit.TenantID)
	builder.Field(parquetCommitColumnCommitID).(*array.StringBuilder).Append(commit.ID)
	builder.Field(parquetCommitColumnVersion).(*array.Int64Builder).Append(commit.Version)
	builder.Field(parquetCommitColumnCreatedAt).(*array.StringBuilder).Append(parquetio.FormatTime(commit.CreatedAt))
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
	builder.Field(parquetCommitColumnCreatedAtValue).(*array.StringBuilder).Append(parquetio.FormatTime(row.CreatedAtValue))
	builder.Field(parquetCommitColumnUpdatedAtValue).(*array.StringBuilder).Append(parquetio.FormatTime(row.UpdatedAtValue))
	builder.Field(parquetCommitColumnFieldSourceSource).(*array.StringBuilder).Append(row.FieldSource.Source)
	builder.Field(parquetCommitColumnFieldSourcePriority).(*array.Int64Builder).Append(int64(row.FieldSource.Priority))
	builder.Field(parquetCommitColumnFieldSourceConfidence).(*array.Float64Builder).Append(row.FieldSource.Confidence)
	builder.Field(parquetCommitColumnFieldSourceVersion).(*array.Int64Builder).Append(row.FieldSource.Version)
	builder.Field(parquetCommitColumnFieldSourceUpdatedAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.FieldSource.UpdatedAt))
	builder.Field(parquetCommitColumnEntitySourceSource).(*array.StringBuilder).Append(row.EntitySource.Source)
	builder.Field(parquetCommitColumnEntitySourceExternalID).(*array.StringBuilder).Append(row.EntitySource.ExternalID)
	builder.Field(parquetCommitColumnEntitySourceConfidence).(*array.Float64Builder).Append(row.EntitySource.Confidence)
	builder.Field(parquetCommitColumnEntitySourcePriority).(*array.Int64Builder).Append(int64(row.EntitySource.Priority))
	builder.Field(parquetCommitColumnEntitySourceObservedAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.EntitySource.ObservedAt))
	builder.Field(parquetCommitColumnEntitySourceStale).(*array.BooleanBuilder).Append(row.EntitySource.Stale)
	builder.Field(parquetCommitColumnEntitySourceStaleAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.EntitySource.StaleAt))
	builder.Field(parquetCommitColumnEdgeSourceSource).(*array.StringBuilder).Append(row.EdgeSource.Source)
	builder.Field(parquetCommitColumnEdgeSourceExternalID).(*array.StringBuilder).Append(row.EdgeSource.ExternalID)
	builder.Field(parquetCommitColumnEdgeSourceEdgeID).(*array.StringBuilder).Append(row.EdgeSource.EdgeID)
	builder.Field(parquetCommitColumnEdgeSourceConfidence).(*array.Float64Builder).Append(row.EdgeSource.Confidence)
	builder.Field(parquetCommitColumnEdgeSourcePriority).(*array.Int64Builder).Append(int64(row.EdgeSource.Priority))
	builder.Field(ParquetCommitColumnEdgeSourceObservedAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.EdgeSource.ObservedAt))
}

func ParquetCommitArrowSchema() *arrow.Schema {
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

func ParquetCommitColumns(batch arrow.RecordBatch) (parquetCommitColumnSet, error) {
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
		{&columns.edgeSourceObservedAt, ParquetCommitColumnEdgeSourceObservedAt, "edge_source_observed_at"},
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
		if *item.target, err = parquetio.Int64Column(batch, item.index, item.name); err != nil {
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
		if *item.target, err = parquetio.BoolColumn(batch, item.index, item.name); err != nil {
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
		if *item.target, err = parquetio.Float64Column(batch, item.index, item.name); err != nil {
			return columns, err
		}
	}
	return columns, nil
}

func ParquetCommitRowFromColumns(columns parquetCommitColumnSet, row int) ParquetCommitRow {
	return ParquetCommitRow{
		Kind:          columns.rowKind.Value(row),
		ComponentKind: columns.componentKind.Value(row),
		ParentOrdinal: int(columns.parentOrdinal.Value(row)),
		ChildOrdinal:  int(columns.childOrdinal.Value(row)),
		NestedOrdinal: int(columns.nestedOrdinal.Value(row)),
		EntryKey:      columns.entryKey.Value(row),
		Value: parquetio.Value{
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
		CreatedAtValue:  parquetio.ParseTime(columns.createdAtValue.Value(row)),
		UpdatedAtValue:  parquetio.ParseTime(columns.updatedAtValue.Value(row)),
		FieldSource: graph.FieldSource{
			Source:     columns.fieldSourceSource.Value(row),
			Priority:   int(columns.fieldSourcePriority.Value(row)),
			Confidence: columns.fieldSourceConfidence.Value(row),
			Version:    columns.fieldSourceVersion.Value(row),
			UpdatedAt:  parquetio.ParseTime(columns.fieldSourceUpdatedAt.Value(row)),
		},
		EntitySource: graph.EntitySource{
			Source:     columns.entitySourceSource.Value(row),
			ExternalID: columns.entitySourceExternalID.Value(row),
			Confidence: columns.entitySourceConfidence.Value(row),
			Priority:   int(columns.entitySourcePriority.Value(row)),
			ObservedAt: parquetio.ParseTime(columns.entitySourceObservedAt.Value(row)),
			Stale:      columns.entitySourceStale.Value(row),
			StaleAt:    parquetio.ParseTime(columns.entitySourceStaleAt.Value(row)),
		},
		EdgeSource: graph.EdgeSource{
			Source:     columns.edgeSourceSource.Value(row),
			ExternalID: columns.edgeSourceExternalID.Value(row),
			EdgeID:     columns.edgeSourceEdgeID.Value(row),
			Confidence: columns.edgeSourceConfidence.Value(row),
			Priority:   int(columns.edgeSourcePriority.Value(row)),
			ObservedAt: parquetio.ParseTime(columns.edgeSourceObservedAt.Value(row)),
		},
	}
}
