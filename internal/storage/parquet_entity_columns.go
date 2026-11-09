package storage

import (
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	parquetEntityColumnTenantID = iota
	parquetEntityColumnShard
	parquetEntityColumnPageVersion
	parquetEntityColumnPageUpdatedAt
	parquetEntityColumnID
	parquetEntityColumnKind
	parquetEntityColumnSource
	parquetEntityColumnExternalID
	parquetEntityColumnEntityVersion
	parquetEntityColumnEntityCreatedAt
	parquetEntityColumnEntityUpdatedAt
	parquetEntityColumnConfidence
	parquetEntityColumnSourceRank
	parquetEntityColumnSplitFrom
	parquetEntityColumnRowKind
	parquetEntityColumnOrdinal
	parquetEntityColumnEntryKey
	parquetEntityColumnValueKind
	parquetEntityColumnStringValue
	parquetEntityColumnBoolValue
	parquetEntityColumnFloatValue
	parquetEntityColumnFieldSourceSource
	parquetEntityColumnFieldSourcePriority
	parquetEntityColumnFieldSourceConfidence
	parquetEntityColumnFieldSourceVersion
	parquetEntityColumnFieldSourceUpdatedAt
	parquetEntityColumnEntitySourceSource
	parquetEntityColumnEntitySourceExternalID
	parquetEntityColumnEntitySourceConfidence
	parquetEntityColumnEntitySourcePriority
	parquetEntityColumnEntitySourceObservedAt
	parquetEntityColumnEntitySourceStale
	parquetEntityColumnEntitySourceStaleAt
)

const (
	parquetEntityPageRowGroupSize  int64 = 4096
	parquetEntityPageReadBatchSize int64 = 128
)

type parquetEntityPageColumnSet struct {
	tenantID               *array.String
	shard                  *array.String
	pageVersion            *array.Int64
	pageUpdatedAt          *array.String
	id                     *array.String
	kind                   *array.String
	source                 *array.String
	externalID             *array.String
	entityVersion          *array.Int64
	entityCreatedAt        *array.String
	entityUpdatedAt        *array.String
	confidence             *array.Float64
	sourceRank             *array.Int64
	splitFrom              *array.String
	rowKind                *array.String
	ordinal                *array.Int64
	entryKey               *array.String
	valueKind              *array.String
	stringValue            *array.String
	boolValue              *array.Boolean
	floatValue             *array.Float64
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
}

func parquetEntityPageColumns(record arrow.RecordBatch) (parquetEntityPageColumnSet, error) {
	var columns parquetEntityPageColumnSet
	var err error
	if columns.tenantID, err = parquetio.StringColumn(record, parquetEntityColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.shard, err = parquetio.StringColumn(record, parquetEntityColumnShard, "shard"); err != nil {
		return columns, err
	}
	if columns.pageVersion, err = parquetio.Int64Column(record, parquetEntityColumnPageVersion, "page_version"); err != nil {
		return columns, err
	}
	if columns.pageUpdatedAt, err = parquetio.StringColumn(record, parquetEntityColumnPageUpdatedAt, "page_updated_at"); err != nil {
		return columns, err
	}
	if columns.id, err = parquetio.StringColumn(record, parquetEntityColumnID, "entity_id"); err != nil {
		return columns, err
	}
	if columns.kind, err = parquetio.StringColumn(record, parquetEntityColumnKind, "kind"); err != nil {
		return columns, err
	}
	if columns.source, err = parquetio.StringColumn(record, parquetEntityColumnSource, "source"); err != nil {
		return columns, err
	}
	if columns.externalID, err = parquetio.StringColumn(record, parquetEntityColumnExternalID, "external_id"); err != nil {
		return columns, err
	}
	if columns.entityVersion, err = parquetio.Int64Column(record, parquetEntityColumnEntityVersion, "entity_version"); err != nil {
		return columns, err
	}
	if columns.entityCreatedAt, err = parquetio.StringColumn(record, parquetEntityColumnEntityCreatedAt, "entity_created_at"); err != nil {
		return columns, err
	}
	if columns.entityUpdatedAt, err = parquetio.StringColumn(record, parquetEntityColumnEntityUpdatedAt, "entity_updated_at"); err != nil {
		return columns, err
	}
	if columns.confidence, err = parquetio.Float64Column(record, parquetEntityColumnConfidence, "confidence"); err != nil {
		return columns, err
	}
	if columns.sourceRank, err = parquetio.Int64Column(record, parquetEntityColumnSourceRank, "source_priority"); err != nil {
		return columns, err
	}
	if columns.splitFrom, err = parquetio.StringColumn(record, parquetEntityColumnSplitFrom, "split_from"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetio.StringColumn(record, parquetEntityColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetio.Int64Column(record, parquetEntityColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.entryKey, err = parquetio.StringColumn(record, parquetEntityColumnEntryKey, "entry_key"); err != nil {
		return columns, err
	}
	if columns.valueKind, err = parquetio.StringColumn(record, parquetEntityColumnValueKind, "value_kind"); err != nil {
		return columns, err
	}
	if columns.stringValue, err = parquetio.StringColumn(record, parquetEntityColumnStringValue, "string_value"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetio.BoolColumn(record, parquetEntityColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetio.Float64Column(record, parquetEntityColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	if columns.fieldSourceSource, err = parquetio.StringColumn(record, parquetEntityColumnFieldSourceSource, "field_source_source"); err != nil {
		return columns, err
	}
	if columns.fieldSourcePriority, err = parquetio.Int64Column(record, parquetEntityColumnFieldSourcePriority, "field_source_priority"); err != nil {
		return columns, err
	}
	if columns.fieldSourceConfidence, err = parquetio.Float64Column(record, parquetEntityColumnFieldSourceConfidence, "field_source_confidence"); err != nil {
		return columns, err
	}
	if columns.fieldSourceVersion, err = parquetio.Int64Column(record, parquetEntityColumnFieldSourceVersion, "field_source_version"); err != nil {
		return columns, err
	}
	if columns.fieldSourceUpdatedAt, err = parquetio.StringColumn(record, parquetEntityColumnFieldSourceUpdatedAt, "field_source_updated_at"); err != nil {
		return columns, err
	}
	if columns.entitySourceSource, err = parquetio.StringColumn(record, parquetEntityColumnEntitySourceSource, "entity_source_source"); err != nil {
		return columns, err
	}
	if columns.entitySourceExternalID, err = parquetio.StringColumn(record, parquetEntityColumnEntitySourceExternalID, "entity_source_external_id"); err != nil {
		return columns, err
	}
	if columns.entitySourceConfidence, err = parquetio.Float64Column(record, parquetEntityColumnEntitySourceConfidence, "entity_source_confidence"); err != nil {
		return columns, err
	}
	if columns.entitySourcePriority, err = parquetio.Int64Column(record, parquetEntityColumnEntitySourcePriority, "entity_source_priority"); err != nil {
		return columns, err
	}
	if columns.entitySourceObservedAt, err = parquetio.StringColumn(record, parquetEntityColumnEntitySourceObservedAt, "entity_source_observed_at"); err != nil {
		return columns, err
	}
	if columns.entitySourceStale, err = parquetio.BoolColumn(record, parquetEntityColumnEntitySourceStale, "entity_source_stale"); err != nil {
		return columns, err
	}
	if columns.entitySourceStaleAt, err = parquetio.StringColumn(record, parquetEntityColumnEntitySourceStaleAt, "entity_source_stale_at"); err != nil {
		return columns, err
	}
	return columns, nil
}

func entityPageRowFromColumns(columns parquetEntityPageColumnSet, row int) entityPageRow {
	return entityPageRow{
		Kind:    columns.rowKind.Value(row),
		Ordinal: int(columns.ordinal.Value(row)),
		Key:     strings.Clone(columns.entryKey.Value(row)),
		Value: parquetValue{
			Kind:        columns.valueKind.Value(row),
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
		EntitySource: graph.EntitySource{
			Source:     strings.Clone(columns.entitySourceSource.Value(row)),
			ExternalID: strings.Clone(columns.entitySourceExternalID.Value(row)),
			Confidence: columns.entitySourceConfidence.Value(row),
			Priority:   int(columns.entitySourcePriority.Value(row)),
			ObservedAt: parquetio.ParseTime(columns.entitySourceObservedAt.Value(row)),
			Stale:      columns.entitySourceStale.Value(row),
			StaleAt:    parquetio.ParseTime(columns.entitySourceStaleAt.Value(row)),
		},
	}
}

func parquetEntityPageArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "shard", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "page_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "page_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "split_from", Type: arrow.BinaryTypes.String, Nullable: false},
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
		{Name: "entity_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "entity_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_source_observed_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_stale", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "entity_source_stale_at", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetEntityPageSchemaHash() string {
	return objectSchemaHash([]string{
		"tenant_id",
		"shard",
		"page_version",
		"page_updated_at",
		"entity_id",
		"kind",
		"source",
		"external_id",
		"entity_version",
		"entity_created_at",
		"entity_updated_at",
		"confidence",
		"source_priority",
		"split_from",
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
		"entity_source_source",
		"entity_source_external_id",
		"entity_source_confidence",
		"entity_source_priority",
		"entity_source_observed_at",
		"entity_source_stale",
		"entity_source_stale_at",
		parquetEntityPageCodec,
	})
}
