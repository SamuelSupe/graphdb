package storage

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const sourcePolicyCodecParquet = "source-policy-arrow-parquet-v1"

const (
	parquetSourcePolicyColumnTenantID = iota
	parquetSourcePolicyColumnDefaultPriority
	parquetSourcePolicyColumnSourceCount
	parquetSourcePolicyColumnContentHash
	parquetSourcePolicyColumnSourceName
	parquetSourcePolicyColumnSourcePriority
	parquetSourcePolicyColumnSourceDescription
	parquetSourcePolicyColumnFieldAliasCount
	parquetSourcePolicyColumnRowKind
	parquetSourcePolicyColumnAliasSource
	parquetSourcePolicyColumnAliasKind
	parquetSourcePolicyColumnAliasName
	parquetSourcePolicyColumnAliasTarget
	parquetSourcePolicyColumnFieldPriorityCount
	parquetSourcePolicyColumnPrioritySource
	parquetSourcePolicyColumnPriorityKind
	parquetSourcePolicyColumnPriorityField
	parquetSourcePolicyColumnPriorityValue
)

const (
	sourcePolicyRowSource        = "source"
	sourcePolicyRowFieldAlias    = "field_alias"
	sourcePolicyRowFieldPriority = "field_priority"
)

func marshalParquetSourcePolicy(ctx context.Context, record sourcePolicyRecord) ([]byte, error) {
	normalized, err := normalizeSourcePolicyRecord(record)
	if err != nil {
		return nil, err
	}
	hash, err := sourcePolicyContentHash(normalized)
	if err != nil {
		return nil, err
	}
	aliasPairs := sourcePolicyAliasPairs(normalized)
	priorityPairs := sourcePolicyPriorityPairs(normalized)

	schema := parquetSourcePolicyArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendCommon := func(rowKind string) {
		builder.Field(parquetSourcePolicyColumnTenantID).(*array.StringBuilder).Append(normalized.TenantID)
		builder.Field(parquetSourcePolicyColumnDefaultPriority).(*array.Int64Builder).Append(int64(normalized.DefaultPriority))
		builder.Field(parquetSourcePolicyColumnSourceCount).(*array.Int64Builder).Append(int64(len(normalized.Sources)))
		builder.Field(parquetSourcePolicyColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetSourcePolicyColumnFieldAliasCount).(*array.Int64Builder).Append(int64(len(aliasPairs)))
		builder.Field(parquetSourcePolicyColumnFieldPriorityCount).(*array.Int64Builder).Append(int64(len(priorityPairs)))
		builder.Field(parquetSourcePolicyColumnRowKind).(*array.StringBuilder).Append(rowKind)
	}
	appendSourcePolicyRow := func(source graph.SourcePolicyItem) {
		appendCommon(sourcePolicyRowSource)
		builder.Field(parquetSourcePolicyColumnSourceName).(*array.StringBuilder).Append(source.Name)
		builder.Field(parquetSourcePolicyColumnSourcePriority).(*array.Int64Builder).Append(int64(source.Priority))
		builder.Field(parquetSourcePolicyColumnSourceDescription).(*array.StringBuilder).Append(source.Description)
		builder.Field(parquetSourcePolicyColumnAliasSource).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasKind).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasName).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasTarget).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPrioritySource).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPriorityKind).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPriorityField).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPriorityValue).(*array.Int64Builder).Append(0)
	}
	appendFieldAliasRow := func(alias sourcePolicyAliasPair) {
		appendCommon(sourcePolicyRowFieldAlias)
		builder.Field(parquetSourcePolicyColumnSourceName).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnSourcePriority).(*array.Int64Builder).Append(0)
		builder.Field(parquetSourcePolicyColumnSourceDescription).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasSource).(*array.StringBuilder).Append(alias.Source)
		builder.Field(parquetSourcePolicyColumnAliasKind).(*array.StringBuilder).Append(alias.Kind)
		builder.Field(parquetSourcePolicyColumnAliasName).(*array.StringBuilder).Append(alias.Alias)
		builder.Field(parquetSourcePolicyColumnAliasTarget).(*array.StringBuilder).Append(alias.Target)
		builder.Field(parquetSourcePolicyColumnPrioritySource).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPriorityKind).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPriorityField).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPriorityValue).(*array.Int64Builder).Append(0)
	}
	appendFieldPriorityRow := func(priority sourcePolicyPriorityPair) {
		appendCommon(sourcePolicyRowFieldPriority)
		builder.Field(parquetSourcePolicyColumnSourceName).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnSourcePriority).(*array.Int64Builder).Append(0)
		builder.Field(parquetSourcePolicyColumnSourceDescription).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasSource).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasKind).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasName).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnAliasTarget).(*array.StringBuilder).Append("")
		builder.Field(parquetSourcePolicyColumnPrioritySource).(*array.StringBuilder).Append(priority.Source)
		builder.Field(parquetSourcePolicyColumnPriorityKind).(*array.StringBuilder).Append(priority.Kind)
		builder.Field(parquetSourcePolicyColumnPriorityField).(*array.StringBuilder).Append(priority.Field)
		builder.Field(parquetSourcePolicyColumnPriorityValue).(*array.Int64Builder).Append(int64(priority.Priority))
	}
	if len(normalized.Sources) == 0 && len(aliasPairs) == 0 && len(priorityPairs) == 0 {
		appendSourcePolicyRow(graph.SourcePolicyItem{})
	} else {
		for _, source := range normalized.Sources {
			appendSourcePolicyRow(source)
		}
		for _, alias := range aliasPairs {
			appendFieldAliasRow(alias)
		}
		for _, priority := range priorityPairs {
			appendFieldPriorityRow(priority)
		}
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, 1, writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetSourcePolicy(ctx context.Context, data []byte) (sourcePolicyRecord, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return sourcePolicyRecord{}, fmt.Errorf("parquet source policy is empty")
	}
	if table.NumCols() < int64(parquetSourcePolicyColumnSourceDescription+1) {
		return sourcePolicyRecord{}, fmt.Errorf("parquet source policy has %d columns, want at least %d", table.NumCols(), parquetSourcePolicyColumnSourceDescription+1)
	}
	if table.NumCols() < int64(parquetSourcePolicyColumnAliasTarget+1) {
		return decodeParquetSourcePolicyV1Table(table)
	}
	hasFieldPriorities := table.NumCols() >= int64(parquetSourcePolicyColumnPriorityValue+1)

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()
	record := sourcePolicyRecord{}
	var sourceCount int64
	var fieldAliasCount int64
	var fieldPriorityCount int64
	var expectedHash string
	rows := 0
	aliasRules := map[string]graph.FieldAliasRule{}
	priorityRules := map[string]graph.FieldPriorityRule{}
	for reader.Next() {
		batch := reader.RecordBatch()
		tenantColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnTenantID, "tenant_id")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		defaultPriorityColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnDefaultPriority, "default_priority")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceCountColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnSourceCount, "source_count")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		hashColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnContentHash, "content_hash")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceNameColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnSourceName, "source_name")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourcePriorityColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnSourcePriority, "source_priority")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceDescriptionColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnSourceDescription, "source_description")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		fieldAliasCountColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnFieldAliasCount, "field_alias_count")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		rowKindColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnRowKind, "row_kind")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		aliasSourceColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnAliasSource, "alias_source")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		aliasKindColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnAliasKind, "alias_kind")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		aliasNameColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnAliasName, "alias_name")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		aliasTargetColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnAliasTarget, "alias_target")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		var fieldPriorityCountColumn *array.Int64
		var prioritySourceColumn *array.String
		var priorityKindColumn *array.String
		var priorityFieldColumn *array.String
		var priorityValueColumn *array.Int64
		if hasFieldPriorities {
			fieldPriorityCountColumn, err = parquetInt64Column(batch, parquetSourcePolicyColumnFieldPriorityCount, "field_priority_count")
			if err != nil {
				return sourcePolicyRecord{}, err
			}
			prioritySourceColumn, err = parquetStringColumn(batch, parquetSourcePolicyColumnPrioritySource, "priority_source")
			if err != nil {
				return sourcePolicyRecord{}, err
			}
			priorityKindColumn, err = parquetStringColumn(batch, parquetSourcePolicyColumnPriorityKind, "priority_kind")
			if err != nil {
				return sourcePolicyRecord{}, err
			}
			priorityFieldColumn, err = parquetStringColumn(batch, parquetSourcePolicyColumnPriorityField, "priority_field")
			if err != nil {
				return sourcePolicyRecord{}, err
			}
			priorityValueColumn, err = parquetInt64Column(batch, parquetSourcePolicyColumnPriorityValue, "priority_value")
			if err != nil {
				return sourcePolicyRecord{}, err
			}
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			if rows == 0 {
				record.TenantID = tenantColumn.Value(i)
				record.DefaultPriority = int(defaultPriorityColumn.Value(i))
				sourceCount = sourceCountColumn.Value(i)
				fieldAliasCount = fieldAliasCountColumn.Value(i)
				if hasFieldPriorities {
					fieldPriorityCount = fieldPriorityCountColumn.Value(i)
				}
				expectedHash = hashColumn.Value(i)
			}
			if record.TenantID != tenantColumn.Value(i) || int64(record.DefaultPriority) != defaultPriorityColumn.Value(i) || sourceCount != sourceCountColumn.Value(i) ||
				fieldAliasCount != fieldAliasCountColumn.Value(i) || expectedHash != hashColumn.Value(i) ||
				(hasFieldPriorities && fieldPriorityCount != fieldPriorityCountColumn.Value(i)) {
				return sourcePolicyRecord{}, fmt.Errorf("source policy identity mismatch")
			}
			switch rowKindColumn.Value(i) {
			case "", sourcePolicyRowSource:
				name := sourceNameColumn.Value(i)
				if name != "" {
					record.Sources = append(record.Sources, graph.SourcePolicyItem{
						Name:        name,
						Priority:    int(sourcePriorityColumn.Value(i)),
						Description: sourceDescriptionColumn.Value(i),
					})
				}
			case sourcePolicyRowFieldAlias:
				source := aliasSourceColumn.Value(i)
				kind := aliasKindColumn.Value(i)
				alias := aliasNameColumn.Value(i)
				target := aliasTargetColumn.Value(i)
				if source != "" && alias != "" && target != "" {
					key := source + "\x00" + kind
					rule := aliasRules[key]
					if rule.Source == "" {
						rule.Source = source
						rule.Kind = kind
						rule.Aliases = map[string]string{}
					}
					rule.Aliases[alias] = target
					aliasRules[key] = rule
				}
			case sourcePolicyRowFieldPriority:
				if !hasFieldPriorities {
					return sourcePolicyRecord{}, fmt.Errorf("source policy field priority row requires newer schema")
				}
				source := prioritySourceColumn.Value(i)
				kind := priorityKindColumn.Value(i)
				field := priorityFieldColumn.Value(i)
				priority := int(priorityValueColumn.Value(i))
				if source != "" && field != "" {
					key := source + "\x00" + kind
					rule := priorityRules[key]
					if rule.Source == "" {
						rule.Source = source
						rule.Kind = kind
						rule.Fields = map[string]int{}
					}
					rule.Fields[field] = priority
					priorityRules[key] = rule
				}
			default:
				return sourcePolicyRecord{}, fmt.Errorf("unknown source policy row kind %q", rowKindColumn.Value(i))
			}
			rows++
		}
	}
	for _, key := range sortedStringKeys(aliasRules) {
		record.FieldAliases = append(record.FieldAliases, aliasRules[key])
	}
	for _, key := range sortedStringKeys(priorityRules) {
		record.FieldPriorities = append(record.FieldPriorities, priorityRules[key])
	}

	if int64(len(record.Sources)) != sourceCount {
		return sourcePolicyRecord{}, fmt.Errorf("source policy source count mismatch")
	}
	if int64(len(sourcePolicyAliasPairs(record))) != fieldAliasCount {
		return sourcePolicyRecord{}, fmt.Errorf("source policy field alias count mismatch")
	}
	if hasFieldPriorities && int64(len(sourcePolicyPriorityPairs(record))) != fieldPriorityCount {
		return sourcePolicyRecord{}, fmt.Errorf("source policy field priority count mismatch")
	}
	hash, err := sourcePolicyContentHash(record)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	if !hasFieldPriorities {
		hash, err = sourcePolicyContentHashV2(record)
		if err != nil {
			return sourcePolicyRecord{}, err
		}
	}
	if expectedHash == "" || expectedHash != hash {
		return sourcePolicyRecord{}, fmt.Errorf("source policy content hash mismatch")
	}
	return normalizeSourcePolicyRecord(record)
}

func decodeParquetSourcePolicyV1Table(table arrow.Table) (sourcePolicyRecord, error) {
	reader := array.NewTableReader(table, 1024)
	defer reader.Release()
	record := sourcePolicyRecord{}
	var sourceCount int64
	var expectedHash string
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		tenantColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnTenantID, "tenant_id")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		defaultPriorityColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnDefaultPriority, "default_priority")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceCountColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnSourceCount, "source_count")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		hashColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnContentHash, "content_hash")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceNameColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnSourceName, "source_name")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourcePriorityColumn, err := parquetInt64Column(batch, parquetSourcePolicyColumnSourcePriority, "source_priority")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		sourceDescriptionColumn, err := parquetStringColumn(batch, parquetSourcePolicyColumnSourceDescription, "source_description")
		if err != nil {
			return sourcePolicyRecord{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			if rows == 0 {
				record.TenantID = tenantColumn.Value(i)
				record.DefaultPriority = int(defaultPriorityColumn.Value(i))
				sourceCount = sourceCountColumn.Value(i)
				expectedHash = hashColumn.Value(i)
			}
			if record.TenantID != tenantColumn.Value(i) || int64(record.DefaultPriority) != defaultPriorityColumn.Value(i) || sourceCount != sourceCountColumn.Value(i) || expectedHash != hashColumn.Value(i) {
				return sourcePolicyRecord{}, fmt.Errorf("source policy identity mismatch")
			}
			name := sourceNameColumn.Value(i)
			if name != "" {
				record.Sources = append(record.Sources, graph.SourcePolicyItem{
					Name:        name,
					Priority:    int(sourcePriorityColumn.Value(i)),
					Description: sourceDescriptionColumn.Value(i),
				})
			}
			rows++
		}
	}
	if int64(len(record.Sources)) != sourceCount {
		return sourcePolicyRecord{}, fmt.Errorf("source policy source count mismatch")
	}
	hash, err := sourcePolicyContentHashV1(record)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return sourcePolicyRecord{}, fmt.Errorf("source policy content hash mismatch")
	}
	return normalizeSourcePolicyRecord(record)
}

func parquetSourcePolicyArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "default_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "source_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "source_description", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_alias_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "alias_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "alias_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "alias_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "alias_target", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_priority_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "priority_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "priority_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "priority_field", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "priority_value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}

func normalizeSourcePolicyRecord(record sourcePolicyRecord) (sourcePolicyRecord, error) {
	normalized, err := graph.NormalizeSourcePolicy(record.SourcePolicy)
	if err != nil {
		return sourcePolicyRecord{}, err
	}
	record.SourcePolicy = normalized
	return record, nil
}

func sourcePolicyContentHash(record sourcePolicyRecord) (string, error) {
	normalized, err := normalizeSourcePolicyRecord(record)
	if err != nil {
		return "", err
	}
	parts := []string{
		normalized.TenantID,
		formatInt64ForHash(int64(normalized.DefaultPriority)),
		formatInt64ForHash(int64(len(normalized.Sources))),
		formatInt64ForHash(int64(len(sourcePolicyAliasPairs(normalized)))),
		formatInt64ForHash(int64(len(sourcePolicyPriorityPairs(normalized)))),
	}
	for _, source := range normalized.Sources {
		parts = append(parts, source.Name, formatInt64ForHash(int64(source.Priority)), source.Description)
	}
	for _, alias := range sourcePolicyAliasPairs(normalized) {
		parts = append(parts, alias.Source, alias.Kind, alias.Alias, alias.Target)
	}
	for _, priority := range sourcePolicyPriorityPairs(normalized) {
		parts = append(parts, priority.Source, priority.Kind, priority.Field, formatInt64ForHash(int64(priority.Priority)))
	}
	return parquetScalarContentHash(parts...), nil
}

func sourcePolicyContentHashV2(record sourcePolicyRecord) (string, error) {
	normalized, err := normalizeSourcePolicyRecord(record)
	if err != nil {
		return "", err
	}
	parts := []string{
		normalized.TenantID,
		formatInt64ForHash(int64(normalized.DefaultPriority)),
		formatInt64ForHash(int64(len(normalized.Sources))),
		formatInt64ForHash(int64(len(sourcePolicyAliasPairs(normalized)))),
	}
	for _, source := range normalized.Sources {
		parts = append(parts, source.Name, formatInt64ForHash(int64(source.Priority)), source.Description)
	}
	for _, alias := range sourcePolicyAliasPairs(normalized) {
		parts = append(parts, alias.Source, alias.Kind, alias.Alias, alias.Target)
	}
	return parquetScalarContentHash(parts...), nil
}

func sourcePolicyContentHashV1(record sourcePolicyRecord) (string, error) {
	normalized, err := normalizeSourcePolicyRecord(record)
	if err != nil {
		return "", err
	}
	parts := []string{
		normalized.TenantID,
		formatInt64ForHash(int64(normalized.DefaultPriority)),
		formatInt64ForHash(int64(len(normalized.Sources))),
	}
	for _, source := range normalized.Sources {
		parts = append(parts, source.Name, formatInt64ForHash(int64(source.Priority)), source.Description)
	}
	return parquetScalarContentHash(parts...), nil
}

type sourcePolicyAliasPair struct {
	Source string
	Kind   string
	Alias  string
	Target string
}

func sourcePolicyAliasPairs(record sourcePolicyRecord) []sourcePolicyAliasPair {
	pairs := make([]sourcePolicyAliasPair, 0)
	for _, rule := range record.FieldAliases {
		for alias, target := range rule.Aliases {
			pairs = append(pairs, sourcePolicyAliasPair{
				Source: rule.Source,
				Kind:   rule.Kind,
				Alias:  alias,
				Target: target,
			})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Source != pairs[j].Source {
			return pairs[i].Source < pairs[j].Source
		}
		if pairs[i].Kind != pairs[j].Kind {
			return pairs[i].Kind < pairs[j].Kind
		}
		return pairs[i].Alias < pairs[j].Alias
	})
	return pairs
}

type sourcePolicyPriorityPair struct {
	Source   string
	Kind     string
	Field    string
	Priority int
}

func sourcePolicyPriorityPairs(record sourcePolicyRecord) []sourcePolicyPriorityPair {
	pairs := make([]sourcePolicyPriorityPair, 0)
	for _, rule := range record.FieldPriorities {
		for field, priority := range rule.Fields {
			pairs = append(pairs, sourcePolicyPriorityPair{
				Source:   rule.Source,
				Kind:     rule.Kind,
				Field:    field,
				Priority: priority,
			})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Source != pairs[j].Source {
			return pairs[i].Source < pairs[j].Source
		}
		if pairs[i].Kind != pairs[j].Kind {
			return pairs[i].Kind < pairs[j].Kind
		}
		return pairs[i].Field < pairs[j].Field
	})
	return pairs
}

func sortedStringKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
