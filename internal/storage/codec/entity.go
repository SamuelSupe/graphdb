package codec

import (
	"fmt"
	"sort"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
)

const (
	EntityPageRowMetadata        = "metadata"
	EntityPageRowField           = "field"
	entityPageRowFieldSource     = "field_source"
	entityPageRowIdentity        = "identity"
	EntityPageRowSource          = "source"
	entityPageRowMergedFrom      = "merged_from"
	EntityPageRowExistenceSource = "existence_source"
)

func DecodedEntityCopy(entity graph.Entity) graph.Entity {
	out := graph.CopyEntity(entity)
	if len(out.Fields) == 0 {
		out.Fields = nil
	}
	if len(out.FieldSources) == 0 {
		out.FieldSources = nil
	}
	if len(out.Identity) == 0 {
		out.Identity = nil
	}
	if len(out.Sources) == 0 {
		out.Sources = nil
	}
	if len(out.MergedFrom) == 0 {
		out.MergedFrom = nil
	}
	return out
}

type EntityRow struct {
	Kind         string
	Ordinal      int
	Key          string
	Value        parquetio.Value
	Strategy     string
	FieldSource  graph.FieldSource
	EntitySource graph.EntitySource
}

func EntityRows(entity graph.Entity) ([]EntityRow, error) {
	rowCount := len(entity.Fields) + len(entity.FieldSources) + len(entity.Identity) + len(entity.Sources) + len(entity.MergedFrom)
	if entity.ExistenceSource != nil {
		rowCount++
	}
	rows := make([]EntityRow, 0, max(1, rowCount))
	fieldNames := SortedAnyMapKeys(entity.Fields)
	for i, field := range fieldNames {
		value, err := parquetio.ValueFromAny(entity.Fields[field])
		if err != nil {
			return nil, err
		}
		rows = append(rows, EntityRow{Kind: EntityPageRowField, Ordinal: i, Key: field, Value: value})
	}
	sourceFields := sortedFieldSourceKeys(entity.FieldSources)
	for i, field := range sourceFields {
		rows = append(rows, EntityRow{Kind: entityPageRowFieldSource, Ordinal: i, Key: field, FieldSource: entity.FieldSources[field]})
	}
	identityKeys := SortedAnyMapKeys(entity.Identity)
	for i, key := range identityKeys {
		value, err := parquetio.ValueFromAny(entity.Identity[key])
		if err != nil {
			return nil, err
		}
		rows = append(rows, EntityRow{Kind: entityPageRowIdentity, Ordinal: i, Key: key, Value: value})
	}
	for i, source := range entity.Sources {
		rows = append(rows, EntityRow{Kind: EntityPageRowSource, Ordinal: i, EntitySource: source})
	}
	for i, mergedFrom := range entity.MergedFrom {
		rows = append(rows, EntityRow{
			Kind:    entityPageRowMergedFrom,
			Ordinal: i,
			Value:   parquetio.Value{Kind: parquetio.ParquetValueKindString, StringValue: mergedFrom},
		})
	}
	if entity.ExistenceSource != nil {
		rows = append(rows, EntityRow{Kind: EntityPageRowExistenceSource, FieldSource: *entity.ExistenceSource})
	}
	if len(rows) == 0 {
		rows = append(rows, EntityRow{Kind: EntityPageRowMetadata})
	}
	return rows, nil
}

func ApplyEntityRow(entity *graph.Entity, row EntityRow) error {
	switch row.Kind {
	case EntityPageRowMetadata:
		return nil
	case EntityPageRowField:
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		if entity.Fields == nil {
			entity.Fields = graph.Fields{}
		}
		entity.Fields[row.Key] = value
		if row.Strategy != "" {
			if entity.FieldWriteModes == nil {
				entity.FieldWriteModes = map[string]string{}
			}
			entity.FieldWriteModes[row.Key] = row.Strategy
		}
	case entityPageRowFieldSource:
		if entity.FieldSources == nil {
			entity.FieldSources = map[string]graph.FieldSource{}
		}
		entity.FieldSources[row.Key] = row.FieldSource
	case entityPageRowIdentity:
		value, err := parquetio.AnyFromValue(row.Value)
		if err != nil {
			return err
		}
		if entity.Identity == nil {
			entity.Identity = map[string]any{}
		}
		entity.Identity[row.Key] = value
	case EntityPageRowSource:
		if row.Ordinal < 0 {
			return fmt.Errorf("entity page source ordinal mismatch")
		}
		for len(entity.Sources) <= row.Ordinal {
			entity.Sources = append(entity.Sources, graph.EntitySource{})
		}
		entity.Sources[row.Ordinal] = row.EntitySource
	case entityPageRowMergedFrom:
		if row.Ordinal < 0 {
			return fmt.Errorf("entity page merged_from ordinal mismatch")
		}
		for len(entity.MergedFrom) <= row.Ordinal {
			entity.MergedFrom = append(entity.MergedFrom, "")
		}
		entity.MergedFrom[row.Ordinal] = row.Value.StringValue
	case EntityPageRowExistenceSource:
		source := row.FieldSource
		entity.ExistenceSource = &source
	default:
		return fmt.Errorf("unknown entity page row kind %q", row.Kind)
	}
	return nil
}

func SortedAnyMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedFieldSourceKeys(values map[string]graph.FieldSource) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
