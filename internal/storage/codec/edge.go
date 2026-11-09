package codec

import (
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
)

const (
	edgeShardRowMetadata        = "metadata"
	edgeShardRowField           = "field"
	edgeShardRowFieldSource     = "field_source"
	edgeShardRowSource          = "source"
	edgeShardRowExistenceSource = "existence_source"
)

type EdgeRow struct {
	Kind        string
	Ordinal     int
	Key         string
	Value       parquetio.Value
	FieldSource graph.FieldSource
	EdgeSource  graph.EdgeSource
}

func EdgeRows(edge graph.Edge) ([]EdgeRow, error) {
	rowCount := len(edge.Fields) + len(edge.FieldSources) + len(edge.Sources)
	if edge.ExistenceSource != nil {
		rowCount++
	}
	rows := make([]EdgeRow, 0, max(1, rowCount))
	fieldNames := SortedAnyMapKeys(edge.Fields)
	for i, field := range fieldNames {
		value, err := parquetio.ValueFromAny(edge.Fields[field])
		if err != nil {
			return nil, err
		}
		rows = append(rows, EdgeRow{Kind: edgeShardRowField, Ordinal: i, Key: field, Value: value})
	}
	sourceFields := sortedFieldSourceKeys(edge.FieldSources)
	for i, field := range sourceFields {
		rows = append(rows, EdgeRow{Kind: edgeShardRowFieldSource, Ordinal: i, Key: field, FieldSource: edge.FieldSources[field]})
	}
	for i, source := range edge.Sources {
		rows = append(rows, EdgeRow{Kind: edgeShardRowSource, Ordinal: i, EdgeSource: source})
	}
	if edge.ExistenceSource != nil {
		rows = append(rows, EdgeRow{Kind: edgeShardRowExistenceSource, FieldSource: *edge.ExistenceSource})
	}
	if len(rows) == 0 {
		rows = append(rows, EdgeRow{Kind: edgeShardRowMetadata})
	}
	return rows, nil
}

func ApplyEdgeRow(edge *graph.Edge, row EdgeRow) error {
	switch row.Kind {
	case edgeShardRowMetadata:
		return nil
	case edgeShardRowField:
		if len(edge.Fields) != row.Ordinal {
			return fmt.Errorf("edge shard field ordinal mismatch")
		}
		value, err := parquetio.AnyFromValue(row.Value)
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

func DecodedEdgeCopy(edge graph.Edge) graph.Edge {
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
