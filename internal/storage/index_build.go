package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func buildIndexCatalog(g *graph.Graph, version int64) (IndexCatalog, error) {
	return buildIndexCatalogWithDefinitions(g, version, nil)
}

func buildIndexCatalogWithDefinitions(g *graph.Graph, version int64, definitions []IndexDefinition) (IndexCatalog, error) {
	artifacts, err := buildIndexArtifactsWithDefinitions(g, version, definitions)
	return artifacts.Catalog, err
}

func (s *TenantStore) decorateIndexCatalog(catalog *IndexCatalog, tenantID string, format string) {
	format = IndexFormatParquet
	for i := range catalog.Indexes {
		index := &catalog.Indexes[i]
		index.Format = format
		index.RowCount = index.EntryCount
		index.Codec = parquetSecondaryIndexCodec
		index.SchemaHash = parquetSecondaryIndexSchemaHash()
		if len(index.Objects) == 0 {
			index.Objects = []IndexObject{{Role: "postings", RowCount: index.EntryCount, ContentHash: index.ContentHash}}
		}
		for j := range index.Objects {
			object := &index.Objects[j]
			object.Format = IndexFormatParquet
			object.Codec = parquetSecondaryIndexCodec
			object.SchemaHash = index.SchemaHash
			switch {
			case object.Role == "postings":
				object.Key = s.parquetSecondaryIndexVersionKey(tenantID, catalog.Version, index.Kind, index.Field)
				object.RowCount = index.EntryCount
				object.ContentHash = index.ContentHash
			case strings.HasPrefix(object.Role, secondaryIndexShardRolePrefix):
				shardID, ok := secondaryIndexShardIDFromRole(object.Role)
				if ok {
					objectID := object.Key
					if objectID == "" {
						objectID = shardID
					}
					object.Key = s.parquetSecondaryIndexShardVersionKey(tenantID, catalog.Version, index.Kind, index.Field, objectID)
				}
			}
		}
	}
	edgePackIDs := edgeShardPackIDs(catalog.EdgeShards)
	for i := range catalog.EdgeShards {
		shard := &catalog.EdgeShards[i]
		shard.Format = format
		shard.RowCount = shard.EdgeCount
		packID := edgePackIDs[shard.RelationType+"\x00"+shard.Shard]
		key := s.parquetEdgeShardVersionKey(tenantID, catalog.Version, shard.RelationType, shard.Shard)
		if packID != "" && packID != shard.Shard {
			key = s.parquetEdgeShardPackVersionKey(tenantID, catalog.Version, shard.RelationType, packID)
		}
		shard.Codec = parquetEdgeShardCodec
		shard.SchemaHash = parquetEdgeShardSchemaHash()
		shard.Objects = []IndexObject{{Role: "shard", Key: key, Format: IndexFormatParquet, Codec: parquetEdgeShardCodec, RowCount: shard.EdgeCount, ContentHash: shard.ContentHash, SchemaHash: shard.SchemaHash}}
	}
	entityPackIDs := entityPagePackIDs(catalog.EntityPages, !s.WriteEntityRecords)
	for i := range catalog.EntityPages {
		page := &catalog.EntityPages[i]
		page.Format = format
		page.RowCount = page.EntityCount
		page.Codec = parquetEntityPageCodec
		page.SchemaHash = parquetEntityPageSchemaHash()
		packID := entityPackIDs["entities\x00"+page.Shard]
		key := s.parquetEntityPageVersionKey(tenantID, catalog.Version, page.Shard)
		if packID != "" && packID != page.Shard {
			key = s.parquetEntityPagePackVersionKey(tenantID, catalog.Version, packID)
		}
		page.Objects = []IndexObject{{Role: "page", Key: key, Format: IndexFormatParquet, Codec: parquetEntityPageCodec, RowCount: page.EntityCount, ContentHash: page.ContentHash, SchemaHash: page.SchemaHash}}
	}
}

func (s *TenantStore) writeSecondaryIndexes(ctx context.Context, tenantID string, g *graph.Graph, version int64) error {
	definitions, err := s.getIndexDefinitions(ctx, tenantID)
	if err != nil {
		return err
	}
	indexes, err := buildSecondaryIndexesWithDefinitions(g, version, definitions)
	if err != nil {
		return err
	}
	return s.writeParquetSecondaryIndexes(ctx, tenantID, indexes)
}

func (s *TenantStore) writeSecondaryIndexesWithFormat(ctx context.Context, tenantID string, indexes []SecondaryIndex, format string) error {
	normalized, err := normalizeIndexFormat(format)
	if err != nil {
		return err
	}
	if normalized != IndexFormatParquet {
		return fmt.Errorf("unsupported index format %q", format)
	}
	return s.writeParquetSecondaryIndexes(ctx, tenantID, indexes)
}

func buildSecondaryIndexes(g *graph.Graph, version int64) ([]SecondaryIndex, error) {
	return buildSecondaryIndexesWithDefinitions(g, version, nil)
}

func buildSecondaryIndexesWithDefinitions(g *graph.Graph, version int64, definitions []IndexDefinition) ([]SecondaryIndex, error) {
	now := time.Now().UTC()
	indexes := map[string]SecondaryIndex{}
	for _, ciType := range g.ListCITypes() {
		fields, err := g.EffectiveFields(ciType.Name)
		if err != nil {
			return nil, err
		}
		for field, spec := range fields {
			if !spec.Indexed && !spec.Unique {
				continue
			}
			index := SecondaryIndex{
				LayoutVersion: CurrentObjectLayoutVersion,
				Kind:          ciType.Name,
				Field:         field,
				Unique:        spec.Unique,
				Values:        map[string][]string{},
				Version:       version,
				UpdatedAt:     now,
				hashCanonical: true,
			}
			addEntitiesToIndex(g, &index)
			indexes[index.Kind+"\x00"+index.Field] = index
		}
	}
	for _, definition := range definitions {
		index := SecondaryIndex{
			LayoutVersion: CurrentObjectLayoutVersion,
			Kind:          definition.Kind,
			Field:         definition.Field,
			Unique:        definition.Unique,
			Values:        map[string][]string{},
			Version:       version,
			UpdatedAt:     now,
			hashCanonical: true,
		}
		if existing, ok := indexes[index.Kind+"\x00"+index.Field]; ok && existing.Unique {
			index.Unique = true
		}
		addEntitiesToIndex(g, &index)
		indexes[index.Kind+"\x00"+index.Field] = index
	}
	items := make([]SecondaryIndex, 0, len(indexes))
	for _, index := range indexes {
		items = append(items, index)
	}
	sort.Slice(items, func(i, j int) bool {
		left := items[i].Kind + "\x00" + items[i].Field
		right := items[j].Kind + "\x00" + items[j].Field
		return left < right
	})
	return items, nil
}

func addEntitiesToIndex(g *graph.Graph, index *SecondaryIndex) {
	for id, entity := range g.Entities {
		if entity.Kind != index.Kind {
			continue
		}
		value, ok := entity.Fields[index.Field]
		if !ok {
			continue
		}
		key, ok := secondaryIndexValue(value)
		if !ok {
			continue
		}
		index.Values[key] = append(index.Values[key], id)
	}
	for key := range index.Values {
		sort.Strings(index.Values[key])
	}
}

type secondaryIndexSummaryData struct {
	EntryCount     int
	DistinctValues int
	TopValues      []IndexValueStat
}

func secondaryIndexSummary(index SecondaryIndex, topN int) secondaryIndexSummaryData {
	entryCount, distinctValues := secondaryIndexCounts(index)
	return secondaryIndexSummaryData{
		EntryCount:     entryCount,
		DistinctValues: distinctValues,
		TopValues:      secondaryIndexTopValues(index, topN),
	}
}

func sortIndexCatalog(catalog *IndexCatalog) {
	sort.Slice(catalog.Indexes, func(i, j int) bool { return catalog.Indexes[i].Name < catalog.Indexes[j].Name })
	sort.Slice(catalog.EdgeShards, func(i, j int) bool {
		left := catalog.EdgeShards[i]
		right := catalog.EdgeShards[j]
		if left.RelationType == right.RelationType {
			return left.Shard < right.Shard
		}
		return left.RelationType < right.RelationType
	})
	sort.Slice(catalog.EntityPages, func(i, j int) bool {
		return catalog.EntityPages[i].Shard < catalog.EntityPages[j].Shard
	})
}

func secondaryIndexType(index SecondaryIndex) string {
	if index.Unique {
		return "unique-field"
	}
	return "secondary-field"
}

func fieldIndexType(spec graph.FieldSpec) string {
	if spec.Unique {
		return "unique-field"
	}
	return "secondary-field"
}

func storageIndexTopValues(values []graph.FieldIndexValueStat) []IndexValueStat {
	out := make([]IndexValueStat, len(values))
	for i, value := range values {
		out[i] = IndexValueStat{Value: value.Value, Count: value.Count}
	}
	return out
}

func splitShard(value string) (string, string) {
	for i, ch := range value {
		if ch == '\x00' {
			return value[:i], value[i+1:]
		}
	}
	return value, "default"
}

func canonicalIndexValue(value any) string {
	key, ok := secondaryIndexValue(value)
	if ok {
		return key
	}
	return fmt.Sprintf("v:%v", value)
}

func secondaryIndexValue(value any) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "null", true
	case string:
		return "s:" + typed, true
	case bool:
		if typed {
			return "b:true", true
		}
		return "b:false", true
	case float64:
		return fmt.Sprintf("n:%g", typed), true
	case int:
		return fmt.Sprintf("n:%d", typed), true
	case int64:
		return fmt.Sprintf("n:%d", typed), true
	case json.Number:
		value, err := typed.Float64()
		if err != nil {
			return "", false
		}
		return fmt.Sprintf("n:%g", value), true
	default:
		return "", false
	}
}
