package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func secondaryIndexObjectSetContentHash(objects []IndexObject) string {
	type digest struct {
		Role        string `json:"role"`
		Key         string `json:"key"`
		RowCount    int    `json:"row_count"`
		ContentHash string `json:"content_hash"`
		SchemaHash  string `json:"schema_hash"`
	}
	items := make([]digest, 0, len(objects))
	for _, object := range objects {
		items = append(items, digest{
			Role: object.Role, Key: object.Key, RowCount: object.RowCount,
			ContentHash: object.ContentHash, SchemaHash: object.SchemaHash,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Role == items[j].Role {
			return items[i].Key < items[j].Key
		}
		return items[i].Role < items[j].Role
	})
	return indexContentHash(items)
}

func secondaryIndexSpecContentHashMatches(index SecondaryIndex, spec IndexSpec) bool {
	return spec.ContentHash != "" &&
		(secondaryIndexContentHash(index) == spec.ContentHash || secondaryIndexObjectSetContentHash(spec.Objects) == spec.ContentHash)
}

func indexContentHash(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func objectContentHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func objectSchemaHash(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return objectContentHash(data)
}

func secondaryIndexContentHash(index SecondaryIndex) string {
	if index.logicalContentHash != "" {
		return index.logicalContentHash
	}
	values := index.Values
	if !index.hashCanonical {
		values = normalizeSecondaryIndexValues(values)
	}
	return indexContentHash(struct {
		Kind   string              `json:"kind"`
		Field  string              `json:"field"`
		Unique bool                `json:"unique"`
		Values map[string][]string `json:"values"`
	}{
		Kind:   index.Kind,
		Field:  index.Field,
		Unique: index.Unique,
		Values: values,
	})
}

func edgeShardContentHash(shard EdgeShardData) string {
	if shard.logicalContentHash != "" {
		return shard.logicalContentHash
	}
	edges := shard.Edges
	if !shard.hashCanonical {
		edges = normalizeGraphEdges(edges)
	}
	return indexContentHash(struct {
		RelationType string       `json:"relation_type"`
		Shard        string       `json:"shard"`
		Edges        []graph.Edge `json:"edges"`
	}{
		RelationType: shard.RelationType,
		Shard:        shard.Shard,
		Edges:        edges,
	})
}

func entityPageContentHash(page EntityPageData) string {
	if page.logicalContentHash != "" {
		return page.logicalContentHash
	}
	entities := page.Entities
	if !page.hashCanonical {
		entities = normalizeGraphEntities(entities)
	}
	return indexContentHash(struct {
		Shard    string         `json:"shard"`
		Entities []legacyEntity `json:"entities"`
	}{
		Shard:    page.Shard,
		Entities: legacyEntities(entities),
	})
}

func graphEntityHashCanonical(entity graph.Entity) bool {
	return graphFieldsHashCanonical(entity.Fields) && graphMapHashCanonical(entity.Identity)
}

func graphEdgeHashCanonical(edge graph.Edge) bool {
	return graphFieldsHashCanonical(edge.Fields)
}

func graphFieldsHashCanonical(fields graph.Fields) bool {
	for _, value := range fields {
		if !graphValueHashCanonical(value) {
			return false
		}
	}
	return true
}

func graphMapHashCanonical(values map[string]any) bool {
	for _, value := range values {
		if !graphValueHashCanonical(value) {
			return false
		}
	}
	return true
}

func graphValueHashCanonical(value any) bool {
	switch typed := value.(type) {
	case nil, string, bool, float64:
		return true
	case graph.Fields:
		return graphFieldsHashCanonical(typed)
	case map[string]any:
		return graphMapHashCanonical(typed)
	case []any:
		for _, item := range typed {
			if !graphValueHashCanonical(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
