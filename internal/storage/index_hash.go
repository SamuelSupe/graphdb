package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

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
	return indexContentHash(struct {
		Kind   string              `json:"kind"`
		Field  string              `json:"field"`
		Unique bool                `json:"unique"`
		Values map[string][]string `json:"values"`
	}{
		Kind:   index.Kind,
		Field:  index.Field,
		Unique: index.Unique,
		Values: normalizeSecondaryIndexValues(index.Values),
	})
}

func edgeShardContentHash(shard EdgeShardData) string {
	return indexContentHash(struct {
		RelationType string       `json:"relation_type"`
		Shard        string       `json:"shard"`
		Edges        []graph.Edge `json:"edges"`
	}{
		RelationType: shard.RelationType,
		Shard:        shard.Shard,
		Edges:        normalizeGraphEdges(shard.Edges),
	})
}

func entityPageContentHash(page EntityPageData) string {
	return indexContentHash(struct {
		Shard    string         `json:"shard"`
		Entities []graph.Entity `json:"entities"`
	}{
		Shard:    page.Shard,
		Entities: normalizeGraphEntities(page.Entities),
	})
}
