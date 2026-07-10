package storage

import (
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

// indexBuildArtifacts keeps the catalog and the objects used to describe it
// together so callers never need to rebuild the same full-graph indexes.
type indexBuildArtifacts struct {
	Catalog     IndexCatalog
	Indexes     []SecondaryIndex
	EdgeShards  []EdgeShardData
	EntityPages []EntityPageData
}

func buildIndexArtifactsWithDefinitions(g *graph.Graph, version int64, definitions []IndexDefinition) (indexBuildArtifacts, error) {
	indexes, err := buildSecondaryIndexesWithDefinitions(g, version, definitions)
	if err != nil {
		return indexBuildArtifacts{}, err
	}
	for i := range indexes {
		prepareSecondaryIndexArtifact(&indexes[i])
	}
	edgeShards := buildEdgeShards(g, version)
	entityPages := buildEntityPages(g, version)
	for i := range edgeShards {
		edgeShards[i].logicalContentHash = edgeShardContentHash(edgeShards[i])
	}
	for i := range entityPages {
		entityPages[i].logicalContentHash = entityPageContentHash(entityPages[i])
	}
	now := time.Now().UTC()
	catalog := IndexCatalog{LayoutVersion: CurrentObjectLayoutVersion, Version: version, UpdatedAt: now}
	for _, index := range indexes {
		summary := secondaryIndexSummary(index, 16)
		catalog.Indexes = append(catalog.Indexes, IndexSpec{
			Name:           index.Kind + "." + index.Field,
			Kind:           index.Kind,
			Field:          index.Field,
			Type:           secondaryIndexType(index),
			Status:         "ready",
			Objects:        secondaryIndexObjects(index),
			EntryCount:     summary.EntryCount,
			DistinctValues: summary.DistinctValues,
			TopValues:      summary.TopValues,
			ContentHash:    secondaryIndexContentHash(index),
			UpdatedAt:      now,
		})
	}
	for _, shard := range edgeShards {
		catalog.EdgeShards = append(catalog.EdgeShards, EdgeShard{
			RelationType: shard.RelationType,
			Shard:        shard.Shard,
			EdgeCount:    len(shard.Edges),
			ContentHash:  edgeShardContentHash(shard),
			UpdatedAt:    now,
		})
	}
	for _, page := range entityPages {
		catalog.EntityPages = append(catalog.EntityPages, EntityPageSpec{
			Shard:          page.Shard,
			EntityCount:    len(page.Entities),
			ContentHash:    entityPageContentHash(page),
			UpdatedAt:      now,
			estimatedBytes: entityPagePackBytes(page),
		})
	}
	sortIndexCatalog(&catalog)
	return indexBuildArtifacts{
		Catalog:     catalog,
		Indexes:     indexes,
		EdgeShards:  edgeShards,
		EntityPages: entityPages,
	}, nil
}

func prepareSecondaryIndexArtifact(index *SecondaryIndex) {
	index.logicalContentHash = secondaryIndexContentHash(*index)
	groups := secondaryIndexObjectGroups(*index)
	if groups == nil {
		groups = []secondaryIndexObjectGroup{}
	}
	for i := range groups {
		if len(groups) == 1 {
			groups[i].Index.logicalContentHash = index.logicalContentHash
		} else {
			groups[i].Index.logicalContentHash = secondaryIndexContentHash(groups[i].Index)
		}
	}
	index.cachedObjectGroups = groups
}
