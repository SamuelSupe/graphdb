package storage

import (
	"context"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// indexBuildArtifacts keeps the catalog and the objects used to describe it
// together so callers never need to rebuild the same full-graph indexes.
type indexBuildArtifacts struct {
	Catalog                IndexCatalog
	Indexes                []SecondaryIndex
	IncrementalIndexWrites []incrementalSecondaryIndexWrite
	EdgeShards             []EdgeShardData
	EntityPages            []EntityPageData
}

type incrementalSecondaryIndexWrite struct {
	Key   string
	Index SecondaryIndex
}

func buildIndexArtifactsWithDefinitions(g *graph.Graph, version int64, definitions []IndexDefinition, at ...time.Time) (indexBuildArtifacts, error) {
	return buildIndexArtifacts(context.Background(), g, version, definitions, nil, at...)
}

func buildIndexArtifacts(ctx context.Context, g *graph.Graph, version int64, definitions []IndexDefinition, emit func(SecondaryIndex) error, at ...time.Time) (indexBuildArtifacts, error) {
	return buildTenantIndexArtifacts(ctx, "", g, version, definitions, emit, at...)
}

func buildTenantIndexArtifacts(ctx context.Context, tenantID string, g *graph.Graph, version int64, definitions []IndexDefinition, emit func(SecondaryIndex) error, at ...time.Time) (indexBuildArtifacts, error) {
	return buildTenantIndexArtifactsMode(ctx, tenantID, g, version, definitions, true, emit, at...)
}

func buildTenantIndexArtifactsMode(ctx context.Context, tenantID string, g *graph.Graph, version int64, definitions []IndexDefinition, ranges bool, emit func(SecondaryIndex) error, at ...time.Time) (indexBuildArtifacts, error) {
	indexes, err := secondaryIndexDefinitions(g, version, definitions, at...)
	if err != nil {
		return indexBuildArtifacts{}, err
	}

	now := artifactTime(at)
	catalog := IndexCatalog{LayoutVersion: CurrentObjectLayoutVersion, Version: version, UpdatedAt: now}
	for i := range indexes {
		index := &indexes[i]
		if err := addEntitiesToIndexContext(ctx, g, index); err != nil {
			return indexBuildArtifacts{}, err
		}
		prepareSecondaryIndexArtifact(index)
		summary := secondaryIndexSummary(*index, 16)
		catalog.Indexes = append(catalog.Indexes, IndexSpec{
			Name:           index.Kind + "." + index.Field,
			Kind:           index.Kind,
			Field:          index.Field,
			Type:           secondaryIndexType(*index),
			Status:         "ready",
			Objects:        secondaryIndexObjects(*index),
			EntryCount:     summary.EntryCount,
			DistinctValues: summary.DistinctValues,
			TopValues:      summary.TopValues,
			ContentHash:    secondaryIndexContentHash(*index),
			UpdatedAt:      now,
		})
		if emit != nil {
			if err := emit(*index); err != nil {
				return indexBuildArtifacts{}, err
			}
			index.Values = nil
			index.cachedObjectGroups = nil
		}
	}
	if emit != nil {
		indexes = nil
	}
	edgeShards := buildEdgeShards(g, version, at...)
	entityPages := buildEntityPages(g, version, at...)
	if ranges {
		edgeShards, err = prepareBoundedEdgeShards(ctx, tenantID, edgeShards)
		if err != nil {
			return indexBuildArtifacts{}, err
		}
		entityPages, err = prepareBoundedEntityPages(ctx, tenantID, entityPages)
		if err != nil {
			return indexBuildArtifacts{}, err
		}
	}
	for i := range edgeShards {
		edgeShards[i].logicalContentHash = edgeShardContentHash(edgeShards[i])
	}
	for i := range entityPages {
		entityPages[i].logicalContentHash = entityPageContentHash(entityPages[i])
	}
	catalog.EdgeShards = edgePageSpecs(edgeShards, g)
	catalog.EntityPages = entityPageSpecs(entityPages)
	sortIndexCatalog(&catalog)
	return indexBuildArtifacts{
		Catalog:     catalog,
		Indexes:     indexes,
		EdgeShards:  edgeShards,
		EntityPages: entityPages,
	}, nil
}

func relationImpactDirection(g *graph.Graph, relationType string) string {
	if g == nil {
		return ""
	}
	relation, ok := g.RelationTypes[relationType]
	if !ok {
		return ""
	}
	return relation.ImpactDirection
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

// Full rebuilds publish one field at a time. Only its postings remain live;
// catalog publication still validates the captured manifest and definitions.
func (s *TenantStore) buildAndWriteIndexArtifacts(ctx context.Context, tenantID string, g *graph.Graph, version int64, definitions []IndexDefinition) (indexBuildArtifacts, error) {
	artifacts, err := buildTenantIndexArtifactsMode(s.rangeEncodeContext(ctx), tenantID, g, version, definitions, s.WriteIndexRangePages, func(index SecondaryIndex) error {
		if err := objectContextErr(ctx); err != nil {
			return err
		}
		return s.writeParquetSecondaryIndexes(ctx, tenantID, []SecondaryIndex{index})
	}, mutationTime(ctx))
	if err == nil {
		err = s.buildAdjacencyRoots(ctx, tenantID, version, artifacts.Catalog.EdgeShards, artifacts.EdgeShards, false)
	}
	return artifacts, err
}
