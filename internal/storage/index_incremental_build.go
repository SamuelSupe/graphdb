package storage

import (
	"context"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) buildIncrementalIndexArtifacts(ctx context.Context, tenantID string, previous IndexCatalog, before *graph.Graph, after *graph.Graph, report graph.ApplyReport, version int64) (indexBuildArtifacts, error) {
	now := time.Now().UTC()
	catalog := cloneIndexCatalog(previous)
	catalog.LayoutVersion = CurrentObjectLayoutVersion
	catalog.TenantID = tenantID
	catalog.Version = version
	catalog.UpdatedAt = now

	indexWrites, specs, err := s.buildIncrementalSecondaryIndexes(ctx, tenantID, previous.Version, catalog.Indexes, before, after, report.AffectedEntityIDs, version, now)
	if err != nil {
		return indexBuildArtifacts{}, err
	}
	catalog.Indexes = specs

	pages, pageSpecs, err := s.buildIncrementalEntityPages(ctx, tenantID, previous.Version, catalog.EntityPages, before, after, report.AffectedEntityIDs, version, now)
	if err != nil {
		return indexBuildArtifacts{}, err
	}
	catalog.EntityPages = pageSpecs

	edges, edgeSpecs, err := s.buildIncrementalEdgeShards(ctx, tenantID, previous.Version, catalog.EdgeShards, before, after, report.AffectedEdgeIDs, version, now)
	if err != nil {
		return indexBuildArtifacts{}, err
	}
	catalog.EdgeShards = edgeSpecs
	sortIndexCatalog(&catalog)
	return indexBuildArtifacts{Catalog: catalog, IncrementalIndexWrites: indexWrites, EntityPages: pages, EdgeShards: edges}, nil
}

func cloneIndexCatalog(catalog IndexCatalog) IndexCatalog {
	clone := catalog
	clone.contentHash = ""
	clone.Indexes = make([]IndexSpec, len(catalog.Indexes))
	for i, spec := range catalog.Indexes {
		clone.Indexes[i] = spec
		clone.Indexes[i].Objects = append([]IndexObject(nil), spec.Objects...)
		clone.Indexes[i].TopValues = append([]IndexValueStat(nil), spec.TopValues...)
	}
	clone.EdgeShards = make([]EdgeShard, len(catalog.EdgeShards))
	for i, spec := range catalog.EdgeShards {
		clone.EdgeShards[i] = spec
		clone.EdgeShards[i].Objects = append([]IndexObject(nil), spec.Objects...)
	}
	clone.EntityPages = make([]EntityPageSpec, len(catalog.EntityPages))
	for i, spec := range catalog.EntityPages {
		clone.EntityPages[i] = spec
		clone.EntityPages[i].Objects = append([]IndexObject(nil), spec.Objects...)
	}
	return clone
}
