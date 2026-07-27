package storage

import (
	"context"
	"errors"
	"reflect"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) checkReverseIndexObjects(
	ctx context.Context,
	tenantID string,
	version int64,
	g *graph.Graph,
	health *IndexHealth,
) {
	catalog, _, err := s.getReverseIndexCatalogWithMeta(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		health.Issues = append(
			health.Issues,
			"reverse index catalog is missing",
		)
		return
	}
	if err != nil {
		health.Issues = append(
			health.Issues,
			"reverse index catalog decode failed: "+err.Error(),
		)
		return
	}
	if catalog.Version != version {
		health.Issues = append(
			health.Issues,
			"reverse index catalog version does not match index catalog",
		)
		return
	}

	expected := expectedReverseShards(g, version)
	seen := make(map[string]struct{}, len(catalog.EdgeShards))
	for _, spec := range catalog.EdgeShards {
		key := edgeShardCatalogKey(spec.RelationType, spec.Shard)
		label := "reverse edge shard " + spec.RelationType + "/" + spec.Shard
		want, exists := expected[key]
		if !exists {
			health.Issues = append(health.Issues, label+" is not expected")
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			health.Issues = append(health.Issues, label+" is duplicated")
			continue
		}
		seen[key] = struct{}{}
		if spec.ImpactDirection != relationImpactDirection(g, spec.RelationType) {
			health.Issues = append(
				health.Issues,
				label+" impact direction mismatch",
			)
		}
		s.checkReverseIndexShard(ctx, tenantID, version, spec, want, label, health)
	}
	for key, shard := range expected {
		if _, exists := seen[key]; !exists {
			health.Issues = append(
				health.Issues,
				"reverse edge shard "+shard.RelationType+"/"+shard.Shard+
					" is missing from catalog",
			)
		}
	}
}

func (s *TenantStore) checkReverseIndexShard(
	ctx context.Context,
	tenantID string,
	version int64,
	spec EdgeShard,
	expected EdgeShardData,
	label string,
	health *IndexHealth,
) {
	if specFormat(spec.Format) != IndexFormatParquet {
		health.Issues = append(health.Issues, label+" is not parquet")
		return
	}
	shard, ok, err := s.loadParquetEdgeShardObject(
		ctx, tenantID, version, spec,
	)
	if err != nil {
		health.Issues = append(
			health.Issues,
			label+" parquet decode failed: "+err.Error(),
		)
		return
	}
	if !ok {
		health.Issues = append(health.Issues, "missing "+label)
		return
	}
	if !indexTenantMatches(shard.TenantID, tenantID) {
		health.Issues = append(health.Issues, label+" tenant mismatch")
	}
	if shard.Version > version {
		health.Issues = append(health.Issues, label+" version is ahead of catalog")
	}
	if spec.ContentHash == "" {
		health.Issues = append(health.Issues, label+" content hash missing")
	} else if edgeShardContentHash(shard) != spec.ContentHash {
		health.Issues = append(health.Issues, label+" content hash mismatch")
	}
	if shard.RelationType != spec.RelationType || shard.Shard != spec.Shard {
		health.Issues = append(health.Issues, label+" metadata mismatch")
	}
	if len(shard.Edges) != spec.EdgeCount {
		health.Issues = append(health.Issues, label+" count mismatch")
	}
	if !reflect.DeepEqual(
		normalizeGraphEdges(shard.Edges),
		normalizeGraphEdges(expected.Edges),
	) {
		health.Issues = append(health.Issues, label+" content mismatch")
	}
}

func expectedReverseShards(
	g *graph.Graph,
	version int64,
) map[string]EdgeShardData {
	shards := buildReverseEdgeShards(g, version)
	out := make(map[string]EdgeShardData, len(shards))
	for _, shard := range shards {
		out[edgeShardCatalogKey(shard.RelationType, shard.Shard)] = shard
	}
	return out
}
