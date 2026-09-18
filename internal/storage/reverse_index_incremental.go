package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) refreshReverseIndexAfterCommit(
	ctx context.Context,
	tenantID string,
	before *graph.Graph,
	after *graph.Graph,
	affectedEdgeIDs []string,
	baseVersion, version int64,
) error {
	previous, meta, err := s.getReverseIndexCatalogWithMeta(
		ctx,
		tenantID,
	)
	if err != nil {
		return err
	}
	if previous.Version != baseVersion {
		return fmt.Errorf(
			"reverse index catalog version %d does not match previous graph version %d",
			previous.Version,
			baseVersion,
		)
	}
	now := time.Now().UTC()
	shards, specs, err := s.buildIncrementalEdgeShardsFor(
		ctx,
		tenantID,
		previous.Version,
		previous.EdgeShards,
		before,
		after,
		affectedEdgeIDs,
		version,
		now,
		func(edge graph.Edge) string {
			return edgeShardID(edge.To)
		},
		func(catalog *IndexCatalog) {
			s.decorateReverseEdgeCatalog(catalog, tenantID)
		},
	)
	if err != nil {
		return err
	}
	if err := s.writeChangedReverseEdgeShards(
		ctx,
		tenantID,
		shards,
		specs,
		version,
	); err != nil {
		return err
	}
	catalog := ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion,
		TenantID:      tenantID,
		Version:       version,
		EdgeShards:    specs,
		UpdatedAt:     now,
	}
	sort.Slice(catalog.EdgeShards, func(i, j int) bool {
		left := catalog.EdgeShards[i]
		right := catalog.EdgeShards[j]
		if left.RelationType != right.RelationType {
			return left.RelationType < right.RelationType
		}
		return left.Shard < right.Shard
	})
	return s.putReverseIndexCatalogWithMeta(
		ctx,
		tenantID,
		catalog,
		meta,
	)
}

func (s *TenantStore) decorateReverseEdgeCatalog(
	catalog *IndexCatalog,
	tenantID string,
) {
	for i := range catalog.EdgeShards {
		spec := &catalog.EdgeShards[i]
		key := s.reverseEdgeShardVersionKey(
			tenantID,
			catalog.Version,
			spec.RelationType,
			spec.Shard,
		)
		spec.Format = IndexFormatParquet
		spec.Codec = parquetEdgeShardCodec
		spec.RowCount = spec.EdgeCount
		spec.SchemaHash = parquetEdgeShardSchemaHash()
		spec.Objects = []IndexObject{{
			Role:        "reverse_shard",
			Key:         key,
			Format:      IndexFormatParquet,
			Codec:       parquetEdgeShardCodec,
			RowCount:    spec.EdgeCount,
			ContentHash: spec.ContentHash,
			SchemaHash:  spec.SchemaHash,
		}}
	}
}

func (s *TenantStore) writeChangedReverseEdgeShards(
	ctx context.Context,
	tenantID string,
	shards []EdgeShardData,
	specs []EdgeShard,
	version int64,
) error {
	byKey := make(map[string]int, len(specs))
	for i, spec := range specs {
		byKey[edgeShardTargetKey(spec.RelationType, spec.Shard)] = i
	}
	changed := make([]EdgeShardData, 0, len(shards))
	for _, shard := range shards {
		index, ok := byKey[edgeShardTargetKey(
			shard.RelationType,
			shard.Shard,
		)]
		if !ok || len(specs[index].Objects) != 1 {
			continue
		}
		expectedKey := s.reverseEdgeShardVersionKey(
			tenantID,
			version,
			shard.RelationType,
			shard.Shard,
		)
		if specs[index].Objects[0].Key != expectedKey {
			continue
		}
		shard.TenantID = tenantID
		changed = append(changed, shard)
	}
	// Reverse point reads decode the selected pack. Keep packs small so fewer
	// writes do not turn into large cold-read amplification.
	for _, group := range edgeShardDataPackGroupsWithTargetRows(changed, 128) {
		pack := mergeEdgeShardPack(group)
		// Reverse index rows must retain their destination shard inside a pack.
		pack.reverse = true
		key := s.reverseEdgeShardVersionKey(tenantID, version, pack.RelationType, pack.Shard)
		if err := s.putParquetEdgeShardObject(ctx, key, tenantID, pack, false); err != nil {
			return err
		}
		for _, shard := range group.Shards {
			index := byKey[edgeShardTargetKey(shard.RelationType, shard.Shard)]
			specs[index].Objects[0].Key = key
		}
	}
	return nil
}
