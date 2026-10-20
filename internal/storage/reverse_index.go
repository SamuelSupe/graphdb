package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

const reverseIndexLayoutVersion = 1

type ReverseIndexCatalog struct {
	LayoutVersion int         `json:"layout_version"`
	TenantID      string      `json:"tenant_id"`
	Version       int64       `json:"version"`
	EdgeShards    []EdgeShard `json:"edge_shards"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

func (s *TenantStore) GetReverseIndexCatalog(ctx context.Context, tenantID string, version int64) (ReverseIndexCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return ReverseIndexCatalog{}, err
	}
	catalog, _, err := s.loadReverseIndexCatalog(ctx, tenantID)
	if err != nil {
		return ReverseIndexCatalog{}, err
	}
	if version > 0 && catalog.Version != version {
		return ReverseIndexCatalog{}, ErrNotFound
	}
	return catalog, nil
}

func (s *TenantStore) rebuildReverseIndex(ctx context.Context, tenantID string, g *graph.Graph, version int64) error {
	catalog, meta, err := s.prepareReverseIndex(ctx, tenantID, g, version)
	if err != nil {
		return err
	}
	return s.putReverseIndexCatalogWithMeta(ctx, tenantID, catalog, meta)
}

func (s *TenantStore) prepareReverseIndex(ctx context.Context, tenantID string, g *graph.Graph, version int64) (ReverseIndexCatalog, ObjectMeta, error) {
	shards := buildReverseEdgeShards(g, version, mutationTime(ctx))
	var err error
	if s.WriteIndexRangePages {
		shards, err = prepareBoundedEdgeShards(s.rangeEncodeContext(ctx), tenantID, shards)
	}
	if err != nil {
		return ReverseIndexCatalog{}, ObjectMeta{}, err
	}
	now := mutationTime(ctx)
	catalog := ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion,
		TenantID:      tenantID,
		Version:       version,
		EdgeShards:    make([]EdgeShard, 0, len(shards)),
		UpdatedAt:     now,
	}
	for i := range shards {
		shards[i].TenantID = tenantID
		shards[i].logicalContentHash = edgeShardContentHash(shards[i])
	}
	catalog.EdgeShards = edgePageSpecs(shards, g)
	mini := IndexCatalog{Version: version, EdgeShards: catalog.EdgeShards}
	s.decorateReverseEdgeCatalog(&mini, tenantID)
	catalog.EdgeShards = mini.EdgeShards
	if err := s.writeChangedReverseEdgeShards(ctx, tenantID, shards, catalog.EdgeShards, version); err != nil {
		return ReverseIndexCatalog{}, ObjectMeta{}, err
	}
	sort.Slice(catalog.EdgeShards, func(i, j int) bool {
		if catalog.EdgeShards[i].RelationType == catalog.EdgeShards[j].RelationType {
			return catalog.EdgeShards[i].Shard < catalog.EdgeShards[j].Shard
		}
		return catalog.EdgeShards[i].RelationType < catalog.EdgeShards[j].RelationType
	})
	_, meta, err := s.getReverseIndexCatalogWithMeta(ctx, tenantID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return ReverseIndexCatalog{}, ObjectMeta{}, err
	}
	return catalog, meta, nil
}

func (s *TenantStore) putReverseIndexCatalogWithMeta(
	ctx context.Context,
	tenantID string,
	catalog ReverseIndexCatalog,
	meta ObjectMeta,
) error {
	s.protectCatalogObjects(tenantID, IndexCatalog{EdgeShards: catalog.EdgeShards})
	data, err := json.Marshal(catalog)
	if err != nil {
		return err
	}
	// Callers already hold the tenant writer fence, matching the core index
	// catalog hot path and avoiding another fixed metadata round trip.
	key := s.reverseIndexCatalogKey(tenantID)
	var nextMeta ObjectMeta

	nextMeta, err = s.putBytesWithMetaResult(ctx, key, data, meta)

	if err == nil {
		s.setCachedReverseIndexCatalog(tenantID, catalog, nextMeta)
	}
	return err
}

func (s *TenantStore) getReverseIndexCatalogWithMeta(ctx context.Context, tenantID string) (ReverseIndexCatalog, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return ReverseIndexCatalog{}, ObjectMeta{}, err
	}
	key := s.reverseIndexCatalogKey(tenantID)

	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return ReverseIndexCatalog{}, ObjectMeta{Key: key}, ErrNotFound
	}
	if err != nil {
		return ReverseIndexCatalog{}, ObjectMeta{}, err
	}
	var catalog ReverseIndexCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return ReverseIndexCatalog{}, ObjectMeta{}, fmt.Errorf("decode reverse index catalog: %w", err)
	}
	if catalog.LayoutVersion != reverseIndexLayoutVersion {
		return ReverseIndexCatalog{}, ObjectMeta{}, fmt.Errorf("unsupported reverse index layout version %d", catalog.LayoutVersion)
	}
	if catalog.TenantID != tenantID {
		return ReverseIndexCatalog{}, ObjectMeta{}, fmt.Errorf("reverse index tenant mismatch: path tenant %q contains tenant %q", tenantID, catalog.TenantID)
	}
	for _, shard := range catalog.EdgeShards {
		if shard.RelationType == "" || shard.Shard == "" || shard.EdgeCount < 0 || shard.ContentHash == "" || (!hasIndexRanges(shard.Objects) && len(shard.Objects) != 1) {
			return ReverseIndexCatalog{}, ObjectMeta{}, fmt.Errorf("invalid reverse edge shard catalog entry")
		}
		if shard.ImpactDirection != "" && !validImpactDirection(shard.ImpactDirection) {
			return ReverseIndexCatalog{}, ObjectMeta{}, fmt.Errorf("invalid reverse edge shard impact direction %q", shard.ImpactDirection)
		}
		if hasIndexRanges(shard.Objects) {
			if err := validateRangeObjects(shard.Objects, shard.EdgeCount, shard.ContentHash); err != nil {
				return ReverseIndexCatalog{}, ObjectMeta{}, err
			}
		}
		for _, object := range shard.Objects {
			if err := s.validateTenantObjectKey(tenantID, object.Key); err != nil {
				return ReverseIndexCatalog{}, ObjectMeta{}, err
			}
		}
	}
	return catalog, meta, nil
}

func validImpactDirection(direction string) bool {
	switch direction {
	case "none", "forward", "reverse", "both":
		return true
	default:
		return false
	}
}

func buildReverseEdgeShards(g *graph.Graph, version int64, at ...time.Time) []EdgeShardData {
	counts := map[string]int{}
	for _, edge := range g.Edges.All() {
		counts[edge.Type+"\x00"+edgeShardID(edge.To)]++
	}
	now := time.Now().UTC()
	if len(at) > 0 {
		now = at[0]
	}
	shards := newEdgeShardBuckets(counts, version, now)
	for _, edge := range g.Edges.All() {
		shardID := edgeShardID(edge.To)
		key := edge.Type + "\x00" + shardID
		shard := shards[key]
		shard.reverse = true
		shard.Edges = append(shard.Edges, edge)
		shard.hashCanonical = shard.hashCanonical && graphEdgeHashCanonical(edge)
		shards[key] = shard
	}
	return finishEdgeShards(shards)
}
