package storage

import (
	"context"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func balancedRangeBoundary(count int, weight func(int) int64) int {
	var total, prefix int64
	for i := 0; i < count; i++ {
		total += weight(i)
	}
	middle, best := count/2, int64(1<<63-1)
	for i := 1; i < count; i++ {
		prefix += weight(i - 1)
		difference := total - 2*prefix
		if difference < 0 {
			difference = -difference
		}
		if difference < best {
			middle, best = i, difference
		}
	}
	return middle
}

type rangeEncodeBudgetKey struct{}

func (s *TenantStore) rangeEncodeContext(ctx context.Context) context.Context {
	if s.localFileStore() == nil {
		return ctx
	}
	return context.WithValue(ctx, rangeEncodeBudgetKey{}, s.maintenance.encodes)
}

func rangeEncodedBytes(ctx context.Context, encode func() ([]byte, error)) (int64, error) {
	if slots, ok := ctx.Value(rangeEncodeBudgetKey{}).(chan struct{}); ok {
		if !acquireTaskSlot(ctx, slots) {
			return 0, ctx.Err()
		}
		defer releaseTaskSlot(slots)
	}
	data, err := encode()
	return int64(len(data)), err
}

// Probe the actual encoding before publication. Splitting by payload first
// bounds temporary encoder memory even for records near the ingest limit.
// A single oversized record remains legal and occupies its own object.
func prepareBoundedEntityPages(ctx context.Context, tenantID string, pages []EntityPageData) ([]EntityPageData, error) {
	var result []EntityPageData
	var prepare func(EntityPageData) error
	prepare = func(page EntityPageData) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		page.TenantID = tenantID
		// The immutable key excludes rebuild time. Derive physical metadata
		// from its payload so rebuilding the same version encodes identical bytes.
		page.UpdatedAt = time.Time{}
		for _, entity := range page.Entities {
			if entity.UpdatedAt.After(page.UpdatedAt) {
				page.UpdatedAt = entity.UpdatedAt
			}
		}
		page.logicalContentHash = ""
		split := func() error {
			middle := balancedRangeBoundary(len(page.Entities), func(i int) int64 {
				return estimateEntityPageBytes(EntityPageData{Entities: []graph.Entity{page.Entities[i]}}) / 5
			})
			right := page
			right.Entities = page.Entities[middle:]
			right.pageRole = rangeRole(right.Entities[0].ID)
			page.Entities = page.Entities[:middle]
			if err := prepare(page); err != nil {
				return err
			}
			return prepare(right)
		}
		if len(page.Entities) > 1 && estimateEntityPageBytes(page)/5 > 2*indexPageMaxBytes {
			return split()
		}
		bytes, err := rangeEncodedBytes(ctx, func() ([]byte, error) { return marshalParquetEntityPage(ctx, page) })
		if err != nil {
			return err
		}
		if len(page.Entities) > 1 && bytes > indexPageMaxBytes {
			return split()
		}
		routing, err := decodeIndexRange(page.pageRole)
		if err != nil {
			return err
		}
		routing.Bytes = bytes
		page.pageRole = encodeRangeRole(routing)
		page.logicalContentHash = entityPageContentHash(page)
		result = append(result, page)
		return nil
	}
	for _, page := range splitBoundedEntityPages(pages) {
		if err := prepare(page); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func prepareBoundedEdgeShards(ctx context.Context, tenantID string, shards []EdgeShardData) ([]EdgeShardData, error) {
	var result []EdgeShardData
	var prepare func(EdgeShardData) error
	prepare = func(shard EdgeShardData) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		shard.TenantID = tenantID
		shard.UpdatedAt = time.Time{}
		for _, edge := range shard.Edges {
			if edge.UpdatedAt.After(shard.UpdatedAt) {
				shard.UpdatedAt = edge.UpdatedAt
			}
		}
		shard.logicalContentHash = ""
		split := func() error {
			middle := balancedRangeBoundary(len(shard.Edges), func(i int) int64 { return estimateEdgeLookupBytes(map[string][]graph.Edge{"": shard.Edges[i : i+1]}) })
			right := shard
			right.Edges = shard.Edges[middle:]
			right.pageRole = rangeRole(right.Edges[0].ID)
			shard.Edges = shard.Edges[:middle]
			if err := prepare(shard); err != nil {
				return err
			}
			return prepare(right)
		}
		payload := estimateEdgeLookupBytes(map[string][]graph.Edge{"": shard.Edges})
		if len(shard.Edges) > 1 && payload > 2*indexPageMaxBytes {
			return split()
		}
		bytes, err := rangeEncodedBytes(ctx, func() ([]byte, error) { return marshalParquetEdgeShard(ctx, shard) })
		if err != nil {
			return err
		}
		if len(shard.Edges) > 1 && bytes > indexPageMaxBytes {
			return split()
		}
		routing, err := decodeIndexRange(shard.pageRole)
		if err != nil {
			return err
		}
		routing.Bytes = bytes
		shard.pageRole = encodeRangeRole(routing)
		shard.logicalContentHash = edgeShardContentHash(shard)
		result = append(result, shard)
		return nil
	}
	for _, shard := range splitBoundedEdgeShards(shards) {
		if err := prepare(shard); err != nil {
			return nil, err
		}
	}
	return result, nil
}
