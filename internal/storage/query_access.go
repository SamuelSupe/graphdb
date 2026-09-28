package storage

import (
	"context"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const UnconstrainedQueryVersion = int64(^uint64(0) >> 1)
const queryIndexBackoff = 5 * time.Second

type queryIndexFailure struct {
	version int64
	until   time.Time
}

func (s *TenantStore) QueryIndexSuppressed(tenant string, version int64) bool {
	if version <= 0 || version == UnconstrainedQueryVersion {
		return false
	}
	s.queryMu.Lock()
	defer s.queryMu.Unlock()
	failure, ok := s.queryUnavailable[tenant]
	if ok && time.Now().Before(failure.until) {
		return failure.version == version
	}
	delete(s.queryUnavailable, tenant)
	return false
}

func (s *TenantStore) MarkQueryIndexUnavailable(tenant string, version int64) {
	if version <= 0 || version == UnconstrainedQueryVersion {
		return
	}
	s.queryMu.Lock()
	defer s.queryMu.Unlock()
	if s.queryUnavailable == nil || len(s.queryUnavailable) >= 4096 {
		s.queryUnavailable = make(map[string]queryIndexFailure)
	}
	s.queryUnavailable[tenant] = queryIndexFailure{version, time.Now().Add(queryIndexBackoff)}
}

// SelectQueryRead keeps index freshness and fallback rules identical for
// buffered and streaming callers. A hot graph avoids opening disk indexes.
func (s *TenantStore) SelectQueryRead(ctx context.Context, tenant string, request query.Request, minVersion, maxVersion int64, cachedGraph bool) (query.ExecuteOptions, int64, bool) {
	if cachedGraph {
		return query.ExecuteOptions{}, 0, false
	}
	if s.QueryIndexSuppressed(tenant, maxVersion) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("graphdb.query.lazy_suppressed", true))
		return query.ExecuteOptions{}, 0, false
	}
	options, version, ok := s.QueryOptions(ctx, tenant, maxVersion, query.RequiresReverseIndex(request))
	if ok && s.QueryIndexSuppressed(tenant, version) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("graphdb.query.lazy_suppressed", true))
		return options, version, false
	}
	return options, version, ok && version >= minVersion && query.SupportsLazyRead(request, options.PlannerStats)
}

func (s *TenantStore) QueryOptions(
	ctx context.Context,
	tenantID string,
	maxVersion int64,
	includeReverse bool,
) (query.ExecuteOptions, int64, bool) {
	expectedVersion := maxVersion
	if maxVersion == UnconstrainedQueryVersion {
		expectedVersion = 0
	}
	catalog, err := s.QueryCatalog(ctx, tenantID, expectedVersion)
	if err != nil || catalog.Version <= 0 || catalog.Version > maxVersion {
		return query.ExecuteOptions{}, 0, false
	}
	return s.queryOptionsForCatalog(
		ctx, tenantID, catalog, includeReverse,
	), catalog.Version, true
}

func (s *TenantStore) queryOptionsForCatalog(
	ctx context.Context,
	tenantID string,
	catalog IndexCatalog,
	includeReverse bool,
) query.ExecuteOptions {
	lookup := &PersistedIndexLookup{Store: s, TenantID: tenantID, Version: catalog.Version, Catalog: catalog}
	stats := catalog.PlannerStats()
	if includeReverse {
		reverse, err := s.QueryReverseCatalog(
			ctx, tenantID, catalog.Version,
		)
		if err == nil {
			lookup.ReverseCatalog = &reverse
			stats.ReverseEdgeIndexAvailable = true
			for _, shard := range reverse.EdgeShards {
				stats.ReverseEdgeShards = append(stats.ReverseEdgeShards, query.PlannerEdgeStat{
					RelationType:    shard.RelationType,
					ImpactDirection: shard.ImpactDirection,
					Shard:           shard.Shard,
					EdgeCount:       shard.EdgeCount,
				})
			}
		}
	}
	return query.ExecuteOptions{
		PlannerStats: stats,
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
}
