package storage

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type entityPageVisitSessionKey struct{}

type entityPageVisitSession struct {
	store           *TenantStore
	tenantID        string
	version         int64
	kind            string
	stats           *entityPageVisitTraceStats
	objectRefCounts map[string]int
	objectBytes     map[string]entityScanObjectBytes
	candidateScans  map[string]parquetEntityCandidateScan
}

func newEntityPageVisitSession(lookup *PersistedIndexLookup, kind string, stats *entityPageVisitTraceStats) *entityPageVisitSession {
	session := &entityPageVisitSession{
		store:           lookup.Store,
		tenantID:        lookup.TenantID,
		version:         lookup.Version,
		kind:            kind,
		stats:           stats,
		objectRefCounts: map[string]int{},
		objectBytes:     map[string]entityScanObjectBytes{},
		candidateScans:  map[string]parquetEntityCandidateScan{},
	}
	for _, spec := range lookup.Catalog.EntityPages {
		if specFormat(spec.Format) != IndexFormatParquet {
			continue
		}
		key := session.objectKey(spec)
		session.objectRefCounts[key]++
	}
	return session
}

func withEntityPageVisitSession(ctx context.Context, session *entityPageVisitSession) context.Context {
	return context.WithValue(ctx, entityPageVisitSessionKey{}, session)
}

func entityPageVisitSessionFromContext(ctx context.Context) *entityPageVisitSession {
	session, _ := ctx.Value(entityPageVisitSessionKey{}).(*entityPageVisitSession)
	return session
}

func (s *entityPageVisitSession) objectKey(spec EntityPageSpec) string {
	return firstIndexObjectKey(spec.Objects, "page", s.store.parquetEntityPageVersionKey(s.tenantID, s.version, spec.Shard))
}

func (s *entityPageVisitSession) visitPage(ctx context.Context, spec EntityPageSpec, visit func(EntityPageData) error) (available bool, skipped bool, err error) {
	key := s.objectKey(spec)
	s.stats.markObject(key)
	if _, _, hit := s.store.borrowCachedEntityPage(s.tenantID, s.version, key, spec.ContentHash, spec.SchemaHash); hit {
		ok, err := s.store.withParquetEntityPageObject(ctx, s.tenantID, s.version, spec, func(page EntityPageData, _ string, validated bool) error {
			if !validated {
				return nil
			}
			return visit(page)
		})
		return ok, false, err
	}

	s.stats.decodedCacheMisses++
	loaded, err := s.loadAndDecodePage(ctx, spec, key)
	if !loaded.available || (err != nil && !loaded.cached) {
		return loaded.available, false, err
	}
	if loaded.skip {
		s.stats.pagesSkippedByKind++
		return true, true, nil
	}
	if (err != nil || !entityPageReadable(loaded.page, s.tenantID, s.version, spec)) && loaded.cached {
		s.store.dropCachedIndexObject("entity_page", s.tenantID, s.version, key, spec.ContentHash, spec.SchemaHash)
		delete(s.objectBytes, key)
		delete(s.candidateScans, key)
		loaded, err = s.loadAndDecodePage(ctx, spec, key)
		if err != nil || !loaded.available {
			return loaded.available, false, err
		}
		if loaded.skip {
			s.stats.pagesSkippedByKind++
			return true, true, nil
		}
	}
	if err != nil {
		return false, false, err
	}
	if !entityPageReadable(loaded.page, s.tenantID, s.version, spec) {
		return false, false, nil
	}
	s.store.putCachedIndexObject("entity_page", s.tenantID, s.version, key, spec.ContentHash, spec.SchemaHash, loaded.data, loaded.meta)
	s.store.putCachedEntityPage(s.tenantID, s.version, key, spec.ContentHash, spec.SchemaHash, loaded.page, loaded.meta.ETag)
	return true, false, visit(loaded.page)
}

func (s *entityPageVisitSession) loadAndDecodePage(ctx context.Context, spec EntityPageSpec, key string) (loaded loadedEntityScanPage, err error) {
	reusePhysicalObject := s.objectRefCounts[key] > 1
	if object, ok := s.objectBytes[key]; reusePhysicalObject && ok {
		loaded.data = object.data
		loaded.meta = object.meta
		loaded.cached = object.cached
		loaded.available = true
		s.stats.physicalObjectReuses++
	} else {
		loaded.data, loaded.meta, loaded.available, loaded.cached, err = s.store.loadParquetEntityPageObjectBytes(ctx, s.tenantID, s.version, spec)
		if err != nil || !loaded.available {
			return loaded, err
		}
		if reusePhysicalObject {
			s.objectBytes[key] = entityScanObjectBytes{data: loaded.data, meta: loaded.meta, cached: loaded.cached}
		}
		if loaded.cached {
			s.stats.rawCacheHits++
		} else {
			s.stats.objectLoads++
		}
	}

	if s.kind != "" {
		s.stats.candidateFilterRequests++
		scan, ok := s.candidateScans[key]
		if !reusePhysicalObject {
			ok = false
		}
		if !ok {
			candidateCtx, candidateSpan := startStorageSpan(ctx, "graphdb.storage.index_lookup.visit_entities.candidate_filter",
				attribute.Int("graphdb.index_lookup.object_bytes", len(loaded.data)),
			)
			started := time.Now()
			scan, err = scanParquetEntityObjectCandidates(candidateCtx, loaded.data, EntityScanOptions{Kind: s.kind})
			duration := time.Since(started)
			candidateSpan.SetAttributes(
				attribute.String("graphdb.index_lookup.kind", s.kind),
				attribute.Int("graphdb.index_lookup.candidate_ids_matched", len(scan.IDs)),
				attribute.Int("graphdb.index_lookup.row_groups_read", scan.RowGroupsRead),
				attribute.Int("graphdb.index_lookup.row_groups_skipped", scan.RowGroupsSkipped),
			)
			endStorageSpan(candidateSpan, err)
			if err != nil {
				return loaded, err
			}
			if reusePhysicalObject {
				s.candidateScans[key] = scan
			}
			s.stats.candidateObjectScans++
			s.stats.candidateRowGroupsRead += scan.RowGroupsRead
			s.stats.candidateRowGroupsSkipped += scan.RowGroupsSkipped
			s.stats.candidateIDsMatched += len(scan.IDs)
			s.stats.candidateScanDuration += duration
		} else {
			s.stats.candidateScanReuses++
		}
		loaded.candidates = filterParquetEntityCandidates(scan, spec.Shard, scanCursor{})
		s.stats.candidateIDsSelected += len(loaded.candidates)
		if len(loaded.candidates) == 0 {
			loaded.skip = true
			return loaded, nil
		}
	}

	decodeCtx, decodeSpan := startStorageSpan(ctx, "graphdb.storage.index_lookup.visit_entities.decode_page",
		attribute.String("graphdb.index_lookup.shard", spec.Shard),
		attribute.Int("graphdb.index_lookup.object_bytes", len(loaded.data)),
	)
	started := time.Now()
	loaded.page, err = decodeParquetEntityPage(decodeCtx, loaded.data, s.tenantID, spec.Shard, 0)
	duration := time.Since(started)
	s.stats.parquetDecodes++
	s.stats.parquetDecodeDuration += duration
	decodeSpan.SetAttributes(
		attribute.Int("graphdb.index_lookup.decoded_entities", len(loaded.page.Entities)),
		attribute.Bool("graphdb.index_lookup.page_readable", err == nil && entityPageReadable(loaded.page, s.tenantID, s.version, spec)),
	)
	endStorageSpan(decodeSpan, err)
	return loaded, err
}
