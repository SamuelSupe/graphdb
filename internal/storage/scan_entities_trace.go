package storage

import (
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type entityScanTraceStats struct {
	path                    string
	fallbackReason          string
	manifestVersion         int64
	catalogVersion          int64
	catalogPages            int
	shardsTotal             int
	shardsVisited           int
	uniqueObjects           map[string]struct{}
	physicalObjectReuses    int
	candidateFilterRequests int
	candidateObjectScans    int
	candidateScanReuses     int
	candidateRowGroupsRead  int
	candidateRowGroupsSkip  int
	candidateIDsMatched     int
	candidateIDsSelected    int
	candidateScanDuration   time.Duration
	decodedCacheHits        int
	decodedCacheMisses      int
	cacheRevalidations      int
	rawCacheHits            int
	objectLoads             int
	parquetDecodes          int
	parquetDecodeDuration   time.Duration
	parquetAdmissions       int
	parquetAdmissionWait    time.Duration
	entitiesExamined        int
	earlyStop               bool
}

func newEntityScanTraceStats() *entityScanTraceStats {
	return &entityScanTraceStats{uniqueObjects: map[string]struct{}{}}
}

func (s *entityScanTraceStats) markObject(key string) {
	if s == nil || key == "" {
		return
	}
	s.uniqueObjects[key] = struct{}{}
}

func (s *entityScanTraceStats) attrs() []attribute.KeyValue {
	if s == nil {
		return nil
	}
	return []attribute.KeyValue{
		attribute.String("graphdb.scan.path", s.path),
		attribute.String("graphdb.scan.fallback_reason", s.fallbackReason),
		attribute.Int64("graphdb.scan.manifest_version", s.manifestVersion),
		attribute.Int64("graphdb.scan.catalog_version", s.catalogVersion),
		attribute.Int("graphdb.scan.catalog_pages", s.catalogPages),
		attribute.Int("graphdb.scan.shards_total", s.shardsTotal),
		attribute.Int("graphdb.scan.shards_visited", s.shardsVisited),
		attribute.Int("graphdb.scan.unique_objects", len(s.uniqueObjects)),
		attribute.Int("graphdb.scan.physical_object_reuses", s.physicalObjectReuses),
		attribute.Int("graphdb.scan.candidate_filter_requests", s.candidateFilterRequests),
		attribute.Int("graphdb.scan.candidate_object_scans", s.candidateObjectScans),
		attribute.Int("graphdb.scan.candidate_scan_reuses", s.candidateScanReuses),
		attribute.Int("graphdb.scan.candidate_row_groups_read", s.candidateRowGroupsRead),
		attribute.Int("graphdb.scan.candidate_row_groups_skipped", s.candidateRowGroupsSkip),
		attribute.Int("graphdb.scan.candidate_ids_matched", s.candidateIDsMatched),
		attribute.Int("graphdb.scan.candidate_ids_selected", s.candidateIDsSelected),
		attribute.Int64("graphdb.scan.candidate_scan_ms", s.candidateScanDuration.Milliseconds()),
		attribute.Int("graphdb.scan.decoded_cache_hits", s.decodedCacheHits),
		attribute.Int("graphdb.scan.decoded_cache_misses", s.decodedCacheMisses),
		attribute.Int("graphdb.scan.cache_revalidations", s.cacheRevalidations),
		attribute.Int("graphdb.scan.raw_cache_hits", s.rawCacheHits),
		attribute.Int("graphdb.scan.object_loads", s.objectLoads),
		attribute.Int("graphdb.scan.parquet_decodes", s.parquetDecodes),
		attribute.Int64("graphdb.scan.parquet_decode_ms", s.parquetDecodeDuration.Milliseconds()),
		attribute.Int("graphdb.scan.parquet_admissions", s.parquetAdmissions),
		attribute.Int64("graphdb.scan.parquet_admission_wait_ms", s.parquetAdmissionWait.Milliseconds()),
		attribute.Int("graphdb.scan.entities_examined", s.entitiesExamined),
		attribute.Bool("graphdb.scan.early_stop", s.earlyStop),
	}
}
