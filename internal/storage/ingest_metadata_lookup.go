package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type ingestMetadataLookupKind int

const (
	ingestMetadataLookupBatch ingestMetadataLookupKind = iota
	ingestMetadataLookupIdempotency
	ingestMetadataLookupCollector
)

type ingestMetadataLookup struct {
	kind        ingestMetadataLookupKind
	source      string
	collectorID string
	value       string
}

func (q ingestMetadataLookup) identity() string {
	switch q.kind {
	case ingestMetadataLookupBatch:
		return ingestMetadataBatchIdentity(q.source, q.collectorID, q.value)
	case ingestMetadataLookupIdempotency:
		return ingestMetadataIdempotencyIdentity(q.source, q.collectorID, q.value)
	default:
		return ingestMetadataCollectorIdentity(q.source, q.collectorID)
	}
}

func (q ingestMetadataLookup) mayMatchSegment(ref ingestMetadataSegmentRef) bool {
	switch q.kind {
	case ingestMetadataLookupBatch:
		return ref.BatchBloom.MayContain(q.identity())
	case ingestMetadataLookupIdempotency:
		return ref.IdempotencyBloom.MayContain(q.identity())
	default:
		return ref.CollectorBloom.MayContain(q.identity())
	}
}

func (q ingestMetadataLookup) mayMatchIndex(ref ingestMetadataIndexRef) bool {
	switch q.kind {
	case ingestMetadataLookupBatch:
		return ref.BatchBloom.MayContain(q.identity())
	case ingestMetadataLookupIdempotency:
		return ref.IdempotencyBloom.MayContain(q.identity())
	default:
		return ref.CollectorBloom.MayContain(q.identity())
	}
}

func (s *TenantStore) findIngestMetadataRecord(
	ctx context.Context,
	tenantID string,
	query ingestMetadataLookup,
) (IngestBatchRecord, bool, error) {
	segment, found, err := s.findIngestMetadataSegment(ctx, tenantID, query)
	if err != nil || !found {
		return IngestBatchRecord{}, false, err
	}
	for i := len(segment.Records) - 1; i >= 0; i-- {
		record := segment.Records[i].Batch
		request := record.Request
		if request.Source != query.source || request.CollectorID != query.collectorID {
			continue
		}
		switch query.kind {
		case ingestMetadataLookupBatch:
			if record.Result.BatchID == query.value {
				return record, true, nil
			}
		case ingestMetadataLookupIdempotency:
			if request.IdempotencyKey == query.value {
				return record, true, nil
			}
		}
	}
	return IngestBatchRecord{}, false, nil
}

func (s *TenantStore) findIngestMetadataCollector(
	ctx context.Context,
	tenantID string,
	source string,
	collectorID string,
) (CollectorStatus, bool, error) {
	query := ingestMetadataLookup{
		kind:        ingestMetadataLookupCollector,
		source:      source,
		collectorID: collectorID,
	}
	segment, found, err := s.findIngestMetadataSegment(ctx, tenantID, query)
	if err != nil || !found {
		return CollectorStatus{}, false, err
	}
	for _, status := range segment.Collectors {
		if status.Source == source && status.CollectorID == collectorID {
			return status, true, nil
		}
	}
	return CollectorStatus{}, false, nil
}

func (s *TenantStore) findIngestMetadataSegment(
	ctx context.Context,
	tenantID string,
	query ingestMetadataLookup,
) (segmentResult ingestMetadataSegment, foundResult bool, resultErr error) {
	kind := query.kindName()
	started := time.Now()
	candidates := 0
	ctx, span := startStorageSpan(
		ctx,
		"graphdb.storage.ingest.metadata_lookup",
		tenantTraceAttr(tenantID),
		attribute.String("graphdb.ingest.metadata.lookup_kind", kind),
	)
	defer func() {
		outcome := "miss"
		if resultErr != nil {
			outcome = "error"
		} else if foundResult {
			outcome = "hit"
		}
		span.SetAttributes(
			attribute.String("graphdb.ingest.metadata.lookup_outcome", outcome),
			attribute.Int("graphdb.ingest.metadata.lookup_candidates", candidates),
		)
		endStorageSpan(span, resultErr)
		if s.IngestObserver != nil {
			s.IngestObserver.RecordIngestMetadataLookup(kind, outcome, candidates, time.Since(started))
		}
	}()
	manifest, _, err := s.loadIngestMetadataManifest(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return ingestMetadataSegment{}, false, nil
	}
	if err != nil {
		return ingestMetadataSegment{}, false, err
	}
	recent := append([]ingestMetadataSegmentRef(nil), manifest.Recent...)
	sort.Slice(recent, func(i, j int) bool { return recent[i].LastLSN > recent[j].LastLSN })
	for _, ref := range recent {
		if !query.mayMatchSegment(ref) {
			continue
		}
		candidates++
		segment, err := s.loadIngestMetadataSegment(ctx, tenantID, ref)
		if err != nil {
			return ingestMetadataSegment{}, false, err
		}
		if ingestMetadataSegmentMatches(segment, query) {
			return segment, true, nil
		}
	}
	indexes := append([]ingestMetadataIndexRef(nil), manifest.Indexes...)
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].LastLSN > indexes[j].LastLSN })
	for _, indexRef := range indexes {
		if !query.mayMatchIndex(indexRef) {
			continue
		}
		index, err := s.loadIngestMetadataIndex(ctx, tenantID, indexRef)
		if err != nil {
			return ingestMetadataSegment{}, false, err
		}
		segments := append([]ingestMetadataSegmentRef(nil), index.Segments...)
		sort.Slice(segments, func(i, j int) bool { return segments[i].LastLSN > segments[j].LastLSN })
		for _, ref := range segments {
			if !query.mayMatchSegment(ref) {
				continue
			}
			candidates++
			segment, err := s.loadIngestMetadataSegment(ctx, tenantID, ref)
			if err != nil {
				return ingestMetadataSegment{}, false, err
			}
			if ingestMetadataSegmentMatches(segment, query) {
				return segment, true, nil
			}
		}
	}
	return ingestMetadataSegment{}, false, nil
}

func (q ingestMetadataLookup) kindName() string {
	switch q.kind {
	case ingestMetadataLookupBatch:
		return "batch"
	case ingestMetadataLookupIdempotency:
		return "idempotency"
	default:
		return "collector"
	}
}

func (s *TenantStore) loadIngestMetadataSegment(
	ctx context.Context,
	tenantID string,
	ref ingestMetadataSegmentRef,
) (ingestMetadataSegment, error) {
	object, err := s.loadCachedIngestMetadataObject(
		ctx,
		ref.Key,
		"segment",
		ingestMetadataImmutableTTL,
		func(loadCtx context.Context) (ingestMetadataCacheObject, error) {
			data, err := s.Objects.Get(loadCtx, ref.Key)
			if err != nil {
				return ingestMetadataCacheObject{}, err
			}
			var segment ingestMetadataSegment
			hash, err := decodeParquetIngestMetadataDocument(
				loadCtx,
				data,
				ingestMetadataSegmentCodec,
				&segment,
			)
			if err != nil {
				return ingestMetadataCacheObject{}, err
			}
			if hash != ref.ContentHash || segment.TenantID != tenantID ||
				segment.FirstLSN != ref.FirstLSN || segment.LastLSN != ref.LastLSN {
				return ingestMetadataCacheObject{}, fmt.Errorf(
					"ingest metadata segment identity mismatch for %q",
					ref.Key,
				)
			}
			return ingestMetadataCacheObject{
				value: segment,
				bytes: ingestMetadataSegmentCacheBytes(segment, int64(len(data))),
			}, nil
		},
	)
	if err != nil {
		return ingestMetadataSegment{}, err
	}
	return object.value.(ingestMetadataSegment), nil
}

func ingestMetadataSegmentMatches(segment ingestMetadataSegment, query ingestMetadataLookup) bool {
	switch query.kind {
	case ingestMetadataLookupCollector:
		for _, status := range segment.Collectors {
			if status.Source == query.source && status.CollectorID == query.collectorID {
				return true
			}
		}
	default:
		for _, item := range segment.Records {
			request := item.Batch.Request
			if request.Source != query.source || request.CollectorID != query.collectorID {
				continue
			}
			if query.kind == ingestMetadataLookupBatch && item.Batch.Result.BatchID == query.value {
				return true
			}
			if query.kind == ingestMetadataLookupIdempotency && request.IdempotencyKey == query.value {
				return true
			}
		}
	}
	return false
}

func (s *TenantStore) getCollectorStatusForMetadataWrite(
	ctx context.Context,
	tenantID string,
	source string,
	collectorID string,
) (CollectorStatus, error) {
	if status, found, err := s.findIngestMetadataCollector(ctx, tenantID, source, collectorID); err != nil {
		return CollectorStatus{}, err
	} else if found {
		return status, nil
	}
	status, _, err := s.loadCollectorStatusWithMeta(ctx, tenantID, source, collectorID)
	if err != nil {
		return CollectorStatus{}, err
	}
	if status.LastBatchID != "" {
		return status, nil
	}
	derived, err := s.deriveCollectorStatusFromBatches(ctx, tenantID, source, collectorID)
	if err == nil {
		return derived, nil
	}
	if errors.Is(err, ErrNotFound) {
		return status, ErrNotFound
	}
	return CollectorStatus{}, err
}
