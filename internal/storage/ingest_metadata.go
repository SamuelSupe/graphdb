package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

const (
	IngestMetadataModeLegacy  = "legacy"
	IngestMetadataModeSegment = "segment"

	ingestMetadataFormatVersion   = 1
	ingestMetadataRecentLimit     = 32
	ingestMetadataIndexesPerLevel = 8
)

var ErrIngestMetadataFormatActivated = errors.New("ingest metadata segment format is already active")

type IngestMetadataConfig struct {
	Mode          string
	FlushInterval time.Duration
	MaxRequests   int
	MaxBytes      int64
	FlushWorkers  int
}

func DefaultIngestMetadataConfig() IngestMetadataConfig {
	return IngestMetadataConfig{
		Mode:          IngestMetadataModeLegacy,
		FlushInterval: 30 * time.Second,
		MaxRequests:   256,
		MaxBytes:      8 * 1024 * 1024,
		FlushWorkers:  2,
	}
}

func (c IngestMetadataConfig) validate() error {
	switch c.Mode {
	case IngestMetadataModeLegacy:
		return nil
	case IngestMetadataModeSegment:
	default:
		return fmt.Errorf("unsupported ingest metadata mode %q", c.Mode)
	}
	if c.FlushInterval <= 0 || c.MaxRequests <= 0 || c.MaxBytes <= 0 || c.FlushWorkers <= 0 {
		return fmt.Errorf("ingest metadata flush limits must be positive")
	}
	return nil
}

type IngestPublishedRecord struct {
	Index  int
	Record IngestBatchRecord
}

type ingestMetadataRecord struct {
	AcceptedLSN uint64            `json:"accepted_lsn"`
	Digest      string            `json:"digest"`
	Trace       walTraceContext   `json:"trace,omitempty"`
	Batch       IngestBatchRecord `json:"batch"`
}

type ingestMetadataSegment struct {
	FormatVersion int                    `json:"format_version"`
	TenantID      string                 `json:"tenant_id"`
	FirstLSN      uint64                 `json:"first_lsn"`
	LastLSN       uint64                 `json:"last_lsn"`
	CreatedAt     time.Time              `json:"created_at"`
	Records       []ingestMetadataRecord `json:"records"`
	Collectors    []CollectorStatus      `json:"collectors"`
}

type ingestMetadataSegmentRef struct {
	Key              string              `json:"key"`
	Codec            string              `json:"codec"`
	FirstLSN         uint64              `json:"first_lsn"`
	LastLSN          uint64              `json:"last_lsn"`
	Count            int                 `json:"count"`
	Bytes            int64               `json:"bytes"`
	ContentHash      string              `json:"content_hash"`
	BatchBloom       ingestMetadataBloom `json:"batch_bloom"`
	IdempotencyBloom ingestMetadataBloom `json:"idempotency_bloom"`
	CollectorBloom   ingestMetadataBloom `json:"collector_bloom"`
}

type ingestMetadataIndex struct {
	FormatVersion int                        `json:"format_version"`
	TenantID      string                     `json:"tenant_id"`
	Level         int                        `json:"level"`
	FirstLSN      uint64                     `json:"first_lsn"`
	LastLSN       uint64                     `json:"last_lsn"`
	CreatedAt     time.Time                  `json:"created_at"`
	Segments      []ingestMetadataSegmentRef `json:"segments"`
}

type ingestMetadataIndexRef struct {
	Key              string              `json:"key"`
	Codec            string              `json:"codec"`
	Level            int                 `json:"level"`
	FirstLSN         uint64              `json:"first_lsn"`
	LastLSN          uint64              `json:"last_lsn"`
	Count            int                 `json:"count"`
	ContentHash      string              `json:"content_hash"`
	BatchBloom       ingestMetadataBloom `json:"batch_bloom"`
	IdempotencyBloom ingestMetadataBloom `json:"idempotency_bloom"`
	CollectorBloom   ingestMetadataBloom `json:"collector_bloom"`
}

type ingestMetadataManifest struct {
	FormatVersion int                        `json:"format_version"`
	TenantID      string                     `json:"tenant_id"`
	UpdatedAt     time.Time                  `json:"updated_at"`
	Recent        []ingestMetadataSegmentRef `json:"recent,omitempty"`
	Indexes       []ingestMetadataIndexRef   `json:"indexes,omitempty"`
}

type ingestMetadataPublishStats struct {
	SegmentBytes      int
	SegmentPublished  bool
	ManifestPublishes int
	ManifestConflicts int
	IndexPublishes    int
	IndexMaxLevel     int
}

func (s *TenantStore) ensureIngestMetadataWriteMode(ctx context.Context, tenantID string) error {
	if s.IngestMetadataMode != IngestMetadataModeLegacy {
		return nil
	}
	_, _, _, err := s.loadIngestMetadataManifestDirect(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if s.IngestLogger != nil {
		s.IngestLogger.Error("ingest_metadata_format_gate", map[string]any{
			"tenant": tenantID, "mode": IngestMetadataModeLegacy,
			"error": ErrIngestMetadataFormatActivated.Error(),
		})
	}
	return fmt.Errorf("%w for tenant %q; use GRAPHDB_INGEST_METADATA_MODE=segment", ErrIngestMetadataFormatActivated, tenantID)
}

func (s *TenantStore) publishIngestMetadataSegment(
	ctx context.Context,
	tenantID string,
	records []ingestMetadataRecord,
) (ingestMetadataPublishStats, error) {
	if len(records) == 0 {
		return ingestMetadataPublishStats{}, nil
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].AcceptedLSN < records[j].AcceptedLSN
	})
	ctx, segmentSpan := startStorageSpan(
		ctx,
		"graphdb.storage.ingest.metadata_segment",
		tenantTraceAttr(tenantID),
		attribute.Int("graphdb.ingest.metadata.requests", len(records)),
	)
	var segmentErr error
	defer func() { endStorageSpan(segmentSpan, segmentErr) }()
	segment, err := s.buildIngestMetadataSegment(ctx, tenantID, records)
	if err != nil {
		segmentErr = err
		return ingestMetadataPublishStats{}, err
	}
	encodeCtx, encodeSpan := startStorageSpan(ctx, "graphdb.storage.ingest.metadata_segment.encode")
	data, contentHash, err := marshalParquetIngestMetadataDocument(encodeCtx, ingestMetadataSegmentCodec, segment)
	endStorageSpan(encodeSpan, err)
	if err != nil {
		segmentErr = err
		return ingestMetadataPublishStats{}, err
	}
	ref := ingestMetadataSegmentReference(s, tenantID, segment, len(data), contentHash)
	stats := ingestMetadataPublishStats{SegmentBytes: len(data)}
	putCtx, putSpan := startStorageSpan(
		ctx,
		"graphdb.storage.ingest.metadata_segment.put",
		attribute.Int("graphdb.ingest.metadata.segment_bytes", len(data)),
	)
	if _, err := s.Objects.PutConditional(putCtx, ref.Key, data, PutCondition{IfNoneMatch: true}); err != nil {
		endStorageSpan(putSpan, err)
		if !errors.Is(err, ErrConflict) {
			segmentErr = err
			return stats, fmt.Errorf("put ingest metadata segment: %w", err)
		}
		existing, getErr := s.Objects.Get(ctx, ref.Key)
		if getErr != nil {
			segmentErr = getErr
			return stats, fmt.Errorf("read conflicting ingest metadata segment: %w", getErr)
		}
		var stored ingestMetadataSegment
		existingHash, decodeErr := decodeParquetIngestMetadataDocument(ctx, existing, ingestMetadataSegmentCodec, &stored)
		if decodeErr != nil || existingHash != contentHash {
			segmentErr = ErrConflict
			return stats, fmt.Errorf("%w: ingest metadata segment key collision", ErrConflict)
		}
	} else {
		endStorageSpan(putSpan, nil)
		stats.SegmentPublished = true
	}
	if err := s.publishIngestMetadataManifest(ctx, tenantID, ref, &stats); err != nil {
		segmentErr = err
		return stats, err
	}
	s.ingestMetadataCache.store(
		ref.Key,
		segment,
		ObjectMeta{Key: ref.Key, Exists: true},
		ingestMetadataSegmentCacheBytes(segment, int64(len(data))),
		ingestMetadataImmutableTTL,
	)
	for _, status := range segment.Collectors {
		key := s.collectorStatusKey(tenantID, status.Source, status.CollectorID)
		s.setCachedCollectorStatus(key, status, ObjectMeta{Key: key})
	}
	return stats, nil
}

func (s *TenantStore) buildIngestMetadataSegment(
	ctx context.Context,
	tenantID string,
	records []ingestMetadataRecord,
) (ingestMetadataSegment, error) {
	firstLSN := records[0].AcceptedLSN
	lastLSN := records[len(records)-1].AcceptedLSN
	statuses := map[string]CollectorStatus{}
	for _, item := range records {
		request := item.Batch.Request
		key := ingestMetadataCollectorIdentity(request.Source, request.CollectorID)
		status, ok := statuses[key]
		if !ok {
			var err error
			status, err = s.getCollectorStatusForMetadataWrite(ctx, tenantID, request.Source, request.CollectorID)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return ingestMetadataSegment{}, err
			}
			if errors.Is(err, ErrNotFound) {
				status = CollectorStatus{TenantID: tenantID, Source: request.Source, CollectorID: request.CollectorID}
			}
		}
		if !collectorStatusCoversResult(status, item.Batch.Result) {
			applyCollectorStatusResult(
				&status,
				tenantID,
				request,
				item.Batch.Result,
				item.Batch.StartedAt,
				item.Batch.FinishedAt,
			)
		}
		statuses[key] = status
	}
	collectors := make([]CollectorStatus, 0, len(statuses))
	for _, status := range statuses {
		collectors = append(collectors, status)
	}
	sort.Slice(collectors, func(i, j int) bool {
		if collectors[i].Source == collectors[j].Source {
			return collectors[i].CollectorID < collectors[j].CollectorID
		}
		return collectors[i].Source < collectors[j].Source
	})
	return ingestMetadataSegment{
		FormatVersion: ingestMetadataFormatVersion,
		TenantID:      tenantID,
		FirstLSN:      firstLSN,
		LastLSN:       lastLSN,
		CreatedAt:     records[len(records)-1].Batch.FinishedAt,
		Records:       records,
		Collectors:    collectors,
	}, nil
}

func ingestMetadataSegmentReference(
	s *TenantStore,
	tenantID string,
	segment ingestMetadataSegment,
	bytes int,
	contentHash string,
) ingestMetadataSegmentRef {
	batchBloom := newIngestMetadataBloom(len(segment.Records))
	idempotencyBloom := newIngestMetadataBloom(len(segment.Records))
	collectorBloom := newIngestMetadataBloom(len(segment.Collectors))
	for _, item := range segment.Records {
		request := item.Batch.Request
		batchBloom.Add(ingestMetadataBatchIdentity(request.Source, request.CollectorID, item.Batch.Result.BatchID))
		if request.IdempotencyKey != "" {
			idempotencyBloom.Add(ingestMetadataIdempotencyIdentity(request.Source, request.CollectorID, request.IdempotencyKey))
		}
	}
	for _, status := range segment.Collectors {
		collectorBloom.Add(ingestMetadataCollectorIdentity(status.Source, status.CollectorID))
	}
	return ingestMetadataSegmentRef{
		Key:              s.ingestMetadataSegmentKey(tenantID, segment.FirstLSN, segment.LastLSN, contentHash),
		Codec:            ingestMetadataSegmentCodec,
		FirstLSN:         segment.FirstLSN,
		LastLSN:          segment.LastLSN,
		Count:            len(segment.Records),
		Bytes:            int64(bytes),
		ContentHash:      contentHash,
		BatchBloom:       batchBloom,
		IdempotencyBloom: idempotencyBloom,
		CollectorBloom:   collectorBloom,
	}
}
