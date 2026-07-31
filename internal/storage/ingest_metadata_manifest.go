package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func (s *TenantStore) loadIngestMetadataManifest(
	ctx context.Context,
	tenantID string,
) (ingestMetadataManifest, ObjectMeta, error) {
	key := s.ingestMetadataManifestKey(tenantID)
	object, err := s.loadCachedIngestMetadataObject(
		ctx,
		key,
		"manifest",
		ingestMetadataManifestTTL,
		func(loadCtx context.Context) (ingestMetadataCacheObject, error) {
			manifest, meta, bytes, err := s.loadIngestMetadataManifestDirect(
				loadCtx,
				tenantID,
			)
			if err != nil {
				return ingestMetadataCacheObject{meta: meta}, err
			}
			return ingestMetadataCacheObject{
				value: manifest,
				meta:  meta,
				bytes: bytes,
			}, nil
		},
	)
	if err != nil {
		return ingestMetadataManifest{}, object.meta, err
	}
	manifest := object.value.(ingestMetadataManifest)
	return cloneIngestMetadataManifest(manifest), object.meta, nil
}

func (s *TenantStore) loadIngestMetadataManifestDirect(
	ctx context.Context,
	tenantID string,
) (ingestMetadataManifest, ObjectMeta, int64, error) {
	key := s.ingestMetadataManifestKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return ingestMetadataManifest{}, meta, 0, err
	}
	if !isParquetBytes(data) {
		return ingestMetadataManifest{}, meta, 0, fmt.Errorf(
			"unsupported ingest metadata manifest %q",
			key,
		)
	}
	var manifest ingestMetadataManifest
	if _, err := decodeParquetIngestMetadataDocument(
		ctx,
		data,
		ingestMetadataManifestCodec,
		&manifest,
	); err != nil {
		return ingestMetadataManifest{}, meta, 0, err
	}
	if manifest.FormatVersion != ingestMetadataFormatVersion ||
		manifest.TenantID != tenantID {
		return ingestMetadataManifest{}, meta, 0, fmt.Errorf(
			"ingest metadata manifest identity mismatch for tenant %q",
			tenantID,
		)
	}
	return manifest, meta, int64(len(data)), nil
}

func (s *TenantStore) publishIngestMetadataManifest(
	ctx context.Context,
	tenantID string,
	segment ingestMetadataSegmentRef,
	stats *ingestMetadataPublishStats,
) (publishErr error) {
	ctx, span := startStorageSpan(
		ctx,
		"graphdb.storage.ingest.metadata_manifest.cas",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.ingest.metadata.first_lsn", int64(segment.FirstLSN)),
		attribute.Int64("graphdb.ingest.metadata.last_lsn", int64(segment.LastLSN)),
	)
	defer func() {
		span.SetAttributes(attribute.Int("graphdb.ingest.metadata.manifest_conflicts", stats.ManifestConflicts))
		endStorageSpan(span, publishErr)
	}()
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		manifest, meta, err := s.loadIngestMetadataManifest(ctx, tenantID)
		if errors.Is(err, ErrNotFound) {
			manifest = ingestMetadataManifest{
				FormatVersion: ingestMetadataFormatVersion,
				TenantID:      tenantID,
			}
			meta = ObjectMeta{Key: s.ingestMetadataManifestKey(tenantID)}
		} else if err != nil {
			return err
		}
		found, err := s.ingestMetadataManifestContains(ctx, manifest, segment)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		manifest.Recent = append(manifest.Recent, segment)
		if len(manifest.Recent) > ingestMetadataRecentLimit {
			overflow := append([]ingestMetadataSegmentRef(nil), manifest.Recent[:len(manifest.Recent)-ingestMetadataRecentLimit]...)
			manifest.Recent = append([]ingestMetadataSegmentRef(nil), manifest.Recent[len(manifest.Recent)-ingestMetadataRecentLimit:]...)
			if err := s.addIngestMetadataIndex(ctx, tenantID, &manifest, 0, overflow, stats); err != nil {
				return err
			}
		}
		manifest.UpdatedAt = time.Now().UTC()
		data, _, err := marshalParquetIngestMetadataDocument(ctx, ingestMetadataManifestCodec, manifest)
		if err != nil {
			return err
		}
		condition := PutCondition{IfNoneMatch: !meta.Exists, IfMatch: meta.ETag}
		if publishedMeta, err := s.Objects.PutConditional(ctx, meta.Key, data, condition); err == nil {
			stats.ManifestPublishes++
			s.ingestMetadataCache.store(
				meta.Key,
				cloneIngestMetadataManifest(manifest),
				publishedMeta,
				int64(len(data)),
				ingestMetadataManifestTTL,
			)
			if s.IngestLogger != nil {
				s.IngestLogger.Info("ingest_metadata_manifest_published", map[string]any{
					"tenant": tenantID, "first_lsn": segment.FirstLSN,
					"last_lsn": segment.LastLSN, "conflicts": stats.ManifestConflicts,
				})
			}
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return fmt.Errorf("publish ingest metadata manifest: %w", err)
		}
		s.ingestMetadataCache.invalidate(meta.Key)
		stats.ManifestConflicts++
		if s.IngestLogger != nil {
			s.IngestLogger.Info("ingest_metadata_manifest_conflict", map[string]any{
				"tenant": tenantID, "attempt": attempt + 1,
				"first_lsn": segment.FirstLSN, "last_lsn": segment.LastLSN,
			})
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: ingest metadata manifest for tenant %q changed while publishing", ErrConflict, tenantID)
}

func (s *TenantStore) addIngestMetadataIndex(
	ctx context.Context,
	tenantID string,
	manifest *ingestMetadataManifest,
	level int,
	segments []ingestMetadataSegmentRef,
	stats *ingestMetadataPublishStats,
) error {
	if len(segments) == 0 {
		return nil
	}
	ref, err := s.putIngestMetadataIndex(ctx, tenantID, level, segments)
	if err != nil {
		return err
	}
	stats.IndexPublishes++
	stats.IndexMaxLevel = max(stats.IndexMaxLevel, level)
	manifest.Indexes = append(manifest.Indexes, ref)

	for {
		atLevel := make([]ingestMetadataIndexRef, 0)
		others := make([]ingestMetadataIndexRef, 0, len(manifest.Indexes))
		for _, candidate := range manifest.Indexes {
			if candidate.Level == level {
				atLevel = append(atLevel, candidate)
			} else {
				others = append(others, candidate)
			}
		}
		if len(atLevel) <= ingestMetadataIndexesPerLevel {
			sort.Slice(manifest.Indexes, func(i, j int) bool {
				if manifest.Indexes[i].Level == manifest.Indexes[j].Level {
					return manifest.Indexes[i].FirstLSN < manifest.Indexes[j].FirstLSN
				}
				return manifest.Indexes[i].Level < manifest.Indexes[j].Level
			})
			return nil
		}
		merged := make([]ingestMetadataSegmentRef, 0)
		for _, indexRef := range atLevel {
			index, err := s.loadIngestMetadataIndex(ctx, tenantID, indexRef)
			if err != nil {
				return err
			}
			merged = append(merged, index.Segments...)
		}
		merged = dedupeIngestMetadataSegmentRefs(merged)
		manifest.Indexes = others
		level++
		ref, err := s.putIngestMetadataIndex(ctx, tenantID, level, merged)
		if err != nil {
			return err
		}
		stats.IndexPublishes++
		stats.IndexMaxLevel = max(stats.IndexMaxLevel, level)
		manifest.Indexes = append(manifest.Indexes, ref)
	}
}

func (s *TenantStore) putIngestMetadataIndex(
	ctx context.Context,
	tenantID string,
	level int,
	segments []ingestMetadataSegmentRef,
) (ingestMetadataIndexRef, error) {
	segments = dedupeIngestMetadataSegmentRefs(segments)
	index := ingestMetadataIndex{
		FormatVersion: ingestMetadataFormatVersion,
		TenantID:      tenantID,
		Level:         level,
		FirstLSN:      segments[0].FirstLSN,
		LastLSN:       segments[len(segments)-1].LastLSN,
		CreatedAt:     time.Time{},
		Segments:      segments,
	}
	data, hash, err := marshalParquetIngestMetadataDocument(ctx, ingestMetadataIndexCodec, index)
	if err != nil {
		return ingestMetadataIndexRef{}, err
	}
	ref := ingestMetadataIndexReference(s, tenantID, index, hash)
	meta, err := s.Objects.PutConditional(ctx, ref.Key, data, PutCondition{IfNoneMatch: true})
	if err != nil && !errors.Is(err, ErrConflict) {
		return ingestMetadataIndexRef{}, fmt.Errorf("put ingest metadata index: %w", err)
	}
	s.ingestMetadataCache.store(
		ref.Key,
		index,
		meta,
		int64(len(data)),
		ingestMetadataImmutableTTL,
	)
	return ref, nil
}

func (s *TenantStore) loadIngestMetadataIndex(
	ctx context.Context,
	tenantID string,
	ref ingestMetadataIndexRef,
) (ingestMetadataIndex, error) {
	object, err := s.loadCachedIngestMetadataObject(
		ctx,
		ref.Key,
		"index",
		ingestMetadataImmutableTTL,
		func(loadCtx context.Context) (ingestMetadataCacheObject, error) {
			data, err := s.Objects.Get(loadCtx, ref.Key)
			if err != nil {
				return ingestMetadataCacheObject{}, err
			}
			var index ingestMetadataIndex
			hash, err := decodeParquetIngestMetadataDocument(
				loadCtx,
				data,
				ingestMetadataIndexCodec,
				&index,
			)
			if err != nil {
				return ingestMetadataCacheObject{}, err
			}
			if hash != ref.ContentHash || index.TenantID != tenantID ||
				index.Level != ref.Level {
				return ingestMetadataCacheObject{}, fmt.Errorf(
					"ingest metadata index identity mismatch for %q",
					ref.Key,
				)
			}
			return ingestMetadataCacheObject{
				value: index,
				bytes: int64(len(data)),
			}, nil
		},
	)
	if err != nil {
		return ingestMetadataIndex{}, err
	}
	return cloneIngestMetadataIndex(object.value.(ingestMetadataIndex)), nil
}

func cloneIngestMetadataManifest(manifest ingestMetadataManifest) ingestMetadataManifest {
	manifest.Recent = append([]ingestMetadataSegmentRef(nil), manifest.Recent...)
	manifest.Indexes = append([]ingestMetadataIndexRef(nil), manifest.Indexes...)
	return manifest
}

func cloneIngestMetadataIndex(index ingestMetadataIndex) ingestMetadataIndex {
	index.Segments = append([]ingestMetadataSegmentRef(nil), index.Segments...)
	return index
}

func ingestMetadataIndexReference(
	s *TenantStore,
	tenantID string,
	index ingestMetadataIndex,
	contentHash string,
) ingestMetadataIndexRef {
	batches := make([]ingestMetadataBloom, 0, len(index.Segments))
	idempotency := make([]ingestMetadataBloom, 0, len(index.Segments))
	collectors := make([]ingestMetadataBloom, 0, len(index.Segments))
	count := 0
	for _, segment := range index.Segments {
		batches = append(batches, segment.BatchBloom)
		idempotency = append(idempotency, segment.IdempotencyBloom)
		collectors = append(collectors, segment.CollectorBloom)
		count += segment.Count
	}
	return ingestMetadataIndexRef{
		Key:              s.ingestMetadataIndexKey(tenantID, index.Level, index.FirstLSN, index.LastLSN, contentHash),
		Codec:            ingestMetadataIndexCodec,
		Level:            index.Level,
		FirstLSN:         index.FirstLSN,
		LastLSN:          index.LastLSN,
		Count:            count,
		ContentHash:      contentHash,
		BatchBloom:       mergeIngestMetadataBlooms(batches...),
		IdempotencyBloom: mergeIngestMetadataBlooms(idempotency...),
		CollectorBloom:   mergeIngestMetadataBlooms(collectors...),
	}
}

func dedupeIngestMetadataSegmentRefs(refs []ingestMetadataSegmentRef) []ingestMetadataSegmentRef {
	byKey := make(map[string]ingestMetadataSegmentRef, len(refs))
	for _, ref := range refs {
		byKey[ref.Key] = ref
	}
	out := make([]ingestMetadataSegmentRef, 0, len(byKey))
	for _, ref := range byKey {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FirstLSN == out[j].FirstLSN {
			return out[i].Key < out[j].Key
		}
		return out[i].FirstLSN < out[j].FirstLSN
	})
	return out
}

func (s *TenantStore) ingestMetadataManifestContains(
	ctx context.Context,
	manifest ingestMetadataManifest,
	segment ingestMetadataSegmentRef,
) (bool, error) {
	for _, ref := range manifest.Recent {
		if ref.Key == segment.Key {
			return true, nil
		}
	}
	for _, indexRef := range manifest.Indexes {
		if segment.FirstLSN < indexRef.FirstLSN || segment.LastLSN > indexRef.LastLSN {
			continue
		}
		index, err := s.loadIngestMetadataIndex(ctx, manifest.TenantID, indexRef)
		if err != nil {
			return false, err
		}
		for _, ref := range index.Segments {
			if ref.Key == segment.Key {
				return true, nil
			}
		}
	}
	return false, nil
}
