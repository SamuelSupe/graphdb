package storage

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func (s *IngestService) flushTenant(items []*ingestPending) []*ingestPending {
	for start := 0; start < len(items); {
		end := start + 1
		flushID := ""
		if items[start].envelope.Prepared != nil {
			flushID = items[start].envelope.Prepared.FlushID
			generation := s.pendingAcceptedGeneration(items[start])
			for end < len(items) &&
				items[end].envelope.Prepared != nil &&
				items[end].envelope.Prepared.FlushID == flushID &&
				s.pendingAcceptedGeneration(items[end]) == generation {
				end++
			}
		} else {
			generation := s.pendingAcceptedGeneration(items[start])
			bytes := items[start].bytes
			for end < len(items) && end-start < s.config.FlushMaxRequests &&
				bytes+items[end].bytes <= s.config.FlushMaxBytes && items[end].envelope.Prepared == nil &&
				s.pendingAcceptedGeneration(items[end]) == generation {
				bytes += items[end].bytes
				end++
			}
		}
		retry := s.flushTenantGroup(items[start:end])
		if len(retry) > 0 {
			return append(retry, items[end:]...)
		}
		start = end
	}
	return nil
}

func (s *IngestService) pendingAcceptedGeneration(pending *ingestPending) int64 {
	if pending.envelope.AcceptedGeneration > 0 {
		return pending.envelope.AcceptedGeneration
	}
	if pending.envelope.Prepared != nil && pending.envelope.Prepared.BaseGeneration > 0 {
		return pending.envelope.Prepared.BaseGeneration
	}

	return legacyUnboundIngestGeneration
}

func (s *IngestService) flushTenantGroup(items []*ingestPending) []*ingestPending {
	// Old binaries could acknowledge identifiers that cannot be persisted.
	// Retire only unprepared requests, preserving any durable publication cohort.
	unprepared := true
	for _, item := range items {
		unprepared = unprepared && item.envelope.Prepared == nil
	}
	if unprepared {
		for index, item := range items {
			if err := ValidateIngestStorageIdentifiers(item.envelope.Request); err != nil {
				if index > 0 {
					if retry := s.flushTenantGroup(items[:index]); len(retry) > 0 {
						return append(retry, items[index:]...)
					}
				}
				result := failedIngestResult(item.envelope.Request, err)
				if _, err := s.appendTerminalBatch(items[index:index+1], []IngestResult{result}); err != nil {
					s.recordError(err)
					s.setPendingRetry(item, err)
					return items[index:]
				}
				if index+1 < len(items) {
					return s.flushTenantGroup(items[index+1:])
				}
				return nil
			}
		}
	}
	tenantID := items[0].envelope.TenantID
	firstLSN, lastLSN := ingestPendingLSNRange(items)
	started := time.Now()
	flushCtx, span := startIngestFlushSpan(s.runCtx, tenantID, items)
	var (
		stats    IngestBatchStats
		flushErr error
		flushID  string
	)
	if s.config.Logger != nil {
		fields := ingestTraceLogFields(items[0].envelope)
		if span.SpanContext().IsValid() {
			fields["flush_trace_id"] = span.SpanContext().TraceID().String()
			fields["flush_span_id"] = span.SpanContext().SpanID().String()
		}
		fields["tenant"] = tenantID
		fields["requests"] = len(items)
		fields["first_lsn"] = firstLSN
		fields["last_lsn"] = lastLSN
		fields["oldest_ms"] = float64(time.Since(items[0].envelope.AcceptedAt).Microseconds()) / 1000
		s.config.Logger.Info("ingest_flush_started", fields)
	}
	defer func() {
		status := "ok"
		if flushErr != nil {
			status = "error"
		}
		duration := time.Since(started)
		span.SetAttributes(
			attribute.String("graphdb.ingest.flush.status", status),
			attribute.Int("graphdb.ingest.flush.logical_commits", stats.LogicalCommits),
			attribute.Int("graphdb.ingest.flush.segments", stats.Segments),
			attribute.Int("graphdb.ingest.flush.manifest_publishes", stats.ManifestPublishes),
			attribute.Int("graphdb.ingest.flush.exact_dedup", stats.ExactDedup),
			attribute.Int("graphdb.ingest.flush.cas_merged", stats.CASMerged),
			attribute.Bool("graphdb.ingest.flush.fallback", stats.Fallback),
		)
		if flushID != "" {
			span.SetAttributes(attribute.String("graphdb.ingest.flush.id", flushID))
		}
		endStorageSpan(span, flushErr)
		if s.config.Observer != nil {
			s.config.Observer.RecordIngestFlush(
				status,
				duration,
				len(items),
				stats.LogicalCommits,
				stats.Segments,
				stats.ManifestPublishes,
				stats.ExactDedup,
				stats.Fallback,
			)
		}
		if s.config.Logger != nil {
			fields := ingestTraceLogFields(items[0].envelope)
			if span.SpanContext().IsValid() {
				fields["flush_trace_id"] = span.SpanContext().TraceID().String()
				fields["flush_span_id"] = span.SpanContext().SpanID().String()
			}
			fields["tenant"] = tenantID
			fields["status"] = status
			fields["requests"] = len(items)
			fields["first_lsn"] = firstLSN
			fields["last_lsn"] = lastLSN
			fields["logical_commits"] = stats.LogicalCommits
			fields["segments"] = stats.Segments
			fields["manifest_publishes"] = stats.ManifestPublishes
			fields["exact_dedup"] = stats.ExactDedup
			fields["cas_merged"] = stats.CASMerged
			fields["fallback"] = stats.Fallback
			fields["duration_ms"] = float64(duration.Microseconds()) / 1000
			if flushID != "" {
				fields["flush_id"] = flushID
			}
			if flushErr != nil {
				fields["error"] = flushErr.Error()
				s.config.Logger.Error("ingest_flush_completed", fields)
			} else {
				s.config.Logger.Info("ingest_flush_completed", fields)
			}
		}
	}()

	entries := make([]IngestBatchEntry, len(items))
	for index, pending := range items {
		entries[index] = IngestBatchEntry{
			Request:            pending.envelope.Request,
			AcceptedAt:         pending.envelope.AcceptedAt,
			AcceptedGeneration: s.pendingAcceptedGeneration(pending),
			Prepared:           pending.envelope.Prepared,
		}
	}
	var preparedHook func(context.Context, []*IngestPreparedRequest) error

	preparedHook = func(ctx context.Context, plans []*IngestPreparedRequest) error {
		return s.appendPreparedBatchState(ctx, items, plans)
	}

	flushCtx, cancel := context.WithTimeout(flushCtx, s.config.FlushTimeout)
	results, err := s.store.IngestDurableBatchWithHooks(
		flushCtx,
		tenantID,
		entries,
		IngestBatchHooks{
			Prepared: preparedHook,
			Published: func() {
				if s.config.OnGraphPublished != nil {
					s.config.OnGraphPublished(tenantID)
				}
			},
			Stats: func(batchStats IngestBatchStats) {
				stats = batchStats
			},
		},
	)
	cancel()
	if prepared := items[0].envelope.Prepared; prepared != nil {
		flushID = prepared.FlushID
	}
	if err != nil {
		flushErr = err
		// A coalesced group can exceed publication or WAL frame limits even
		// though every request was admitted. Split only before a durable plan
		// exists: a prepared cohort must retain its original atomic publication.
		var pressure *BackpressureError
		split := errors.Is(err, ErrIngestWALRecordTooLarge) || errors.Is(err, ErrIngestWALRecordExceedsSegment)
		if errors.As(err, &pressure) {
			for _, reason := range pressure.Reasons {
				if reason.Code == "commit_tail_too_long" {
					split = true
				}
			}
		}
		if split && len(items) > 1 {
			for _, item := range items {
				if item.envelope.Prepared != nil {
					split = false
					break
				}
			}
			if split {
				middle := len(items) / 2
				if retry := s.flushTenantGroup(items[:middle]); len(retry) > 0 {
					return append(retry, items[middle:]...)
				}
				return s.flushTenantGroup(items[middle:])
			}
		}
		if terminalIngestFlushError(err) {
			if errors.Is(err, ErrTenantDisabled) || errors.Is(err, ErrTenantDeleted) {
			}
			failureCtx, failureCancel := context.WithTimeout(s.runCtx, s.config.FlushTimeout)
			defer failureCancel()
			terminalResults := make([]IngestResult, len(items))
			for index, pending := range items {
				result := failedIngestResult(pending.envelope.Request, err)
				finishedAt := time.Now().UTC()
				var persistErr error
				if !errors.Is(err, errIngestGenerationFenced) {
					if resolver, ok := s.store.(ingestFailureResolver); ok {
						result, persistErr = resolver.ResolveIngestFailure(
							failureCtx,
							pending.envelope.TenantID,
							pending.envelope.Request,
							result,
							pending.envelope.AcceptedAt,
							finishedAt,
						)
					} else if failureStore, ok := s.store.(ingestFailureStore); ok {
						persistErr = failureStore.PersistIngestFailure(
							failureCtx,
							pending.envelope.TenantID,
							pending.envelope.Request,
							result,
							pending.envelope.AcceptedAt,
							finishedAt,
						)
					}
				}
				if persistErr != nil {
					flushErr = persistErr
					s.recordError(persistErr)
					for _, retry := range items {
						s.setPendingRetry(retry, persistErr)
					}
					return items
				}
				terminalResults[index] = result
			}
			if retryIndex, appendErr := s.appendTerminalBatch(items, terminalResults); appendErr != nil {
				flushErr = appendErr
				s.recordError(appendErr)
				for _, retry := range items[retryIndex:] {
					s.setPendingRetry(retry, appendErr)
				}
				return items[retryIndex:]
			}
			return nil
		}
		for _, pending := range items {
			s.setPendingRetry(pending, err)
		}
		if errors.Is(err, ErrIngestRepairRequired) {
			s.recordError(err)
		}
		return items
	}
	if retryIndex, err := s.appendTerminalBatch(items, results); err != nil {
		flushErr = err
		s.recordError(err)
		for _, retry := range items[retryIndex:] {
			s.setPendingRetry(retry, err)
		}
		return items[retryIndex:]
	}
	return nil
}
