package storage

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func (s *IngestService) scheduleMetadata() {
	defer close(s.metadataSchedulerOK)
	defer close(s.metadataReadyCh)
	queues := map[string]*ingestTenantQueue{}
	busy := map[string]bool{}
	forceThrough := map[string]uint64{}
	deadlines := ingestDeadlineHeap{}
	heap.Init(&deadlines)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	draining := false
	shutdownCh := s.metadataShutdownCh

	resetTimer := func() <-chan time.Time {
		if deadlines.Len() == 0 {
			return nil
		}
		delay := time.Until(deadlines[0].deadline)
		if delay < 0 {
			delay = 0
		}
		timer.Reset(delay)
		return timer.C
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	dispatch := func(now time.Time) {
		dispatchReadyIngestQueues(
			now,
			draining,
			&deadlines,
			queues,
			busy,
			s.metadataReadyCh,
		)
	}

	for {
		dispatch(time.Now())
		if draining && len(queues) == 0 && len(busy) == 0 {
			return
		}
		stopTimer()
		timerCh := resetTimer()
		select {
		case pending := <-s.metadataEnqueueCh:
			tenantID := pending.envelope.TenantID
			queue := queues[tenantID]
			if queue == nil {
				publishedAt := pending.envelope.FinishedAt
				if publishedAt.IsZero() {
					publishedAt = time.Now().UTC()
				}
				deadline := publishedAt.Add(s.config.Metadata.FlushInterval)
				queue = &ingestTenantQueue{
					tenantID:      tenantID,
					deadline:      deadline,
					flushDeadline: deadline,
					index:         -1,
				}
				if draining || pending.envelope.Request.FullSync {
					now := time.Now()
					queue.deadline = now
					queue.flushDeadline = now
				}
				queues[tenantID] = queue
				heap.Push(&deadlines, queue)
			}
			queue.items = append(queue.items, pending)
			queue.bytes += pending.bytes
			if len(queue.items) >= s.config.Metadata.MaxRequests ||
				queue.bytes >= s.config.Metadata.MaxBytes ||
				pending.envelope.Request.FullSync ||
				pending.acceptedLSN <= forceThrough[tenantID] {
				now := time.Now()
				queue.deadline = now
				queue.flushDeadline = now
				heap.Fix(&deadlines, queue.index)
			}
		case force := <-s.metadataForceCh:
			forceThrough[force.tenantID] = max(forceThrough[force.tenantID], force.throughLSN)
			if queue := queues[force.tenantID]; queue != nil {
				now := time.Now()
				queue.deadline = now
				queue.flushDeadline = now
				heap.Fix(&deadlines, queue.index)
			}
		case completion := <-s.metadataCompleteCh:
			delete(busy, completion.tenantID)
			if len(completion.retry) > 0 {
				queue := queues[completion.tenantID]
				if queue == nil {
					deadline := time.Now().Add(s.config.RetryInterval)
					queue = &ingestTenantQueue{
						tenantID:      completion.tenantID,
						deadline:      deadline,
						flushDeadline: deadline,
						index:         -1,
					}
					queues[completion.tenantID] = queue
					heap.Push(&deadlines, queue)
				}
				queue.items = append(append([]*ingestPending(nil), completion.retry...), queue.items...)
				for _, pending := range completion.retry {
					queue.bytes += pending.bytes
				}
				if draining {
					now := time.Now()
					queue.deadline = now
					queue.flushDeadline = now
					heap.Fix(&deadlines, queue.index)
				}
			}
		case <-timerCh:
		case <-shutdownCh:
			draining = true
			shutdownCh = nil
			for _, queue := range queues {
				now := time.Now()
				queue.deadline = now
				queue.flushDeadline = now
			}
			heap.Init(&deadlines)
		case <-s.runCtx.Done():
			return
		}
	}
}

func (s *IngestService) runMetadataWorker() {
	defer s.metadataWorkers.Done()
	for flush := range s.metadataReadyCh {
		overshoot := max(time.Duration(0), time.Since(flush.deadline))
		if s.config.Observer != nil {
			s.config.Observer.RecordIngestMetadataDispatch(overshoot)
		}
		retry := s.flushMetadata(flush.tenantID, flush.items, overshoot)
		select {
		case s.metadataCompleteCh <- ingestWorkerCompletion{tenantID: flush.tenantID, retry: retry}:
		case <-s.runCtx.Done():
			return
		}
	}
}

func (s *IngestService) flushMetadata(
	tenantID string,
	items []*ingestPending,
	deadlineOvershoot time.Duration,
) []*ingestPending {
	for start := 0; start < len(items); {
		end := start + 1
		groupID := items[start].envelope.MetadataFlushID
		for end < len(items) && items[end].envelope.MetadataFlushID == groupID {
			end++
		}
		retry := s.flushMetadataGroup(tenantID, items[start:end], deadlineOvershoot)
		if len(retry) > 0 {
			return append(retry, items[end:]...)
		}
		start = end
	}
	return nil
}

func (s *IngestService) flushMetadataGroup(
	tenantID string,
	items []*ingestPending,
	deadlineOvershoot time.Duration,
) []*ingestPending {
	ctx, span := startIngestMetadataFlushSpan(s.runCtx, tenantID, items)
	started := time.Now()
	var flushErr error
	var publishStats ingestMetadataPublishStats
	if s.config.Logger != nil {
		firstLSN, lastLSN := ingestPendingLSNRange(items)
		s.config.Logger.Info("ingest_metadata_flush_started", map[string]any{
			"tenant": tenantID, "requests": len(items),
			"first_lsn": firstLSN, "last_lsn": lastLSN,
		})
	}
	defer func() {
		status := "ok"
		if flushErr != nil {
			status = "error"
		}
		span.SetAttributes(
			attribute.String("graphdb.ingest.metadata.status", status),
			attribute.Int("graphdb.ingest.metadata.requests", len(items)),
			attribute.Float64(
				"graphdb.ingest.metadata.deadline_overshoot_ms",
				float64(deadlineOvershoot.Microseconds())/1000,
			),
		)
		endStorageSpan(span, flushErr)
		if s.config.Observer != nil {
			segmentPuts := 0
			if publishStats.SegmentPublished {
				segmentPuts = 1
			}
			s.config.Observer.RecordIngestMetadataFlush(
				status,
				time.Since(started),
				len(items),
				publishStats.SegmentBytes,
				segmentPuts,
				publishStats.ManifestPublishes,
				publishStats.ManifestConflicts,
				publishStats.IndexPublishes,
			)
		}
		if s.config.Logger != nil {
			firstLSN, lastLSN := ingestPendingLSNRange(items)
			fields := map[string]any{
				"tenant": tenantID, "status": status, "requests": len(items),
				"first_lsn": firstLSN, "last_lsn": lastLSN,
				"duration_ms":           float64(time.Since(started).Microseconds()) / 1000,
				"deadline_overshoot_ms": float64(deadlineOvershoot.Microseconds()) / 1000,
			}
			if flushErr != nil {
				fields["error"] = flushErr.Error()
				s.config.Logger.Error("ingest_metadata_flush_completed", fields)
			} else {
				s.config.Logger.Info("ingest_metadata_flush_completed", fields)
			}
		}
	}()
	if err := s.ensureMetadataFlushState(items); err != nil {
		flushErr = err
		for _, pending := range items {
			s.setPendingRetry(pending, err)
		}
		return items
	}

	records := make([]ingestMetadataRecord, 0, len(items))
	for _, pending := range items {
		record, required, err := s.metadataRecordForPending(ctx, pending)
		if err != nil {
			flushErr = err
			s.setPendingRetry(pending, err)
			return items
		}
		if !required {
			continue
		}
		if record.Result.Failed > 0 {
			if err := s.store.saveDeadLetter(ctx, tenantID, record.Request, record.Result); err != nil {
				flushErr = fmt.Errorf("save dead letter: %w", err)
				s.setPendingRetry(pending, flushErr)
				return items
			}
		}
		records = append(records, ingestMetadataRecord{
			AcceptedLSN: pending.acceptedLSN,
			Digest:      pending.envelope.Digest,
			Trace:       pending.envelope.Trace,
			Batch:       record,
		})
	}
	if len(records) > 0 {
		stats, err := s.store.publishIngestMetadataSegment(ctx, tenantID, records)
		publishStats = stats
		if err != nil {
			flushErr = err
			for _, pending := range items {
				s.setPendingRetry(pending, err)
			}
			return items
		}
		span.SetAttributes(
			attribute.Int("graphdb.ingest.metadata.segment_bytes", stats.SegmentBytes),
			attribute.Int("graphdb.ingest.metadata.manifest_publishes", stats.ManifestPublishes),
			attribute.Int("graphdb.ingest.metadata.manifest_conflicts", stats.ManifestConflicts),
			attribute.Int("graphdb.ingest.metadata.index_publishes", stats.IndexPublishes),
		)
		if s.config.Logger != nil {
			s.config.Logger.Info("ingest_metadata_segment_completed", map[string]any{
				"tenant": tenantID, "requests": len(records),
				"segment_bytes":      stats.SegmentBytes,
				"segment_put":        stats.SegmentPublished,
				"manifest_publishes": stats.ManifestPublishes,
				"manifest_conflicts": stats.ManifestConflicts,
				"index_publishes":    stats.IndexPublishes,
				"index_max_level":    stats.IndexMaxLevel,
			})
		}
	}
	if err := s.finalizeMetadataItems(items); err != nil {
		flushErr = err
		s.recordError(err)
		return items
	}
	return nil
}

func (s *IngestService) ensureMetadataFlushState(items []*ingestPending) error {
	if len(items) == 0 || items[0].envelope.MetadataFlushID != "" {
		return nil
	}
	type identity struct {
		LSN      uint64 `json:"lsn"`
		RecordID string `json:"record_id"`
	}
	identities := make([]identity, len(items))
	for index, pending := range items {
		if pending.envelope.MetadataFlushID != "" {
			return fmt.Errorf("%w: mixed assigned metadata flush", ErrIngestRepairRequired)
		}
		identities[index] = identity{LSN: pending.acceptedLSN, RecordID: pending.envelope.RecordID}
	}
	data, err := json.Marshal(identities)
	if err != nil {
		return err
	}
	groupID := sha256Hex(data)
	envelopes := make([]walIngestEnvelope, len(items))
	for index, pending := range items {
		envelope := pending.envelope
		envelope.MetadataFlushID = groupID
		envelopes[index] = envelope
	}
	if err := s.appendIngestStateBatch(IngestWALPublished, envelopes); err != nil {
		return err
	}
	s.mu.Lock()
	for index, pending := range items {
		pending.envelope = envelopes[index]
	}
	s.mu.Unlock()
	return nil
}

func (s *IngestService) metadataRecordForPending(
	ctx context.Context,
	pending *ingestPending,
) (IngestBatchRecord, bool, error) {
	if pending.metadataRecord != nil {
		return *pending.metadataRecord, true, nil
	}
	record, found, err := s.store.loadIngestRecord(ctx, pending.envelope.TenantID, pending.envelope.Request)
	if err != nil {
		return IngestBatchRecord{}, false, err
	}
	if found {
		return record, false, nil
	}
	if pending.envelope.Result == nil {
		return IngestBatchRecord{}, false, fmt.Errorf("%w: published ingest has no result", ErrIngestRepairRequired)
	}
	return IngestBatchRecord{
		TenantID:   pending.envelope.TenantID,
		Request:    pending.envelope.Request,
		Result:     *pending.envelope.Result,
		StartedAt:  pending.envelope.AcceptedAt,
		FinishedAt: pending.envelope.FinishedAt,
	}, true, nil
}

func (s *IngestService) forceMetadataThrough(ctx context.Context, tenantID string, throughLSN uint64) error {
	if s.config.Metadata.Mode != IngestMetadataModeSegment {
		return nil
	}
	select {
	case s.metadataForceCh <- ingestForceRequest{tenantID: tenantID, throughLSN: throughLSN}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.runCtx.Done():
		return ErrIngestWALClosed
	}
}

func (s *IngestService) WaitCommitted(ctx context.Context, acceptance IngestAcceptance) (IngestResult, error) {
	if acceptance.pending == nil {
		return IngestResult{}, fmt.Errorf("invalid ingest acceptance")
	}
	force := ingestForceRequest{
		tenantID:   acceptance.tenantID,
		throughLSN: acceptance.acceptedLSN,
	}
	select {
	case s.forceCh <- force:
	case <-ctx.Done():
		return IngestResult{}, ctx.Err()
	case <-s.runCtx.Done():
		return IngestResult{}, ErrIngestWALClosed
	}
	if err := s.forceMetadataThrough(ctx, force.tenantID, force.throughLSN); err != nil {
		return IngestResult{}, err
	}
	return s.Wait(ctx, acceptance)
}
