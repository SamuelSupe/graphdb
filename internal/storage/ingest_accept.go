package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func (s *IngestService) Accept(ctx context.Context, tenantID string, request IngestRequest) (acceptance IngestAcceptance, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.ingest.accept", tenantTraceAttr(tenantID))
	defer func() { endStorageSpan(span, err) }()
	request, err = PrepareIngestRequest(tenantID, request)
	if err != nil {
		return IngestAcceptance{}, err
	}
	if store, ok := s.store.(ingestAdmissionStore); ok {
		release, err := store.beginIngestAcceptance(ctx, tenantID)
		if err != nil {
			return IngestAcceptance{}, err
		}
		defer release()
	}
	span.SetAttributes(
		attribute.String("graphdb.ingest.source", request.Source),
		attribute.Bool("graphdb.ingest.idempotency_key_present", request.IdempotencyKey != ""),
		attribute.Int("graphdb.ingest.items", len(request.Items)),
	)
	digestRequest := request
	if digestRequest.IdempotencyKey != "" {
		digestRequest.BatchID = ""
	}
	requestJSON, err := json.Marshal(digestRequest)
	if err != nil {
		return IngestAcceptance{}, err
	}
	digestSum := sha256.Sum256(requestJSON)
	digest := hex.EncodeToString(digestSum[:])
	identity := ingestRequestIdentity(tenantID, request)
	recordID := ingestRecordID(identity)
	statusKey := ingestStatusKey(tenantID, request.Source, request.CollectorID, request.BatchID)

	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return IngestAcceptance{}, ErrIngestWALClosed
		}
		if s.fatalErr != nil {
			err := s.fatalErr
			s.mu.Unlock()
			return IngestAcceptance{}, err
		}
		if pending := s.active[identity]; pending != nil {
			if pending.envelope.Digest != digest {
				s.mu.Unlock()
				return IngestAcceptance{}, fmt.Errorf("%w for source %q collector %q batch %q", ErrIngestIdentityConflict, request.Source, request.CollectorID, request.BatchID)
			}
			accepted := acceptanceFromPending(pending, s.config.OwnerID, s.config.WAL.Durability)
			s.mu.Unlock()
			return accepted, nil
		}
		if pending := s.activeByStatus[statusKey]; pending != nil && pending.state != IngestStateFailed {
			s.mu.Unlock()
			return IngestAcceptance{}, fmt.Errorf(
				"%w for source %q collector %q batch %q",
				ErrIngestIdentityConflict, request.Source, request.CollectorID, request.BatchID,
			)
		}
		if flight := s.accepting[identity]; flight != nil {
			if flight.digest != digest {
				s.mu.Unlock()
				return IngestAcceptance{}, fmt.Errorf("%w for source %q collector %q batch %q", ErrIngestIdentityConflict, request.Source, request.CollectorID, request.BatchID)
			}
			done := flight.done
			s.mu.Unlock()
			select {
			case <-done:
				if flight.err != nil {
					return IngestAcceptance{}, flight.err
				}
				continue
			case <-ctx.Done():
				return IngestAcceptance{}, ctx.Err()
			}
		}
		if s.acceptingStatus[statusKey] != nil {
			s.mu.Unlock()
			return IngestAcceptance{}, fmt.Errorf(
				"%w for source %q collector %q batch %q",
				ErrIngestIdentityConflict, request.Source, request.CollectorID, request.BatchID,
			)
		}
		flight := &ingestAcceptFlight{digest: digest, done: make(chan struct{}), retainLSN: s.highestLSN + 1}
		s.accepting[identity] = flight
		s.acceptingStatus[statusKey] = flight
		s.acceptors.Add(1)
		s.mu.Unlock()
		defer s.acceptors.Done()

		// A WAL on a separate filesystem can still have space when the data
		// filesystem cannot publish another batch. Refuse new work before
		// assigning a durable LSN, while allowing existing pending retries.
		if store, ok := s.store.(interface {
			CheckWriteDiskSpace(context.Context, int64) error
		}); ok {
			if err := store.CheckWriteDiskSpace(ctx, 0); err != nil {
				s.failAcceptFlight(identity, statusKey, flight, err)
				return IngestAcceptance{}, err
			}
		}

		generationCtx, cancelGeneration := context.WithTimeout(ctx, ingestWALGenerationCaptureTimeout)
		generation, generationErr := s.captureIngestWALGeneration(generationCtx, tenantID)
		cancelGeneration()
		if ctxErr := ctx.Err(); ctxErr != nil {
			s.failAcceptFlight(identity, statusKey, flight, ctxErr)
			return IngestAcceptance{}, ctxErr
		}
		if generationErr != nil {
			s.failAcceptFlight(identity, statusKey, flight, generationErr)
			return IngestAcceptance{}, generationErr
		}
		acceptedAt := time.Now().UTC()
		envelope := walIngestEnvelope{
			RecordID:           recordID,
			WriterID:           s.config.OwnerID,
			TenantID:           tenantID,
			Request:            request,
			Digest:             digest,
			AcceptedAt:         acceptedAt,
			AcceptedGeneration: generation,
			State:              IngestStateAccepted,
			Trace:              captureWALTraceContext(ctx),
		}
		payload, marshalErr := json.Marshal(envelope)
		if marshalErr != nil {
			s.failAcceptFlight(identity, statusKey, flight, marshalErr)
			return IngestAcceptance{}, marshalErr
		}
		recordBytes := int64(len(payload) + ingestWALHeaderBytes + ingestWALChecksumBytes)
		s.mu.Lock()
		if s.closed {
			delete(s.accepting, identity)
			if s.acceptingStatus[statusKey] == flight {
				delete(s.acceptingStatus, statusKey)
			}
			flight.err = ErrIngestWALClosed
			close(flight.done)
			s.mu.Unlock()
			return IngestAcceptance{}, ErrIngestWALClosed
		}
		if s.fatalErr != nil {
			delete(s.accepting, identity)
			if s.acceptingStatus[statusKey] == flight {
				delete(s.acceptingStatus, statusKey)
			}
			flight.err = s.fatalErr
			close(flight.done)
			err := s.fatalErr
			s.mu.Unlock()
			return IngestAcceptance{}, err
		}
		if s.pendingBytes+recordBytes > s.config.QueueMemoryBytes {
			delete(s.accepting, identity)
			if s.acceptingStatus[statusKey] == flight {
				delete(s.acceptingStatus, statusKey)
			}
			flight.err = ErrIngestQueueFull
			close(flight.done)
			s.mu.Unlock()
			return IngestAcceptance{}, ErrIngestQueueFull
		}
		s.pendingBytes += recordBytes
		s.mu.Unlock()

		checkpointGeneration := s.wal.checkpointGeneration.Load()
		appendResult, appendErr := s.wal.Append(ctx, IngestWALAccepted, payload)
		if errors.Is(appendErr, ErrIngestWALFull) {
			if pruneErr := s.prune(ctx, checkpointGeneration); pruneErr == nil {
				appendResult, appendErr = s.wal.Append(ctx, IngestWALAccepted, payload)
			}
		}
		s.mu.Lock()
		delete(s.accepting, identity)
		if s.acceptingStatus[statusKey] == flight {
			delete(s.acceptingStatus, statusKey)
		}
		if appendErr != nil {
			s.pendingBytes -= recordBytes
			if errors.Is(appendErr, ErrIngestWALFull) {
				s.walFull = true
			}
			walFailed := errors.Is(appendErr, ErrIngestWALFailed)
			if walFailed {
				s.walFailed = true
				s.fatalErr = appendErr
				s.lastError = appendErr.Error()
			}
			flight.err = appendErr
			close(flight.done)
			s.mu.Unlock()
			if walFailed {
				s.cancel()
			}
			if s.config.Logger != nil {
				fields := ingestTraceLogFields(envelope)
				fields["tenant"] = tenantID
				fields["source"] = request.Source
				fields["collector_id"] = request.CollectorID
				fields["batch_id"] = request.BatchID
				fields["record_bytes"] = recordBytes
				fields["error"] = appendErr.Error()
				s.config.Logger.Error("ingest_wal_accept_failed", fields)
			}
			return IngestAcceptance{}, appendErr
		}
		envelope.AcceptedLSN = appendResult.LSN
		s.walFull = false
		pending := &ingestPending{
			envelope:         envelope,
			acceptedLSN:      appendResult.LSN,
			acceptedSequence: appendResult.acceptedSequence,
			estimated:        acceptedAt.Add(s.config.FlushInterval),
			bytes:            recordBytes,
			state:            IngestStateAccepted,
			done:             make(chan struct{}),
		}
		if request.FullSync {
			pending.estimated = acceptedAt
		}
		s.active[identity] = pending
		s.activeByStatus[statusKey] = pending
		s.highestLSN = max(s.highestLSN, appendResult.LSN)
		if s.oldestPending.IsZero() || acceptedAt.Before(s.oldestPending) {
			s.oldestPending = acceptedAt
		}
		s.observeQueueLocked()
		close(flight.done)
		accepted := acceptanceFromPending(pending, s.config.OwnerID, s.config.WAL.Durability)
		s.mu.Unlock()
		span.SetAttributes(
			attribute.Int64("graphdb.ingest.wal.accepted_lsn", int64(appendResult.LSN)),
			attribute.String("graphdb.ingest.wal.durability", accepted.Durability),
			attribute.Int64("graphdb.ingest.wal.record_bytes", recordBytes),
		)
		if s.config.Logger != nil {
			fields := ingestTraceLogFields(envelope)
			fields["tenant"] = tenantID
			fields["source"] = request.Source
			fields["collector_id"] = request.CollectorID
			fields["batch_id"] = request.BatchID
			fields["lsn"] = appendResult.LSN
			fields["record_bytes"] = recordBytes
			fields["durability"] = accepted.Durability
			s.config.Logger.Info("ingest_wal_accepted", fields)
		}

		select {
		case s.enqueueCh <- pending:
			return accepted, nil
		case <-s.runCtx.Done():
			return accepted, nil
		}
	}
}

func (s *IngestService) captureIngestWALGeneration(ctx context.Context, tenantID string) (int64, error) {
	if store, ok := s.store.(ingestGenerationStore); ok {
		return store.CaptureIngestWALGeneration(ctx, tenantID)
	}
	return 0, nil
}

func (s *IngestService) failAcceptFlight(identity string, statusKey string, flight *ingestAcceptFlight, err error) {
	s.mu.Lock()
	if s.accepting[identity] == flight {
		delete(s.accepting, identity)
		if s.acceptingStatus[statusKey] == flight {
			delete(s.acceptingStatus, statusKey)
		}
		flight.err = err
		close(flight.done)
	}
	s.mu.Unlock()
}
