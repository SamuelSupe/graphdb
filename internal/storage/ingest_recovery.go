package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Only requests still active at the current WAL position retain their decoded
// data. Terminal history is released as it is scanned, even when an older
// request prevents its segments from being pruned.
type ingestRecovery struct {
	pending    map[string]*ingestPending
	live       ingestWALLiveRecords
	selective  bool
	highestLSN uint64
	records    int
}

func (r *ingestRecovery) apply(record IngestWALRecord) error {
	if r.pending == nil {
		r.pending = make(map[string]*ingestPending)
	}
	r.records++
	ref := ingestWALFrameRef{kind: record.Type, lsn: record.LSN, segment: record.Segment, offset: record.Offset}
	if record.Type == IngestWALPrepared {
		var batch walPreparedBatchEnvelope
		if err := json.Unmarshal(record.Payload, &batch); err == nil && len(batch.Items) > 0 {
			frameBytes := int64(len(record.Payload) + ingestWALHeaderBytes + ingestWALChecksumBytes)
			share := max(int64(1), frameBytes/int64(len(batch.Items)))
			for _, prepared := range batch.Items {
				pending := r.pending[prepared.RecordID]
				if pending == nil && r.selective {
					// A shared frame can also contain plans retired after it.
					continue
				}
				if pending == nil || prepared.Prepared == nil {
					return fmt.Errorf("%w: incomplete prepared batch at LSN %d", ErrIngestWALCorrupt, record.LSN)
				}
				pending.bytes += share
				pending.envelope.State = IngestStatePrepared
				pending.envelope.Prepared = prepared.Prepared
				pending.envelope.Result = &prepared.Prepared.Result
				pending.envelope.Error = ""
				pending.state = IngestStatePrepared
				r.live.update(prepared.RecordID, ref)
			}
			r.highestLSN = max(r.highestLSN, record.LSN)
			return nil
		}
	}
	var envelope walIngestEnvelope
	if err := json.Unmarshal(record.Payload, &envelope); err != nil {
		return fmt.Errorf("%w: decode LSN %d: %v", ErrIngestWALCorrupt, record.LSN, err)
	}
	if envelope.RecordID == "" || (record.Type == IngestWALAccepted && envelope.TenantID == "") {
		return fmt.Errorf("%w: incomplete envelope at LSN %d", ErrIngestWALCorrupt, record.LSN)
	}
	r.highestLSN = max(r.highestLSN, record.LSN)
	r.live.update(envelope.RecordID, ref)
	switch record.Type {
	case IngestWALAccepted:
		envelope.AcceptedLSN = record.LSN
		pending := &ingestPending{
			envelope:    envelope,
			acceptedLSN: record.LSN,
			estimated:   time.Now().UTC(),
			bytes:       int64(len(record.Payload) + ingestWALHeaderBytes + ingestWALChecksumBytes),
			state:       IngestStateAccepted,
			done:        make(chan struct{}),
		}
		if envelope.Prepared != nil {
			pending.state = IngestStatePrepared
			if pending.envelope.Result == nil {
				pending.envelope.Result = &envelope.Prepared.Result
			}
		}
		r.pending[envelope.RecordID] = pending
	case IngestWALPrepared, IngestWALPublished:
		if pending := r.pending[envelope.RecordID]; pending != nil {
			frameBytes := int64(len(record.Payload) + ingestWALHeaderBytes + ingestWALChecksumBytes)
			if frameBytes > pending.bytes {
				pending.bytes = frameBytes
			}
			pending.envelope.State = envelope.State
			if envelope.Prepared != nil {
				pending.envelope.Prepared = envelope.Prepared
			}
			if envelope.Result != nil {
				pending.envelope.Result = envelope.Result
			}
			pending.envelope.Error = envelope.Error
			pending.envelope.FinishedAt = envelope.FinishedAt
			pending.state = envelope.State
		}
	case IngestWALFinalized, IngestWALFailed:
		delete(r.pending, envelope.RecordID)
	}
	return nil
}

func (r *ingestRecovery) finish(s *IngestService) ([]*ingestPending, error) {
	s.highestLSN = r.highestLSN
	out := make([]*ingestPending, 0, len(r.pending))
	for _, pending := range r.pending {
		if pending.envelope.WriterID == "" {
			pending.envelope.WriterID = s.config.OwnerID
		} else if pending.envelope.WriterID != s.config.OwnerID {
			return nil, fmt.Errorf(
				"ingest WAL owner mismatch: volume belongs to %q, configured owner is %q",
				pending.envelope.WriterID,
				s.config.OwnerID,
			)
		}
		identity := ingestRequestIdentity(pending.envelope.TenantID, pending.envelope.Request)
		statusKey := ingestStatusKey(
			pending.envelope.TenantID,
			pending.envelope.Request.Source,
			pending.envelope.Request.CollectorID,
			pending.envelope.Request.BatchID,
		)
		if existing := s.active[identity]; existing != nil && existing.envelope.RecordID != pending.envelope.RecordID {
			return nil, fmt.Errorf("%w: duplicate active ingest identity in WAL", ErrIngestWALCorrupt)
		}
		if existing := s.activeByStatus[statusKey]; existing != nil && existing.envelope.RecordID != pending.envelope.RecordID {
			return nil, fmt.Errorf(
				"%w: source %q collector %q batch %q has multiple active WAL identities",
				ErrIngestWALCorrupt,
				pending.envelope.Request.Source,
				pending.envelope.Request.CollectorID,
				pending.envelope.Request.BatchID,
			)
		}
		s.active[identity] = pending
		s.activeByStatus[statusKey] = pending
		s.pendingBytes += pending.bytes
		if s.oldestPending.IsZero() || pending.envelope.AcceptedAt.Before(s.oldestPending) {
			s.oldestPending = pending.envelope.AcceptedAt
		}
		out = append(out, pending)
	}
	s.observeQueueLocked()
	sort.Slice(out, func(i, j int) bool {
		return out[i].acceptedLSN < out[j].acceptedLSN
	})
	return out, nil
}
