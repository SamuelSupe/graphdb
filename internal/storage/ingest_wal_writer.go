package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func (w *IngestWAL) run(segments []ingestWALSegment, nextLSN uint64, totalBytes int64) {
	defer close(w.done)
	state := ingestWALWriterState{
		wal:        w,
		segments:   segments,
		nextLSN:    nextLSN,
		totalBytes: totalBytes,
		writtenLSN: nextLSN - 1,
		durableLSN: nextLSN - 1,
	}
	_, checkpointErr := os.Stat(filepath.Join(w.config.Dir, ingestWALCheckpointFile))
	state.checkpointRequired = checkpointErr == nil
	err := state.openCurrent()
	if err == nil {
		err = state.initializeDurableTail()
	}
	if err != nil {
		if state.file != nil {
			err = errors.Join(err, state.file.Close())
		}
		if state.tailFile != nil {
			err = errors.Join(err, state.tailFile.Close())
		}
		w.recordError(err)
		w.ready <- err
		state.failPending(err)
		return
	}
	w.ready <- nil
	defer func() {
		closeErr := state.syncAndClose()
		w.recordError(closeErr)
		state.failPending(w.unavailableError())
	}()
	for {
		select {
		case first := <-w.appendCh:
			if err := state.writeGroup(first); err != nil {
				state.failPending(err)
				return
			}
		case request := <-w.pruneCh:
			var err error
			if request.checkpoint {
				if request.generation >= w.checkpointGeneration.Load() {
					err = state.checkpoint()
					if err == nil {
						w.checkpointGeneration.Add(1)
					}
				}
			} else {
				err = state.prune(request.beforeLSN)
			}
			if errors.Is(err, ErrIngestWALFailed) {
				w.recordError(err)
				request.done <- err
				state.failPending(err)
				return
			}
			request.done <- err
		case <-w.closeCh:
			return
		}
	}
}

type ingestWALWriterState struct {
	checkpointRequired bool
	tailFile           *os.File
	tailSequence       uint64
	wal                *IngestWAL
	file               ingestWALWriteFile
	segments           []ingestWALSegment
	current            int
	nextLSN            uint64
	totalBytes         int64
	writtenLSN         uint64
	durableLSN         uint64
	acceptedSequence   uint64
}

func (s *ingestWALWriterState) openCurrent() error {
	if len(s.segments) == 0 {
		if err := s.createSegment(s.nextLSN); err != nil {
			return err
		}
	}
	s.current = len(s.segments) - 1
	path := s.segments[s.current].path
	openWriterFile := s.wal.config.openWriterFile
	if openWriterFile == nil {
		openWriterFile = func(path string) (ingestWALWriteFile, error) {
			return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		}
	}
	file, err := openWriterFile(path)
	if err != nil {
		return fmt.Errorf("open ingest WAL segment %q: %w", path, err)
	}
	s.file = file
	s.observeState(0)
	return nil
}

func (s *ingestWALWriterState) createSegment(startLSN uint64) error {
	path := filepath.Join(s.wal.config.Dir, ingestWALSegmentName(startLSN))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create ingest WAL segment %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := syncDirectory(s.wal.config.Dir); err != nil {
		return fmt.Errorf("sync ingest WAL directory: %w", err)
	}
	s.segments = append(s.segments, ingestWALSegment{path: path, startLSN: startLSN})
	return nil
}

func (s *ingestWALWriterState) writeGroup(first ingestWALAppendRequest) error {
	requests := []ingestWALAppendRequest{first}
	payloadBytes := len(first.payload)
	timer := time.NewTimer(s.wal.config.FsyncInterval)
collect:
	for payloadBytes < s.wal.config.BufferBytes {
		select {
		case request := <-s.wal.appendCh:
			requests = append(requests, request)
			payloadBytes += len(request.payload)
		case <-timer.C:
			break collect
		case <-s.wal.closeCh:
			break collect
		}
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	return s.writeRequests(requests)
}

func (s *ingestWALWriterState) writeRequests(requests []ingestWALAppendRequest) error {
	pending := make([]ingestWALAppendRequest, 0, len(requests))
	results := make([]IngestWALAppendResult, 0, len(requests))
	var buffer bytes.Buffer
	flush := func() error {
		if buffer.Len() == 0 {
			return nil
		}
		groupStarted := time.Now()
		groupBytes := buffer.Len()
		groupRecords := len(pending)
		groupSpan := startIngestWALGroupSpan(pending, groupBytes, s.wal.config.Durability)
		if len(results) > 0 {
			groupSpan.SetAttributes(
				attribute.Int64("graphdb.ingest.wal.first_lsn", int64(results[0].LSN)),
				attribute.Int64("graphdb.ingest.wal.last_lsn", int64(results[len(results)-1].LSN)),
			)
		}
		s.observeState(groupBytes)
		written, err := s.file.Write(buffer.Bytes())
		if err == nil && written != buffer.Len() {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.observeSync("error", groupRecords, groupBytes, time.Since(groupStarted), err)
			endStorageSpan(groupSpan, err)
			return err
		}
		s.segments[s.current].size += int64(written)
		s.totalBytes += int64(written)
		if s.wal.config.Durability == IngestWALDurabilitySync {
			if err := syncStorageFile(s.file); err != nil {
				s.observeSync("error", groupRecords, groupBytes, time.Since(groupStarted), err)
				endStorageSpan(groupSpan, err)
				return err
			}
			// The boundary becomes durable after the frames, before any ACK.
			// Recovery may discard a corrupt suffix only beyond this proof.
			if err := s.persistDurableTail(results[len(results)-1].LSN); err != nil {
				s.observeSync("error", groupRecords, groupBytes, time.Since(groupStarted), err)
				endStorageSpan(groupSpan, err)
				return err
			}
		}
		if len(results) > 0 {
			s.writtenLSN = results[len(results)-1].LSN
			if s.wal.config.Durability == IngestWALDurabilitySync {
				s.durableLSN = s.writtenLSN
			}
		}
		syncStatus := "ok"
		if s.wal.config.Durability != IngestWALDurabilitySync {
			syncStatus = "os"
		}
		s.observeSync(syncStatus, groupRecords, groupBytes, time.Since(groupStarted), nil)
		s.observeState(0)
		endStorageSpan(groupSpan, nil)
		for i, request := range pending {
			result := results[i]
			result.Durable = s.wal.config.Durability == IngestWALDurabilitySync
			if request.kind == IngestWALAccepted {
				s.acceptedSequence++
				result.acceptedSequence = s.acceptedSequence
			}
			request.done <- ingestWALAppendResponse{result: result}
		}
		buffer.Reset()
		pending = pending[:0]
		results = results[:0]
		return nil
	}
	failPending := func(err error) {
		for _, request := range pending {
			request.done <- ingestWALAppendResponse{err: err}
		}
		pending = pending[:0]
		results = results[:0]
		buffer.Reset()
	}

	for index, request := range requests {
		if err := request.ctx.Err(); err != nil {
			request.done <- ingestWALAppendResponse{err: err}
			continue
		}
		frameBytes := int64(ingestWALHeaderBytes + len(request.payload) + ingestWALChecksumBytes)
		if frameBytes > s.wal.config.SegmentBytes {
			request.done <- ingestWALAppendResponse{err: fmt.Errorf(
				"%w: record=%d segment=%d",
				ErrIngestWALRecordExceedsSegment,
				frameBytes,
				s.wal.config.SegmentBytes,
			)}
			continue
		}
		limit := s.wal.config.MaxBytes
		if request.kind == IngestWALAccepted {
			limit -= s.wal.config.controlReserveBytes()
		}
		if s.totalBytes+int64(buffer.Len())+frameBytes > limit {
			request.done <- ingestWALAppendResponse{err: ErrIngestWALFull}
			continue
		}
		if s.segments[s.current].size+int64(buffer.Len())+frameBytes > s.wal.config.SegmentBytes {
			if err := flush(); err != nil {
				fatalErr := errors.Join(ErrIngestWALFailed, err)
				s.wal.recordError(fatalErr)
				failPending(fatalErr)
				s.failRequests(requests[index:], fatalErr)
				return fatalErr
			}
			if err := s.rotate(); err != nil {
				fatalErr := errors.Join(ErrIngestWALFailed, err)
				s.wal.recordError(fatalErr)
				s.failRequests(requests[index:], fatalErr)
				return fatalErr
			}
		}
		lsn := s.nextLSN
		offset := s.segments[s.current].size + int64(buffer.Len())
		frame := encodeIngestWALFrame(request.kind, lsn, request.payload)
		_, _ = buffer.Write(frame)
		s.nextLSN++
		s.segments[s.current].maxLSN = lsn
		pending = append(pending, request)
		results = append(results, IngestWALAppendResult{
			LSN:     lsn,
			Segment: filepath.Base(s.segments[s.current].path),
			Offset:  offset,
		})
		if buffer.Len() >= s.wal.config.BufferBytes {
			if err := flush(); err != nil {
				fatalErr := errors.Join(ErrIngestWALFailed, err)
				s.wal.recordError(fatalErr)
				failPending(fatalErr)
				s.failRequests(requests[index+1:], fatalErr)
				return fatalErr
			}
		}
	}
	if err := flush(); err != nil {
		fatalErr := errors.Join(ErrIngestWALFailed, err)
		s.wal.recordError(fatalErr)
		failPending(fatalErr)
		return fatalErr
	}
	return nil
}

func (s *ingestWALWriterState) failRequests(requests []ingestWALAppendRequest, err error) {
	for _, request := range requests {
		request.done <- ingestWALAppendResponse{err: err}
	}
}

func (s *ingestWALWriterState) rotate() error {
	if err := s.syncAndClose(); err != nil {
		return err
	}
	if err := s.createSegment(s.nextLSN); err != nil {
		return err
	}
	if s.wal.config.Logger != nil {
		s.wal.config.Logger.Info("ingest_wal_segment_rotated", map[string]any{
			"start_lsn":  s.nextLSN,
			"disk_bytes": s.totalBytes,
		})
	}
	if err := s.openCurrent(); err != nil {
		return err
	}
	// Pruning may immediately remove the previous segment. Move the durable
	// boundary to the new empty segment before its predecessor can disappear.
	return s.persistDurableTail(s.writtenLSN)
}

func (s *ingestWALWriterState) prune(beforeLSN uint64) error {
	if beforeLSN == 0 {
		return nil
	}
	current := s.segments[s.current]
	if current.size > 0 && current.maxLSN < beforeLSN {
		if err := s.rotate(); err != nil {
			return errors.Join(ErrIngestWALFailed, err)
		}
	}
	kept := make([]ingestWALSegment, 0, len(s.segments))
	removed := 0
	var reclaimed int64
	var removeErr error
	for index, segment := range s.segments {
		if index == s.current || segment.maxLSN == 0 || segment.maxLSN >= beforeLSN {
			kept = append(kept, segment)
			continue
		}
		if err := os.Remove(segment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			kept = append(kept, s.segments[index:]...)
			removeErr = err
			break
		}
		s.totalBytes -= segment.size
		reclaimed += segment.size
		removed++
	}
	currentPath := s.segments[s.current].path
	s.segments = kept
	for index := range s.segments {
		if s.segments[index].path == currentPath {
			s.current = index
			break
		}
	}
	syncErr := syncDirectory(s.wal.config.Dir)
	s.observeState(0)
	if removed > 0 && s.wal.config.Logger != nil {
		s.wal.config.Logger.Info("ingest_wal_segments_pruned", map[string]any{
			"segments":        removed,
			"reclaimed_bytes": reclaimed,
			"disk_bytes":      s.totalBytes,
			"before_lsn":      beforeLSN,
		})
	}
	return errors.Join(removeErr, syncErr)
}

func (s *ingestWALWriterState) syncAndClose() (err error) {
	defer func() {
		if s.tailFile != nil {
			err = errors.Join(err, s.tailFile.Close())
			s.tailFile = nil
		}
	}()
	if s.file == nil {
		return nil
	}
	syncErr := syncStorageFile(s.file)
	if syncErr == nil && s.writtenLSN != s.durableLSN {
		syncErr = s.persistDurableTail(s.writtenLSN)
	}
	closeErr := s.file.Close()
	s.file = nil
	return errors.Join(syncErr, closeErr)
}

func (s *ingestWALWriterState) failPending(err error) {
	for {
		select {
		case request := <-s.wal.appendCh:
			request.done <- ingestWALAppendResponse{err: err}
		case request := <-s.wal.pruneCh:
			request.done <- err
		default:
			return
		}
	}
}

func (s *ingestWALWriterState) observeSync(status string, records int, bytes int, duration time.Duration, syncErr error) {
	if s.wal.config.Observer != nil {
		s.wal.config.Observer.RecordIngestWALSync(status, records, bytes, duration)
	}
	if status == "error" && s.wal.config.Logger != nil {
		s.wal.config.Logger.Error("ingest_wal_sync_failed", map[string]any{
			"records":     records,
			"bytes":       bytes,
			"error":       syncErr.Error(),
			"duration_ms": duration.Milliseconds(),
		})
	}
}

func (s *ingestWALWriterState) observeState(bufferBytes int) {
	if s.wal.config.Observer != nil {
		s.wal.config.Observer.RecordIngestWALState(
			bufferBytes,
			s.totalBytes,
			s.writtenLSN,
			s.durableLSN,
		)
	}
}
