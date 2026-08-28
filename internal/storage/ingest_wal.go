package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

const (
	ingestWALMagic          = "GIWL"
	ingestWALFormatVersion  = uint16(1)
	ingestWALHeaderBytes    = 20
	ingestWALChecksumBytes  = 4
	ingestWALMaxPayload     = 64 * 1024 * 1024
	ingestWALFileExtension  = ".wal"
	ingestWALLockFile       = ".lock"
	IngestWALDurabilitySync = "sync"
	IngestWALDurabilityOS   = "os"
)

var (
	ErrIngestWALClosed  = errors.New("ingest WAL is closed")
	ErrIngestWALFull    = errors.New("ingest WAL disk limit reached")
	ErrIngestWALLocked  = errors.New("ingest WAL is locked by another process")
	ErrIngestWALCorrupt = errors.New("ingest WAL is corrupt")
)

type IngestWALRecordType uint8

const (
	IngestWALAccepted IngestWALRecordType = iota + 1
	IngestWALPrepared
	IngestWALPublished
	IngestWALFinalized
	IngestWALFailed
)

type IngestWALConfig struct {
	Dir           string
	Durability    string
	BufferBytes   int
	FsyncInterval time.Duration
	MaxBytes      int64
	SegmentBytes  int64
	AppendQueue   int
	Observer      IngestObserver
	Logger        IngestLogger
}

func DefaultIngestWALConfig(dir string) IngestWALConfig {
	return IngestWALConfig{
		Dir:           dir,
		Durability:    IngestWALDurabilitySync,
		BufferBytes:   4 * 1024 * 1024,
		FsyncInterval: 5 * time.Millisecond,
		MaxBytes:      10 * 1024 * 1024 * 1024,
		SegmentBytes:  256 * 1024 * 1024,
		AppendQueue:   4096,
	}
}

func (c IngestWALConfig) validate() error {
	if strings.TrimSpace(c.Dir) == "" {
		return fmt.Errorf("ingest WAL directory is required")
	}
	if c.Durability != IngestWALDurabilitySync && c.Durability != IngestWALDurabilityOS {
		return fmt.Errorf("unsupported ingest WAL durability %q", c.Durability)
	}
	if c.BufferBytes <= 0 || c.FsyncInterval <= 0 || c.MaxBytes <= 0 || c.SegmentBytes <= 0 || c.AppendQueue <= 0 {
		return fmt.Errorf("ingest WAL buffer, fsync interval, disk limits, and append queue must be positive")
	}
	if c.SegmentBytes > c.MaxBytes {
		return fmt.Errorf("ingest WAL segment size must not exceed the WAL disk limit")
	}
	return nil
}

func (c IngestWALConfig) Validate() error {
	return c.validate()
}

type IngestWALRecord struct {
	Type    IngestWALRecordType
	LSN     uint64
	Segment string
	Offset  int64
	Payload []byte
}

type IngestWALAppendResult struct {
	LSN     uint64
	Segment string
	Offset  int64
	Durable bool
}

type ingestWALAppendRequest struct {
	ctx     context.Context
	kind    IngestWALRecordType
	payload []byte
	done    chan ingestWALAppendResponse
}

type ingestWALAppendResponse struct {
	result IngestWALAppendResult
	err    error
}

type ingestWALPruneRequest struct {
	beforeLSN uint64
	done      chan error
}

type ingestWALSegment struct {
	path     string
	startLSN uint64
	maxLSN   uint64
	size     int64
}

type IngestWAL struct {
	config   IngestWALConfig
	lockFile *os.File

	appendCh chan ingestWALAppendRequest
	pruneCh  chan ingestWALPruneRequest
	closeCh  chan struct{}
	done     chan struct{}

	closeOnce sync.Once
}

func OpenIngestWAL(config IngestWALConfig) (*IngestWAL, []IngestWALRecord, error) {
	if err := config.validate(); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("create ingest WAL directory: %w", err)
	}
	if err := os.Chmod(config.Dir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("secure ingest WAL directory: %w", err)
	}
	lockFile, err := os.OpenFile(filepath.Join(config.Dir, ingestWALLockFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open ingest WAL process lock: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, nil, fmt.Errorf("%w: %v", ErrIngestWALLocked, err)
	}
	records, segments, nextLSN, totalBytes, err := recoverIngestWAL(config.Dir)
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return nil, nil, err
	}
	wal := &IngestWAL{
		config:   config,
		lockFile: lockFile,
		appendCh: make(chan ingestWALAppendRequest, config.AppendQueue),
		pruneCh:  make(chan ingestWALPruneRequest),
		closeCh:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	go wal.run(segments, nextLSN, totalBytes)
	return wal, records, nil
}

func (w *IngestWAL) Append(ctx context.Context, kind IngestWALRecordType, payload []byte) (result IngestWALAppendResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	started := time.Now()
	ctx, span := startStorageSpan(ctx, "graphdb.storage.ingest_wal.append",
		attribute.String("graphdb.ingest.wal.record_type", ingestWALRecordTypeName(kind)),
		attribute.Int("graphdb.ingest.wal.payload_bytes", len(payload)),
	)
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		if w.config.Observer != nil {
			w.config.Observer.RecordIngestWALAppend(
				ingestWALRecordTypeName(kind),
				status,
				len(payload)+ingestWALHeaderBytes+ingestWALChecksumBytes,
				time.Since(started),
			)
		}
		span.SetAttributes(
			attribute.Int64("graphdb.ingest.wal.lsn", int64(result.LSN)),
			attribute.Bool("graphdb.ingest.wal.durable", result.Durable),
		)
		endStorageSpan(span, err)
	}()
	if !validIngestWALRecordType(kind) {
		return IngestWALAppendResult{}, fmt.Errorf("unsupported ingest WAL record type %d", kind)
	}
	if len(payload) > ingestWALMaxPayload {
		return IngestWALAppendResult{}, fmt.Errorf("ingest WAL payload is too large: %d bytes", len(payload))
	}
	request := ingestWALAppendRequest{
		ctx:     ctx,
		kind:    kind,
		payload: append([]byte(nil), payload...),
		done:    make(chan ingestWALAppendResponse, 1),
	}
	select {
	case w.appendCh <- request:
	case <-ctx.Done():
		return IngestWALAppendResult{}, ctx.Err()
	case <-w.done:
		return IngestWALAppendResult{}, ErrIngestWALClosed
	}
	select {
	case response := <-request.done:
		return response.result, response.err
	case <-w.done:
		return IngestWALAppendResult{}, ErrIngestWALClosed
	}
}

// Prune removes closed segments whose records are all older than beforeLSN.
// The caller must only advance beforeLSN past requests that no longer need WAL recovery.
func (w *IngestWAL) Prune(ctx context.Context, beforeLSN uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	request := ingestWALPruneRequest{beforeLSN: beforeLSN, done: make(chan error, 1)}
	select {
	case w.pruneCh <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return ErrIngestWALClosed
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return ErrIngestWALClosed
	}
}

func (w *IngestWAL) Close() error {
	w.closeOnce.Do(func() { close(w.closeCh) })
	<-w.done
	if w.lockFile == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(w.lockFile.Fd()), syscall.LOCK_UN)
	closeErr := w.lockFile.Close()
	w.lockFile = nil
	return errors.Join(unlockErr, closeErr)
}

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
	if err := state.openCurrent(); err != nil {
		state.failPending(err)
		return
	}
	defer func() {
		_ = state.syncAndClose()
		state.failPending(ErrIngestWALClosed)
	}()
	for {
		select {
		case first := <-w.appendCh:
			state.writeGroup(first)
		case request := <-w.pruneCh:
			request.done <- state.prune(request.beforeLSN)
		case <-w.closeCh:
			return
		}
	}
}

type ingestWALWriterState struct {
	wal        *IngestWAL
	file       *os.File
	segments   []ingestWALSegment
	current    int
	nextLSN    uint64
	totalBytes int64
	writtenLSN uint64
	durableLSN uint64
}

func (s *ingestWALWriterState) openCurrent() error {
	if len(s.segments) == 0 {
		if err := s.createSegment(s.nextLSN); err != nil {
			return err
		}
	}
	s.current = len(s.segments) - 1
	path := s.segments[s.current].path
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
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

func (s *ingestWALWriterState) writeGroup(first ingestWALAppendRequest) {
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
	s.writeRequests(requests)
}

func (s *ingestWALWriterState) writeRequests(requests []ingestWALAppendRequest) {
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
			if err := s.file.Sync(); err != nil {
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
			request.done <- ingestWALAppendResponse{err: fmt.Errorf("ingest WAL record exceeds segment size")}
			continue
		}
		if s.totalBytes+int64(buffer.Len())+frameBytes > s.wal.config.MaxBytes {
			request.done <- ingestWALAppendResponse{err: ErrIngestWALFull}
			continue
		}
		if s.segments[s.current].size+int64(buffer.Len())+frameBytes > s.wal.config.SegmentBytes {
			if err := flush(); err != nil {
				failPending(err)
				s.failRequests(requests[index:], err)
				return
			}
			if err := s.rotate(); err != nil {
				s.failRequests(requests[index:], err)
				return
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
				failPending(err)
				s.failRequests(requests[index+1:], err)
				return
			}
		}
	}
	if err := flush(); err != nil {
		failPending(err)
	}
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
	return s.openCurrent()
}

func (s *ingestWALWriterState) prune(beforeLSN uint64) error {
	if beforeLSN == 0 {
		return nil
	}
	current := s.segments[s.current]
	if current.size > 0 && current.maxLSN < beforeLSN {
		if err := s.rotate(); err != nil {
			return err
		}
	}
	kept := make([]ingestWALSegment, 0, len(s.segments))
	removed := 0
	var reclaimed int64
	for index, segment := range s.segments {
		if index == s.current || segment.maxLSN == 0 || segment.maxLSN >= beforeLSN {
			kept = append(kept, segment)
			continue
		}
		if err := os.Remove(segment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
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
	if err := syncDirectory(s.wal.config.Dir); err != nil {
		return err
	}
	s.observeState(0)
	if removed > 0 && s.wal.config.Logger != nil {
		s.wal.config.Logger.Info("ingest_wal_segments_pruned", map[string]any{
			"segments":        removed,
			"reclaimed_bytes": reclaimed,
			"disk_bytes":      s.totalBytes,
			"before_lsn":      beforeLSN,
		})
	}
	return nil
}

func (s *ingestWALWriterState) syncAndClose() error {
	if s.file == nil {
		return nil
	}
	syncErr := s.file.Sync()
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

func recoverIngestWAL(dir string) ([]IngestWALRecord, []ingestWALSegment, uint64, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ingestWALFileExtension) {
			if _, ok := parseIngestWALSegmentName(entry.Name()); ok {
				paths = append(paths, filepath.Join(dir, entry.Name()))
			}
		}
	}
	sort.Strings(paths)
	records := make([]IngestWALRecord, 0)
	segments := make([]ingestWALSegment, 0, len(paths))
	var lastLSN uint64
	var totalBytes int64
	for index, path := range paths {
		startLSN, _ := parseIngestWALSegmentName(filepath.Base(path))
		isLast := index == len(paths)-1
		segmentRecords, size, err := recoverIngestWALSegment(path, isLast, lastLSN)
		if err != nil {
			return nil, nil, 0, 0, err
		}
		segment := ingestWALSegment{path: path, startLSN: startLSN, size: size}
		if len(segmentRecords) > 0 {
			segment.maxLSN = segmentRecords[len(segmentRecords)-1].LSN
			lastLSN = segment.maxLSN
		}
		records = append(records, segmentRecords...)
		segments = append(segments, segment)
		totalBytes += size
	}
	return records, segments, lastLSN + 1, totalBytes, nil
}

func recoverIngestWALSegment(path string, isLast bool, previousLSN uint64) ([]IngestWALRecord, int64, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	var offset int64
	var lastLSN = previousLSN
	records := make([]IngestWALRecord, 0)
	for offset < size {
		recordOffset := offset
		header := make([]byte, ingestWALHeaderBytes)
		read, readErr := io.ReadFull(file, header)
		if readErr != nil {
			if isLast && errors.Is(readErr, io.ErrUnexpectedEOF) {
				truncatedSize, truncateErr := truncateIngestWALTail(file, path, recordOffset)
				return records, truncatedSize, truncateErr
			}
			return nil, 0, fmt.Errorf("%w: read header at %s:%d: %v", ErrIngestWALCorrupt, path, recordOffset, readErr)
		}
		offset += int64(read)
		if string(header[:4]) != ingestWALMagic || binary.BigEndian.Uint16(header[4:6]) != ingestWALFormatVersion {
			return nil, 0, fmt.Errorf("%w: invalid header at %s:%d", ErrIngestWALCorrupt, path, recordOffset)
		}
		kind := IngestWALRecordType(header[6])
		lsn := binary.BigEndian.Uint64(header[8:16])
		payloadBytes := binary.BigEndian.Uint32(header[16:20])
		if !validIngestWALRecordType(kind) || payloadBytes > ingestWALMaxPayload || lsn != lastLSN+1 {
			return nil, 0, fmt.Errorf("%w: invalid record metadata at %s:%d", ErrIngestWALCorrupt, path, recordOffset)
		}
		body := make([]byte, int(payloadBytes)+ingestWALChecksumBytes)
		read, readErr = io.ReadFull(file, body)
		if readErr != nil {
			if isLast && errors.Is(readErr, io.ErrUnexpectedEOF) {
				truncatedSize, truncateErr := truncateIngestWALTail(file, path, recordOffset)
				return records, truncatedSize, truncateErr
			}
			return nil, 0, fmt.Errorf("%w: read payload at %s:%d: %v", ErrIngestWALCorrupt, path, recordOffset, readErr)
		}
		offset += int64(read)
		payload := body[:payloadBytes]
		wantCRC := binary.BigEndian.Uint32(body[payloadBytes:])
		checksumInput := append(append([]byte(nil), header...), payload...)
		if crc32.Checksum(checksumInput, crc32.MakeTable(crc32.Castagnoli)) != wantCRC {
			return nil, 0, fmt.Errorf("%w: checksum mismatch at %s:%d", ErrIngestWALCorrupt, path, recordOffset)
		}
		records = append(records, IngestWALRecord{
			Type:    kind,
			LSN:     lsn,
			Segment: filepath.Base(path),
			Offset:  recordOffset,
			Payload: append([]byte(nil), payload...),
		})
		lastLSN = lsn
	}
	return records, size, nil
}

func truncateIngestWALTail(file *os.File, path string, size int64) (int64, error) {
	if err := file.Truncate(size); err != nil {
		return 0, fmt.Errorf("truncate incomplete ingest WAL tail %q: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}
	return size, nil
}

func encodeIngestWALFrame(kind IngestWALRecordType, lsn uint64, payload []byte) []byte {
	frame := make([]byte, ingestWALHeaderBytes+len(payload)+ingestWALChecksumBytes)
	copy(frame[:4], ingestWALMagic)
	binary.BigEndian.PutUint16(frame[4:6], ingestWALFormatVersion)
	frame[6] = byte(kind)
	binary.BigEndian.PutUint64(frame[8:16], lsn)
	binary.BigEndian.PutUint32(frame[16:20], uint32(len(payload)))
	copy(frame[ingestWALHeaderBytes:], payload)
	checksumOffset := ingestWALHeaderBytes + len(payload)
	binary.BigEndian.PutUint32(
		frame[checksumOffset:],
		crc32.Checksum(frame[:checksumOffset], crc32.MakeTable(crc32.Castagnoli)),
	)
	return frame
}

func validIngestWALRecordType(kind IngestWALRecordType) bool {
	return kind >= IngestWALAccepted && kind <= IngestWALFailed
}

func ingestWALRecordTypeName(kind IngestWALRecordType) string {
	switch kind {
	case IngestWALAccepted:
		return "accepted"
	case IngestWALPrepared:
		return "prepared"
	case IngestWALPublished:
		return "published"
	case IngestWALFinalized:
		return "finalized"
	case IngestWALFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func ingestWALSegmentName(startLSN uint64) string {
	return fmt.Sprintf("%020d%s", startLSN, ingestWALFileExtension)
}

func parseIngestWALSegmentName(name string) (uint64, bool) {
	base := strings.TrimSuffix(name, ingestWALFileExtension)
	if base == name || len(base) != 20 {
		return 0, false
	}
	value, err := strconv.ParseUint(base, 10, 64)
	return value, err == nil && value > 0
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
