package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/dirlock"
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
	ErrIngestWALClosed               = errors.New("ingest WAL is closed")
	ErrIngestWALFull                 = errors.New("ingest WAL disk limit reached")
	ErrIngestWALLocked               = errors.New("ingest WAL is locked by another process")
	ErrIngestWALCorrupt              = errors.New("ingest WAL is corrupt")
	ErrIngestWALFailed               = errors.New("ingest WAL writer failed")
	ErrIngestWALRecordTooLarge       = errors.New("ingest WAL payload is too large")
	ErrIngestWALRecordExceedsSegment = errors.New("ingest WAL record exceeds segment size")
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
	DiskSpace     DiskSpacePolicy
	Dir           string
	Durability    string
	BufferBytes   int
	FsyncInterval time.Duration
	MaxBytes      int64
	// ControlReserveBytes keeps disk space available for PREPARED and terminal
	// records after client admission has stopped. OpenIngestService derives a
	// bounded reserve when this is zero.
	ControlReserveBytes int64
	SegmentBytes        int64
	AppendQueue         int
	Observer            IngestObserver
	Logger              IngestLogger
	openWriterFile      func(string) (ingestWALWriteFile, error)
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
	if err := c.DiskSpace.Validate(); err != nil {
		return err
	}
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
	if c.ControlReserveBytes < 0 || c.ControlReserveBytes >= c.MaxBytes {
		return fmt.Errorf("ingest WAL control reserve must be non-negative and smaller than the WAL disk limit")
	}
	return nil
}

func (c IngestWALConfig) controlReserveBytes() int64 {
	if c.ControlReserveBytes > 0 {
		return c.ControlReserveBytes
	}
	return 0
}

func defaultIngestWALControlReserve(c IngestWALConfig) int64 {
	reserve := c.MaxBytes / 10
	if reserve > c.SegmentBytes {
		reserve = c.SegmentBytes
	}
	reserve = max(reserve, int64(ingestWALHeaderBytes+ingestWALChecksumBytes+1))
	return min(reserve, c.MaxBytes-1)
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

	acceptedSequence uint64
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

type ingestWALBatchRecord struct {
	kind    IngestWALRecordType
	payload []byte
}

type ingestWALPruneRequest struct {
	beforeLSN  uint64
	checkpoint bool
	generation uint64
	done       chan error
}

type ingestWALSegment struct {
	path     string
	startLSN uint64
	maxLSN   uint64
	size     int64
}

type ingestWALWriteFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type IngestWAL struct {
	checkpointGeneration atomic.Uint64
	recoveredLSN         uint64
	config               IngestWALConfig
	lockFile             *os.File

	appendCh chan ingestWALAppendRequest
	pruneCh  chan ingestWALPruneRequest
	closeCh  chan struct{}
	done     chan struct{}
	ready    chan error

	closeOnce sync.Once
	errMu     sync.Mutex
	closeErr  error
}

// OpenIngestWAL replays validated records while holding the process lock,
// before starting the writer. Replay must release completed history as it goes
// to keep recovery memory proportional to outstanding requests.
func OpenIngestWAL(config IngestWALConfig, replay func(IngestWALRecord) error) (*IngestWAL, error) {
	if replay == nil {
		return nil, fmt.Errorf("ingest WAL recovery callback is required")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if err := ensureDurableDirectoryMode(config.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("create ingest WAL directory: %w", err)
	}
	if err := os.Chmod(config.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("secure ingest WAL directory: %w", err)
	}
	lockFile, err := dirlock.Acquire(filepath.Join(config.Dir, ingestWALLockFile))
	if err != nil {
		if errors.Is(err, dirlock.ErrLocked) {
			return nil, fmt.Errorf("%w: %v", ErrIngestWALLocked, err)
		}
		return nil, fmt.Errorf("open ingest WAL process lock: %w", err)
	}
	if err := cleanupIngestWALTemps(config.Dir); err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return nil, err
	}
	segments, nextLSN, totalBytes, err := scanIngestWAL(config.Dir, replay)
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return nil, err
	}
	wal := &IngestWAL{
		recoveredLSN: nextLSN - 1,
		config:       config,
		lockFile:     lockFile,
		appendCh:     make(chan ingestWALAppendRequest, config.AppendQueue),
		pruneCh:      make(chan ingestWALPruneRequest),
		closeCh:      make(chan struct{}),
		done:         make(chan struct{}),
		ready:        make(chan error, 1),
	}
	go wal.run(segments, nextLSN, totalBytes)
	if err := <-wal.ready; err != nil {
		closeErr := wal.Close()
		if closeErr == nil {
			closeErr = err
		}
		return nil, fmt.Errorf("open ingest WAL writer: %w", closeErr)
	}
	return wal, nil
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
		return IngestWALAppendResult{}, fmt.Errorf("%w: %d bytes", ErrIngestWALRecordTooLarge, len(payload))
	}
	if kind == IngestWALAccepted && w.config.DiskSpace.enabled() {
		status, spaceErr := inspectDiskSpace(ctx, w.config.Dir, w.config.DiskSpace, nil)
		if spaceErr != nil {
			return result, spaceErr
		}
		if spaceErr = checkAvailableDiskSpace(status, int64(len(payload)+ingestWALHeaderBytes+ingestWALChecksumBytes), false); spaceErr != nil {
			return result, spaceErr
		}
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
		return IngestWALAppendResult{}, w.unavailableError()
	}
	select {
	case response := <-request.done:
		return response.result, response.err
	case <-w.done:
		return IngestWALAppendResult{}, w.unavailableError()
	}
}

func (w *IngestWAL) appendBatch(ctx context.Context, records []ingestWALBatchRecord) []ingestWALAppendResponse {
	responses := make([]ingestWALAppendResponse, len(records))
	if len(records) == 0 {
		return responses
	}
	if ctx == nil {
		ctx = context.Background()
	}
	requests := make([]ingestWALAppendRequest, len(records))
	for index, record := range records {
		if !validIngestWALRecordType(record.kind) {
			err := fmt.Errorf("unsupported ingest WAL record type %d", record.kind)
			for responseIndex := range responses {
				responses[responseIndex].err = err
			}
			return responses
		}
		if len(record.payload) > ingestWALMaxPayload {
			err := fmt.Errorf("%w: %d bytes", ErrIngestWALRecordTooLarge, len(record.payload))
			for responseIndex := range responses {
				responses[responseIndex].err = err
			}
			return responses
		}
		requests[index] = ingestWALAppendRequest{
			ctx:     ctx,
			kind:    record.kind,
			payload: append([]byte(nil), record.payload...),
			done:    make(chan ingestWALAppendResponse, 1),
		}
	}

	started := time.Now()
	enqueued := 0
	enqueueStopped := false
	for index := range requests {
		select {
		case w.appendCh <- requests[index]:
			enqueued++
		case <-ctx.Done():
			for remaining := index; remaining < len(responses); remaining++ {
				responses[remaining].err = ctx.Err()
			}
			enqueueStopped = true
		case <-w.done:
			for remaining := index; remaining < len(responses); remaining++ {
				responses[remaining].err = w.unavailableError()
			}
			enqueueStopped = true
		}
		if enqueueStopped {
			break
		}
	}
	for index := 0; index < enqueued; index++ {
		select {
		case responses[index] = <-requests[index].done:
		case <-w.done:
			responses[index].err = w.unavailableError()
		}
	}
	for index, record := range records {
		if w.config.Observer != nil {
			status := "ok"
			if responses[index].err != nil {
				status = "error"
			}
			w.config.Observer.RecordIngestWALAppend(
				ingestWALRecordTypeName(record.kind),
				status,
				len(record.payload)+ingestWALHeaderBytes+ingestWALChecksumBytes,
				time.Since(started),
			)
		}
	}
	return responses
}

// Prune removes closed segments whose records are all older than beforeLSN.
// The caller must only advance beforeLSN past requests that no longer need WAL recovery.
func (w *IngestWAL) Prune(ctx context.Context, beforeLSN uint64) error {
	return w.prune(ctx, beforeLSN, false)
}

func (w *IngestWAL) prune(ctx context.Context, beforeLSN uint64, checkpoint bool, since ...uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	generation := w.checkpointGeneration.Load()
	if len(since) > 0 {
		generation = since[0]
	}
	request := ingestWALPruneRequest{beforeLSN: beforeLSN, checkpoint: checkpoint, generation: generation, done: make(chan error, 1)}
	select {
	case w.pruneCh <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return w.unavailableError()
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return w.unavailableError()
	}
}

func (w *IngestWAL) Close() error {
	w.closeOnce.Do(func() {
		close(w.closeCh)
		<-w.done
		var unlockErr error
		var lockCloseErr error
		if w.lockFile != nil {
			unlockErr = syscall.Flock(int(w.lockFile.Fd()), syscall.LOCK_UN)
			lockCloseErr = w.lockFile.Close()
			w.lockFile = nil
		}
		w.recordError(errors.Join(unlockErr, lockCloseErr))
	})
	return w.currentError()
}

func (w *IngestWAL) recordError(err error) {
	if err == nil {
		return
	}
	w.errMu.Lock()
	w.closeErr = errors.Join(w.closeErr, err)
	w.errMu.Unlock()
}

func (w *IngestWAL) currentError() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.closeErr
}

func (w *IngestWAL) unavailableError() error {
	if err := w.currentError(); err != nil {
		return errors.Join(ErrIngestWALClosed, err)
	}
	return ErrIngestWALClosed
}
