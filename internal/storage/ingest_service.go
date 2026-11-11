package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/ingest"
	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrIngestQueueFull        = errors.New("ingest WAL memory queue limit reached")
	ErrIngestIdentityConflict = ingest.ErrIngestIdentityConflict
)

const (
	IngestStateAccepted  = "accepted"
	IngestStatePrepared  = "prepared"
	IngestStatePublished = "published"
	IngestStateCommitted = "committed"
	IngestStateRetrying  = "retrying"
	IngestStateFailed    = "failed"
)

type IngestServiceConfig struct {
	OwnerID          string
	WAL              IngestWALConfig
	QueueMemoryBytes int64
	FlushInterval    time.Duration
	FlushMaxRequests int
	FlushMaxBytes    int64
	FlushWorkers     int
	FlushTimeout     time.Duration
	RetryInterval    time.Duration
	Observer         IngestObserver
	Logger           IngestLogger
	OnGraphPublished func(string)
}

func DefaultIngestServiceConfig(walDir string) IngestServiceConfig {
	return IngestServiceConfig{
		WAL:              DefaultIngestWALConfig(walDir),
		QueueMemoryBytes: 256 * 1024 * 1024,
		FlushInterval:    10 * time.Second,
		FlushMaxRequests: 256,
		FlushMaxBytes:    8 * 1024 * 1024,
		FlushWorkers:     1,
		FlushTimeout:     90 * time.Second,
		RetryInterval:    time.Second,
	}
}

func (c IngestServiceConfig) validate() error {
	if err := c.WAL.validate(); err != nil {
		return err
	}
	if c.OwnerID != "" {
		if err := ingest.ValidateStatusPathIdentifier("ingest WAL owner ID", c.OwnerID); err != nil {
			return err
		}
	}
	if c.QueueMemoryBytes <= 0 || c.FlushInterval <= 0 || c.FlushMaxRequests <= 0 ||
		c.FlushMaxBytes <= 0 || c.FlushWorkers <= 0 || c.FlushTimeout <= 0 || c.RetryInterval <= 0 {
		return fmt.Errorf("ingest WAL queue and flush limits must be positive")
	}
	reserve := c.WAL.ControlReserveBytes
	if reserve == 0 {
		reserve = defaultIngestWALControlReserve(c.WAL)
	}
	if c.QueueMemoryBytes >= c.WAL.MaxBytes-reserve {
		return fmt.Errorf("ingest memory queue must be smaller than WAL payload capacity")
	}
	return nil
}

func (c IngestServiceConfig) Validate() error {
	return c.validate()
}

type IngestAcceptance struct {
	WriterID       string    `json:"writer_id,omitempty"`
	BatchID        string    `json:"batch_id"`
	Source         string    `json:"source"`
	CollectorID    string    `json:"collector_id"`
	State          string    `json:"state"`
	Durability     string    `json:"durability"`
	AcceptedAt     time.Time `json:"accepted_at"`
	EstimatedFlush time.Time `json:"estimated_flush_at"`
	acceptedLSN    uint64
	recordID       string
	completion     <-chan struct{}
	pending        *ingestPending
}

type IngestBatchStatus struct {
	WriterID         string        `json:"writer_id,omitempty"`
	TenantID         string        `json:"tenant_id"`
	Source           string        `json:"source"`
	CollectorID      string        `json:"collector_id"`
	BatchID          string        `json:"batch_id"`
	State            string        `json:"state"`
	Durability       string        `json:"durability"`
	AcceptedLSN      uint64        `json:"accepted_lsn,omitempty"`
	AcceptedAt       time.Time     `json:"accepted_at,omitempty"`
	EstimatedFlush   time.Time     `json:"estimated_flush_at,omitempty"`
	FinishedAt       time.Time     `json:"finished_at,omitempty"`
	ReceiptExpiresAt *time.Time    `json:"receipt_expires_at,omitempty"`
	Result           *IngestResult `json:"result,omitempty"`
	LastError        string        `json:"last_error,omitempty"`
	RecoveryPending  bool          `json:"recovery_pending,omitempty"`
}

type IngestServiceReadiness struct {
	WriterID     string    `json:"writer_id,omitempty"`
	Ready        bool      `json:"ready"`
	Writable     bool      `json:"writable"`
	Recovered    bool      `json:"recovered"`
	Pending      int       `json:"pending"`
	PendingBytes int64     `json:"pending_bytes"`
	Oldest       time.Time `json:"oldest_pending_at,omitempty"`
	LastError    string    `json:"last_error,omitempty"`
}

type IngestStore interface {
	SetIngestBarrier(func(context.Context, string) error)
	GetIngestBatch(context.Context, string, string, string, string) (IngestBatchRecord, error)
	IngestDurableBatchWithHooks(context.Context, string, []IngestBatchEntry, IngestBatchHooks) ([]IngestResult, error)
}

type ingestFailureStore interface {
	PersistIngestFailure(context.Context, string, IngestRequest, IngestResult, time.Time, time.Time) error
}

type ingestFailureResolver interface {
	ResolveIngestFailure(context.Context, string, IngestRequest, IngestResult, time.Time, time.Time) (IngestResult, error)
}

type ingestAttemptFailureStore interface {
	GetIngestAttemptFailure(context.Context, string, string, string, string, string) (IngestBatchRecord, error)
	PersistIngestAttemptFailure(context.Context, string, string, IngestRequest, IngestResult, time.Time, time.Time) error
}

type ingestAttemptFailureResolver interface {
	ResolveIngestAttemptFailure(context.Context, string, string, IngestRequest, IngestResult, time.Time, time.Time) error
}

type ingestGenerationStore interface {
	CaptureIngestWALGeneration(context.Context, string) (int64, error)
}

type ingestAdmissionStore interface {
	beginIngestAcceptance(context.Context, string) (func(), error)
}

const (
	ingestWALGenerationCaptureTimeout = 25 * time.Millisecond
)

type walIngestEnvelope struct {
	RecordID           string                 `json:"record_id"`
	WriterID           string                 `json:"writer_id,omitempty"`
	TenantID           string                 `json:"tenant_id"`
	Request            IngestRequest          `json:"request"`
	Digest             string                 `json:"digest"`
	AcceptedAt         time.Time              `json:"accepted_at"`
	AcceptedLSN        uint64                 `json:"accepted_lsn,omitempty"`
	AcceptedGeneration int64                  `json:"accepted_generation,omitempty"`
	State              string                 `json:"state"`
	Result             *IngestResult          `json:"result,omitempty"`
	Prepared           *IngestPreparedRequest `json:"prepared,omitempty"`
	Trace              walTraceContext        `json:"trace,omitempty"`
	Error              string                 `json:"error,omitempty"`
	FinishedAt         time.Time              `json:"finished_at,omitempty"`
}

type walPreparedBatchEnvelope struct {
	Items []walPreparedEnvelope `json:"items"`
}

type walPreparedEnvelope struct {
	RecordID string                 `json:"record_id"`
	Prepared *IngestPreparedRequest `json:"prepared"`
}

type walPendingStateEnvelope struct {
	RecordID   string    `json:"record_id"`
	State      string    `json:"state"`
	Error      string    `json:"error,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

type ingestPending struct {
	envelope         walIngestEnvelope
	acceptedLSN      uint64
	acceptedSequence uint64
	estimated        time.Time
	bytes            int64
	state            string
	result           IngestResult
	err              error
	finishedAt       time.Time
	done             chan struct{}
	completedOnce    sync.Once

	retryAttempts  int
	flushRequested bool // scheduler-owned
}

type ingestAcceptFlight struct {
	digest    string
	done      chan struct{}
	err       error
	retainLSN uint64
}

type IngestService struct {
	store  IngestStore
	wal    *IngestWAL
	config IngestServiceConfig

	mu                sync.Mutex
	active            map[string]*ingestPending
	activeByStatus    map[string]*ingestPending
	accepting         map[string]*ingestAcceptFlight
	acceptingStatus   map[string]*ingestAcceptFlight
	pendingBytes      int64
	highestLSN        uint64
	completedBytes    int64
	closed            bool
	lastError         string
	oldestPending     time.Time
	failedStatuses    []ingestFailedStatus
	failedStatusBytes int64
	walFull           bool
	walFailed         bool
	fatalErr          error

	enqueueCh   chan *ingestPending
	forceCh     chan ingestForceRequest
	completeCh  chan ingestWorkerCompletion
	shutdownCh  chan struct{}
	schedulerOK chan struct{}
	readyCh     chan ingestTenantFlush
	runCtx      context.Context
	cancel      context.CancelFunc
	workers     sync.WaitGroup
	acceptors   sync.WaitGroup
	closeOnce   sync.Once
	closeErr    error
}

func (s *IngestService) DiskSpace(ctx context.Context) (DiskSpaceStatus, error) {
	return InspectDiskSpace(ctx, s.config.WAL.Dir, s.config.WAL.DiskSpace)
}

func OpenIngestService(store IngestStore, config IngestServiceConfig) (*IngestService, error) {
	recoveryStarted := time.Now()
	if store == nil {
		return nil, fmt.Errorf("tenant store is required")
	}

	if config.WAL.ControlReserveBytes == 0 {
		config.WAL.ControlReserveBytes = defaultIngestWALControlReserve(config.WAL)
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	_, recoverySpan := startStorageSpan(
		context.Background(),
		"graphdb.storage.ingest.recovery",
	)
	config.WAL.Observer = config.Observer
	config.WAL.Logger = config.Logger
	var recovery ingestRecovery
	wal, err := OpenIngestWAL(config.WAL, recovery.apply)
	if err != nil {
		recordIngestRecovery(config, "error", 0, 0, 0, recoveryStarted, err)
		endStorageSpan(recoverySpan, err)
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	service := &IngestService{
		store:           store,
		wal:             wal,
		config:          config,
		active:          map[string]*ingestPending{},
		activeByStatus:  map[string]*ingestPending{},
		accepting:       map[string]*ingestAcceptFlight{},
		acceptingStatus: map[string]*ingestAcceptFlight{},

		enqueueCh:   make(chan *ingestPending, config.WAL.AppendQueue),
		forceCh:     make(chan ingestForceRequest),
		completeCh:  make(chan ingestWorkerCompletion, config.FlushWorkers*2),
		shutdownCh:  make(chan struct{}),
		schedulerOK: make(chan struct{}),
		readyCh:     make(chan ingestTenantFlush, config.FlushWorkers*2),
		runCtx:      runCtx,
		cancel:      cancel,
	}
	recovered, err := recovery.finish(service)
	service.highestLSN = max(service.highestLSN, wal.recoveredLSN)
	if err != nil {
		recordIngestRecovery(config, "error", recovery.records, 0, 0, recoveryStarted, err)
		endStorageSpan(recoverySpan, err)
		cancel()
		_ = wal.Close()
		return nil, err
	}
	if err := service.prune(context.Background()); err != nil {
		recordIngestRecovery(config, "error", recovery.records, len(recovered), 0, recoveryStarted, err)
		endStorageSpan(recoverySpan, err)
		cancel()
		_ = wal.Close()
		return nil, fmt.Errorf("prune recovered ingest WAL: %w", err)
	}
	go service.schedule()
	for range config.FlushWorkers {
		service.workers.Add(1)
		go service.runWorker()
	}
	for _, pending := range recovered {
		service.enqueueCh <- pending
	}
	prepared := 0
	for _, pending := range recovered {
		if pending.envelope.Prepared != nil {
			prepared++
		}
	}
	recordIngestRecovery(config, "ok", recovery.records, len(recovered), prepared, recoveryStarted, nil)
	recoverySpan.SetAttributes(
		attribute.Int("graphdb.ingest.recovery.records", recovery.records),
		attribute.Int("graphdb.ingest.recovery.pending", len(recovered)),
		attribute.Int("graphdb.ingest.recovery.prepared", prepared),
	)
	endStorageSpan(recoverySpan, nil)
	store.SetIngestBarrier(service.FlushTenant)
	return service, nil
}

func (s *IngestService) FlushTenant(ctx context.Context, tenantID string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrIngestWALClosed
	}
	pending := make([]*ingestPending, 0)
	var throughLSN uint64
	for _, item := range s.active {
		if item.envelope.TenantID != tenantID {
			continue
		}
		pending = append(pending, item)
		throughLSN = max(throughLSN, item.acceptedLSN)
	}
	s.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	select {
	case s.forceCh <- ingestForceRequest{tenantID: tenantID, throughLSN: throughLSN}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.runCtx.Done():
		return ErrIngestWALClosed
	}
	for _, item := range pending {
		select {
		case <-item.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.runCtx.Done():
			return ErrIngestWALClosed
		}
	}
	return nil
}

func (s *IngestService) Close(ctx context.Context) error {
	started := time.Now()
	s.mu.Lock()
	pending := len(s.active)
	s.mu.Unlock()
	if s.config.Logger != nil {
		s.config.Logger.Info("ingest_wal_shutdown_started", map[string]any{
			"pending": pending,
		})
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		accepted := make(chan struct{})
		go func() {
			s.acceptors.Wait()
			close(accepted)
		}()
		select {
		case <-accepted:
		case <-ctx.Done():
			s.closeErr = ctx.Err()
			s.cancel()
		}
		close(s.shutdownCh)
		select {
		case <-s.schedulerOK:
		case <-ctx.Done():
			s.closeErr = ctx.Err()
			s.cancel()
			<-s.schedulerOK
		}
		s.workers.Wait()
		s.cancel()
		if s.closeErr == nil {
			if err := s.prune(context.Background()); err != nil {
				s.closeErr = err
			}
		}
		if err := s.wal.Close(); err != nil {
			s.closeErr = errors.Join(s.closeErr, err)
		}
	})
	if s.config.Logger != nil {
		fields := map[string]any{
			"pending_at_start": pending,
			"duration_ms":      float64(time.Since(started).Microseconds()) / 1000,
		}
		if s.closeErr != nil {
			fields["error"] = s.closeErr.Error()
			s.config.Logger.Error("ingest_wal_shutdown_completed", fields)
		} else {
			s.config.Logger.Info("ingest_wal_shutdown_completed", fields)
		}
	}
	return s.closeErr
}

func (s *IngestService) recordError(err error) {
	if err == nil {
		return
	}
	fatal := errors.Is(err, ErrIngestWALFailed) || errors.Is(err, ErrIngestRepairRequired)
	s.mu.Lock()
	s.lastError = err.Error()
	if fatal {
		s.walFailed = true
		s.fatalErr = err
	}
	s.mu.Unlock()
	if fatal {
		s.cancel()
	}
}

func (s *IngestService) observeQueueLocked() {
	if s.config.Observer == nil {
		return
	}
	oldest := time.Duration(0)
	if !s.oldestPending.IsZero() {
		oldest = time.Since(s.oldestPending)
	}
	s.config.Observer.RecordIngestQueue(len(s.active), s.pendingBytes, oldest)
}

func recordIngestRecovery(
	config IngestServiceConfig,
	status string,
	records int,
	pending int,
	prepared int,
	started time.Time,
	recoveryErr error,
) {
	duration := time.Since(started)
	if config.Observer != nil {
		config.Observer.RecordIngestRecovery(status, records, pending, prepared, duration)
	}
	if config.Logger == nil {
		return
	}
	fields := map[string]any{
		"status":      status,
		"records":     records,
		"pending":     pending,
		"prepared":    prepared,
		"duration_ms": float64(duration.Microseconds()) / 1000,
	}
	if recoveryErr != nil {
		fields["error"] = recoveryErr.Error()
		config.Logger.Error("ingest_wal_recovery", fields)
		return
	}
	config.Logger.Info("ingest_wal_recovery", fields)
}
