package storage

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrIngestQueueFull        = errors.New("ingest WAL memory queue limit reached")
	ErrIngestIdentityConflict = errors.New("ingest identity conflict")
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
		if err := validateIngestStatusPathIdentifier("ingest WAL owner ID", c.OwnerID); err != nil {
			return err
		}
	}
	if c.QueueMemoryBytes <= 0 || c.FlushInterval <= 0 || c.FlushMaxRequests <= 0 ||
		c.FlushMaxBytes <= 0 || c.FlushWorkers <= 0 || c.FlushTimeout <= 0 || c.RetryInterval <= 0 {
		return fmt.Errorf("ingest WAL queue and flush limits must be positive")
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
	WriterID        string        `json:"writer_id,omitempty"`
	TenantID        string        `json:"tenant_id"`
	Source          string        `json:"source"`
	CollectorID     string        `json:"collector_id"`
	BatchID         string        `json:"batch_id"`
	State           string        `json:"state"`
	Durability      string        `json:"durability"`
	AcceptedLSN     uint64        `json:"accepted_lsn,omitempty"`
	AcceptedAt      time.Time     `json:"accepted_at,omitempty"`
	EstimatedFlush  time.Time     `json:"estimated_flush_at,omitempty"`
	FinishedAt      time.Time     `json:"finished_at,omitempty"`
	Result          *IngestResult `json:"result,omitempty"`
	LastError       string        `json:"last_error,omitempty"`
	RecoveryPending bool          `json:"recovery_pending,omitempty"`
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
	CoordinationBackend() string
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

const (
	ingestWALGenerationCaptureTimeout  = 25 * time.Millisecond
	ingestWALGenerationCacheTTL        = time.Second
	ingestWALGenerationCacheMaxTenants = 4096
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
	casConflicts     int
	retryAttempts    int
}

type ingestAcceptFlight struct {
	digest    string
	done      chan struct{}
	err       error
	retainLSN uint64
}

type ingestGenerationCacheEntry struct {
	generation int64
	expiresAt  time.Time
}

type ingestGenerationFlight struct {
	done       chan struct{}
	generation int64
	err        error
}

type IngestService struct {
	store  IngestStore
	wal    *IngestWAL
	config IngestServiceConfig

	mu              sync.Mutex
	active          map[string]*ingestPending
	activeByStatus  map[string]*ingestPending
	accepting       map[string]*ingestAcceptFlight
	acceptingStatus map[string]*ingestAcceptFlight
	pendingBytes    int64
	highestLSN      uint64
	completedSince  int
	closed          bool
	lastError       string
	oldestPending   time.Time
	failedStatuses  []string
	walFull         bool
	walFailed       bool
	fatalErr        error
	generationMu    sync.Mutex
	generations     map[string]ingestGenerationCacheEntry
	generationLoad  map[string]*ingestGenerationFlight

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
}

func OpenIngestService(store IngestStore, config IngestServiceConfig) (*IngestService, error) {
	recoveryStarted := time.Now()
	if store == nil {
		return nil, fmt.Errorf("tenant store is required")
	}
	if store.CoordinationBackend() == CoordinationPostgres && config.OwnerID == "" {
		return nil, fmt.Errorf("ingest WAL with PostgreSQL coordination requires a stable owner ID")
	}
	if store.CoordinationBackend() == CoordinationPostgres {
		if _, ok := store.(ingestGenerationStore); !ok {
			return nil, fmt.Errorf("ingest WAL with PostgreSQL coordination requires tenant generation fencing")
		}
	}
	if store.CoordinationBackend() != CoordinationLocal && store.CoordinationBackend() != CoordinationPostgres {
		return nil, fmt.Errorf("unsupported ingest WAL coordination backend %q", store.CoordinationBackend())
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
	wal, records, err := OpenIngestWAL(config.WAL)
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
		generations:     map[string]ingestGenerationCacheEntry{},
		generationLoad:  map[string]*ingestGenerationFlight{},
		enqueueCh:       make(chan *ingestPending, config.WAL.AppendQueue),
		forceCh:         make(chan ingestForceRequest),
		completeCh:      make(chan ingestWorkerCompletion, config.FlushWorkers*2),
		shutdownCh:      make(chan struct{}),
		schedulerOK:     make(chan struct{}),
		readyCh:         make(chan ingestTenantFlush, config.FlushWorkers*2),
		runCtx:          runCtx,
		cancel:          cancel,
	}
	recovered, err := service.recover(records)
	if err != nil {
		recordIngestRecovery(config, "error", len(records), 0, 0, recoveryStarted, err)
		endStorageSpan(recoverySpan, err)
		cancel()
		_ = wal.Close()
		return nil, err
	}
	if err := service.prune(context.Background()); err != nil {
		recordIngestRecovery(config, "error", len(records), len(recovered), 0, recoveryStarted, err)
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
	recordIngestRecovery(config, "ok", len(records), len(recovered), prepared, recoveryStarted, nil)
	recoverySpan.SetAttributes(
		attribute.Int("graphdb.ingest.recovery.records", len(records)),
		attribute.Int("graphdb.ingest.recovery.pending", len(recovered)),
		attribute.Int("graphdb.ingest.recovery.prepared", prepared),
	)
	endStorageSpan(recoverySpan, nil)
	store.SetIngestBarrier(service.FlushTenant)
	return service, nil
}

func (s *IngestService) Accept(ctx context.Context, tenantID string, request IngestRequest) (acceptance IngestAcceptance, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.ingest.accept", tenantTraceAttr(tenantID))
	defer func() { endStorageSpan(span, err) }()
	request, err = PrepareIngestRequest(tenantID, request)
	if err != nil {
		return IngestAcceptance{}, err
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

		generationCtx, cancelGeneration := context.WithTimeout(ctx, ingestWALGenerationCaptureTimeout)
		generation, generationErr := s.captureIngestWALGeneration(generationCtx, tenantID)
		cancelGeneration()
		if ctxErr := ctx.Err(); ctxErr != nil {
			s.failAcceptFlight(identity, statusKey, flight, ctxErr)
			return IngestAcceptance{}, ctxErr
		}
		if generationErr != nil {
			// Admission is owned by the writer-local WAL, not PostgreSQL. If the
			// coordinator cannot supply a generation promptly, retain the record
			// as conservatively unbound: generation one may recover it, while a
			// later lifecycle generation fences it instead of risking stale data.
			generation = legacyUnboundIngestGeneration
			if s.config.Logger != nil {
				s.config.Logger.Info("ingest_wal_generation_capture_deferred", map[string]any{
					"tenant": tenantID,
					"error":  generationErr.Error(),
				})
			}
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

		appendResult, appendErr := s.wal.Append(ctx, IngestWALAccepted, payload)
		if errors.Is(appendErr, ErrIngestWALFull) {
			if pruneErr := s.prune(ctx); pruneErr == nil {
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
	if s.store.CoordinationBackend() != CoordinationPostgres {
		return 0, nil
	}
	generationStore, ok := s.store.(ingestGenerationStore)
	if !ok {
		return 0, fmt.Errorf("ingest WAL tenant generation fencing is unavailable")
	}
	now := time.Now()
	s.generationMu.Lock()
	cached, cachedOK := s.generations[tenantID]
	if cachedOK && now.Before(cached.expiresAt) {
		s.generationMu.Unlock()
		return cached.generation, nil
	}
	if active := s.generationLoad[tenantID]; active != nil {
		if cachedOK {
			s.generationMu.Unlock()
			return cached.generation, nil
		}
		done := active.done
		s.generationMu.Unlock()
		select {
		case <-done:
			return active.generation, active.err
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	flight := &ingestGenerationFlight{done: make(chan struct{})}
	s.generationLoad[tenantID] = flight
	s.generationMu.Unlock()
	if cachedOK {
		go func() {
			refreshCtx, cancel := context.WithTimeout(s.runCtx, ingestWALGenerationCaptureTimeout)
			defer cancel()
			s.refreshIngestWALGeneration(refreshCtx, generationStore, tenantID, flight)
		}()
		return cached.generation, nil
	}
	s.refreshIngestWALGeneration(ctx, generationStore, tenantID, flight)
	return flight.generation, flight.err
}

func (s *IngestService) refreshIngestWALGeneration(
	ctx context.Context,
	store ingestGenerationStore,
	tenantID string,
	flight *ingestGenerationFlight,
) {
	generation, err := store.CaptureIngestWALGeneration(ctx, tenantID)
	if err == nil && generation <= 0 {
		err = fmt.Errorf("invalid ingest WAL tenant generation %d", generation)
	}
	s.generationMu.Lock()
	flight.generation = generation
	flight.err = err
	current := s.generationLoad[tenantID] == flight
	if err == nil && current {
		if _, exists := s.generations[tenantID]; !exists && len(s.generations) >= ingestWALGenerationCacheMaxTenants {
			oldestTenant := ""
			var oldestExpiry time.Time
			for cachedTenant, cached := range s.generations {
				if oldestTenant == "" || cached.expiresAt.Before(oldestExpiry) {
					oldestTenant = cachedTenant
					oldestExpiry = cached.expiresAt
				}
			}
			delete(s.generations, oldestTenant)
		}
		s.generations[tenantID] = ingestGenerationCacheEntry{
			generation: generation,
			expiresAt:  time.Now().Add(ingestWALGenerationCacheTTL),
		}
	}
	if current {
		delete(s.generationLoad, tenantID)
	}
	close(flight.done)
	s.generationMu.Unlock()
}

func (s *IngestService) invalidateIngestWALGeneration(tenantID string) {
	if s.store.CoordinationBackend() != CoordinationPostgres {
		return
	}
	s.generationMu.Lock()
	delete(s.generations, tenantID)
	// Detach an in-flight refresh so it cannot republish a generation observed
	// before the lifecycle failure. Existing callers may still use that result;
	// publish-time fencing keeps their already accepted WAL records safe.
	delete(s.generationLoad, tenantID)
	s.generationMu.Unlock()
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

func (s *IngestService) Wait(ctx context.Context, acceptance IngestAcceptance) (IngestResult, error) {
	if acceptance.completion == nil {
		return IngestResult{}, fmt.Errorf("invalid ingest acceptance")
	}
	select {
	case <-acceptance.completion:
	case <-ctx.Done():
		return IngestResult{}, ctx.Err()
	}
	pending := acceptance.pending
	if pending == nil || pending.envelope.RecordID != acceptance.recordID {
		return IngestResult{}, fmt.Errorf("invalid ingest acceptance")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return pending.result, pending.err
}

func (s *IngestService) Status(ctx context.Context, tenantID string, source string, collectorID string, batchID string) (IngestBatchStatus, error) {
	key := ingestStatusKey(tenantID, source, collectorID, batchID)
	s.mu.Lock()
	if pending := s.activeByStatus[key]; pending != nil {
		status := statusFromPending(pending, s.config.OwnerID, s.config.WAL.Durability)
		if s.config.Observer != nil {
			s.config.Observer.RecordIngestQueueCache("hit")
		}
		s.mu.Unlock()
		return status, nil
	}
	s.mu.Unlock()
	record, err := s.store.GetIngestBatch(ctx, tenantID, source, collectorID, batchID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return IngestBatchStatus{}, err
	}
	failureStore, durableFailures := s.store.(ingestAttemptFailureStore)
	if durableFailures {
		failure, failureErr := failureStore.GetIngestAttemptFailure(
			ctx, s.config.OwnerID, tenantID, source, collectorID, batchID,
		)
		if failureErr != nil && !errors.Is(failureErr, ErrNotFound) {
			return IngestBatchStatus{}, failureErr
		}
		if failureErr == nil && (errors.Is(err, ErrNotFound) || !record.FinishedAt.After(failure.FinishedAt)) {
			return ingestBatchStatusFromRecord(
				s.config.OwnerID, tenantID, source, collectorID, batchID,
				failure, IngestStateFailed,
			), nil
		}
	}
	if err != nil {
		return IngestBatchStatus{}, err
	}
	state := IngestStateCommitted
	if record.Result.Failed > 0 && record.Result.Applied == 0 {
		state = IngestStateFailed
	}
	return ingestBatchStatusFromRecord(
		s.config.OwnerID, tenantID, source, collectorID, batchID,
		record, state,
	), nil
}

func ingestBatchStatusFromRecord(
	ownerID string,
	tenantID string,
	source string,
	collectorID string,
	batchID string,
	record IngestBatchRecord,
	state string,
) IngestBatchStatus {
	result := record.Result
	return IngestBatchStatus{
		WriterID:    ownerID,
		TenantID:    tenantID,
		Source:      source,
		CollectorID: collectorID,
		BatchID:     batchID,
		State:       state,
		Durability:  "durable",
		AcceptedAt:  record.StartedAt,
		FinishedAt:  record.FinishedAt,
		Result:      &result,
	}
}

func (s *IngestService) Readiness() IngestServiceReadiness {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := IngestServiceReadiness{
		WriterID:     s.config.OwnerID,
		Ready:        !s.closed && s.fatalErr == nil,
		Writable:     !s.closed && s.fatalErr == nil && !s.walFull && s.pendingBytes < s.config.QueueMemoryBytes,
		Recovered:    true,
		Pending:      len(s.active),
		PendingBytes: s.pendingBytes,
		LastError:    s.lastError,
	}
	status.Oldest = s.oldestPending
	for _, pending := range s.active {
		if status.LastError == "" && pending.state == IngestStateRetrying && pending.err != nil {
			status.LastError = pending.err.Error()
		}
	}
	return status
}

func (s *IngestService) WriterID() string {
	return s.config.OwnerID
}

func (s *IngestService) ObserveMetrics() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeQueueLocked()
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
	var closeErr error
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
			closeErr = ctx.Err()
			s.cancel()
		}
		close(s.shutdownCh)
		select {
		case <-s.schedulerOK:
		case <-ctx.Done():
			closeErr = ctx.Err()
			s.cancel()
			<-s.schedulerOK
		}
		s.workers.Wait()
		s.cancel()
		if closeErr == nil {
			if err := s.prune(context.Background()); err != nil {
				closeErr = err
			}
		}
		if err := s.wal.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	})
	if s.config.Logger != nil {
		fields := map[string]any{
			"pending_at_start": pending,
			"duration_ms":      float64(time.Since(started).Microseconds()) / 1000,
		}
		if closeErr != nil {
			fields["error"] = closeErr.Error()
			s.config.Logger.Error("ingest_wal_shutdown_completed", fields)
		} else {
			s.config.Logger.Info("ingest_wal_shutdown_completed", fields)
		}
	}
	return closeErr
}

func (s *IngestService) recover(records []IngestWALRecord) ([]*ingestPending, error) {
	recovered := map[string]*ingestPending{}
	for _, record := range records {
		if record.Type == IngestWALPrepared {
			var batch walPreparedBatchEnvelope
			if err := json.Unmarshal(record.Payload, &batch); err == nil && len(batch.Items) > 0 {
				for _, prepared := range batch.Items {
					pending := recovered[prepared.RecordID]
					if pending == nil || prepared.Prepared == nil {
						return nil, fmt.Errorf("%w: incomplete prepared batch at LSN %d", ErrIngestWALCorrupt, record.LSN)
					}
					pending.envelope.State = IngestStatePrepared
					pending.envelope.Prepared = prepared.Prepared
					pending.envelope.Result = &prepared.Prepared.Result
					pending.envelope.Error = ""
					pending.state = IngestStatePrepared
				}
				s.highestLSN = max(s.highestLSN, record.LSN)
				continue
			}
		}
		var envelope walIngestEnvelope
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			return nil, fmt.Errorf("%w: decode LSN %d: %v", ErrIngestWALCorrupt, record.LSN, err)
		}
		if envelope.RecordID == "" || (record.Type == IngestWALAccepted && envelope.TenantID == "") {
			return nil, fmt.Errorf("%w: incomplete envelope at LSN %d", ErrIngestWALCorrupt, record.LSN)
		}
		s.highestLSN = max(s.highestLSN, record.LSN)
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
			recovered[envelope.RecordID] = pending
		case IngestWALPrepared, IngestWALPublished:
			if pending := recovered[envelope.RecordID]; pending != nil {
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
			delete(recovered, envelope.RecordID)
		}
	}
	out := make([]*ingestPending, 0, len(recovered))
	for _, pending := range recovered {
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

type ingestTenantFlush struct {
	tenantID string
	items    []*ingestPending
}

type ingestWorkerCompletion struct {
	tenantID string
	retry    []*ingestPending
}

type ingestForceRequest struct {
	tenantID   string
	throughLSN uint64
}

type ingestTenantQueue struct {
	tenantID string
	items    []*ingestPending
	bytes    int64
	deadline time.Time
	index    int
}

type ingestDeadlineHeap []*ingestTenantQueue

func (h ingestDeadlineHeap) Len() int           { return len(h) }
func (h ingestDeadlineHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h ingestDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *ingestDeadlineHeap) Push(value any) {
	queue := value.(*ingestTenantQueue)
	queue.index = len(*h)
	*h = append(*h, queue)
}
func (h *ingestDeadlineHeap) Pop() any {
	old := *h
	last := len(old) - 1
	queue := old[last]
	queue.index = -1
	*h = old[:last]
	return queue
}

func (s *IngestService) schedule() {
	defer close(s.schedulerOK)
	defer close(s.readyCh)
	queues := map[string]*ingestTenantQueue{}
	busy := map[string]bool{}
	forceThrough := map[string]uint64{}
	awaitingAdmission := map[uint64]*ingestPending{}
	nextAcceptedSequence := uint64(1)
	deadlines := ingestDeadlineHeap{}
	heap.Init(&deadlines)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	draining := false
	shutdownCh := s.shutdownCh
	enqueue := func(pending *ingestPending) {
		tenantID := pending.envelope.TenantID
		queue := queues[tenantID]
		if queue == nil {
			queue = &ingestTenantQueue{
				tenantID: tenantID,
				deadline: pending.envelope.AcceptedAt.Add(s.config.FlushInterval),
				index:    -1,
			}
			if draining || pending.envelope.Request.FullSync {
				queue.deadline = time.Now()
			}
			queues[tenantID] = queue
			heap.Push(&deadlines, queue)
		}
		queue.items = append(queue.items, pending)
		queue.bytes += pending.bytes
		if len(queue.items) >= s.config.FlushMaxRequests ||
			queue.bytes >= s.config.FlushMaxBytes ||
			pending.envelope.Request.FullSync ||
			pending.acceptedLSN <= forceThrough[tenantID] {
			queue.deadline = time.Now()
			heap.Fix(&deadlines, queue.index)
		}
	}

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
	nextQueue := func(now time.Time) *ingestTenantQueue {
		for deadlines.Len() > 0 {
			queue := deadlines[0]
			if queue.deadline.After(now) {
				return nil
			}
			if !busy[queue.tenantID] {
				return queue
			}
			heap.Pop(&deadlines)
			queue.deadline = now.Add(10 * time.Millisecond)
			heap.Push(&deadlines, queue)
		}
		return nil
	}

	for {
		queue := nextQueue(time.Now())
		var readyCh chan ingestTenantFlush
		var flush ingestTenantFlush
		if queue != nil {
			readyCh = s.readyCh
			flush = ingestTenantFlush{tenantID: queue.tenantID, items: queue.items}
		}
		if draining && len(queues) == 0 && len(busy) == 0 && len(s.enqueueCh) == 0 && len(awaitingAdmission) == 0 {
			return
		}
		stopTimer()
		var timerCh <-chan time.Time
		if queue == nil {
			timerCh = resetTimer()
		}
		select {
		case readyCh <- flush:
			// Keep completion handling live when the worker queue is full.
			heap.Pop(&deadlines)
			delete(queues, queue.tenantID)
			busy[queue.tenantID] = true
		case pending := <-s.enqueueCh:
			if pending.acceptedSequence == 0 {
				// Recovered records were already sorted by their persisted LSN.
				enqueue(pending)
				continue
			}
			// WAL append responses can resume their callers in a different order.
			// This process-local sequence excludes prepared and terminal records.
			awaitingAdmission[pending.acceptedSequence] = pending
			for awaitingAdmission[nextAcceptedSequence] != nil {
				next := awaitingAdmission[nextAcceptedSequence]
				delete(awaitingAdmission, nextAcceptedSequence)
				nextAcceptedSequence++
				enqueue(next)
			}
		case force := <-s.forceCh:
			forceThrough[force.tenantID] = max(forceThrough[force.tenantID], force.throughLSN)
			if queue := queues[force.tenantID]; queue != nil {
				queue.deadline = time.Now()
				heap.Fix(&deadlines, queue.index)
			}
		case completion := <-s.completeCh:
			delete(busy, completion.tenantID)
			if len(completion.retry) > 0 {
				retryDeadline := time.Now().Add(s.ingestRetryDelay(completion.retry[0]))
				queue := queues[completion.tenantID]
				if queue == nil {
					queue = &ingestTenantQueue{
						tenantID: completion.tenantID,
						deadline: retryDeadline,
						index:    -1,
					}
					queues[completion.tenantID] = queue
					heap.Push(&deadlines, queue)
				} else if queue.deadline.Before(retryDeadline) {
					queue.deadline = retryDeadline
					heap.Fix(&deadlines, queue.index)
				}
				queue.items = append(append([]*ingestPending(nil), completion.retry...), queue.items...)
				for _, pending := range completion.retry {
					queue.bytes += pending.bytes
				}
				if draining {
					queue.deadline = time.Now()
					heap.Fix(&deadlines, queue.index)
				}
			}
		case <-timerCh:
		case <-shutdownCh:
			draining = true
			shutdownCh = nil
			for _, queue := range queues {
				queue.deadline = time.Now()
			}
			heap.Init(&deadlines)
		case <-s.runCtx.Done():
			return
		}
	}
}

func (s *IngestService) runWorker() {
	defer s.workers.Done()
	for flush := range s.readyCh {
		retry := s.flushTenant(flush.items)
		select {
		case s.completeCh <- ingestWorkerCompletion{tenantID: flush.tenantID, retry: retry}:
		case <-s.runCtx.Done():
			return
		}
	}
}

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
			for end < len(items) && items[end].envelope.Prepared == nil &&
				s.pendingAcceptedGeneration(items[end]) == generation {
				end++
			}
		}
		end = s.adaptiveFlushEnd(items, start, end)
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
	if s.store.CoordinationBackend() == CoordinationPostgres {
		// WAL records written before generation fencing did not carry the accepted
		// generation. They are safe only while the coordinator is still on its
		// initial generation; later generations must prefer lifecycle fencing.
		return legacyUnboundIngestGeneration
	}
	return 0
}

func (s *IngestService) adaptiveFlushEnd(items []*ingestPending, start int, end int) int {
	if s.store.CoordinationBackend() != CoordinationPostgres || start >= end {
		return end
	}
	conflicts := items[start].casConflicts
	if conflicts <= 1 {
		return end
	}
	batchSize := end - start
	for range min(conflicts-1, 62) {
		batchSize = max(1, batchSize/2)
	}
	return preserveIngestCASCohortBoundary(items, start, start+batchSize, end)
}

func preserveIngestCASCohortBoundary(items []*ingestPending, start int, split int, end int) int {
	if split <= start || split >= end || !samePendingIngestCASCohort(items[split-1], items[split]) {
		return split
	}
	left := split - 1
	for left > start && samePendingIngestCASCohort(items[left-1], items[left]) {
		left--
	}
	if left > start {
		return left
	}
	right := split + 1
	for right < end && samePendingIngestCASCohort(items[right-1], items[right]) {
		right++
	}
	return right
}

func samePendingIngestCASCohort(left *ingestPending, right *ingestPending) bool {
	if left == nil || right == nil {
		return false
	}
	leftRequest := left.envelope.Request
	rightRequest := right.envelope.Request
	return leftRequest.ExpectedVersion != nil &&
		rightRequest.ExpectedVersion != nil &&
		!ingestRequestAtomic(leftRequest) &&
		!ingestRequestAtomic(rightRequest) &&
		*leftRequest.ExpectedVersion == *rightRequest.ExpectedVersion
}

func (s *IngestService) flushTenantGroup(items []*ingestPending) []*ingestPending {
	tenantID := items[0].envelope.TenantID
	firstLSN, lastLSN := ingestPendingLSNRange(items)
	started := time.Now()
	flushCtx, span := startIngestFlushSpan(s.runCtx, tenantID, items)
	var (
		stats    IngestBatchStats
		flushErr error
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
		if items[0].envelope.Prepared != nil {
			span.SetAttributes(attribute.String("graphdb.ingest.flush.id", items[0].envelope.Prepared.FlushID))
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
			if items[0].envelope.Prepared != nil {
				fields["flush_id"] = items[0].envelope.Prepared.FlushID
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
	if s.store.CoordinationBackend() != CoordinationPostgres {
		preparedHook = func(ctx context.Context, plans []*IngestPreparedRequest) error {
			return s.appendPreparedBatchState(ctx, items, plans)
		}
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
	if err != nil {
		flushErr = err
		if terminalIngestFlushError(err) {
			if errors.Is(err, ErrTenantDisabled) || errors.Is(err, ErrTenantDeleted) {
				s.invalidateIngestWALGeneration(tenantID)
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

func (s *IngestService) appendTerminalBatch(items []*ingestPending, results []IngestResult) (int, error) {
	if len(items) != len(results) {
		return 0, fmt.Errorf("terminal ingest result count mismatch")
	}
	records := make([]ingestWALBatchRecord, 0, len(items))
	// Preparation has not finalized any prefix; a failure here retries all items.
	for index, pending := range items {
		result := results[index]
		if result.Failed > 0 && result.Applied == 0 {
			if failureStore, ok := s.store.(ingestAttemptFailureStore); ok {
				if err := failureStore.PersistIngestAttemptFailure(
					s.runCtx,
					s.config.OwnerID,
					pending.envelope.TenantID,
					pending.envelope.Request,
					result,
					pending.envelope.AcceptedAt,
					time.Now().UTC(),
				); err != nil {
					return 0, fmt.Errorf("persist ingest attempt failure: %w", err)
				}
			}
			payload, err := pendingStatePayload(
				pending, IngestWALFailed, IngestStateFailed, &result, "",
			)
			if err != nil {
				return 0, err
			}
			records = append(records, ingestWALBatchRecord{kind: IngestWALFailed, payload: payload})
			continue
		}
		if result.SkipReason == IngestSkipReasonIdempotentReplay {
			if resolver, ok := s.store.(ingestAttemptFailureResolver); ok {
				if err := resolver.ResolveIngestAttemptFailure(
					s.runCtx,
					s.config.OwnerID,
					pending.envelope.TenantID,
					pending.envelope.Request,
					result,
					pending.envelope.AcceptedAt,
					time.Now().UTC(),
				); err != nil {
					return 0, fmt.Errorf("resolve ingest attempt failure: %w", err)
				}
			}
		}
		finalized, err := pendingStatePayload(
			pending, IngestWALFinalized, IngestStateCommitted, &result, "",
		)
		if err != nil {
			return 0, err
		}
		records = append(records, ingestWALBatchRecord{kind: IngestWALFinalized, payload: finalized})
	}

	responses := s.appendWALBatchWithPrune(records)
	for index, response := range responses {
		if response.err != nil {
			s.completePendingBatch(items[:index], results[:index])
			return index, response.err
		}
	}
	s.completePendingBatch(items, results)
	return len(items), nil
}

func (s *IngestService) appendWALBatchWithPrune(records []ingestWALBatchRecord) []ingestWALAppendResponse {
	responses := s.wal.appendBatch(s.runCtx, records)
	retryIndexes := make([]int, 0)
	for index, response := range responses {
		if errors.Is(response.err, ErrIngestWALFull) {
			retryIndexes = append(retryIndexes, index)
		}
	}
	if len(retryIndexes) > 0 {
		if err := s.prune(s.runCtx); err == nil {
			retryRecords := make([]ingestWALBatchRecord, len(retryIndexes))
			for index, recordIndex := range retryIndexes {
				retryRecords[index] = records[recordIndex]
			}
			retried := s.wal.appendBatch(s.runCtx, retryRecords)
			for index, recordIndex := range retryIndexes {
				responses[recordIndex] = retried[index]
			}
		}
	}
	var highest uint64
	for _, response := range responses {
		if response.err == nil {
			highest = max(highest, response.result.LSN)
		}
	}
	if highest > 0 {
		s.mu.Lock()
		s.highestLSN = max(s.highestLSN, highest)
		s.mu.Unlock()
	}
	return responses
}

func (s *IngestService) appendPreparedBatchState(
	ctx context.Context,
	items []*ingestPending,
	plans []*IngestPreparedRequest,
) error {
	if len(items) != len(plans) {
		return fmt.Errorf("prepared ingest plan count mismatch")
	}
	envelopes := make([]walPreparedEnvelope, 0, len(items))
	for index, pending := range items {
		if plans[index] == nil {
			continue
		}
		envelopes = append(envelopes, walPreparedEnvelope{
			RecordID: pending.envelope.RecordID,
			Prepared: plans[index],
		})
	}
	if len(envelopes) == 0 {
		return nil
	}
	payload, err := json.Marshal(walPreparedBatchEnvelope{Items: envelopes})
	if err != nil {
		return err
	}
	appendResult, err := s.wal.Append(ctx, IngestWALPrepared, payload)
	if errors.Is(err, ErrIngestWALFull) {
		if pruneErr := s.prune(ctx); pruneErr == nil {
			appendResult, err = s.wal.Append(ctx, IngestWALPrepared, payload)
		}
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.highestLSN = max(s.highestLSN, appendResult.LSN)
	for index, pending := range items {
		if plans[index] == nil {
			continue
		}
		current := s.activeByStatus[ingestStatusKey(
			pending.envelope.TenantID,
			pending.envelope.Request.Source,
			pending.envelope.Request.CollectorID,
			pending.envelope.Request.BatchID,
		)]
		if current == nil || current.envelope.RecordID != pending.envelope.RecordID {
			s.mu.Unlock()
			return fmt.Errorf("prepared ingest request is no longer active")
		}
		current.envelope.State = IngestStatePrepared
		current.envelope.Prepared = plans[index]
		current.envelope.Result = &plans[index].Result
		current.envelope.Error = ""
		current.state = IngestStatePrepared
		current.err = nil
	}
	s.mu.Unlock()
	return nil
}

func pendingStatePayload(pending *ingestPending, kind IngestWALRecordType, state string, _ *IngestResult, errorMessage string) ([]byte, error) {
	envelope := walPendingStateEnvelope{
		RecordID: pending.envelope.RecordID,
		State:    state,
		Error:    errorMessage,
	}
	if kind == IngestWALFinalized || kind == IngestWALFailed {
		envelope.FinishedAt = time.Now().UTC()
	}
	return json.Marshal(envelope)
}

func (s *IngestService) setPendingRetry(pending *ingestPending, err error) {
	s.mu.Lock()
	pending.state = IngestStateRetrying
	pending.err = err
	pending.retryAttempts++
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrWriteConflict) {
		pending.casConflicts++
	}
	s.mu.Unlock()
}

func (s *IngestService) ingestRetryDelay(pending *ingestPending) time.Duration {
	if s.store.CoordinationBackend() == CoordinationPostgres &&
		(errors.Is(pending.err, ErrConflict) || errors.Is(pending.err, ErrWriteConflict)) {
		return coordinatorRetryBackoff(max(0, pending.casConflicts-1))
	}
	if s.store.CoordinationBackend() == CoordinationPostgres &&
		errors.Is(pending.err, ErrTaskLeaseHeld) {
		return max(25*time.Millisecond, coordinatorRetryBackoff(max(0, pending.retryAttempts+1)))
	}
	var pressure *BackpressureError
	if errors.As(pending.err, &pressure) {
		if pressure.RetryAfter > 0 {
			return pressure.RetryAfter
		}
		return s.config.RetryInterval
	}
	delay := s.config.RetryInterval
	maximum := max(30*time.Second, delay)
	for attempt := 1; attempt < min(pending.retryAttempts, 8) && delay < maximum; attempt++ {
		delay = min(maximum, delay*2)
	}
	jitterPercent := 80 + int((pending.acceptedLSN+uint64(pending.retryAttempts*17))%41)
	return delay * time.Duration(jitterPercent) / 100
}

func (s *IngestService) completePendingBatch(items []*ingestPending, results []IngestResult) {
	if len(items) == 0 {
		return
	}
	s.mu.Lock()
	for index, pending := range items {
		state := IngestStateCommitted
		if results[index].Failed > 0 && results[index].Applied == 0 {
			state = IngestStateFailed
		}
		s.completePendingStateLocked(pending, results[index], nil, state)
	}
	shouldPrune := s.finishPendingCompletionsLocked()
	s.mu.Unlock()
	if shouldPrune {
		_ = s.prune(context.Background())
	}
}

func (s *IngestService) completePendingState(pending *ingestPending, result IngestResult, err error, state string) {
	s.mu.Lock()
	s.completePendingStateLocked(pending, result, err, state)
	shouldPrune := s.finishPendingCompletionsLocked()
	s.mu.Unlock()
	if shouldPrune {
		_ = s.prune(context.Background())
	}
}

func (s *IngestService) completePendingStateLocked(pending *ingestPending, result IngestResult, err error, state string) {
	pending.state = state
	pending.result = result
	pending.err = err
	pending.finishedAt = time.Now().UTC()
	s.pendingBytes -= pending.bytes
	delete(s.active, ingestRequestIdentity(pending.envelope.TenantID, pending.envelope.Request))
	statusKey := ingestStatusKey(
		pending.envelope.TenantID,
		pending.envelope.Request.Source,
		pending.envelope.Request.CollectorID,
		pending.envelope.Request.BatchID,
	)
	if s.activeByStatus[statusKey] == pending {
		if state == IngestStateFailed {
			s.failedStatuses = append(s.failedStatuses, statusKey)
			if len(s.failedStatuses) > 1024 {
				oldest := s.failedStatuses[0]
				s.failedStatuses = s.failedStatuses[1:]
				if cached := s.activeByStatus[oldest]; cached != nil && cached.state == IngestStateFailed {
					delete(s.activeByStatus, oldest)
				}
			}
		} else {
			delete(s.activeByStatus, statusKey)
			if s.config.Observer != nil {
				s.config.Observer.RecordIngestQueueCache("eviction")
			}
		}
	}
	if pending.envelope.AcceptedAt.Equal(s.oldestPending) {
		s.oldestPending = time.Time{}
	}
	s.completedSince++
	pending.completedOnce.Do(func() { close(pending.done) })
}

func (s *IngestService) finishPendingCompletionsLocked() bool {
	if s.oldestPending.IsZero() {
		for _, active := range s.active {
			if s.oldestPending.IsZero() || active.envelope.AcceptedAt.Before(s.oldestPending) {
				s.oldestPending = active.envelope.AcceptedAt
			}
		}
	}
	shouldPrune := s.walFull || s.completedSince >= 128
	if shouldPrune {
		s.completedSince = 0
	}
	s.observeQueueLocked()
	return shouldPrune
}

func terminalIngestFlushError(err error) bool {
	return errors.Is(err, ErrTenantDisabled) ||
		errors.Is(err, ErrTenantDeleted) ||
		errors.Is(err, ErrIngestIdentityConflict) ||
		errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrIngestWALRecordTooLarge) ||
		errors.Is(err, ErrIngestWALRecordExceedsSegment)
}

func failedIngestResult(request IngestRequest, failure error) IngestResult {
	result, _, appliedIndices := buildIngestMutations(request)
	result.BatchID = request.BatchID
	result.Cursor = request.Cursor
	result.Failed += result.Applied
	result.Applied = 0
	if result.Failed == 0 {
		result.Failed = 1
	}
	result.ErrorCode = ingestErrorCode(failure)
	for _, index := range appliedIndices {
		result.Failures = append(result.Failures, IngestFailure{
			Index:      index,
			ExternalID: request.Items[index].ExternalID,
			Error:      failure.Error(),
		})
	}
	result.Conflicts = append(result.Conflicts, IngestConflict{Message: failure.Error()})
	return result
}

func (s *IngestService) prune(ctx context.Context) error {
	s.mu.Lock()
	before := s.highestLSN + 1
	// A durable append can still be returning to Accept and not yet be in active.
	for _, flight := range s.accepting {
		before = min(before, flight.retainLSN)
	}
	for _, pending := range s.active {
		if pending.acceptedLSN < before {
			before = pending.acceptedLSN
		}
	}
	s.mu.Unlock()
	err := s.wal.Prune(ctx, before)
	if err == nil {
		s.mu.Lock()
		s.walFull = false
		s.mu.Unlock()
	}
	return err
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

func acceptanceFromPending(pending *ingestPending, ownerID string, durability string) IngestAcceptance {
	reportedDurability := "memory"
	if durability == IngestWALDurabilitySync {
		reportedDurability = "durable"
	}
	return IngestAcceptance{
		WriterID:       ownerID,
		BatchID:        pending.envelope.Request.BatchID,
		Source:         pending.envelope.Request.Source,
		CollectorID:    pending.envelope.Request.CollectorID,
		State:          pending.state,
		Durability:     reportedDurability,
		AcceptedAt:     pending.envelope.AcceptedAt,
		EstimatedFlush: pending.estimated,
		acceptedLSN:    pending.acceptedLSN,
		recordID:       pending.envelope.RecordID,
		completion:     pending.done,
		pending:        pending,
	}
}

func statusFromPending(pending *ingestPending, ownerID string, durability string) IngestBatchStatus {
	reportedDurability := "memory"
	if durability == IngestWALDurabilitySync {
		reportedDurability = "durable"
	}
	status := IngestBatchStatus{
		WriterID:        ownerID,
		TenantID:        pending.envelope.TenantID,
		Source:          pending.envelope.Request.Source,
		CollectorID:     pending.envelope.Request.CollectorID,
		BatchID:         pending.envelope.Request.BatchID,
		State:           pending.state,
		Durability:      reportedDurability,
		AcceptedLSN:     pending.acceptedLSN,
		AcceptedAt:      pending.envelope.AcceptedAt,
		EstimatedFlush:  pending.estimated,
		FinishedAt:      pending.finishedAt,
		RecoveryPending: pending.state != IngestStateCommitted && pending.state != IngestStateFailed,
	}
	if pending.state == IngestStateCommitted || pending.state == IngestStateFailed {
		result := pending.result
		status.Result = &result
	}
	if pending.err != nil {
		status.LastError = pending.err.Error()
	}
	return status
}

func ingestRequestIdentity(tenantID string, request IngestRequest) string {
	key := request.BatchID
	kind := "batch"
	if request.IdempotencyKey != "" {
		key = request.IdempotencyKey
		kind = "idempotency"
	}
	return tenantID + "\x00" + request.Source + "\x00" + request.CollectorID + "\x00" + kind + "\x00" + key
}

func ingestRecordID(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func ingestStatusKey(tenantID string, source string, collectorID string, batchID string) string {
	return tenantID + "\x00" + source + "\x00" + collectorID + "\x00" + batchID
}
