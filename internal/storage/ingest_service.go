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
	WAL              IngestWALConfig
	QueueMemoryBytes int64
	FlushInterval    time.Duration
	FlushMaxRequests int
	FlushMaxBytes    int64
	FlushWorkers     int
	FlushTimeout     time.Duration
	RetryInterval    time.Duration
	Metadata         IngestMetadataConfig
	Observer         IngestObserver
	Logger           IngestLogger
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
		Metadata:         DefaultIngestMetadataConfig(),
	}
}

func (c IngestServiceConfig) validate() error {
	if err := c.WAL.validate(); err != nil {
		return err
	}
	if c.QueueMemoryBytes <= 0 || c.FlushInterval <= 0 || c.FlushMaxRequests <= 0 ||
		c.FlushMaxBytes <= 0 || c.FlushWorkers <= 0 || c.FlushTimeout <= 0 || c.RetryInterval <= 0 {
		return fmt.Errorf("ingest WAL queue and flush limits must be positive")
	}
	if err := c.Metadata.validate(); err != nil {
		return err
	}
	return nil
}

func (c IngestServiceConfig) Validate() error {
	return c.validate()
}

type IngestAcceptance struct {
	BatchID        string    `json:"batch_id"`
	Source         string    `json:"source"`
	CollectorID    string    `json:"collector_id"`
	State          string    `json:"state"`
	Durability     string    `json:"durability"`
	AcceptedAt     time.Time `json:"accepted_at"`
	EstimatedFlush time.Time `json:"estimated_flush_at"`
	tenantID       string
	acceptedLSN    uint64
	recordID       string
	completion     <-chan struct{}
	pending        *ingestPending
}

type IngestBatchStatus struct {
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
	Ready        bool      `json:"ready"`
	Writable     bool      `json:"writable"`
	Recovered    bool      `json:"recovered"`
	Pending      int       `json:"pending"`
	PendingBytes int64     `json:"pending_bytes"`
	Oldest       time.Time `json:"oldest_pending_at,omitempty"`
	LastError    string    `json:"last_error,omitempty"`
}

type walIngestEnvelope struct {
	RecordID        string                 `json:"record_id"`
	TenantID        string                 `json:"tenant_id"`
	Request         IngestRequest          `json:"request"`
	Digest          string                 `json:"digest"`
	AcceptedAt      time.Time              `json:"accepted_at"`
	AcceptedLSN     uint64                 `json:"accepted_lsn,omitempty"`
	State           string                 `json:"state"`
	Result          *IngestResult          `json:"result,omitempty"`
	Prepared        *IngestPreparedRequest `json:"prepared,omitempty"`
	Trace           walTraceContext        `json:"trace,omitempty"`
	Error           string                 `json:"error,omitempty"`
	FinishedAt      time.Time              `json:"finished_at,omitempty"`
	MetadataFlushID string                 `json:"metadata_flush_id,omitempty"`
}

type walPreparedBatchEnvelope struct {
	Items []walIngestEnvelope `json:"items"`
}

type ingestPending struct {
	envelope       walIngestEnvelope
	acceptedLSN    uint64
	estimated      time.Time
	bytes          int64
	state          string
	result         IngestResult
	err            error
	finishedAt     time.Time
	metadataRecord *IngestBatchRecord
	done           chan struct{}
	completedOnce  sync.Once
}

type ingestAcceptFlight struct {
	digest string
	done   chan struct{}
	err    error
}

type IngestService struct {
	store  *TenantStore
	wal    *IngestWAL
	config IngestServiceConfig

	mu             sync.Mutex
	active         map[string]*ingestPending
	activeByStatus map[string]*ingestPending
	accepting      map[string]*ingestAcceptFlight
	pendingBytes   int64
	highestLSN     uint64
	completedSince int
	closed         bool
	lastError      string
	oldestPending  time.Time

	enqueueCh           chan *ingestPending
	forceCh             chan ingestForceRequest
	completeCh          chan ingestWorkerCompletion
	shutdownCh          chan struct{}
	schedulerOK         chan struct{}
	readyCh             chan ingestTenantFlush
	metadataEnqueueCh   chan *ingestPending
	metadataForceCh     chan ingestForceRequest
	metadataCompleteCh  chan ingestWorkerCompletion
	metadataShutdownCh  chan struct{}
	metadataSchedulerOK chan struct{}
	metadataReadyCh     chan ingestTenantFlush
	runCtx              context.Context
	cancel              context.CancelFunc
	workers             sync.WaitGroup
	metadataWorkers     sync.WaitGroup
	closeOnce           sync.Once
}

func OpenIngestService(store *TenantStore, config IngestServiceConfig) (*IngestService, error) {
	recoveryStarted := time.Now()
	if store == nil {
		return nil, fmt.Errorf("tenant store is required")
	}
	if store.coordinated() {
		return nil, fmt.Errorf("ingest WAL mode requires local coordination")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	store.IngestMetadataMode = config.Metadata.Mode
	store.IngestObserver = config.Observer
	store.IngestLogger = config.Logger
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
		store:               store,
		wal:                 wal,
		config:              config,
		active:              map[string]*ingestPending{},
		activeByStatus:      map[string]*ingestPending{},
		accepting:           map[string]*ingestAcceptFlight{},
		enqueueCh:           make(chan *ingestPending, config.WAL.AppendQueue),
		forceCh:             make(chan ingestForceRequest),
		completeCh:          make(chan ingestWorkerCompletion, config.FlushWorkers*2),
		shutdownCh:          make(chan struct{}),
		schedulerOK:         make(chan struct{}),
		readyCh:             make(chan ingestTenantFlush, config.FlushWorkers*2),
		metadataEnqueueCh:   make(chan *ingestPending, config.WAL.AppendQueue),
		metadataForceCh:     make(chan ingestForceRequest),
		metadataCompleteCh:  make(chan ingestWorkerCompletion, config.Metadata.FlushWorkers*2),
		metadataShutdownCh:  make(chan struct{}),
		metadataSchedulerOK: make(chan struct{}),
		metadataReadyCh:     make(chan ingestTenantFlush, config.Metadata.FlushWorkers*2),
		runCtx:              runCtx,
		cancel:              cancel,
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
	if config.Metadata.Mode == IngestMetadataModeSegment {
		go service.scheduleMetadata()
		for range config.Metadata.FlushWorkers {
			service.metadataWorkers.Add(1)
			go service.runMetadataWorker()
		}
	}
	for _, pending := range recovered {
		if config.Metadata.Mode == IngestMetadataModeSegment && pending.state == IngestStatePublished {
			service.metadataEnqueueCh <- pending
		} else {
			service.enqueueCh <- pending
		}
	}
	if config.Observer != nil && config.Metadata.Mode == IngestMetadataModeSegment {
		var replayBytes int64
		for _, pending := range recovered {
			if pending.state == IngestStatePublished {
				replayBytes += pending.bytes
			}
		}
		config.Observer.RecordIngestMetadataReplay(replayBytes)
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
	store.IngestBarrier = service.FlushTenant
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
		if pending := s.active[identity]; pending != nil {
			if pending.envelope.Digest != digest {
				s.mu.Unlock()
				return IngestAcceptance{}, fmt.Errorf("%w for source %q collector %q batch %q", ErrIngestIdentityConflict, request.Source, request.CollectorID, request.BatchID)
			}
			accepted := acceptanceFromPending(pending, s.config.WAL.Durability)
			s.mu.Unlock()
			return accepted, nil
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
		acceptedAt := time.Now().UTC()
		envelope := walIngestEnvelope{
			RecordID:   recordID,
			TenantID:   tenantID,
			Request:    request,
			Digest:     digest,
			AcceptedAt: acceptedAt,
			State:      IngestStateAccepted,
			Trace:      captureWALTraceContext(ctx),
		}
		payload, marshalErr := json.Marshal(envelope)
		if marshalErr != nil {
			s.mu.Unlock()
			return IngestAcceptance{}, marshalErr
		}
		recordBytes := int64(len(payload) + ingestWALHeaderBytes + ingestWALChecksumBytes)
		if s.pendingBytes+recordBytes > s.config.QueueMemoryBytes {
			s.mu.Unlock()
			return IngestAcceptance{}, ErrIngestQueueFull
		}
		flight := &ingestAcceptFlight{digest: digest, done: make(chan struct{})}
		s.accepting[identity] = flight
		s.pendingBytes += recordBytes
		s.mu.Unlock()

		appendResult, appendErr := s.wal.Append(ctx, IngestWALAccepted, payload)
		s.mu.Lock()
		delete(s.accepting, identity)
		if appendErr != nil {
			s.pendingBytes -= recordBytes
			flight.err = appendErr
			close(flight.done)
			s.mu.Unlock()
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
		pending := &ingestPending{
			envelope:    envelope,
			acceptedLSN: appendResult.LSN,
			estimated:   acceptedAt.Add(s.config.FlushInterval),
			bytes:       recordBytes,
			state:       IngestStateAccepted,
			done:        make(chan struct{}),
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
		accepted := acceptanceFromPending(pending, s.config.WAL.Durability)
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
		status := statusFromPending(pending, s.config.WAL.Durability)
		if s.config.Observer != nil {
			s.config.Observer.RecordIngestQueueCache("hit")
		}
		s.mu.Unlock()
		return status, nil
	}
	s.mu.Unlock()
	record, err := s.store.GetIngestBatch(ctx, tenantID, source, collectorID, batchID)
	if err != nil {
		return IngestBatchStatus{}, err
	}
	result := record.Result
	return IngestBatchStatus{
		TenantID:    tenantID,
		Source:      source,
		CollectorID: collectorID,
		BatchID:     batchID,
		State:       IngestStateCommitted,
		Durability:  "durable",
		AcceptedAt:  record.StartedAt,
		FinishedAt:  record.FinishedAt,
		Result:      &result,
	}, nil
}

func (s *IngestService) Readiness() IngestServiceReadiness {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := IngestServiceReadiness{
		Ready:        !s.closed && s.lastError == "",
		Writable:     !s.closed,
		Recovered:    true,
		Pending:      len(s.active),
		PendingBytes: s.pendingBytes,
		LastError:    s.lastError,
	}
	status.Oldest = s.oldestPending
	for _, pending := range s.active {
		if pending.state == IngestStateRetrying {
			status.Ready = false
			if status.LastError == "" && pending.err != nil {
				status.LastError = pending.err.Error()
			}
		}
	}
	return status
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
	if err := s.forceMetadataThrough(ctx, tenantID, throughLSN); err != nil {
		return err
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
		close(s.shutdownCh)
		select {
		case <-s.schedulerOK:
		case <-ctx.Done():
			closeErr = ctx.Err()
			s.cancel()
			<-s.schedulerOK
		}
		s.workers.Wait()
		if s.config.Metadata.Mode == IngestMetadataModeSegment {
			close(s.metadataShutdownCh)
			select {
			case <-s.metadataSchedulerOK:
			case <-ctx.Done():
				closeErr = errors.Join(closeErr, ctx.Err())
				s.cancel()
				<-s.metadataSchedulerOK
			}
			s.metadataWorkers.Wait()
		}
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
		if record.Type == IngestWALPrepared || record.Type == IngestWALPublished ||
			record.Type == IngestWALFinalized || record.Type == IngestWALFailed {
			var batch walPreparedBatchEnvelope
			if err := json.Unmarshal(record.Payload, &batch); err == nil && len(batch.Items) > 0 {
				for _, envelope := range batch.Items {
					pending := recovered[envelope.RecordID]
					if pending == nil {
						return nil, fmt.Errorf("%w: state for unknown ingest record at LSN %d", ErrIngestWALCorrupt, record.LSN)
					}
					switch record.Type {
					case IngestWALPrepared:
						if envelope.Prepared == nil {
							return nil, fmt.Errorf("%w: incomplete prepared batch at LSN %d", ErrIngestWALCorrupt, record.LSN)
						}
						pending.envelope = envelope
						pending.state = IngestStatePrepared
					case IngestWALPublished:
						pending.envelope = envelope
						pending.state = IngestStatePublished
						if envelope.Result != nil {
							pending.result = *envelope.Result
						}
						pending.finishedAt = envelope.FinishedAt
					case IngestWALFinalized, IngestWALFailed:
						delete(recovered, envelope.RecordID)
					}
				}
				s.highestLSN = max(s.highestLSN, record.LSN)
				continue
			}
		}
		var envelope walIngestEnvelope
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			return nil, fmt.Errorf("%w: decode LSN %d: %v", ErrIngestWALCorrupt, record.LSN, err)
		}
		if envelope.RecordID == "" || envelope.TenantID == "" {
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
				pending.envelope = envelope
				pending.state = envelope.State
			}
		case IngestWALFinalized, IngestWALFailed:
			delete(recovered, envelope.RecordID)
		}
	}
	out := make([]*ingestPending, 0, len(recovered))
	for _, pending := range recovered {
		identity := ingestRequestIdentity(pending.envelope.TenantID, pending.envelope.Request)
		statusKey := ingestStatusKey(
			pending.envelope.TenantID,
			pending.envelope.Request.Source,
			pending.envelope.Request.CollectorID,
			pending.envelope.Request.BatchID,
		)
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
	deadlines := ingestDeadlineHeap{}
	heap.Init(&deadlines)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	draining := false
	shutdownCh := s.shutdownCh

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
		for deadlines.Len() > 0 {
			queue := deadlines[0]
			if !draining && queue.deadline.After(now) {
				return
			}
			heap.Pop(&deadlines)
			if busy[queue.tenantID] {
				queue.deadline = now.Add(10 * time.Millisecond)
				heap.Push(&deadlines, queue)
				continue
			}
			delete(queues, queue.tenantID)
			busy[queue.tenantID] = true
			s.readyCh <- ingestTenantFlush{tenantID: queue.tenantID, items: queue.items}
		}
	}

	for {
		dispatch(time.Now())
		if draining && len(queues) == 0 && len(busy) == 0 {
			return
		}
		stopTimer()
		timerCh := resetTimer()
		select {
		case pending := <-s.enqueueCh:
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
		case force := <-s.forceCh:
			forceThrough[force.tenantID] = max(forceThrough[force.tenantID], force.throughLSN)
			if queue := queues[force.tenantID]; queue != nil {
				queue.deadline = time.Now()
				heap.Fix(&deadlines, queue.index)
			}
		case completion := <-s.completeCh:
			delete(busy, completion.tenantID)
			if len(completion.retry) > 0 {
				queue := queues[completion.tenantID]
				if queue == nil {
					queue = &ingestTenantQueue{
						tenantID: completion.tenantID,
						deadline: time.Now().Add(s.config.RetryInterval),
						index:    -1,
					}
					queues[completion.tenantID] = queue
					heap.Push(&deadlines, queue)
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
			for end < len(items) &&
				items[end].envelope.Prepared != nil &&
				items[end].envelope.Prepared.FlushID == flushID {
				end++
			}
		} else {
			for end < len(items) && items[end].envelope.Prepared == nil {
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

func (s *IngestService) flushTenantGroup(items []*ingestPending) []*ingestPending {
	firstEnvelope := items[0].envelope
	tenantID := firstEnvelope.TenantID
	flushID := ""
	if firstEnvelope.Prepared != nil {
		flushID = firstEnvelope.Prepared.FlushID
	}
	firstLSN, lastLSN := ingestPendingLSNRange(items)
	started := time.Now()
	flushCtx, span := startIngestFlushSpan(s.runCtx, tenantID, items)
	var (
		stats    IngestBatchStats
		flushErr error
	)
	if s.config.Logger != nil {
		fields := ingestTraceLogFields(firstEnvelope)
		if span.SpanContext().IsValid() {
			fields["flush_trace_id"] = span.SpanContext().TraceID().String()
			fields["flush_span_id"] = span.SpanContext().SpanID().String()
		}
		fields["tenant"] = tenantID
		fields["requests"] = len(items)
		fields["first_lsn"] = firstLSN
		fields["last_lsn"] = lastLSN
		fields["oldest_ms"] = float64(time.Since(firstEnvelope.AcceptedAt).Microseconds()) / 1000
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
			fields := ingestTraceLogFields(firstEnvelope)
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
			Request:    pending.envelope.Request,
			AcceptedAt: pending.envelope.AcceptedAt,
			FinishedAt: pending.envelope.FinishedAt,
			Prepared:   pending.envelope.Prepared,
		}
	}
	var publishedRecords []IngestPublishedRecord
	flushCtx, cancel := context.WithTimeout(flushCtx, s.config.FlushTimeout)
	results, err := s.store.IngestDurableBatchWithHooks(
		flushCtx,
		tenantID,
		entries,
		IngestBatchHooks{
			Prepared: func(ctx context.Context, plans []*IngestPreparedRequest) error {
				for _, plan := range plans {
					if plan != nil {
						flushID = plan.FlushID
						break
					}
				}
				return s.appendPreparedBatchState(ctx, items, plans)
			},
			Published: func(_ context.Context, records []IngestPublishedRecord) error {
				publishedRecords = append([]IngestPublishedRecord(nil), records...)
				return nil
			},
			DeferMetadata: s.config.Metadata.Mode == IngestMetadataModeSegment,
			Stats: func(batchStats IngestBatchStats) {
				stats = batchStats
			},
		},
	)
	cancel()
	if err != nil {
		flushErr = err
		for _, pending := range items {
			s.setPendingRetry(pending, err)
		}
		return items
	}
	if s.config.Metadata.Mode == IngestMetadataModeSegment {
		if err := s.appendPublishedBatchState(items, results, publishedRecords); err != nil {
			flushErr = err
			s.recordError(err)
			return items
		}
		required := make(map[int]IngestBatchRecord, len(publishedRecords))
		for _, item := range publishedRecords {
			required[item.Index] = item.Record
		}
		finalizeNow := make([]*ingestPending, 0)
		for index, pending := range items {
			s.setPendingState(pending, IngestStatePublished)
			record, ok := required[index]
			if !ok {
				finalizeNow = append(finalizeNow, pending)
				continue
			}
			pending.metadataRecord = &record
			select {
			case s.metadataEnqueueCh <- pending:
			case <-s.runCtx.Done():
				flushErr = ErrIngestWALClosed
				return items[index:]
			}
		}
		if len(finalizeNow) > 0 {
			if err := s.finalizeMetadataItems(finalizeNow); err != nil {
				flushErr = err
				s.recordError(err)
				return finalizeNow
			}
		}
		return nil
	}
	for index, pending := range items {
		result := results[index]
		if err := s.appendPendingState(pending, IngestWALPublished, IngestStatePublished, &result, ""); err != nil {
			flushErr = err
			s.recordError(err)
			return items[index:]
		}
		s.setPendingState(pending, IngestStatePublished)
		if err := s.appendPendingState(pending, IngestWALFinalized, IngestStateCommitted, &result, ""); err != nil {
			flushErr = err
			s.recordError(err)
			return items[index:]
		}
		s.completePending(pending, result, nil)
	}
	return nil
}

func (s *IngestService) appendPreparedBatchState(
	ctx context.Context,
	items []*ingestPending,
	plans []*IngestPreparedRequest,
) error {
	if len(items) != len(plans) {
		return fmt.Errorf("prepared ingest plan count mismatch")
	}
	envelopes := make([]walIngestEnvelope, 0, len(items))
	for index, pending := range items {
		if plans[index] == nil {
			continue
		}
		envelope := pending.envelope
		envelope.AcceptedLSN = pending.acceptedLSN
		envelope.State = IngestStatePrepared
		envelope.Result = &plans[index].Result
		envelope.Prepared = plans[index]
		envelope.Error = ""
		envelopes = append(envelopes, envelope)
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
	for _, envelope := range envelopes {
		pending := s.activeByStatus[ingestStatusKey(
			envelope.TenantID,
			envelope.Request.Source,
			envelope.Request.CollectorID,
			envelope.Request.BatchID,
		)]
		if pending == nil || pending.envelope.RecordID != envelope.RecordID {
			s.mu.Unlock()
			return fmt.Errorf("prepared ingest request is no longer active")
		}
		pending.envelope = envelope
		pending.state = IngestStatePrepared
		pending.err = nil
	}
	s.mu.Unlock()
	return nil
}

func (s *IngestService) appendPublishedBatchState(
	items []*ingestPending,
	results []IngestResult,
	records []IngestPublishedRecord,
) error {
	byIndex := make(map[int]IngestBatchRecord, len(records))
	for _, item := range records {
		byIndex[item.Index] = item.Record
	}
	envelopes := make([]walIngestEnvelope, len(items))
	now := time.Now().UTC()
	for index, pending := range items {
		envelope := pending.envelope
		envelope.AcceptedLSN = pending.acceptedLSN
		envelope.State = IngestStatePublished
		envelope.Result = &results[index]
		envelope.Error = ""
		if record, ok := byIndex[index]; ok {
			envelope.FinishedAt = record.FinishedAt
		} else if envelope.FinishedAt.IsZero() {
			envelope.FinishedAt = now
		}
		envelopes[index] = envelope
	}
	if err := s.appendIngestStateBatch(IngestWALPublished, envelopes); err != nil {
		return err
	}
	s.mu.Lock()
	for index, pending := range items {
		pending.envelope = envelopes[index]
		pending.state = IngestStatePublished
		pending.result = results[index]
		pending.finishedAt = envelopes[index].FinishedAt
		pending.err = nil
	}
	s.observeQueueLocked()
	s.mu.Unlock()
	return nil
}

func (s *IngestService) appendIngestStateBatch(
	kind IngestWALRecordType,
	envelopes []walIngestEnvelope,
) error {
	if len(envelopes) == 0 {
		return nil
	}
	payload, err := json.Marshal(walPreparedBatchEnvelope{Items: envelopes})
	if err != nil {
		return err
	}
	appendResult, err := s.wal.Append(s.runCtx, kind, payload)
	if errors.Is(err, ErrIngestWALFull) {
		if pruneErr := s.prune(s.runCtx); pruneErr == nil {
			appendResult, err = s.wal.Append(s.runCtx, kind, payload)
		}
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.highestLSN = max(s.highestLSN, appendResult.LSN)
	s.mu.Unlock()
	return nil
}

func (s *IngestService) finalizeMetadataItems(items []*ingestPending) error {
	envelopes := make([]walIngestEnvelope, len(items))
	for index, pending := range items {
		envelope := pending.envelope
		envelope.AcceptedLSN = pending.acceptedLSN
		envelope.State = IngestStateCommitted
		envelope.Error = ""
		if envelope.FinishedAt.IsZero() {
			envelope.FinishedAt = time.Now().UTC()
		}
		envelopes[index] = envelope
	}
	if err := s.appendIngestStateBatch(IngestWALFinalized, envelopes); err != nil {
		return err
	}
	for _, pending := range items {
		s.completePending(pending, pending.result, nil)
	}
	return nil
}

func (s *IngestService) appendPendingState(pending *ingestPending, kind IngestWALRecordType, state string, result *IngestResult, errorMessage string) error {
	envelope := pending.envelope
	envelope.AcceptedLSN = pending.acceptedLSN
	envelope.State = state
	envelope.Result = result
	envelope.Error = errorMessage
	if kind == IngestWALFinalized || kind == IngestWALFailed {
		envelope.FinishedAt = time.Now().UTC()
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	appendResult, err := s.wal.Append(s.runCtx, kind, payload)
	if errors.Is(err, ErrIngestWALFull) {
		if pruneErr := s.prune(s.runCtx); pruneErr == nil {
			appendResult, err = s.wal.Append(s.runCtx, kind, payload)
		}
	}
	if err == nil {
		s.mu.Lock()
		s.highestLSN = max(s.highestLSN, appendResult.LSN)
		s.mu.Unlock()
	}
	return err
}

func (s *IngestService) setPendingState(pending *ingestPending, state string) {
	s.mu.Lock()
	pending.state = state
	pending.err = nil
	s.mu.Unlock()
}

func (s *IngestService) setPendingRetry(pending *ingestPending, err error) {
	s.mu.Lock()
	pending.state = IngestStateRetrying
	pending.err = err
	s.observeQueueLocked()
	s.mu.Unlock()
}

func (s *IngestService) completePending(pending *ingestPending, result IngestResult, err error) {
	s.mu.Lock()
	pending.state = IngestStateCommitted
	if err != nil {
		pending.state = IngestStateFailed
	}
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
		delete(s.activeByStatus, statusKey)
		if s.config.Observer != nil {
			s.config.Observer.RecordIngestQueueCache("eviction")
		}
	}
	if pending.envelope.AcceptedAt.Equal(s.oldestPending) {
		s.oldestPending = time.Time{}
		for _, active := range s.active {
			if s.oldestPending.IsZero() || active.envelope.AcceptedAt.Before(s.oldestPending) {
				s.oldestPending = active.envelope.AcceptedAt
			}
		}
	}
	s.completedSince++
	shouldPrune := s.completedSince >= 128
	if shouldPrune {
		s.completedSince = 0
	}
	pending.completedOnce.Do(func() { close(pending.done) })
	s.observeQueueLocked()
	s.mu.Unlock()
	if shouldPrune {
		_ = s.prune(context.Background())
	}
}

func (s *IngestService) prune(ctx context.Context) error {
	s.mu.Lock()
	before := s.highestLSN + 1
	for _, pending := range s.active {
		if pending.acceptedLSN < before {
			before = pending.acceptedLSN
		}
	}
	s.mu.Unlock()
	return s.wal.Prune(ctx, before)
}

func (s *IngestService) recordError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
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
	if s.config.Metadata.Mode == IngestMetadataModeSegment {
		metadataPending := 0
		var metadataBytes int64
		var metadataOldest time.Time
		for _, pending := range s.active {
			if pending.state != IngestStatePublished && !(pending.state == IngestStateRetrying && pending.envelope.State == IngestStatePublished) {
				continue
			}
			metadataPending++
			metadataBytes += pending.bytes
			publishedAt := pending.envelope.FinishedAt
			if publishedAt.IsZero() {
				publishedAt = pending.envelope.AcceptedAt
			}
			if metadataOldest.IsZero() || publishedAt.Before(metadataOldest) {
				metadataOldest = publishedAt
			}
		}
		metadataAge := time.Duration(0)
		if !metadataOldest.IsZero() {
			metadataAge = time.Since(metadataOldest)
		}
		s.config.Observer.RecordIngestMetadataQueue(metadataPending, metadataBytes, metadataAge)
	}
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

func acceptanceFromPending(pending *ingestPending, durability string) IngestAcceptance {
	reportedDurability := "memory"
	if durability == IngestWALDurabilitySync {
		reportedDurability = "durable"
	}
	return IngestAcceptance{
		BatchID:        pending.envelope.Request.BatchID,
		Source:         pending.envelope.Request.Source,
		CollectorID:    pending.envelope.Request.CollectorID,
		State:          pending.state,
		Durability:     reportedDurability,
		AcceptedAt:     pending.envelope.AcceptedAt,
		EstimatedFlush: pending.estimated,
		tenantID:       pending.envelope.TenantID,
		acceptedLSN:    pending.acceptedLSN,
		recordID:       pending.envelope.RecordID,
		completion:     pending.done,
		pending:        pending,
	}
}

func statusFromPending(pending *ingestPending, durability string) IngestBatchStatus {
	reportedDurability := "memory"
	if durability == IngestWALDurabilitySync {
		reportedDurability = "durable"
	}
	status := IngestBatchStatus{
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
