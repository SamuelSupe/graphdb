package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestServiceShrinksCASConflictBatchWithoutTerminalFailure(t *testing.T) {
	store := &shrinkingCASIngestStore{}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 3
	config.RetryInterval = time.Millisecond
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	accepted := make([]IngestAcceptance, 0, 3)
	for index := range 3 {
		acceptance, err := service.Accept(context.Background(), "tenant-a", IngestRequest{
			Source: "agent", CollectorID: "collector-a", BatchID: "batch-" + string(rune('a'+index)),
			Items: []IngestItem{{
				ExternalID: "host-" + string(rune('a'+index)),
				Entity:     &graph.Entity{ID: "host:" + string(rune('a'+index)), Kind: "host"},
			}},
		})
		if err != nil {
			t.Fatalf("accept %d: %v", index, err)
		}
		accepted = append(accepted, acceptance)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("flush after CAS conflicts: %v", err)
	}
	for index, acceptance := range accepted {
		result, err := service.Wait(ctx, acceptance)
		if err != nil {
			t.Fatalf("wait %d: %v", index, err)
		}
		if result.Applied != 1 || result.Failed != 0 {
			t.Fatalf("result %d = %#v", index, result)
		}
	}

	calls := store.batchSizes()
	if len(calls) < 2 || calls[0] != 3 {
		t.Fatalf("batch sizes = %v, want initial three-request CAS attempt", calls)
	}
	shrunk := false
	for _, size := range calls[1:] {
		if size == 1 {
			shrunk = true
			break
		}
	}
	if !shrunk {
		t.Fatalf("batch sizes = %v, want a single-request retry after CAS conflict", calls)
	}
}

func TestIngestServiceKeepsBatchAfterFirstCASConflict(t *testing.T) {
	service := &IngestService{store: &shrinkingCASIngestStore{}}
	items := make([]*ingestPending, 8)
	for index := range items {
		items[index] = &ingestPending{casConflicts: 1}
	}
	if got := service.adaptiveFlushEnd(items, 0, len(items)); got != len(items) {
		t.Fatalf("first-conflict batch end = %d, want %d", got, len(items))
	}

	items[0].casConflicts = 2
	if got := service.adaptiveFlushEnd(items, 0, len(items)); got != 4 {
		t.Fatalf("second-conflict batch end = %d, want 4", got)
	}
}

func TestIngestServiceDoesNotSplitCASCohortOnConflictRetry(t *testing.T) {
	service := &IngestService{store: &shrinkingCASIngestStore{}}
	expected := int64(0)
	items := make([]*ingestPending, 8)
	for index := range items {
		items[index] = &ingestPending{
			casConflicts: 2,
			envelope:     walIngestEnvelope{Request: IngestRequest{ExpectedVersion: &expected}},
		}
	}
	if got := service.adaptiveFlushEnd(items, 0, len(items)); got != len(items) {
		t.Fatalf("same-expected CAS cohort end = %d, want %d so the cohort is retried intact", got, len(items))
	}
}

func TestIngestServiceShrinkingCASBatchReturnsToCohortBoundary(t *testing.T) {
	service := &IngestService{store: &shrinkingCASIngestStore{}}
	expected := int64(0)
	items := make([]*ingestPending, 10)
	for index := 0; index < 2; index++ {
		items[index] = &ingestPending{casConflicts: 2}
	}
	for index := 2; index < len(items); index++ {
		items[index] = &ingestPending{
			envelope: walIngestEnvelope{Request: IngestRequest{ExpectedVersion: &expected}},
		}
	}
	if got := service.adaptiveFlushEnd(items, 0, len(items)); got != 2 {
		t.Fatalf("ordinary-prefix/CAS-cohort split = %d, want cohort start 2", got)
	}
}

func TestIngestServiceCanShrinkAcrossAtomicAndExpectedVersionBarriers(t *testing.T) {
	service := &IngestService{store: &shrinkingCASIngestStore{}}
	expectedZero := int64(0)
	expectedOne := int64(1)
	newPending := func(expected *int64, atomic bool) *ingestPending {
		request := IngestRequest{ExpectedVersion: expected}
		if atomic {
			request.FailureMode = IngestFailureModeAtomic
		}
		return &ingestPending{envelope: walIngestEnvelope{Request: request}}
	}
	for _, test := range []struct {
		name  string
		items []*ingestPending
	}{
		{
			name: "atomic barrier",
			items: []*ingestPending{
				{casConflicts: 2, envelope: walIngestEnvelope{Request: IngestRequest{ExpectedVersion: &expectedZero}}},
				newPending(&expectedZero, false), newPending(&expectedZero, false), newPending(&expectedZero, false),
				newPending(&expectedZero, true), newPending(&expectedZero, false), newPending(&expectedZero, false), newPending(&expectedZero, false),
			},
		},
		{
			name: "different expected version",
			items: []*ingestPending{
				{casConflicts: 2, envelope: walIngestEnvelope{Request: IngestRequest{ExpectedVersion: &expectedZero}}},
				newPending(&expectedZero, false), newPending(&expectedZero, false), newPending(&expectedZero, false),
				newPending(&expectedOne, false), newPending(&expectedOne, false), newPending(&expectedOne, false), newPending(&expectedOne, false),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := service.adaptiveFlushEnd(test.items, 0, len(test.items)); got != 4 {
				t.Fatalf("barrier split = %d, want 4", got)
			}
		})
	}
}

func TestIngestServiceUsesShortBackoffForCASConflict(t *testing.T) {
	service := &IngestService{
		store:  &shrinkingCASIngestStore{},
		config: IngestServiceConfig{RetryInterval: time.Hour},
	}
	pending := &ingestPending{err: ErrConflict, casConflicts: 1}
	if got := service.ingestRetryDelay(pending); got != 5*time.Millisecond {
		t.Fatalf("first CAS conflict retry delay = %s, want 5ms", got)
	}
	pending.casConflicts = 2
	for sample := 0; sample < 20; sample++ {
		got := service.ingestRetryDelay(pending)
		if got < 5*time.Millisecond || got > 10*time.Millisecond {
			t.Fatalf("second CAS conflict retry delay = %s, want [5ms,10ms]", got)
		}
	}
}

func TestIngestServicePublishSlotContentionKeepsBatchAndUsesParkedBackoff(t *testing.T) {
	service := &IngestService{
		store:  &shrinkingCASIngestStore{},
		config: IngestServiceConfig{RetryInterval: time.Hour},
	}
	pending := &ingestPending{}
	service.setPendingRetry(pending, ErrTaskLeaseHeld)
	if pending.retryAttempts != 1 || pending.casConflicts != 0 {
		t.Fatalf("publish slot retry state = attempts %d CAS conflicts %d, want 1 and 0", pending.retryAttempts, pending.casConflicts)
	}
	items := make([]*ingestPending, 4)
	items[0] = pending
	for index := 1; index < len(items); index++ {
		items[index] = &ingestPending{}
	}
	if got := service.adaptiveFlushEnd(items, 0, len(items)); got != len(items) {
		t.Fatalf("publish slot retry batch end = %d, want %d", got, len(items))
	}
	if got := service.ingestRetryDelay(pending); got != 25*time.Millisecond {
		t.Fatalf("first publish slot retry delay = %s, want 25ms parking floor", got)
	}
	pending.retryAttempts = 3
	for sample := 0; sample < 20; sample++ {
		got := service.ingestRetryDelay(pending)
		if got <= 25*time.Millisecond || got > 80*time.Millisecond {
			t.Fatalf("later publish slot retry delay = %s, want (25ms,80ms] after exponential growth", got)
		}
	}
}

type shrinkingCASIngestStore struct {
	IngestStore
	mu         sync.Mutex
	calls      []int
	depth      int64
	generation int64
}

func (s *shrinkingCASIngestStore) CoordinationBackend() string {
	return CoordinationPostgres
}

func (s *shrinkingCASIngestStore) SetIngestBarrier(func(context.Context, string) error) {}

func (s *shrinkingCASIngestStore) CaptureIngestWALGeneration(context.Context, string) (int64, error) {
	if s.generation > 0 {
		return s.generation, nil
	}
	return 1, nil
}

func (s *shrinkingCASIngestStore) GetIngestBatch(context.Context, string, string, string, string) (IngestBatchRecord, error) {
	return IngestBatchRecord{}, ErrNotFound
}

func (s *shrinkingCASIngestStore) PersistIngestFailure(
	context.Context,
	string,
	IngestRequest,
	IngestResult,
	time.Time,
	time.Time,
) error {
	return nil
}

func (s *shrinkingCASIngestStore) IngestDurableBatchWithHooks(
	_ context.Context,
	_ string,
	entries []IngestBatchEntry,
	_ IngestBatchHooks,
) ([]IngestResult, error) {
	s.mu.Lock()
	s.calls = append(s.calls, len(entries))
	s.mu.Unlock()
	if len(entries) > 1 {
		return nil, ErrConflict
	}
	if len(entries) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	s.depth++
	version := s.depth
	s.mu.Unlock()
	return []IngestResult{{
		BatchID: entries[0].Request.BatchID,
		Version: version,
		Applied: len(entries[0].Request.Items),
	}}, nil
}

func (s *shrinkingCASIngestStore) batchSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.calls...)
}

var _ IngestStore = (*shrinkingCASIngestStore)(nil)
