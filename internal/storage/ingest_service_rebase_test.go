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

type shrinkingCASIngestStore struct {
	IngestStore
	mu    sync.Mutex
	calls []int
	depth int64
}

func (s *shrinkingCASIngestStore) CoordinationBackend() string {
	return CoordinationPostgres
}

func (s *shrinkingCASIngestStore) SetIngestBarrier(func(context.Context, string) error) {}

func (s *shrinkingCASIngestStore) GetIngestBatch(context.Context, string, string, string, string) (IngestBatchRecord, error) {
	return IngestBatchRecord{}, ErrNotFound
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
