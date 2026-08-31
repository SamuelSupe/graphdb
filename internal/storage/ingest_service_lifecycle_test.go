package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestServiceFinalizesLifecycleFenceAsFailed(t *testing.T) {
	store := &lifecycleFencingIngestStore{err: ErrTenantDeleted}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	request := IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "batch-deleted",
		Items: []IngestItem{{
			ExternalID: "host-a",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host"},
		}},
	}
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush after lifecycle fence: %v", err)
	}
	result, err := service.Wait(flushCtx, accepted)
	if err != nil {
		t.Fatalf("wait for terminal lifecycle failure: %v", err)
	}
	if result.Applied != 0 || result.Failed != 1 || len(result.Failures) != 1 ||
		!strings.Contains(result.Failures[0].Error, ErrTenantDeleted.Error()) {
		t.Fatalf("terminal lifecycle result = %#v", result)
	}

	status, err := service.Status(flushCtx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("status after terminal lifecycle failure: %v", err)
	}
	if status.State != IngestStateFailed || status.RecoveryPending || status.Result == nil || status.Result.Failed != 1 {
		t.Fatalf("status after lifecycle fence = %#v", status)
	}
}

func TestIngestServiceStatusRestoresPersistedFailureState(t *testing.T) {
	started := time.Unix(100, 0).UTC()
	finished := started.Add(time.Second)
	store := &lifecycleFencingIngestStore{
		record: &IngestBatchRecord{
			TenantID: "tenant-a",
			Request: IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "batch-persisted-failed",
			},
			Result: IngestResult{
				BatchID: "batch-persisted-failed",
				Failed:  1,
			},
			StartedAt:  started,
			FinishedAt: finished,
		},
	}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	status, err := service.Status(context.Background(), "tenant-a", "agent", "collector-a", "batch-persisted-failed")
	if err != nil {
		t.Fatalf("status from persisted failure: %v", err)
	}
	if status.State != IngestStateFailed || status.RecoveryPending || status.Result == nil ||
		status.Result.Failed != 1 || status.Result.Applied != 0 {
		t.Fatalf("status from persisted failure = %#v", status)
	}
}

type lifecycleFencingIngestStore struct {
	IngestStore
	err    error
	record *IngestBatchRecord
}

func (s *lifecycleFencingIngestStore) CoordinationBackend() string {
	return CoordinationPostgres
}

func (s *lifecycleFencingIngestStore) SetIngestBarrier(func(context.Context, string) error) {}

func (s *lifecycleFencingIngestStore) GetIngestBatch(context.Context, string, string, string, string) (IngestBatchRecord, error) {
	if s.record != nil {
		return *s.record, nil
	}
	return IngestBatchRecord{}, ErrNotFound
}

func (s *lifecycleFencingIngestStore) IngestDurableBatchWithHooks(
	context.Context,
	string,
	[]IngestBatchEntry,
	IngestBatchHooks,
) ([]IngestResult, error) {
	return nil, s.err
}

var _ IngestStore = (*lifecycleFencingIngestStore)(nil)
