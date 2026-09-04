package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const benchmarkIngestRequestsPerFlush = 8
const benchmarkIngestSeedEntities = 10_000

func BenchmarkIngestWriteModes(b *testing.B) {
	b.Run("direct_per_request", func(b *testing.B) {
		store := NewTenantStore(NewMemoryStore(), "bench")
		ctx := context.Background()
		seedIngestBenchmarkGraph(b, ctx, store)
		b.ReportAllocs()
		b.ReportMetric(benchmarkIngestRequestsPerFlush, "requests/op")
		b.ResetTimer()
		for iteration := range b.N {
			for requestIndex := range benchmarkIngestRequestsPerFlush {
				request := benchmarkIngestRequest(iteration, requestIndex)
				if _, err := store.Ingest(ctx, "tenant-a", request); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("wal_tenant_flush", func(b *testing.B) {
		store := NewTenantStore(NewMemoryStore(), "bench")
		ctx := context.Background()
		seedIngestBenchmarkGraph(b, ctx, store)
		warmup := make([]IngestBatchEntry, benchmarkIngestRequestsPerFlush)
		for requestIndex := range benchmarkIngestRequestsPerFlush {
			warmup[requestIndex].Request = benchmarkIngestRequest(-1, requestIndex)
		}
		if _, err := store.IngestDurableBatch(ctx, "tenant-a", warmup); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ReportMetric(benchmarkIngestRequestsPerFlush, "requests/op")
		b.ResetTimer()
		for iteration := range b.N {
			entries := make([]IngestBatchEntry, benchmarkIngestRequestsPerFlush)
			for requestIndex := range benchmarkIngestRequestsPerFlush {
				entries[requestIndex].Request = benchmarkIngestRequest(iteration, requestIndex)
			}
			if _, err := store.IngestDurableBatch(ctx, "tenant-a", entries); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkIngestTerminalCompletion(b *testing.B) {
	benchmarks := []struct {
		active   int
		complete int
	}{
		{active: 256, complete: 256},
		{active: 4096, complete: 256},
		{active: 8192, complete: 256},
	}

	for _, benchmark := range benchmarks {
		benchmark := benchmark
		b.Run(fmt.Sprintf("active_%d_complete_%d", benchmark.active, benchmark.complete), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			tempRoot := b.TempDir()
			for range b.N {
				service, wal, items, results, walDir := setupIngestTerminalCompletionBenchmark(b, tempRoot, benchmark.active)

				b.StartTimer()
				retryIndex, err := service.appendTerminalBatch(items[:benchmark.complete], results[:benchmark.complete])
				b.StopTimer()

				if err != nil {
					b.Fatalf("append terminal batch: %v", err)
				}
				if retryIndex != benchmark.complete {
					b.Fatalf("retry index = %d, want %d", retryIndex, benchmark.complete)
				}
				assertIngestTerminalCompletionBenchmarkState(b, service, items, results, benchmark.complete)
				if err := wal.Close(); err != nil {
					b.Fatalf("close benchmark WAL: %v", err)
				}
				if err := os.RemoveAll(walDir); err != nil {
					b.Fatalf("remove benchmark WAL: %v", err)
				}
			}
		})
	}
}

func setupIngestTerminalCompletionBenchmark(
	b *testing.B,
	tempRoot string,
	activeCount int,
) (*IngestService, *IngestWAL, []*ingestPending, []IngestResult, string) {
	b.Helper()
	walDir, err := os.MkdirTemp(tempRoot, "ingest-terminal-wal-")
	if err != nil {
		b.Fatalf("create benchmark WAL directory: %v", err)
	}
	config := DefaultIngestServiceConfig(walDir)
	config.WAL.BufferBytes = 1 << 20
	config.WAL.FsyncInterval = time.Millisecond
	config.WAL.MaxBytes = 32 << 20
	config.WAL.SegmentBytes = 16 << 20
	config.WAL.AppendQueue = activeCount
	wal, _, err := OpenIngestWAL(config.WAL)
	if err != nil {
		b.Fatalf("open benchmark WAL: %v", err)
	}

	const tenantID = "tenant-a"
	accepted := make([]ingestWALBatchRecord, activeCount)
	items := make([]*ingestPending, activeCount)
	results := make([]IngestResult, activeCount)
	acceptedAt := time.Unix(1_700_000_000, 0).UTC()
	for index := range activeCount {
		request := benchmarkTerminalCompletionRequest(index)
		envelope := walIngestEnvelope{
			RecordID:   ingestRecordID(ingestRequestIdentity(tenantID, request)),
			TenantID:   tenantID,
			Request:    request,
			AcceptedAt: acceptedAt.Add(time.Duration(index) * time.Nanosecond),
			State:      IngestStateAccepted,
		}
		payload, err := json.Marshal(envelope)
		if err != nil {
			_ = wal.Close()
			b.Fatalf("marshal accepted benchmark item %d: %v", index, err)
		}
		accepted[index] = ingestWALBatchRecord{kind: IngestWALAccepted, payload: payload}
		items[index] = &ingestPending{
			envelope: envelope,
			bytes:    int64(len(payload) + ingestWALHeaderBytes + ingestWALChecksumBytes),
			state:    IngestStateAccepted,
			done:     make(chan struct{}),
		}
		results[index] = IngestResult{BatchID: request.BatchID, Version: int64(index + 1), Applied: 1}
	}
	responses := wal.appendBatch(context.Background(), accepted)
	service := &IngestService{
		store:          NewTenantStore(NewMemoryStore(), "benchmark"),
		wal:            wal,
		config:         config,
		active:         make(map[string]*ingestPending, activeCount),
		activeByStatus: make(map[string]*ingestPending, activeCount),
		runCtx:         context.Background(),
	}
	for index, response := range responses {
		if response.err != nil {
			_ = wal.Close()
			b.Fatalf("append accepted benchmark item %d: %v", index, response.err)
		}
		items[index].acceptedLSN = response.result.LSN
		identity := ingestRequestIdentity(tenantID, items[index].envelope.Request)
		statusKey := ingestStatusKey(
			tenantID,
			items[index].envelope.Request.Source,
			items[index].envelope.Request.CollectorID,
			items[index].envelope.Request.BatchID,
		)
		service.active[identity] = items[index]
		service.activeByStatus[statusKey] = items[index]
		service.pendingBytes += items[index].bytes
		if service.oldestPending.IsZero() || items[index].envelope.AcceptedAt.Before(service.oldestPending) {
			service.oldestPending = items[index].envelope.AcceptedAt
		}
		service.highestLSN = max(service.highestLSN, response.result.LSN)
	}
	return service, wal, items, results, walDir
}

func benchmarkTerminalCompletionRequest(index int) IngestRequest {
	identity := fmt.Sprintf("completion-%d", index)
	return IngestRequest{
		Source:      "benchmark",
		CollectorID: "collector",
		BatchID:     identity,
		Items: []IngestItem{{
			ExternalID: identity,
			Entity: &graph.Entity{
				ID:   identity,
				Kind: "host",
			},
		}},
	}
}

func assertIngestTerminalCompletionBenchmarkState(
	b *testing.B,
	service *IngestService,
	items []*ingestPending,
	results []IngestResult,
	completed int,
) {
	b.Helper()
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.active) != len(items)-completed {
		b.Fatalf("active requests = %d, want %d", len(service.active), len(items)-completed)
	}
	if len(service.activeByStatus) != len(items)-completed {
		b.Fatalf("active status entries = %d, want %d", len(service.activeByStatus), len(items)-completed)
	}
	var wantPendingBytes int64
	for index, pending := range items {
		identity := ingestRequestIdentity(pending.envelope.TenantID, pending.envelope.Request)
		_, active := service.active[identity]
		statusKey := ingestStatusKey(
			pending.envelope.TenantID,
			pending.envelope.Request.Source,
			pending.envelope.Request.CollectorID,
			pending.envelope.Request.BatchID,
		)
		_, activeStatus := service.activeByStatus[statusKey]
		if index < completed {
			result := results[index]
			if active || activeStatus || pending.state != IngestStateCommitted ||
				pending.result.BatchID != result.BatchID ||
				pending.result.Version != result.Version ||
				pending.result.Applied != result.Applied ||
				pending.result.Failed != result.Failed {
				b.Fatalf("completed item %d active=%t state=%q result=%#v", index, active, pending.state, pending.result)
			}
			select {
			case <-pending.done:
			default:
				b.Fatalf("completed item %d is not signaled", index)
			}
			continue
		}
		if !active || !activeStatus || pending.state != IngestStateAccepted {
			b.Fatalf("remaining item %d active=%t state=%q", index, active, pending.state)
		}
		wantPendingBytes += pending.bytes
	}
	if service.pendingBytes != wantPendingBytes {
		b.Fatalf("pending bytes = %d, want %d", service.pendingBytes, wantPendingBytes)
	}
	if completed < len(items) && !service.oldestPending.Equal(items[completed].envelope.AcceptedAt) {
		b.Fatalf("oldest pending = %s, want %s", service.oldestPending, items[completed].envelope.AcceptedAt)
	}
}

func seedIngestBenchmarkGraph(b *testing.B, ctx context.Context, store *TenantStore) {
	b.Helper()
	entities := make([]graph.Entity, benchmarkIngestSeedEntities)
	for index := range entities {
		identity := fmt.Sprintf("seed:%d", index)
		entities[index] = graph.Entity{ID: identity, Kind: "seed", Fields: graph.Fields{"name": identity}}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
}

func benchmarkIngestRequest(iteration int, requestIndex int) IngestRequest {
	identity := fmt.Sprintf("%d-%d", iteration, requestIndex)
	return ingestEntityRequest("batch-"+identity, "host:"+identity)
}
