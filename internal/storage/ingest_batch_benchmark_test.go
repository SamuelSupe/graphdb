package storage

import (
	"context"
	"fmt"
	"testing"

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
