package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func benchmarkEntityRecord(id string) EntityRecord {
	entity := graph.Entity{
		ID:         id,
		Kind:       "host",
		Source:     "benchmark",
		ExternalID: id,
		Fields: map[string]any{
			"hostname": id,
			"region":   "us-east-1",
			"cpu":      float64(8),
		},
		Version:    1,
		CreatedAt:  time.Unix(1, 0).UTC(),
		UpdatedAt:  time.Unix(2, 0).UTC(),
		Confidence: 1,
	}
	return newEntityRecord("tenant-a", entity, "00", "page-hash", "page-etag", 1, time.Unix(2, 0).UTC())
}

func BenchmarkMarshalEntityRecord(b *testing.B) {
	ctx := context.Background()
	record := benchmarkEntityRecord("host:benchmark")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalParquetEntityRecord(ctx, record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutEntityRecordBatch(b *testing.B) {
	const recordCount = 100
	jobs := benchmarkEntityRecordJobs(recordCount)

	b.Run("new", func(b *testing.B) {
		ctx := context.Background()
		b.ReportAllocs()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			store := NewTenantStore(NewMemoryStore(), fmt.Sprintf("bench-%d", i))
			iterationJobs := append([]entityRecordWriteJob(nil), jobs...)
			for j := range iterationJobs {
				iterationJobs[j].Key = store.entityRecordKey("tenant-a", iterationJobs[j].Record.ID)
			}
			b.StartTimer()
			if err := store.putEntityRecordBatch(ctx, iterationJobs); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		}
	})

	b.Run("unchanged", func(b *testing.B) {
		ctx := context.Background()
		store := NewTenantStore(NewMemoryStore(), "bench")
		for i := range jobs {
			jobs[i].Key = store.entityRecordKey("tenant-a", jobs[i].Record.ID)
		}
		if err := store.putEntityRecordBatch(ctx, jobs); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := store.putEntityRecordBatch(ctx, jobs); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("page-etag-changed", func(b *testing.B) {
		ctx := context.Background()
		b.ReportAllocs()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			store := NewTenantStore(NewMemoryStore(), fmt.Sprintf("bench-changed-%d", i))
			seedJobs := append([]entityRecordWriteJob(nil), jobs...)
			changedJobs := append([]entityRecordWriteJob(nil), jobs...)
			for j := range seedJobs {
				key := store.entityRecordKey("tenant-a", seedJobs[j].Record.ID)
				seedJobs[j].Key = key
				changedJobs[j].Key = key
				changedJobs[j].Record.PageETag = "new-page-etag"
				changedJobs[j].Record.Version++
				stampEntityRecordHash(&changedJobs[j].Record)
			}
			b.StopTimer()
			if err := store.putEntityRecordBatch(ctx, seedJobs); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := store.putEntityRecordBatch(ctx, changedJobs); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
		}
	})
}

func BenchmarkPutEntityRecordBatch10KNew(b *testing.B) {
	jobs := benchmarkEntityRecordJobs(10_000)
	ctx := context.Background()
	b.ReportAllocs()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		store := NewTenantStore(NewMemoryStore(), fmt.Sprintf("bench-10k-%d", i))
		iterationJobs := append([]entityRecordWriteJob(nil), jobs...)
		for j := range iterationJobs {
			iterationJobs[j].Key = store.entityRecordKey("tenant-a", iterationJobs[j].Record.ID)
		}
		b.StartTimer()
		if err := store.putEntityRecordBatch(ctx, iterationJobs); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func benchmarkEntityRecordJobs(count int) []entityRecordWriteJob {
	jobs := make([]entityRecordWriteJob, count)
	for i := range jobs {
		id := fmt.Sprintf("host:%04d", i)
		jobs[i].Record = benchmarkEntityRecord(id)
	}
	return jobs
}
