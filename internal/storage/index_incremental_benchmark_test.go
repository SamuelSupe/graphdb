package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func BenchmarkIncrementalIndexedEntityCommit10K(b *testing.B) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "bench")
	entities := make([]graph.Entity, 10_000)
	for i := range entities {
		id := fmt.Sprintf("host:%05d", i)
		entities[i] = graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{
			"hostname": id,
			"region":   fmt.Sprintf("region-%02d", i%16),
		}}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{
			"hostname": {Type: "string", Indexed: true},
			"region":   {Type: "string", Indexed: true},
		}}},
		UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("host:%05d", i%len(entities))
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: id, Kind: "host", Fields: graph.Fields{"hostname": fmt.Sprintf("changed-%05d", i)},
		}}}, CommitOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalIndexedConcurrentCommit10K(b *testing.B) {
	ctx := context.Background()
	files, err := OpenFileStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "bench")
	entities := make([]graph.Entity, 10_000)
	for i := range entities {
		id := fmt.Sprintf("host:%05d", i)
		entities[i] = graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"hostname": id}}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes:  []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}}}},
		UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		b.Fatal(err)
	}
	var warnings atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	for round := 0; round < b.N; round++ {
		var wg sync.WaitGroup
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				updates := make([]graph.Entity, 20)
				for i := range updates {
					id := (round*80 + worker*20 + i) % len(entities)
					updates[i] = graph.Entity{ID: entities[id].ID, Kind: "host", Fields: graph.Fields{"hostname": fmt.Sprintf("changed-%d-%d", round, id)}}
				}
				result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: updates}, CommitOptions{})
				if err != nil {
					b.Error(err)
				}
				warnings.Add(int64(len(result.IndexWarnings)))
			}(worker)
		}
		wg.Wait()
	}
	b.ReportMetric(float64(warnings.Load())/float64(b.N*4), "warnings/commit")
}
