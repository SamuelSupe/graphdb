package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func BenchmarkIncrementalEntityPageSmallUpdate(b *testing.B) {
	for _, count := range []int{10_000, 100_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			entities := make([]graph.Entity, count)
			for i := range entities {
				entities[i] = graph.Entity{ID: fmt.Sprintf("host:%06d", i), Kind: "host", Fields: graph.Fields{"state": "ready"}}
			}
			before, err := graph.FromSnapshot(graph.Snapshot{Version: 1, Entities: entities})
			if err != nil {
				b.Fatal(err)
			}
			after, _, err := before.ApplyCommitStorageCopyWithOptions(graph.Commit{Version: 2, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: entities[0].ID, Kind: "host", Fields: graph.Fields{"state": "changed"}}}}}, graph.ApplyOptions{})
			if err != nil {
				b.Fatal(err)
			}
			store := NewTenantStore(NewMemoryStore(), "bench")
			now := time.Now()
			build := func() {
				if _, _, err := store.buildIncrementalEntityPages(context.Background(), "tenant", 1, nil, before, after, []string{entities[0].ID}, 2, now); err != nil {
					b.Fatal(err)
				}
			}
			build()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				build()
			}
		})
	}
}

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
