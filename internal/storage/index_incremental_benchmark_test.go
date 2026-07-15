package storage

import (
	"context"
	"fmt"
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
