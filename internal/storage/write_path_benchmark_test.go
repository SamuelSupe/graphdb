package storage

import (
	"context"
	"fmt"
	"testing"

	"graphdb/internal/graph"
)

func BenchmarkSingleEntityIndexedCommit(b *testing.B) {
	benchmarkSingleEntityIndexedCommit(b, false)
}

func BenchmarkSingleEntityIndexedCommitWithEntityRecords(b *testing.B) {
	benchmarkSingleEntityIndexedCommit(b, true)
}

func benchmarkSingleEntityIndexedCommit(b *testing.B, writeEntityRecords bool) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "bench")
	store.WriteEntityRecords = writeEntityRecords
	entities := make([]graph.Entity, 10_000)
	for i := range entities {
		entities[i] = graph.Entity{
			ID:     fmt.Sprintf("host:%05d", i),
			Kind:   "host",
			Fields: graph.Fields{"state": "ready"},
		}
	}
	seed := graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"state": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: entities,
	}
	if _, err := store.Commit(ctx, "tenant-a", seed, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID:     "host:00000",
			Kind:   "host",
			Fields: graph.Fields{"state": fmt.Sprintf("updated-%d", i)},
		}}}, CommitOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}
