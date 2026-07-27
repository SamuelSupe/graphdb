package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func BenchmarkSingleEntityIndexedCommit(b *testing.B) {
	benchmarkSingleEntityIndexedCommit(b, false)
}

func BenchmarkSingleEntityIndexedCommitWithEntityRecords(b *testing.B) {
	benchmarkSingleEntityIndexedCommit(b, true)
}

func BenchmarkSingleEntityPostgresCoordinatedCommit(b *testing.B) {
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		b.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("graphdb_bench_%d", time.Now().UnixNano())
	coordinator, err := NewPostgresCoordinator(
		ctx, dsn, schema, "single-entity-commit",
	)
	if err != nil {
		b.Fatal(err)
	}
	if err := coordinator.Migrate(ctx); err != nil {
		coordinator.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_, _ = coordinator.pool.Exec(
			context.Background(),
			`DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`,
		)
		coordinator.Close()
	})
	store := NewTenantStore(NewMemoryStore(), "bench")
	store.SetCoordinator(coordinator)
	seedBenchmarkEntities(b, ctx, store)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID:     "host:00000",
				Kind:   "host",
				Fields: graph.Fields{"state": fmt.Sprintf("updated-%d", i)},
			}},
		}, CommitOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSingleEntityIndexedCommit(b *testing.B, writeEntityRecords bool) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "bench")
	store.WriteEntityRecords = writeEntityRecords
	seedBenchmarkEntities(b, ctx, store)
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

func seedBenchmarkEntities(
	b *testing.B,
	ctx context.Context,
	store *TenantStore,
) {
	b.Helper()
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
}
