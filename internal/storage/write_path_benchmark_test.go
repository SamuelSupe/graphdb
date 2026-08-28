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

func BenchmarkSingleEntityIndexedNoopCommit(b *testing.B) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "bench")
	seedBenchmarkEntities(b, ctx, store)
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:00000", Kind: "host", Fields: graph.Fields{"state": "ready"},
	}}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if !result.Skipped {
			b.Fatal("identical entity commit was not skipped")
		}
	}
}

func BenchmarkIngestConflictAssociationLargeBatch(b *testing.B) {
	const itemCount = 10_000
	request := IngestRequest{Items: make([]IngestItem, itemCount)}
	for i := range request.Items {
		id := fmt.Sprintf("host:%05d", i)
		request.Items[i] = IngestItem{ExternalID: id, Entity: &graph.Entity{ID: id, Kind: "host"}}
	}
	conflicts := make([]graph.FieldConflict, itemCount)
	for i := range conflicts {
		conflicts[i] = graph.FieldConflict{ResourceType: "entity", IncomingID: "host:09999", Field: "state"}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ingestConflicts(request, conflicts)
	}
}

func BenchmarkSaveIngestBatchDualKeys10K(b *testing.B) {
	const itemCount = 10_000
	request := IngestRequest{
		Source:         "loadtest",
		CollectorID:    "collector-a",
		BatchID:        "batch-a",
		IdempotencyKey: "idempotency-a",
		Items:          make([]IngestItem, itemCount),
	}
	for i := range request.Items {
		id := fmt.Sprintf("host:%05d", i)
		request.Items[i] = IngestItem{
			ExternalID: id,
			Entity: &graph.Entity{
				ID:     id,
				Kind:   "host",
				Fields: graph.Fields{"state": "ready"},
			},
		}
	}
	record := IngestBatchRecord{
		Request: request,
		Result: IngestResult{
			BatchID: request.BatchID,
			Applied: len(request.Items),
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store := NewTenantStore(NewMemoryStore(), "bench")
		if err := store.saveIngestBatch(ctx, "tenant-a", record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSaveIngestBatchDualKeyLatency(b *testing.B) {
	record := IngestBatchRecord{
		Request: IngestRequest{
			Source:         "loadtest",
			CollectorID:    "collector-a",
			BatchID:        "batch-a",
			IdempotencyKey: "idempotency-a",
		},
		Result: IngestResult{BatchID: "batch-a"},
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store := NewTenantStore(&delayedIngestPutStore{
			ObjectStore: NewMemoryStore(),
			delay:       5 * time.Millisecond,
		}, "bench")
		if err := store.saveIngestBatch(ctx, "tenant-a", record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoadNewIngestRecordDualKeys(b *testing.B) {
	objects := &delayedIngestHeadStore{
		ObjectStore: NewMemoryStore(),
		delay:       5 * time.Millisecond,
	}
	store := NewTenantStore(objects, "bench")
	request := IngestRequest{
		Source:         "loadtest",
		CollectorID:    "collector-a",
		BatchID:        "batch-a",
		IdempotencyKey: "idempotency-a",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, found, err := store.loadIngestRecord(ctx, "tenant-a", request); err != nil {
			b.Fatal(err)
		} else if found {
			b.Fatal("unexpected ingest record")
		}
	}
}

type delayedIngestHeadStore struct {
	ObjectStore
	delay time.Duration
}

type delayedIngestPutStore struct {
	ObjectStore
	delay time.Duration
}

func (s *delayedIngestPutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return s.ObjectStore.PutConditional(ctx, key, data, condition)
	case <-ctx.Done():
		return ObjectMeta{Key: key}, ctx.Err()
	}
}

func (s *delayedIngestHeadStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ObjectMeta{Key: key}, ErrNotFound
	case <-ctx.Done():
		return ObjectMeta{Key: key}, ctx.Err()
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
