package storage

import (
	"context"
	"testing"
	"time"
)

func TestPostgresDeadLetterListIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	letter := DeadLetter{
		ID:       "batch-a",
		TenantID: "tenant-a",
		Source:   "agent",
		BatchID:  "batch-a",
		Request: IngestRequest{
			Source:  "agent",
			BatchID: "batch-a",
		},
		LastResult: IngestResult{BatchID: "batch-a", Failed: 1},
		Status:     "pending",
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	key := store.deadLetterKey(
		letter.TenantID,
		letter.Source,
		letter.ID,
	)
	putDeadLetterCacheFixture(t, ctx, objects, key, letter)
	if listed, err := store.ListDeadLetters(
		ctx,
		letter.TenantID,
		letter.Source,
	); err != nil || len(listed) != 1 ||
		listed[0].Status != "pending" {
		t.Fatalf("prime deadletters = %#v, err %v", listed, err)
	}

	letter.Status = "resolved"
	letter.UpdatedAt = time.Now().UTC()
	putDeadLetterCacheFixture(t, ctx, objects, key, letter)
	listed, err := store.ListDeadLetters(
		ctx,
		letter.TenantID,
		letter.Source,
	)
	if err != nil {
		t.Fatalf("list updated deadletters: %v", err)
	}
	if len(listed) != 1 || listed[0].Status != "resolved" {
		t.Fatalf("updated deadletters = %#v", listed)
	}
}

func putDeadLetterCacheFixture(
	t *testing.T,
	ctx context.Context,
	objects ObjectStore,
	key string,
	letter DeadLetter,
) {
	t.Helper()
	data, err := marshalParquetDeadLetter(ctx, letter)
	if err != nil {
		t.Fatalf("marshal deadletter: %v", err)
	}
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put deadletter: %v", err)
	}
}
