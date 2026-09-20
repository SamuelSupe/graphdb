package storage

import (
	"context"
	"testing"
)

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
