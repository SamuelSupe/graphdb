package storage

import (
	"context"
	"testing"
)

func putEntityRecordCacheFixture(
	t *testing.T,
	ctx context.Context,
	objects ObjectStore,
	key string,
	record EntityRecord,
) {
	t.Helper()
	data, err := marshalParquetEntityRecord(ctx, record)
	if err != nil {
		t.Fatalf("marshal entity record: %v", err)
	}
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put entity record: %v", err)
	}
}
