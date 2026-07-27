package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReaderHeartbeatScanDoesNotDeleteFreshCachedReader(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(
		NewWriterObjectCache(objects, WriterObjectCacheConfig{
			MaxBytes:    1 << 20,
			MaxKeys:     100,
			NegativeTTL: time.Hour,
		}),
		"test",
	)
	key := store.readerHeartbeatKey("tenant-a", "reader-a")
	old := ReaderHeartbeat{
		ReaderID:       "reader-a",
		TenantID:       "tenant-a",
		VisibleVersion: 1,
		LastSeenAt:     time.Now().UTC().Add(-time.Hour),
	}
	putReaderHeartbeatFixture(t, ctx, objects, key, old)
	if _, err := store.ListReaderHeartbeatsWithOptions(
		ctx,
		"tenant-a",
		ReaderHeartbeatListOptions{
			Limit:         10,
			ScanLimit:     10,
			DeleteExpired: false,
		},
	); err != nil {
		t.Fatalf("prime reader heartbeat cache: %v", err)
	}

	fresh := old
	fresh.VisibleVersion = 2
	fresh.LastSeenAt = time.Now().UTC()
	putReaderHeartbeatFixture(t, ctx, objects, key, fresh)
	listed, err := store.ListReaderHeartbeatsWithOptions(
		ctx,
		"tenant-a",
		ReaderHeartbeatListOptions{
			MaxAge:        time.Minute,
			Limit:         10,
			ScanLimit:     10,
			DeleteExpired: true,
		},
	)
	if err != nil {
		t.Fatalf("list fresh reader heartbeat: %v", err)
	}
	if len(listed) != 1 || listed[0].VisibleVersion != 2 {
		t.Fatalf("listed heartbeats = %#v", listed)
	}
	if _, err := objects.Get(ctx, key); errors.Is(err, ErrNotFound) {
		t.Fatal("fresh reader heartbeat was deleted")
	} else if err != nil {
		t.Fatalf("get fresh reader heartbeat: %v", err)
	}
}

func putReaderHeartbeatFixture(
	t *testing.T,
	ctx context.Context,
	objects ObjectStore,
	key string,
	heartbeat ReaderHeartbeat,
) {
	t.Helper()
	data, err := marshalParquetReaderHeartbeat(ctx, heartbeat)
	if err != nil {
		t.Fatalf("marshal reader heartbeat: %v", err)
	}
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put reader heartbeat: %v", err)
	}
}
