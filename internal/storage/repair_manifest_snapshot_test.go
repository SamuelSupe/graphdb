package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type snapshotGetCountingStore struct {
	ObjectStore
	prefix   string
	getCalls int
}

func (s *snapshotGetCountingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if len(key) >= len(s.prefix) && key[:len(s.prefix)] == s.prefix {
		s.getCalls++
	}
	return s.ObjectStore.Get(ctx, key)
}

func TestLatestValidSnapshotStopsAfterNewestValidSnapshot(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	putEmptySnapshots(t, writer, "tenant-a", 1, 2, 3)

	counting := &snapshotGetCountingStore{
		ObjectStore: base,
		prefix:      writer.snapshotPrefix("tenant-a"),
	}
	store := NewTenantStore(counting, "test")
	snapshot, key, err := store.latestValidSnapshot(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("latest valid snapshot: %v", err)
	}
	if snapshot == nil || snapshot.Version != 3 {
		t.Fatalf("snapshot = %#v, want version 3", snapshot)
	}
	if want := store.snapshotKey("tenant-a", 3); key != want {
		t.Fatalf("snapshot key = %q, want %q", key, want)
	}
	if counting.getCalls != 1 {
		t.Fatalf("snapshot reads = %d, want 1", counting.getCalls)
	}
}

func TestLatestValidSnapshotFallsBackFromCorruptNewestSnapshot(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	putEmptySnapshots(t, writer, "tenant-a", 2)
	if err := base.Put(
		ctx,
		writer.snapshotKey("tenant-a", 3),
		[]byte("corrupt"),
	); err != nil {
		t.Fatalf("put corrupt snapshot: %v", err)
	}

	counting := &snapshotGetCountingStore{
		ObjectStore: base,
		prefix:      writer.snapshotPrefix("tenant-a"),
	}
	store := NewTenantStore(counting, "test")
	snapshot, key, err := store.latestValidSnapshot(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("latest valid snapshot: %v", err)
	}
	if snapshot == nil || snapshot.Version != 2 {
		t.Fatalf("snapshot = %#v, want version 2", snapshot)
	}
	if want := store.snapshotKey("tenant-a", 2); key != want {
		t.Fatalf("snapshot key = %q, want %q", key, want)
	}
	if counting.getCalls != 2 {
		t.Fatalf("snapshot reads = %d, want 2", counting.getCalls)
	}
}

func putEmptySnapshots(
	t *testing.T,
	store *TenantStore,
	tenantID string,
	versions ...int64,
) {
	t.Helper()
	for _, version := range versions {
		if err := store.putSnapshotRecordIfAbsentOrEquivalent(
			context.Background(),
			store.snapshotKey(tenantID, version),
			snapshotRecord{
				TenantID: tenantID,
				Snapshot: graph.Snapshot{Version: version},
			},
		); err != nil {
			t.Fatalf("put snapshot %d: %v", version, err)
		}
	}
}
