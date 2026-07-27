package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type keyGetCountingStore struct {
	ObjectStore
	calls map[string]int
}

func (s *keyGetCountingStore) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	s.calls[key]++
	return s.ObjectStore.Get(ctx, key)
}

func TestReconstructManifestSkipsCommitObjectsCoveredBySnapshot(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	if err := writer.putSnapshotRecordIfAbsentOrEquivalent(
		ctx,
		writer.snapshotKey("tenant-a", 2),
		snapshotRecord{
			TenantID: "tenant-a",
			Snapshot: graph.Snapshot{Version: 2},
		},
	); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}

	oldCommit := graph.Commit{
		ID:       "old-loose",
		TenantID: "tenant-a",
		Version:  1,
	}
	oldKey := writer.commitKey("tenant-a", oldCommit.Version, oldCommit.ID)
	if err := writer.putCommitObjectIfAbsent(ctx, oldKey, oldCommit); err != nil {
		t.Fatalf("put old loose commit: %v", err)
	}
	newCommit := graph.Commit{
		ID:       "new-loose",
		TenantID: "tenant-a",
		Version:  3,
	}
	newKey := writer.commitKey("tenant-a", newCommit.Version, newCommit.ID)
	if err := writer.putCommitObjectIfAbsent(ctx, newKey, newCommit); err != nil {
		t.Fatalf("put new loose commit: %v", err)
	}

	segmentItems := make([]commitSegmentItem, 0, 2)
	for version := int64(1); version <= 2; version++ {
		commit := graph.Commit{
			ID:       "segment-" + string(rune('0'+version)),
			TenantID: "tenant-a",
			Version:  version,
		}
		segmentItems = append(segmentItems, commitSegmentItem{
			Key:    writer.commitKey("tenant-a", version, commit.ID),
			Commit: commit,
		})
	}
	segment, err := writer.putCommitSegment(ctx, "tenant-a", segmentItems)
	if err != nil {
		t.Fatalf("put old commit segment: %v", err)
	}

	counting := &keyGetCountingStore{
		ObjectStore: base,
		calls:       map[string]int{},
	}
	store := NewTenantStore(counting, "test")
	loaded, err := store.reconstructManifestFromObjects(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("reconstruct manifest: %v", err)
	}
	if loaded.Manifest.Version != 3 {
		t.Fatalf("manifest version = %d, want 3", loaded.Manifest.Version)
	}
	if counting.calls[newKey] != 1 {
		t.Fatalf("new commit reads = %d, want 1", counting.calls[newKey])
	}
	if counting.calls[oldKey] != 0 {
		t.Fatalf("snapshot-covered loose commit reads = %d, want 0", counting.calls[oldKey])
	}
	if counting.calls[segment.Key] != 0 {
		t.Fatalf("snapshot-covered segment reads = %d, want 0", counting.calls[segment.Key])
	}
}
