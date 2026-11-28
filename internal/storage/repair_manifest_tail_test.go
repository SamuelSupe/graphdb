package storage

import (
	"context"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestRepairManifestReopensLooseCommitBeforeSegment(t *testing.T) {
	ctx := t.Context()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	const tenant = "tenant-a"
	if _, err := store.InitTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	sharded, err := store.putShardedSnapshot(ctx, tenant, graph.Snapshot{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := graph.Commit{ID: "loose", TenantID: tenant, Version: 2}
	if err := store.putCommitObjectIfAbsent(ctx, store.commitKey(tenant, first.Version, first.ID), first); err != nil {
		t.Fatal(err)
	}
	second := graph.Commit{ID: "segment", TenantID: tenant, Version: 3}
	if _, err := store.putCommitSegment(ctx, tenant, []commitSegmentItem{
		{Key: store.commitKey(tenant, second.Version, second.ID), Commit: second},
	}); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.repairManifest(ctx, tenant)
	if err != nil || manifest.Version != 3 || manifest.SnapshotCatalogKey != sharded.Key {
		t.Fatalf("repaired manifest = %+v, %v", manifest, err)
	}
	reader := NewTenantStore(objects, "test")
	loaded, _, err := reader.Load(ctx, tenant)
	if err != nil || loaded == nil || loaded.Version != 3 {
		t.Fatalf("cold load repaired manifest = %v, %v", loaded, err)
	}
}

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
