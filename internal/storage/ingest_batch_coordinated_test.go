package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCoordinatedPreparedIngestDistinguishesStaleBaseFromCorruptManifest(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       3,
		HeadCommitID:  "commit-3",
	}
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	head := CoordinationHead{
		TenantID:             "tenant-a",
		Generation:           1,
		Status:               TenantStatusActive,
		Revision:             4,
		GraphVersion:         manifest.Version,
		ManifestKey:          "test/tenants/tenant-a/coordination/manifests/v3.parquet",
		ManifestHash:         objectContentHash(data),
		CommitID:             manifest.HeadCommitID,
		WriteContextRevision: 0,
	}
	if err := objects.Put(ctx, head.ManifestKey, data); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	store.SetCoordinator(fixedHeadCoordinator{head: head})

	stale, err := store.coordinatedPreparedIngestStale(ctx, "tenant-a", IngestPreparedRequest{
		FlushID:           "flush-stale",
		BaseVersion:       2,
		BaseHeadCommitID:  "commit-2",
		BaseHeadRevision:  3,
		BaseGeneration:    1,
		FinalVersion:      3,
		FinalHeadCommitID: "commit-3",
	})
	if err != nil || !stale {
		t.Fatalf("stale prepared check = stale:%v err:%v, want stale without error", stale, err)
	}

	fresh, err := store.coordinatedPreparedIngestStale(ctx, "tenant-a", IngestPreparedRequest{
		FlushID:           "flush-fresh",
		BaseVersion:       3,
		BaseHeadCommitID:  "commit-3",
		BaseHeadRevision:  4,
		BaseGeneration:    1,
		FinalVersion:      4,
		FinalHeadCommitID: "commit-4",
	})
	if err != nil || fresh {
		t.Fatalf("fresh prepared check = stale:%v err:%v, want current base", fresh, err)
	}

	corruptHead := head
	corruptHead.ManifestHash = strings.Repeat("0", len(head.ManifestHash))
	corruptStore := NewTenantStore(objects, "test")
	corruptStore.SetCoordinator(fixedHeadCoordinator{head: corruptHead})
	stale, err = corruptStore.coordinatedPreparedIngestStale(ctx, "tenant-a", IngestPreparedRequest{
		FlushID:           "flush-corrupt",
		BaseVersion:       2,
		BaseHeadCommitID:  "commit-2",
		BaseHeadRevision:  3,
		BaseGeneration:    1,
		FinalVersion:      3,
		FinalHeadCommitID: "commit-3",
	})
	if err == nil || stale || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("corrupt prepared check = stale:%v err:%v, want corruption error", stale, err)
	}

	recreatedHead := head
	recreatedHead.Generation = 2
	recreatedStore := NewTenantStore(objects, "test")
	recreatedStore.SetCoordinator(fixedHeadCoordinator{head: recreatedHead})
	stale, err = recreatedStore.coordinatedPreparedIngestStale(ctx, "tenant-a", IngestPreparedRequest{
		FlushID:                  "flush-recreated",
		BaseVersion:              3,
		BaseHeadCommitID:         "commit-3",
		BaseHeadRevision:         4,
		BaseGeneration:           1,
		BaseWriteContextRevision: 0,
		FinalVersion:             4,
		FinalHeadCommitID:        "commit-4",
	})
	if stale || !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("recreated tenant prepared check = stale:%v err:%v, want lifecycle fencing", stale, err)
	}
}
