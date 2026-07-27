package storage

import (
	"context"
	"testing"
	"time"
)

func TestRunGCCheckpointDoesNotSkipCoordinatorCandidates(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	coordinator := &gcReachabilityCoordinator{
		taskLeaseTestCoordinator: newTaskLeaseTestCoordinator(),
	}
	store.SetCoordinator(coordinator)

	headManifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       2,
		UpdatedAt:     time.Now().UTC(),
	}
	headData, err := marshalParquetManifest(ctx, headManifest)
	if err != nil {
		t.Fatalf("marshal head manifest: %v", err)
	}
	head := CoordinationHead{
		TenantID:             "tenant-a",
		Generation:           1,
		Status:               TenantStatusActive,
		Revision:             2,
		GraphVersion:         headManifest.Version,
		ManifestHash:         objectContentHash(headData),
		WriteContextRevision: 1,
	}
	head.ManifestKey = store.coordinatorManifestKey(
		head.TenantID, head.GraphVersion, head.Revision, head.ManifestHash,
	)
	if err := objects.Put(ctx, head.ManifestKey, headData); err != nil {
		t.Fatalf("put head manifest: %v", err)
	}
	coordinator.head = head
	coordinator.roots = CoordinatorReachability{
		Head:             head,
		ManifestKeys:     map[string]struct{}{head.ManifestKey: {}},
		WriteContextKeys: map[string]struct{}{},
	}

	candidateManifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       1,
		UpdatedAt:     time.Now().UTC().Add(-2 * coordinatorCandidateGracePeriod),
	}
	candidateData, err := marshalParquetManifest(ctx, candidateManifest)
	if err != nil {
		t.Fatalf("marshal candidate manifest: %v", err)
	}
	candidateKey := store.coordinatorManifestKey(
		"tenant-a", candidateManifest.Version, 1,
		objectContentHash(candidateData),
	)
	if err := objects.Put(ctx, candidateKey, candidateData); err != nil {
		t.Fatalf("put candidate manifest: %v", err)
	}
	deadLetterKey := putExpiredDeadLetter(
		t, ctx, store, "tenant-a", "agent", "gc-checkpoint",
	)

	cursor := ""
	for attempt := 0; attempt < 3; attempt++ {
		report, err := store.RunGC(ctx, "tenant-a", GCOptions{
			DeadLetterMaxAge: time.Hour,
			CheckpointCursor: cursor,
			MaxDeletes:       1,
		})
		if err != nil {
			t.Fatalf("gc attempt %d: %v", attempt+1, err)
		}
		if report.Checkpoint.Completed {
			break
		}
		cursor = report.Checkpoint.NextCursor
		if cursor == "" {
			t.Fatalf("gc attempt %d paused without a cursor", attempt+1)
		}
	}

	for label, key := range map[string]string{
		"coordinator candidate": candidateKey,
		"deadletter":            deadLetterKey,
	} {
		if _, err := objects.Get(ctx, key); err == nil {
			t.Fatalf("%s %q survived completed checkpoint GC", label, key)
		}
	}
}

type gcReachabilityCoordinator struct {
	*taskLeaseTestCoordinator
	roots CoordinatorReachability
}

func (c *gcReachabilityCoordinator) Reachability(
	context.Context,
	string,
) (CoordinatorReachability, error) {
	return c.roots, nil
}
