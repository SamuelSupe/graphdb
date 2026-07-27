package storage

import (
	"context"
	"errors"
	"testing"
)

func TestCoordinatedTenantCandidateResumesAndReplacesPartialObjects(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	candidate := newCoordinatedTenantCandidate(
		"migration", "tenant-a", "source/tenants/tenant-a/", "tenant-a",
	)
	resumed, err := store.prepareCoordinatedTenantCandidate(
		ctx, "tenant-a", candidate,
	)
	if err != nil || resumed {
		t.Fatalf("prepare candidate resumed=%v err=%v", resumed, err)
	}
	key := store.tenantObjectPrefix("tenant-a") + "snapshots/data.parquet"
	if err := store.putCoordinatedCandidateObject(
		ctx, key, []byte("first"),
	); err != nil {
		t.Fatalf("put first candidate object: %v", err)
	}

	resumed, err = store.prepareCoordinatedTenantCandidate(
		ctx, "tenant-a", candidate,
	)
	if err != nil || !resumed {
		t.Fatalf("resume candidate resumed=%v err=%v", resumed, err)
	}
	if err := store.putCoordinatedCandidateObject(
		ctx, key, []byte("second"),
	); err != nil {
		t.Fatalf("replace partial candidate object: %v", err)
	}
	data, err := store.Objects.Get(ctx, key)
	if err != nil || string(data) != "second" {
		t.Fatalf("candidate data=%q err=%v", data, err)
	}
}

func TestCoordinatedTenantCandidateRejectsUnownedData(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	key := store.tenantObjectPrefix("tenant-a") + "snapshots/data.parquet"
	if err := store.Objects.Put(ctx, key, []byte("legacy")); err != nil {
		t.Fatalf("put unowned object: %v", err)
	}
	candidate := newCoordinatedTenantCandidate(
		"clone", "tenant-source", store.tenantObjectPrefix("tenant-source"),
		"tenant-a",
	)
	_, err := store.prepareCoordinatedTenantCandidate(
		ctx, "tenant-a", candidate,
	)
	if !errors.Is(err, ErrCoordinatorHeadMissing) {
		t.Fatalf("prepare candidate err=%v, want ErrCoordinatorHeadMissing", err)
	}
}

func TestCompleteCoordinatedTenantCandidateDeletesOnlyExpectedMarker(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	candidate := newCoordinatedTenantCandidate(
		"clone", "tenant-source", store.tenantObjectPrefix("tenant-source"),
		"tenant-target",
	)
	if _, err := store.prepareCoordinatedTenantCandidate(
		ctx, "tenant-target", candidate,
	); err != nil {
		t.Fatalf("prepare candidate: %v", err)
	}
	different := candidate
	different.SourceTenantID = "tenant-other"
	if err := store.completeCoordinatedTenantCandidate(
		ctx, "tenant-target", different,
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("complete different candidate err=%v, want conflict", err)
	}
	if _, exists, _, err := store.getCoordinatedTenantCandidate(
		ctx, "tenant-target",
	); err != nil || !exists {
		t.Fatalf("candidate after conflict exists=%v err=%v", exists, err)
	}
	if err := store.completeCoordinatedTenantCandidate(
		ctx, "tenant-target", candidate,
	); err != nil {
		t.Fatalf("complete candidate: %v", err)
	}
	if _, exists, _, err := store.getCoordinatedTenantCandidate(
		ctx, "tenant-target",
	); err != nil || exists {
		t.Fatalf("candidate after completion exists=%v err=%v", exists, err)
	}
}
