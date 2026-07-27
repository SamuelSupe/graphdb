package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestPostgresLifecycleCandidateIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	candidate := newCoordinatedTenantCandidate(
		"clone",
		"tenant-source",
		store.tenantObjectPrefix("tenant-source"),
		"tenant-a",
	)
	if _, exists, _, err := store.getCoordinatedTenantCandidate(
		ctx,
		"tenant-a",
	); err != nil || exists {
		t.Fatalf("prime missing lifecycle candidate: exists=%t err=%v", exists, err)
	}
	data, err := json.Marshal(candidate)
	if err != nil {
		t.Fatalf("marshal lifecycle candidate: %v", err)
	}
	key := store.coordinatedTenantCandidateKey("tenant-a")
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put lifecycle candidate: %v", err)
	}
	if loaded, exists, _, err := store.getCoordinatedTenantCandidate(
		ctx,
		"tenant-a",
	); err != nil || !exists || loaded != candidate {
		t.Fatalf(
			"load lifecycle candidate = %#v, exists=%t err=%v",
			loaded,
			exists,
			err,
		)
	}

	if err := objects.Delete(ctx, key); err != nil &&
		!errors.Is(err, ErrNotFound) {
		t.Fatalf("delete lifecycle candidate: %v", err)
	}
	if _, exists, _, err := store.getCoordinatedTenantCandidate(
		ctx,
		"tenant-a",
	); err != nil || exists {
		t.Fatalf("deleted lifecycle candidate: exists=%t err=%v", exists, err)
	}
}
