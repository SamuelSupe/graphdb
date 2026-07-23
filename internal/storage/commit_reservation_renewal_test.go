package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommitReservationRenewalLossCancelsCommit(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Coordinator = losingCommitRenewalCoordinator{}
	store.CoordinatorPendingTTL = 30 * time.Millisecond
	reservation := &directCommitReservation{
		key:         "request-a",
		record:      DirectCommitRecord{TenantID: "tenant-a"},
		coordinated: true,
		requestHash: "hash-a",
		ownerToken:  "owner-a",
	}
	commitCtx := store.startCommitReservationRenewal(context.Background(), reservation)
	select {
	case <-commitCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("commit context was not canceled after reservation loss")
	}
	err := stopCommitReservationRenewal(reservation)
	if !errors.Is(err, ErrIdempotencyInProgress) {
		t.Fatalf("renewal error = %v, want ErrIdempotencyInProgress", err)
	}
}

type losingCommitRenewalCoordinator struct {
	WriteCoordinator
}

func (losingCommitRenewalCoordinator) RenewCommit(
	context.Context,
	string,
	string,
	string,
	string,
) (bool, error) {
	return false, nil
}
