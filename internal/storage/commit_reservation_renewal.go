package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type commitReservationRenewal struct {
	done         chan struct{}
	result       chan error
	cancelCommit context.CancelFunc
	stopOnce     sync.Once
	stopErr      error
}

func (s *TenantStore) startCommitReservationRenewal(
	ctx context.Context,
	reservation *directCommitReservation,
) context.Context {
	if reservation == nil || !reservation.coordinated {
		return ctx
	}
	commitCtx, cancelCommit := context.WithCancel(ctx)
	renewal := &commitReservationRenewal{
		done:         make(chan struct{}),
		result:       make(chan error, 1),
		cancelCommit: cancelCommit,
	}
	reservation.renewal = renewal
	go s.runCommitReservationRenewal(commitCtx, reservation, renewal)
	return commitCtx
}

func (s *TenantStore) runCommitReservationRenewal(
	ctx context.Context,
	reservation *directCommitReservation,
	renewal *commitReservationRenewal,
) {
	ttl := s.coordinatorPendingReservationTTL()
	ticker := time.NewTicker(max(ttl/3, 10*time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-renewal.done:
			renewal.result <- nil
			return
		case <-ctx.Done():
			if commitReservationRenewalStopped(renewal.done) {
				renewal.result <- nil
			} else {
				renewal.result <- ctx.Err()
			}
			return
		case <-ticker.C:
			timeout := max(min(ttl/2, 5*time.Second), 10*time.Millisecond)
			renewCtx, cancel := context.WithTimeout(ctx, timeout)
			ok, err := s.Coordinator.RenewCommit(
				renewCtx,
				reservation.record.TenantID,
				reservation.key,
				reservation.requestHash,
				reservation.ownerToken,
			)
			cancel()
			if err != nil {
				if commitReservationRenewalStopped(renewal.done) {
					renewal.result <- nil
				} else {
					renewal.cancelCommit()
					renewal.result <- fmt.Errorf("renew commit idempotency reservation: %w", err)
				}
				return
			}
			if !ok {
				renewal.cancelCommit()
				renewal.result <- fmt.Errorf(
					"%w: commit idempotency reservation %q is no longer owned",
					ErrIdempotencyInProgress,
					reservation.key,
				)
				return
			}
		}
	}
}

func stopCommitReservationRenewal(reservation *directCommitReservation) error {
	if reservation == nil || reservation.renewal == nil {
		return nil
	}
	renewal := reservation.renewal
	renewal.stopOnce.Do(func() {
		close(renewal.done)
		renewal.cancelCommit()
		renewal.stopErr = <-renewal.result
	})
	return renewal.stopErr
}

func commitReservationRenewalStopped(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
