package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRetryDelayHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := retryDelay(ctx, 50)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry delay err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("retry delay ignored canceled context, elapsed=%s", elapsed)
	}
}

func TestCoordinatorRetryBackoffBounds(t *testing.T) {
	tests := []struct {
		attempt int
		min     time.Duration
		max     time.Duration
	}{
		{attempt: 0, min: 5 * time.Millisecond, max: 5 * time.Millisecond},
		{attempt: 1, min: 5 * time.Millisecond, max: 10 * time.Millisecond},
		{attempt: 2, min: 10 * time.Millisecond, max: 20 * time.Millisecond},
		{attempt: 7, min: 100 * time.Millisecond, max: 200 * time.Millisecond},
		{attempt: 20, min: 100 * time.Millisecond, max: 200 * time.Millisecond},
	}
	for _, test := range tests {
		for sample := 0; sample < 100; sample++ {
			delay := coordinatorRetryBackoff(test.attempt)
			if delay < test.min || delay > test.max {
				t.Fatalf("attempt %d delay %s outside [%s,%s]",
					test.attempt, delay, test.min, test.max)
			}
		}
	}
}

func TestDefinitiveCommitFailure(t *testing.T) {
	if !definitiveCommitFailure(ErrWriteConflict) {
		t.Fatal("write conflict must release its pending idempotency reservation")
	}
	for _, err := range []error{
		ErrCoordinatorUnavailable,
		context.Canceled,
		context.DeadlineExceeded,
	} {
		if definitiveCommitFailure(err) {
			t.Fatalf("ambiguous error %v must retain its idempotency reservation", err)
		}
	}
}

func TestAcquireWriterLeaseReturnsNonConflictPutError(t *testing.T) {
	sentinel := errors.New("object store unavailable")
	base := NewMemoryStore()
	store := NewTenantStore(&leasePutErrorStore{ObjectStore: base, err: sentinel}, "test")
	store.MaxRetries = 5

	_, err := store.InitTenant(context.Background(), "tenant-a")
	if !errors.Is(err, sentinel) {
		t.Fatalf("init err = %v, want sentinel put error", err)
	}
}

type leasePutErrorStore struct {
	ObjectStore
	err error
}

func (s *leasePutErrorStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.HasSuffix(key, "/writer-lease.parquet") {
		return ObjectMeta{}, s.err
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}
