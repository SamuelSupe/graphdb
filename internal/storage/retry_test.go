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
