package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDelayedReadObjectStoreDelaysReadsOnly(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewDelayedReadObjectStore(base, 20*time.Millisecond)

	start := time.Now()
	if err := store.Put(ctx, "objects/a", []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Fatalf("put was delayed: %s", elapsed)
	}

	start = time.Now()
	data, err := store.Get(ctx, "objects/a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(data) != "value" {
		t.Fatalf("data = %q", string(data))
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("get elapsed = %s, want delayed", elapsed)
	}
}

func TestDelayedReadObjectStoreHonorsContext(t *testing.T) {
	base := NewMemoryStore()
	store := NewDelayedReadObjectStore(base, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := store.Get(ctx, "objects/missing")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("get err = %v, want deadline exceeded", err)
	}
}
