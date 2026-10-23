package storage

import (
	"context"
	"testing"
	"time"
)

type capturedReadStore struct {
	ObjectStore
	entered chan struct{}
	release chan struct{}
}

func (s *capturedReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	data, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	if string(data) == "old" {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, meta, ctx.Err()
		}
	}
	return data, meta, err
}
func TestReadAfterMutationDoesNotJoinOldSingleflight(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "put", true: "delete"}[remove], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			base := NewMemoryStore()
			base.Put(ctx, "key", []byte("old"))
			inner := &capturedReadStore{ObjectStore: base, entered: make(chan struct{}), release: make(chan struct{})}
			store := NewReadProtectedObjectStore(inner, ReadProtectionConfig{Singleflight: true})
			done := make(chan struct{})
			go func() { defer close(done); store.Get(ctx, "key") }()
			<-inner.entered
			if remove {
				if err := store.Delete(ctx, "key"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := store.Put(ctx, "key", []byte("new")); err != nil {
					t.Fatal(err)
				}
			}
			data, err := store.Get(ctx, "key")
			close(inner.release)
			<-done
			if remove {
				if err != ErrNotFound {
					t.Fatalf("read after delete: %q %v", data, err)
				}
			} else if err != nil || string(data) != "new" {
				t.Fatalf("read after put: %q %v", data, err)
			}
		})
	}
}
func TestDecodedLookupPanicReleasesSingleflight(t *testing.T) {
	cache := newDecodedLookupCache[int](2, 1024, func(int) int64 { return 8 })
	func() {
		defer func() {
			if recover() == nil {
				t.Error("loader panic was swallowed")
			}
		}()
		cache.load(context.Background(), "key", func() (int, bool, error) { panic("loader") })
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	value, ok, err := cache.load(ctx, "key", func() (int, bool, error) { return 42, true, nil })
	if err != nil || !ok || value != 42 {
		t.Fatalf("load after panic: %d %v %v", value, ok, err)
	}
}
