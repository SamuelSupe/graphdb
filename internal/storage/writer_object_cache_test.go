package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriterObjectCachePutConditionalCachesMetaReads(t *testing.T) {
	ctx := context.Background()
	base := newCountingObjectStore(NewMemoryStore())
	cache := NewWriterObjectCache(base, WriterObjectCacheConfig{
		MaxBytes:    1024 * 1024,
		MaxKeys:     100,
		NegativeTTL: time.Minute,
	})
	meta, err := cache.PutConditional(ctx, "objects/a.json", []byte("value"), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("PutConditional: %v", err)
	}
	base.reset()

	data, loaded, err := cache.GetWithMeta(ctx, "objects/a.json")
	if err != nil {
		t.Fatalf("GetWithMeta: %v", err)
	}
	if string(data) != "value" || loaded.ETag != meta.ETag || !loaded.Exists {
		t.Fatalf("loaded = %q %#v, want cached value/meta", data, loaded)
	}
	head, err := cache.Head(ctx, "objects/a.json")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head.ETag != meta.ETag || !head.Exists {
		t.Fatalf("head = %#v, want cached meta", head)
	}
	if base.getWithMetaCalls != 0 || base.headCalls != 0 || base.getCalls != 0 {
		t.Fatalf("underlying reads get=%d get_with_meta=%d head=%d, want zeros", base.getCalls, base.getWithMetaCalls, base.headCalls)
	}
}

func TestWriterObjectCacheDoesNotRetainOversizedList(t *testing.T) {
	ctx := context.Background()
	memory := NewMemoryStore()
	for i := 0; i < 8; i++ {
		key := "objects/" + strings.Repeat(string(rune('a'+i)), 80)
		if err := memory.Put(ctx, key, []byte("value")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	base := newCountingObjectStore(memory)
	cache := NewWriterObjectCache(base, WriterObjectCacheConfig{MaxBytes: 256, MaxKeys: 100})
	for i := 0; i < 2; i++ {
		if _, err := cache.List(ctx, "objects/"); err != nil {
			t.Fatalf("list %d: %v", i, err)
		}
	}
	if base.listCalls != 2 {
		t.Fatalf("underlying list calls = %d, want 2 for an oversized uncached list", base.listCalls)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.listBytes != 0 || len(cache.lists) != 0 {
		t.Fatalf("oversized list retained: bytes=%d lists=%d", cache.listBytes, len(cache.lists))
	}
}

func TestWriterObjectCacheCountsListsAgainstByteLimit(t *testing.T) {
	cache := NewWriterObjectCache(NewMemoryStore(), WriterObjectCacheConfig{MaxBytes: 512, MaxKeys: 100})
	cache.cacheList("objects/", []ObjectInfo{{Key: "objects/a", ETag: "etag"}}, 0)
	cache.fillObject("objects/data", cache.generation("objects/data"), writerObjectEntry{data: make([]byte, 480), hasData: true})
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.bytes+cache.listBytes > cache.maxBytes {
		t.Fatalf("cache bytes = %d + %d, max = %d", cache.bytes, cache.listBytes, cache.maxBytes)
	}
}

func TestWriterObjectCacheListTracksWritesAndDeletes(t *testing.T) {
	ctx := context.Background()
	base := newCountingObjectStore(NewMemoryStore())
	cache := NewWriterObjectCache(base, WriterObjectCacheConfig{
		MaxBytes:    1024 * 1024,
		MaxKeys:     100,
		NegativeTTL: time.Minute,
	})
	if _, err := cache.PutConditional(ctx, "objects/a.json", []byte("a"), PutCondition{}); err != nil {
		t.Fatalf("PutConditional a: %v", err)
	}
	items, err := cache.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !reflect.DeepEqual(keysOf(items), []string{"objects/a.json"}) {
		t.Fatalf("items = %#v, want a", items)
	}
	if base.listCalls != 1 {
		t.Fatalf("list calls = %d, want first list to hit underlying", base.listCalls)
	}
	base.reset()

	if _, err := cache.PutConditional(ctx, "objects/b.json", []byte("b"), PutCondition{}); err != nil {
		t.Fatalf("PutConditional b: %v", err)
	}
	items, err = cache.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("List cached: %v", err)
	}
	if !reflect.DeepEqual(keysOf(items), []string{"objects/a.json", "objects/b.json"}) {
		t.Fatalf("items = %#v, want a,b", items)
	}
	if base.listCalls != 0 {
		t.Fatalf("list calls = %d, want cached list", base.listCalls)
	}

	if err := cache.Delete(ctx, "objects/a.json"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	items, err = cache.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if !reflect.DeepEqual(keysOf(items), []string{"objects/b.json"}) {
		t.Fatalf("items = %#v, want b", items)
	}
	if _, _, err := cache.GetWithMeta(ctx, "objects/a.json"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetWithMeta deleted err = %v, want ErrNotFound", err)
	}
	if base.getWithMetaCalls != 0 {
		t.Fatalf("get_with_meta calls = %d, want negative cache hit", base.getWithMetaCalls)
	}
}

func TestWriterObjectCacheConflictInvalidatesStaleEntry(t *testing.T) {
	ctx := context.Background()
	baseMemory := NewMemoryStore()
	base := newCountingObjectStore(baseMemory)
	cache := NewWriterObjectCache(base, WriterObjectCacheConfig{
		MaxBytes:    1024 * 1024,
		MaxKeys:     100,
		NegativeTTL: time.Minute,
	})
	meta, err := cache.PutConditional(ctx, "objects/current.json", []byte("old"), PutCondition{})
	if err != nil {
		t.Fatalf("PutConditional old: %v", err)
	}
	if err := baseMemory.Put(ctx, "objects/current.json", []byte("outside")); err != nil {
		t.Fatalf("outside put: %v", err)
	}
	if _, err := cache.PutConditional(ctx, "objects/current.json", []byte("new"), PutCondition{IfMatch: meta.ETag}); !errors.Is(err, ErrConflict) {
		t.Fatalf("PutConditional conflict err = %v, want ErrConflict", err)
	}
	base.reset()
	data, _, err := cache.GetWithMeta(ctx, "objects/current.json")
	if err != nil {
		t.Fatalf("GetWithMeta after conflict: %v", err)
	}
	if string(data) != "outside" {
		t.Fatalf("data = %q, want outside", data)
	}
	if base.getWithMetaCalls != 1 {
		t.Fatalf("get_with_meta calls = %d, want stale entry invalidated", base.getWithMetaCalls)
	}
}

func TestWriterObjectCacheKeyInvalidationRefreshesLists(t *testing.T) {
	ctx := context.Background()
	memory := NewMemoryStore()
	if err := memory.Put(ctx, "objects/a.json", []byte("a")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := memory.Put(ctx, "objects/b.json", []byte("b")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	base := newCountingObjectStore(memory)
	cache := NewWriterObjectCache(base, WriterObjectCacheConfig{
		MaxBytes:    1 << 20,
		MaxKeys:     100,
		NegativeTTL: time.Minute,
	})
	if _, err := cache.List(ctx, "objects/"); err != nil {
		t.Fatalf("prime list: %v", err)
	}
	base.reset()

	cache.invalidateKey("objects/a.json")
	items, err := cache.List(ctx, "objects/")
	if err != nil {
		t.Fatalf("list after key invalidation: %v", err)
	}
	if !reflect.DeepEqual(
		keysOf(items),
		[]string{"objects/a.json", "objects/b.json"},
	) {
		t.Fatalf("items = %#v, want a,b", items)
	}
	if base.listCalls != 1 {
		t.Fatalf("underlying list calls = %d, want 1", base.listCalls)
	}
}

func keysOf(items []ObjectInfo) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	return keys
}

type countingObjectStore struct {
	ObjectStore
	mu               sync.Mutex
	getCalls         int
	getWithMetaCalls int
	headCalls        int
	listCalls        int
}

func newCountingObjectStore(inner ObjectStore) *countingObjectStore {
	return &countingObjectStore{ObjectStore: inner}
}

func (s *countingObjectStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls = 0
	s.getWithMetaCalls = 0
	s.headCalls = 0
	s.listCalls = 0
}

func (s *countingObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.getCalls++
	s.mu.Unlock()
	return s.ObjectStore.Get(ctx, key)
}

func (s *countingObjectStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.mu.Lock()
	s.getWithMetaCalls++
	s.mu.Unlock()
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *countingObjectStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	s.mu.Lock()
	s.headCalls++
	s.mu.Unlock()
	if head, ok := s.ObjectStore.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	return meta, err
}

func (s *countingObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	s.mu.Lock()
	s.listCalls++
	s.mu.Unlock()
	return s.ObjectStore.List(ctx, prefix)
}

type delayedCacheRead struct {
	ObjectStore
	entered chan struct{}
	release chan struct{}
}

func (s *delayedCacheRead) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	data, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	if ctx.Value(s) == true {
		close(s.entered)
		<-s.release
	}
	return data, meta, err
}

func TestConcurrentColdReadCannotReplaceCompletedWrite(t *testing.T) {
	for _, initial := range []string{"missing", "present"} {
		t.Run(initial, func(t *testing.T) {
			base := NewMemoryStore()
			source := &delayedCacheRead{base, make(chan struct{}), make(chan struct{})}
			cache := NewWriterObjectCache(source, WriterObjectCacheConfig{MaxKeys: 8, MaxBytes: 100, NegativeTTL: time.Minute})
			const key = "query"
			if initial == "present" {
				if err := base.Put(t.Context(), key, []byte("old")); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan struct{})
			go func() { defer close(done); cache.GetWithMeta(context.WithValue(t.Context(), source, true), key) }()
			<-source.entered
			_, err := cache.PutConditional(t.Context(), key, []byte("updated"), PutCondition{})
			close(source.release)
			<-done
			if err != nil {
				t.Fatal(err)
			}
			data, _, err := cache.GetWithMeta(t.Context(), key)
			if err != nil || string(data) != "updated" {
				t.Fatalf("after both calls: %q, %v", data, err)
			}
		})
	}
}

func TestOversizedObjectRetainsOtherHotKeys(t *testing.T) {
	base := newCountingObjectStore(NewMemoryStore())
	cache := NewWriterObjectCache(base, WriterObjectCacheConfig{MaxBytes: 64, MaxKeys: 8})
	for _, key := range []string{"hot", "large"} {
		if err := cache.Put(t.Context(), key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := cache.Put(t.Context(), "large", make([]byte, 128)); err != nil {
		t.Fatal(err)
	}
	if data, err := cache.Get(t.Context(), "large"); err != nil || len(data) != 128 {
		t.Fatalf("updated oversized value: %d, %v", len(data), err)
	}
	base.reset()
	if data, err := cache.Get(t.Context(), "hot"); err != nil || string(data) != "hot" || base.getCalls != 0 {
		t.Fatalf("hot read: %q, %v, backing reads %d", data, err, base.getCalls)
	}
}
