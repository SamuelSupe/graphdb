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
	cache.cacheList("objects/", []ObjectInfo{{Key: "objects/a", ETag: "etag"}})
	cache.cachePositive("objects/data", make([]byte, 480), ObjectMeta{Key: "objects/data"}, false)
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
