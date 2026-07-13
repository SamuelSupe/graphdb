package storage

import (
	"container/list"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

type WriterObjectCacheConfig struct {
	MaxBytes    int64
	MaxKeys     int
	NegativeTTL time.Duration
}

type WriterObjectCache struct {
	Inner       ObjectStore
	maxBytes    int64
	maxKeys     int
	negativeTTL time.Duration

	mu           sync.Mutex
	bytes        int64
	listBytes    int64
	objects      map[string]*writerObjectEntry
	objectLRU    *list.List
	lists        map[string]*writerListEntry
	listLRU      *list.List
	maxListItems int
}

type writerObjectEntry struct {
	key       string
	data      []byte
	hasData   bool
	meta      ObjectMeta
	hasMeta   bool
	negative  bool
	expiresAt time.Time
	size      int64
	elem      *list.Element
}

type writerListEntry struct {
	prefix string
	items  map[string]ObjectInfo
	size   int64
	elem   *list.Element
}

func NewWriterObjectCache(inner ObjectStore, cfg WriterObjectCacheConfig) *WriterObjectCache {
	if cfg.MaxKeys < 0 {
		cfg.MaxKeys = 0
	}
	if cfg.MaxBytes < 0 {
		cfg.MaxBytes = 0
	}
	maxListItems := cfg.MaxKeys / 128
	if cfg.MaxKeys == 0 {
		maxListItems = 0
	} else if maxListItems < 128 {
		maxListItems = 128
	}
	if maxListItems > 4096 {
		maxListItems = 4096
	}
	return &WriterObjectCache{
		Inner:        inner,
		maxBytes:     cfg.MaxBytes,
		maxKeys:      cfg.MaxKeys,
		negativeTTL:  cfg.NegativeTTL,
		objects:      map[string]*writerObjectEntry{},
		objectLRU:    list.New(),
		lists:        map[string]*writerListEntry{},
		listLRU:      list.New(),
		maxListItems: maxListItems,
	}
}

func (s *WriterObjectCache) UnwrapObjectStore() ObjectStore {
	return s.Inner
}

func FindWriterObjectCache(objects ObjectStore) *WriterObjectCache {
	for objects != nil {
		if cache, ok := objects.(*WriterObjectCache); ok {
			return cache
		}
		unwrapper, ok := objects.(objectStoreUnwrapper)
		if !ok {
			return nil
		}
		next := unwrapper.UnwrapObjectStore()
		if next == objects {
			return nil
		}
		objects = next
	}
	return nil
}

func (s *WriterObjectCache) Get(ctx context.Context, key string) ([]byte, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	if data, hit, err := s.getData(key); hit || err != nil {
		return data, err
	}
	data, err := s.Inner.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.cacheNegative(key)
		}
		return nil, err
	}
	s.cachePositive(key, data, ObjectMeta{Key: key}, false)
	return append([]byte(nil), data...), nil
}

func (s *WriterObjectCache) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	if data, meta, hit, err := s.getDataWithMeta(key); hit || err != nil {
		return data, meta, err
	}
	data, meta, err := s.Inner.GetWithMeta(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.cacheNegative(key)
		}
		return nil, ObjectMeta{Key: key}, err
	}
	s.cachePositive(key, data, meta, true)
	return append([]byte(nil), data...), meta, nil
}

func (s *WriterObjectCache) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if meta, hit, err := s.getMeta(key); hit || err != nil {
		return meta, err
	}
	var (
		meta ObjectMeta
		err  error
	)
	if head, ok := s.Inner.(objectHeadStore); ok {
		meta, err = head.Head(ctx, key)
	} else {
		_, meta, err = s.Inner.GetWithMeta(ctx, key)
	}
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.cacheNegative(key)
		}
		return ObjectMeta{Key: key}, err
	}
	s.cacheMeta(key, meta)
	return meta, nil
}

func (s *WriterObjectCache) Put(ctx context.Context, key string, data []byte) error {
	if err := s.Inner.Put(ctx, key, data); err != nil {
		return err
	}
	// Put does not return the store ETag. Keep cached bytes but do not synthesize
	// metadata that may later be used as an If-Match value.
	s.cachePositive(key, data, ObjectMeta{Key: key}, false)
	s.updateCachedLists(key, ObjectInfo{Key: key, Size: int64(len(data))}, false)
	return nil
}

func (s *WriterObjectCache) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	meta, err := s.Inner.PutConditional(ctx, key, data, condition)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			s.invalidateKey(key)
		}
		return meta, err
	}
	s.cachePositive(key, data, meta, true)
	s.updateCachedLists(key, ObjectInfo{Key: key, Size: int64(len(data)), ETag: meta.ETag}, false)
	return meta, nil
}

func (s *WriterObjectCache) Delete(ctx context.Context, key string) error {
	if err := s.Inner.Delete(ctx, key); err != nil {
		return err
	}
	s.cacheNegative(key)
	s.updateCachedLists(key, ObjectInfo{Key: key}, true)
	return nil
}

func (s *WriterObjectCache) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	err := s.Inner.DeleteConditional(ctx, key, condition)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			s.invalidateKey(key)
		}
		return err
	}
	s.cacheNegative(key)
	s.updateCachedLists(key, ObjectInfo{Key: key}, true)
	return nil
}

func (s *WriterObjectCache) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	cacheable := prefix == "" || strings.HasSuffix(prefix, "/")
	if cacheable {
		if items, ok := s.cachedList(prefix); ok {
			return items, nil
		}
	}
	items, err := s.Inner.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if cacheable {
		s.cacheList(prefix, items)
	}
	return cloneObjectInfos(items), nil
}

func (s *WriterObjectCache) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bytes = 0
	s.listBytes = 0
	s.objects = map[string]*writerObjectEntry{}
	s.objectLRU.Init()
	s.lists = map[string]*writerListEntry{}
	s.listLRU.Init()
}

func (s *WriterObjectCache) ClearPrefix(prefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			s.removeObjectLocked(key)
		}
	}
	for cachedPrefix := range s.lists {
		if strings.HasPrefix(cachedPrefix, prefix) || strings.HasPrefix(prefix, cachedPrefix) {
			s.removeListLocked(cachedPrefix)
		}
	}
}

func (s *WriterObjectCache) getData(key string) ([]byte, bool, error) {
	now := time.Now()
	s.mu.Lock()
	entry, ok := s.objects[key]
	if !ok {
		s.mu.Unlock()
		return nil, false, nil
	}
	if entry.negative {
		if s.negativeExpiredLocked(key, entry, now) {
			s.mu.Unlock()
			return nil, false, nil
		}
		s.mu.Unlock()
		return nil, true, ErrNotFound
	}
	if !entry.hasData {
		s.mu.Unlock()
		return nil, false, nil
	}
	s.objectLRU.MoveToFront(entry.elem)
	data := entry.data
	s.mu.Unlock()
	return append([]byte(nil), data...), true, nil
}

func (s *WriterObjectCache) getDataWithMeta(key string) ([]byte, ObjectMeta, bool, error) {
	now := time.Now()
	s.mu.Lock()
	entry, ok := s.objects[key]
	if !ok {
		s.mu.Unlock()
		return nil, ObjectMeta{Key: key}, false, nil
	}
	if entry.negative {
		if s.negativeExpiredLocked(key, entry, now) {
			s.mu.Unlock()
			return nil, ObjectMeta{Key: key}, false, nil
		}
		s.mu.Unlock()
		return nil, ObjectMeta{Key: key}, true, ErrNotFound
	}
	if !entry.hasData || !entry.hasMeta {
		s.mu.Unlock()
		return nil, ObjectMeta{Key: key}, false, nil
	}
	s.objectLRU.MoveToFront(entry.elem)
	data, meta := entry.data, entry.meta
	s.mu.Unlock()
	return append([]byte(nil), data...), meta, true, nil
}

func (s *WriterObjectCache) getMeta(key string) (ObjectMeta, bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.objects[key]
	if !ok {
		return ObjectMeta{Key: key}, false, nil
	}
	if entry.negative {
		if s.negativeExpiredLocked(key, entry, now) {
			return ObjectMeta{Key: key}, false, nil
		}
		return ObjectMeta{Key: key}, true, ErrNotFound
	}
	if !entry.hasMeta {
		return ObjectMeta{Key: key}, false, nil
	}
	s.objectLRU.MoveToFront(entry.elem)
	return entry.meta, true, nil
}

func (s *WriterObjectCache) cachedList(prefix string) ([]ObjectInfo, bool) {
	s.mu.Lock()
	entry, ok := s.lists[prefix]
	if !ok {
		s.mu.Unlock()
		return nil, false
	}
	s.listLRU.MoveToFront(entry.elem)
	items := make([]ObjectInfo, 0, len(entry.items))
	for _, item := range entry.items {
		items = append(items, item)
	}
	s.mu.Unlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, true
}

func (s *WriterObjectCache) cachePositive(key string, data []byte, meta ObjectMeta, hasMeta bool) {
	data = append([]byte(nil), data...)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.ensureObjectEntryLocked(key)
	s.bytes -= entry.size
	entry.data = data
	entry.hasData = true
	entry.size = int64(len(entry.data))
	entry.meta = normalizeObjectMeta(key, meta)
	entry.hasMeta = hasMeta
	entry.negative = false
	entry.expiresAt = time.Time{}
	s.bytes += entry.size
	s.objectLRU.MoveToFront(entry.elem)
	s.evictObjectsLocked()
	s.evictListsLocked()
}

func (s *WriterObjectCache) cacheMeta(key string, meta ObjectMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.ensureObjectEntryLocked(key)
	entry.meta = normalizeObjectMeta(key, meta)
	entry.hasMeta = true
	entry.negative = false
	entry.expiresAt = time.Time{}
	s.objectLRU.MoveToFront(entry.elem)
	s.evictObjectsLocked()
}

func (s *WriterObjectCache) cacheNegative(key string) {
	if s.negativeTTL <= 0 {
		s.invalidateKey(key)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.ensureObjectEntryLocked(key)
	s.bytes -= entry.size
	entry.data = nil
	entry.hasData = false
	entry.meta = ObjectMeta{Key: key}
	entry.hasMeta = false
	entry.negative = true
	entry.expiresAt = time.Now().Add(s.negativeTTL)
	entry.size = 0
	s.objectLRU.MoveToFront(entry.elem)
	s.evictObjectsLocked()
}

func (s *WriterObjectCache) cacheList(prefix string, items []ObjectInfo) {
	if s.maxListItems == 0 {
		return
	}
	itemMap := make(map[string]ObjectInfo, len(items))
	size := int64(len(prefix) + 64)
	for _, item := range items {
		itemMap[item.Key] = item
		size += writerObjectInfoCacheSize(item)
	}
	if s.maxBytes > 0 && size > s.maxBytes {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.lists[prefix]
	if entry == nil {
		entry = &writerListEntry{prefix: prefix, elem: s.listLRU.PushFront(prefix)}
		s.lists[prefix] = entry
	} else {
		s.listBytes -= entry.size
		s.listLRU.MoveToFront(entry.elem)
	}
	entry.items = itemMap
	entry.size = size
	s.listBytes += size
	s.evictListsLocked()
}

func (s *WriterObjectCache) updateCachedLists(key string, info ObjectInfo, deleted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, prefix := range writerObjectListPrefixes(key) {
		entry := s.lists[prefix]
		if entry == nil {
			continue
		}
		before, existed := entry.items[key]
		if deleted {
			delete(entry.items, key)
			if existed {
				delta := writerObjectInfoCacheSize(before)
				entry.size -= delta
				s.listBytes -= delta
			}
		} else {
			entry.items[key] = info
			delta := writerObjectInfoCacheSize(info)
			if existed {
				delta -= writerObjectInfoCacheSize(before)
			}
			entry.size += delta
			s.listBytes += delta
		}
	}
	s.evictListsLocked()
}

func (s *WriterObjectCache) invalidateKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeObjectLocked(key)
	for _, prefix := range writerObjectListPrefixes(key) {
		entry := s.lists[prefix]
		if entry == nil {
			continue
		}
		if previous, ok := entry.items[key]; ok {
			delete(entry.items, key)
			delta := writerObjectInfoCacheSize(previous)
			entry.size -= delta
			s.listBytes -= delta
		}
	}
}

func (s *WriterObjectCache) ensureObjectEntryLocked(key string) *writerObjectEntry {
	if entry := s.objects[key]; entry != nil {
		return entry
	}
	entry := &writerObjectEntry{key: key}
	entry.elem = s.objectLRU.PushFront(key)
	s.objects[key] = entry
	return entry
}

func (s *WriterObjectCache) negativeExpiredLocked(key string, entry *writerObjectEntry, now time.Time) bool {
	if entry.expiresAt.IsZero() || now.Before(entry.expiresAt) {
		s.objectLRU.MoveToFront(entry.elem)
		return false
	}
	s.removeObjectLocked(key)
	return true
}

func (s *WriterObjectCache) evictObjectsLocked() {
	if s.maxKeys == 0 {
		for len(s.objects) > 0 {
			s.removeOldestObjectLocked()
		}
		return
	}
	for len(s.objects) > s.maxKeys || (s.maxBytes > 0 && s.bytes > s.maxBytes) {
		if !s.removeOldestObjectLocked() {
			return
		}
	}
}

func (s *WriterObjectCache) evictListsLocked() {
	for len(s.lists) > s.maxListItems || (s.maxBytes > 0 && s.bytes+s.listBytes > s.maxBytes) {
		elem := s.listLRU.Back()
		if elem == nil {
			return
		}
		prefix, _ := elem.Value.(string)
		s.removeListLocked(prefix)
	}
}

func (s *WriterObjectCache) removeOldestObjectLocked() bool {
	elem := s.objectLRU.Back()
	if elem == nil {
		return false
	}
	key, _ := elem.Value.(string)
	s.removeObjectLocked(key)
	return true
}

func (s *WriterObjectCache) removeObjectLocked(key string) {
	entry := s.objects[key]
	if entry == nil {
		return
	}
	s.bytes -= entry.size
	if s.bytes < 0 {
		s.bytes = 0
	}
	s.objectLRU.Remove(entry.elem)
	delete(s.objects, key)
}

func (s *WriterObjectCache) removeListLocked(prefix string) {
	entry := s.lists[prefix]
	if entry == nil {
		return
	}
	s.listBytes -= entry.size
	if s.listBytes < 0 {
		s.listBytes = 0
	}
	s.listLRU.Remove(entry.elem)
	delete(s.lists, prefix)
}

func writerObjectInfoCacheSize(info ObjectInfo) int64 {
	return int64(len(info.Key) + len(info.ETag) + 48)
}

func writerObjectListPrefixes(key string) []string {
	prefixes := []string{""}
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			prefixes = append(prefixes, key[:i+1])
		}
	}
	return prefixes
}

func normalizeObjectMeta(key string, meta ObjectMeta) ObjectMeta {
	if meta.Key == "" {
		meta.Key = key
	}
	meta.Exists = true
	return meta
}

func cloneObjectInfos(items []ObjectInfo) []ObjectInfo {
	out := append([]ObjectInfo(nil), items...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out
}
