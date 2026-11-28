package storage

import (
	"container/list"
	"context"
	"errors"
	"hash/maphash"
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

	mu   sync.Mutex
	seed maphash.Seed
	// Bounded generations fence slow fills without retaining every key ever written.
	generations  [128]uint64
	listFills    map[string]*writerListFill
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

type writerListFill struct {
	readers int
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
		seed:         maphash.MakeSeed(),
		maxBytes:     cfg.MaxBytes,
		maxKeys:      cfg.MaxKeys,
		negativeTTL:  cfg.NegativeTTL,
		objects:      map[string]*writerObjectEntry{},
		objectLRU:    list.New(),
		lists:        map[string]*writerListEntry{},
		listFills:    map[string]*writerListFill{},
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

func (s *WriterObjectCache) Get(ctx context.Context, key string) (result []byte, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	if data, hit, err := s.getData(key); hit || err != nil {
		return data, err
	}
	generation := s.generation(key)
	data, err := s.Inner.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.fillObject(key, generation, writerObjectEntry{negative: true})
		}
		return nil, err
	}
	s.fillObject(key, generation, writerObjectEntry{data: data, hasData: true})
	return append([]byte(nil), data...), nil
}

func (s *WriterObjectCache) GetWithMeta(ctx context.Context, key string) (result []byte, resultMeta ObjectMeta, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	if err := objectContextErr(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	if data, meta, hit, err := s.getDataWithMeta(key); hit || err != nil {
		return data, meta, err
	}
	generation := s.generation(key)
	data, meta, err := s.Inner.GetWithMeta(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.fillObject(key, generation, writerObjectEntry{negative: true})
		}
		return nil, ObjectMeta{Key: key}, err
	}
	s.fillObject(key, generation, writerObjectEntry{data: data, hasData: true, meta: meta, hasMeta: true})
	return append([]byte(nil), data...), meta, nil
}

func (s *WriterObjectCache) Head(ctx context.Context, key string) (result ObjectMeta, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if meta, hit, err := s.getMeta(key); hit || err != nil {
		return meta, err
	}
	generation := s.generation(key)
	var meta ObjectMeta
	if head, ok := s.Inner.(objectHeadStore); ok {
		meta, err = head.Head(ctx, key)
	} else {
		_, meta, err = s.Inner.GetWithMeta(ctx, key)
	}
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.fillObject(key, generation, writerObjectEntry{negative: true})
		}
		return ObjectMeta{Key: key}, err
	}
	s.fillObject(key, generation, writerObjectEntry{meta: meta, hasMeta: true})
	return meta, nil
}

type writerObjectCacheMutationKey struct{}

func (s *WriterObjectCache) withMutationContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, writerObjectCacheMutationKey{}, s)
}

func (s *WriterObjectCache) Put(ctx context.Context, key string, data []byte) error {
	generation := s.generation(key)
	if err := s.Inner.Put(s.withMutationContext(ctx), key, data); err != nil {
		s.invalidateKey(key)
		return err
	}
	// Put does not return the store ETag. Keep cached bytes but do not synthesize
	// metadata that may later be used as an If-Match value.
	s.finishMutation(key, generation, data, ObjectMeta{Key: key}, false, false)
	return nil
}

func (s *WriterObjectCache) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	generation := s.generation(key)
	meta, err := s.Inner.PutConditional(s.withMutationContext(ctx), key, data, condition)
	if err != nil {
		s.invalidateKey(key)
		return meta, err
	}
	s.finishMutation(key, generation, data, meta, true, false)
	return meta, nil
}

func (s *WriterObjectCache) Delete(ctx context.Context, key string) error {
	generation := s.generation(key)
	if err := s.Inner.Delete(s.withMutationContext(ctx), key); err != nil {
		s.invalidateKey(key)
		return err
	}
	s.finishMutation(key, generation, nil, ObjectMeta{}, false, true)
	return nil
}

func (s *WriterObjectCache) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	generation := s.generation(key)
	err := s.Inner.DeleteConditional(s.withMutationContext(ctx), key, condition)
	if err != nil {
		s.invalidateKey(key)
		return err
	}
	s.finishMutation(key, generation, nil, ObjectMeta{}, false, true)
	return nil
}

func (s *WriterObjectCache) List(ctx context.Context, prefix string) (result []ObjectInfo, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	cacheable := prefix == "" || strings.HasSuffix(prefix, "/")
	var fill *writerListFill
	if cacheable {
		var cached []ObjectInfo
		var ok bool
		if cached, fill, ok = s.cachedList(prefix); ok {
			return cached, nil
		}
		if fill != nil {
			defer s.releaseListFill(prefix, fill)
		}
	}
	items, err := s.Inner.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	s.cacheList(prefix, items, fill)
	return cloneObjectInfos(items), nil
}

func (s *WriterObjectCache) ClearPrefix(prefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.generations {
		s.generations[i]++
	}
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
	for fillingPrefix := range s.listFills {
		if strings.HasPrefix(fillingPrefix, prefix) || strings.HasPrefix(prefix, fillingPrefix) {
			delete(s.listFills, fillingPrefix)
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

func (s *WriterObjectCache) cachedList(prefix string) ([]ObjectInfo, *writerListFill, bool) {
	s.mu.Lock()
	entry, ok := s.lists[prefix]
	if !ok {
		var fill *writerListFill
		if s.maxListItems > 0 {
			fill = s.listFills[prefix]
			if fill == nil {
				fill = &writerListFill{}
				s.listFills[prefix] = fill
			}
			fill.readers++
		}
		s.mu.Unlock()
		return nil, fill, false
	}
	s.listLRU.MoveToFront(entry.elem)
	items := make([]ObjectInfo, 0, len(entry.items))
	for _, item := range entry.items {
		items = append(items, item)
	}
	s.mu.Unlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil, true
}

func (s *WriterObjectCache) releaseListFill(prefix string, fill *writerListFill) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listFills[prefix] == fill {
		fill.readers--
		if fill.readers == 0 {
			delete(s.listFills, prefix)
		}
	}
}

func (s *WriterObjectCache) generationSlot(key string) *uint64 {
	return &s.generations[maphash.String(s.seed, key)%uint64(len(s.generations))]
}

func (s *WriterObjectCache) generation(key string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.generationSlot(key)
}

func (s *WriterObjectCache) cacheableData(data []byte) bool {
	return s.maxKeys > 0 && (s.maxBytes == 0 || int64(len(data)) <= s.maxBytes)
}

func (s *WriterObjectCache) fillObject(key string, generation uint64, entry writerObjectEntry) {
	cacheable := !entry.hasData || s.cacheableData(entry.data)
	if entry.hasData && cacheable {
		entry.data = append([]byte(nil), entry.data...)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if *s.generationSlot(key) != generation {
		return
	}
	switch {
	case !cacheable:
		s.removeObjectLocked(key)
	case entry.negative && s.negativeTTL <= 0:
		s.removeObjectLocked(key)
		s.removeKeyFromCachedListsLocked(key)
	case entry.negative:
		s.cacheNegativeLocked(key)
		s.removeKeyFromCachedListsLocked(key)
	case entry.hasData:
		s.cachePositiveLocked(key, entry.data, entry.meta, entry.hasMeta)
	default:
		s.cacheMetaLocked(key, entry.meta)
	}
}

func (s *WriterObjectCache) finishMutation(key string, generation uint64, data []byte, meta ObjectMeta, hasMeta, deleted bool) {
	size := int64(len(data))
	cacheable := s.cacheableData(data)
	if cacheable {
		data = append([]byte(nil), data...)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	slot := s.generationSlot(key)
	previous := *slot
	*slot++
	if previous != generation {
		// Concurrent completions can arrive in a different order than the durable writes.
		s.invalidateKeyLocked(key)
		return
	}
	switch {
	case deleted:
		s.cacheNegativeLocked(key)
	case cacheable:
		s.cachePositiveLocked(key, data, meta, hasMeta)
	default:
		s.removeObjectLocked(key)
	}
	s.updateCachedListsLocked(key, ObjectInfo{Key: key, Size: size, ETag: meta.ETag}, deleted)
}

func (s *WriterObjectCache) cachePositiveLocked(key string, data []byte, meta ObjectMeta, hasMeta bool) {
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

func (s *WriterObjectCache) removeKeyFromCachedListsLocked(key string) {
	for _, prefix := range writerObjectListPrefixes(key) {
		entry := s.lists[prefix]
		if entry == nil {
			continue
		}
		if before, existed := entry.items[key]; existed {
			delete(entry.items, key)
			delta := writerObjectInfoCacheSize(before)
			entry.size -= delta
			s.listBytes -= delta
			delete(s.listFills, prefix)
		}
	}
}

func (s *WriterObjectCache) cacheMetaLocked(key string, meta ObjectMeta) {
	entry := s.ensureObjectEntryLocked(key)
	// Head and Get may have observed different revisions of the object.
	if entry.hasData && (!entry.hasMeta || entry.meta.ETag != meta.ETag) {
		s.bytes -= entry.size
		entry.data = nil
		entry.size = 0
		entry.hasData = false
	}
	entry.meta = normalizeObjectMeta(key, meta)
	entry.hasMeta = true
	entry.negative = false
	entry.expiresAt = time.Time{}
	s.objectLRU.MoveToFront(entry.elem)
	s.evictObjectsLocked()
}

func (s *WriterObjectCache) cacheNegativeLocked(key string) {
	if s.negativeTTL <= 0 {
		s.removeObjectLocked(key)
		return
	}
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

func (s *WriterObjectCache) cacheList(prefix string, items []ObjectInfo, fill *writerListFill) {
	if fill == nil {
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
	if s.listFills[prefix] != fill {
		return
	}
	delete(s.listFills, prefix)
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

func (s *WriterObjectCache) updateCachedListsLocked(key string, info ObjectInfo, deleted bool) {
	for _, prefix := range writerObjectListPrefixes(key) {
		delete(s.listFills, prefix)
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
	*s.generationSlot(key)++
	s.invalidateKeyLocked(key)
}

func (s *WriterObjectCache) invalidateKeyLocked(key string) {
	s.removeObjectLocked(key)
	for _, prefix := range writerObjectListPrefixes(key) {
		delete(s.listFills, prefix)
		if s.lists[prefix] != nil {
			s.removeListLocked(prefix)
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

func (s *TenantStore) clearWriterObjectKey(key string) {
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.invalidateKey(key)
	}
}
