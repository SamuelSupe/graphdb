package storage

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
)

const (
	ingestMetadataCacheMaxEntries = 1024
	ingestMetadataCacheMaxBytes   = 64 * 1024 * 1024
	ingestMetadataManifestTTL     = time.Second
	ingestMetadataImmutableTTL    = 15 * time.Minute
)

type ingestMetadataCacheObject struct {
	value any
	meta  ObjectMeta
	bytes int64
}

type ingestMetadataCacheEntry struct {
	key       string
	object    ingestMetadataCacheObject
	expiresAt time.Time
	element   *list.Element
}

type ingestMetadataCacheLoadResult struct {
	object  ingestMetadataCacheObject
	outcome string
	err     error
}

type ingestMetadataObjectCache struct {
	mu           sync.Mutex
	entries      map[string]*ingestMetadataCacheEntry
	order        *list.List
	currentBytes int64
	epochs       map[string]uint64
	loads        singleflight.Group
}

func newIngestMetadataObjectCache() *ingestMetadataObjectCache {
	return &ingestMetadataObjectCache{
		entries: map[string]*ingestMetadataCacheEntry{},
		order:   list.New(),
		epochs:  map[string]uint64{},
	}
}

func (c *ingestMetadataObjectCache) get(
	key string,
	now time.Time,
) (ingestMetadataCacheObject, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return ingestMetadataCacheObject{}, false
	}
	if !entry.expiresAt.After(now) {
		c.removeLocked(entry)
		return ingestMetadataCacheObject{}, false
	}
	c.order.MoveToFront(entry.element)
	return entry.object, true
}

func (c *ingestMetadataObjectCache) put(
	key string,
	object ingestMetadataCacheObject,
	ttl time.Duration,
) {
	if object.bytes > ingestMetadataCacheMaxBytes {
		return
	}
	if object.bytes <= 0 {
		object.bytes = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putLocked(key, object, ttl)
}

func (c *ingestMetadataObjectCache) putLocked(
	key string,
	object ingestMetadataCacheObject,
	ttl time.Duration,
) {
	if existing := c.entries[key]; existing != nil {
		c.removeLocked(existing)
	}
	entry := &ingestMetadataCacheEntry{
		key:       key,
		object:    object,
		expiresAt: time.Now().Add(ttl),
	}
	entry.element = c.order.PushFront(entry)
	c.entries[key] = entry
	c.currentBytes += object.bytes
	for len(c.entries) > ingestMetadataCacheMaxEntries ||
		c.currentBytes > ingestMetadataCacheMaxBytes {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.removeLocked(oldest.Value.(*ingestMetadataCacheEntry))
	}
}

func (c *ingestMetadataObjectCache) invalidate(key string) {
	c.mu.Lock()
	if entry := c.entries[key]; entry != nil {
		c.removeLocked(entry)
	}
	c.epochs[key]++
	c.mu.Unlock()
	c.loads.Forget(key)
}

func (c *ingestMetadataObjectCache) epoch(key string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epochs[key]
}

func (c *ingestMetadataObjectCache) putIfEpoch(
	key string,
	object ingestMetadataCacheObject,
	ttl time.Duration,
	epoch uint64,
) {
	if object.bytes > ingestMetadataCacheMaxBytes {
		return
	}
	if object.bytes <= 0 {
		object.bytes = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epochs[key] != epoch {
		return
	}
	c.putLocked(key, object, ttl)
}

func (c *ingestMetadataObjectCache) removeLocked(entry *ingestMetadataCacheEntry) {
	delete(c.entries, entry.key)
	c.order.Remove(entry.element)
	c.currentBytes -= entry.object.bytes
}

func (c *ingestMetadataObjectCache) load(
	ctx context.Context,
	key string,
	ttl time.Duration,
	loader func(context.Context) (ingestMetadataCacheObject, error),
) (ingestMetadataCacheObject, string, error) {
	if object, ok := c.get(key, time.Now()); ok {
		if object.value == nil {
			return object, "negative", ErrNotFound
		}
		return object, "hit", nil
	}
	resultCh := c.loads.DoChan(key, func() (any, error) {
		if object, ok := c.get(key, time.Now()); ok {
			if object.value == nil {
				return ingestMetadataCacheLoadResult{
					object:  object,
					outcome: "negative",
					err:     ErrNotFound,
				}, nil
			}
			return ingestMetadataCacheLoadResult{object: object, outcome: "hit"}, nil
		}
		epoch := c.epoch(key)
		object, err := loader(ctx)
		if errors.Is(err, ErrNotFound) {
			c.putIfEpoch(
				key,
				ingestMetadataCacheObject{meta: object.meta, bytes: 1},
				ingestMetadataManifestTTL,
				epoch,
			)
			return ingestMetadataCacheLoadResult{
				object:  ingestMetadataCacheObject{meta: object.meta, bytes: 1},
				outcome: "negative",
				err:     ErrNotFound,
			}, nil
		}
		if err != nil {
			return ingestMetadataCacheLoadResult{outcome: "error", err: err}, nil
		}
		c.putIfEpoch(key, object, ttl, epoch)
		return ingestMetadataCacheLoadResult{object: object, outcome: "miss"}, nil
	})
	select {
	case result := <-resultCh:
		loaded := result.Val.(ingestMetadataCacheLoadResult)
		if result.Shared && loaded.outcome != "hit" && loaded.outcome != "negative" {
			loaded.outcome = "shared"
		}
		return loaded.object, loaded.outcome, loaded.err
	case <-ctx.Done():
		return ingestMetadataCacheObject{}, "error", ctx.Err()
	}
}

func (c *ingestMetadataObjectCache) store(
	key string,
	value any,
	meta ObjectMeta,
	bytes int64,
	ttl time.Duration,
) {
	c.invalidate(key)
	c.put(key, ingestMetadataCacheObject{
		value: value,
		meta:  meta,
		bytes: bytes,
	}, ttl)
}

func (s *TenantStore) loadCachedIngestMetadataObject(
	ctx context.Context,
	key string,
	kind string,
	ttl time.Duration,
	loader func(context.Context) (ingestMetadataCacheObject, error),
) (ingestMetadataCacheObject, error) {
	ctx, span := startStorageSpan(
		ctx,
		"graphdb.storage.ingest.metadata_cache",
		attribute.String("graphdb.ingest.metadata.cache_kind", kind),
	)
	object, outcome, err := s.ingestMetadataCache.load(ctx, key, ttl, loader)
	span.SetAttributes(attribute.String("graphdb.ingest.metadata.cache_outcome", outcome))
	endStorageSpan(span, err)
	if s.IngestObserver != nil {
		s.IngestObserver.RecordIngestMetadataCache(kind, outcome)
	}
	return object, err
}
