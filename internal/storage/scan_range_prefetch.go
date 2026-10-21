package storage

import (
	"context"
	"time"
)

// One speculative physical page per scan; share the existing global admission
// limit, but keep cancellation and joining tied to this scan's lifetime.
type rangeScanPrefetch struct {
	store  *TenantStore
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (p *rangeScanPrefetch) close() {
	if p.cancel != nil {
		p.cancel()
		<-p.done
		p.cancel = nil
		p.done = nil
	}
}

func (p *rangeScanPrefetch) advance(kind, tenant string, version int64, objects []IndexObject, n int) error {
	if p.done != nil {
		select {
		case <-p.done:
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
		p.close()
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	s := p.store
	if n+1 >= len(objects) || exclusiveFileStore(s.Objects) != nil || s.indexCache == nil || s.indexCache.max == 0 {
		return nil
	}
	object := objects[n+1]
	bounds, err := objectIndexRange(object)
	if err != nil || bounds.Bytes > indexPageMaxBytes || bounds.Bytes > s.indexCache.maxBytes {
		return nil
	}
	key := indexObjectCacheKey(kind, tenant, version, object.Key, object.ContentHash, object.SchemaHash)
	cache := s.indexCache
	if cache.has(key) || !cache.beginPrefetch(key) {
		return nil
	}
	timeout := s.IndexPrefetchTimeout
	if timeout <= 0 {
		timeout = defaultIndexObjectPrefetchTimeout
	}
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	done := make(chan struct{})
	p.cancel, p.done = cancel, done
	go func() {
		defer close(done)
		defer cache.finishPrefetch(key)
		defer cancel()
		data, meta, err := s.Objects.GetWithMeta(ctx, object.Key)
		if err == nil && ctx.Err() == nil {
			cache.put(key, cachedIndexObject{data: data, meta: meta, validatedAt: time.Now()})
		}
	}()
	return nil
}
