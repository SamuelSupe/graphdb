package storage

import (
	"container/list"
	"context"
	"sync"
)

type decodedLookupCache[T any] struct {
	mu       sync.Mutex
	max      int
	maxBytes int64
	bytes    int64
	data     map[string]*decodedLookupEntry[T]
	order    *list.List
	loading  map[string]*decodedLookupLoad[T]
	estimate func(T) int64
}

type decodedLookupEntry[T any] struct {
	value T
	size  int64
	node  *list.Element
}

type decodedLookupLoad[T any] struct {
	done     chan struct{}
	value    T
	ok       bool
	err      error
	canceled bool
}

func (c *decodedLookupCache[T]) load(
	ctx context.Context,
	key string,
	loader func() (T, bool, error),
) (T, bool, error) {
	if c == nil || c.max <= 0 || c.maxBytes <= 0 || key == "" {
		return loader()
	}
	for {
		c.mu.Lock()
		if entry := c.data[key]; entry != nil {
			c.order.MoveToBack(entry.node)
			value := entry.value
			c.mu.Unlock()
			return value, true, nil
		}
		if active := c.loading[key]; active != nil {
			c.mu.Unlock()
			select {
			case <-active.done:
				if active.canceled && ctx.Err() == nil {
					continue
				}
				return active.value, active.ok, active.err
			case <-ctx.Done():
				var zero T
				return zero, false, ctx.Err()
			}
		}
		active := &decodedLookupLoad[T]{done: make(chan struct{})}
		c.loading[key] = active
		c.mu.Unlock()

		value, ok, err := loader()
		if err == nil && ok {
			value = c.put(key, value)
		}
		c.mu.Lock()
		active.value = value
		active.ok = ok
		active.err = err
		active.canceled = loadCanceledByContext(ctx, err)
		delete(c.loading, key)
		close(active.done)
		c.mu.Unlock()
		return value, ok, err
	}
}

func (c *decodedLookupCache[T]) put(
	key string,
	value T,
) T {
	if c == nil || c.max <= 0 || c.maxBytes <= 0 || key == "" {
		return value
	}
	size := c.estimate(value)
	if size > c.maxBytes {
		return value
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.data[key]; existing != nil {
		c.order.MoveToBack(existing.node)
		return existing.value
	}
	entry := &decodedLookupEntry[T]{value: value, size: size}
	entry.node = c.order.PushBack(key)
	c.data[key] = entry
	c.bytes += size
	for len(c.data) > c.max || c.bytes > c.maxBytes {
		front := c.order.Front()
		if front == nil {
			break
		}
		evictedKey := front.Value.(string)
		c.order.Remove(front)
		evicted := c.data[evictedKey]
		delete(c.data, evictedKey)
		c.bytes -= evicted.size
	}
	return value
}

func newDecodedLookupCache[T any](max int, maxBytes int64, estimate func(T) int64) *decodedLookupCache[T] {
	if max < 0 {
		max = 0
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	return &decodedLookupCache[T]{max: max, maxBytes: maxBytes, data: map[string]*decodedLookupEntry[T]{}, order: list.New(), loading: map[string]*decodedLookupLoad[T]{}, estimate: estimate}
}
