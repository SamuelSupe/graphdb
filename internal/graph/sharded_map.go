package graph

import (
	"encoding/json"
	"iter"
	"maps"
	"sync/atomic"
)

const graphMapShards = 256

type graphMapBucket[V any] struct {
	values map[string]V
	shared atomic.Bool
}

// ShardedMap copies only touched buckets after Clone. Values are shallow:
// callers must copy nested maps or slices before modifying a shared value.
// Published maps support concurrent reads and clones; writes require isolation.
type ShardedMap[V any] struct {
	small   *graphMapBucket[V]
	buckets *[graphMapShards]*graphMapBucket[V]
	size    int
}

func NewShardedMap[V any]() *ShardedMap[V] { return &ShardedMap[V]{} }

func ShardedMapFrom[V any](values map[string]V) *ShardedMap[V] {
	result := NewShardedMap[V]()
	for key, value := range values {
		result.Set(key, value)
	}
	return result
}

func (m *ShardedMap[V]) Get(key string) (value V, ok bool) {
	if m != nil {
		bucket := m.small
		if m.buckets != nil {
			bucket = m.buckets[logicalHashShard(key)]
		}
		if bucket != nil {
			value, ok = bucket.values[key]
		}
	}
	return
}

func (m *ShardedMap[V]) At(key string) V {
	value, _ := m.Get(key)
	return value
}

func (m *ShardedMap[V]) Len() int {
	if m == nil {
		return 0
	}
	return m.size
}

func (m *ShardedMap[V]) writable(key string) map[string]V {
	if m.buckets == nil {
		if m.small == nil {
			m.small = &graphMapBucket[V]{values: make(map[string]V)}
		} else if m.small.shared.Load() {
			m.small = &graphMapBucket[V]{values: maps.Clone(m.small.values)}
		}
		return m.small.values
	}
	index := logicalHashShard(key)
	bucket := m.buckets[index]
	if bucket == nil {
		bucket = &graphMapBucket[V]{values: make(map[string]V)}
		m.buckets[index] = bucket
	} else if bucket.shared.Load() {
		bucket = &graphMapBucket[V]{values: maps.Clone(bucket.values)}
		m.buckets[index] = bucket
	}
	return bucket.values
}

func (m *ShardedMap[V]) Set(key string, value V) {
	values := m.writable(key)
	if _, exists := values[key]; !exists {
		m.size++
	}
	values[key] = value
	// Most field-index values have only one ID. Allocate the shard directory
	// only when copying a small native map would become more expensive.
	if m.small != nil && m.size > 32 {
		m.buckets = new([graphMapShards]*graphMapBucket[V])
		m.small = nil
		for key, value := range values {
			m.writable(key)[key] = value
		}
	}
}

func (m *ShardedMap[V]) Delete(key string) {
	if _, exists := m.Get(key); exists {
		delete(m.writable(key), key)
		m.size--
	}
}

func (m *ShardedMap[V]) Clone() *ShardedMap[V] {
	clone := NewShardedMap[V]()
	if m != nil && m.small != nil {
		m.small.shared.Store(true)
		clone.small, clone.size = m.small, m.size
	} else if m != nil && m.buckets != nil {
		clone.buckets = new([graphMapShards]*graphMapBucket[V])
		clone.size = m.size
		for index, bucket := range m.buckets {
			if bucket != nil {
				bucket.shared.Store(true)
				clone.buckets[index] = bucket
			}
		}
	}
	return clone
}

func (m *ShardedMap[V]) All() iter.Seq2[string, V] {
	return func(yield func(string, V) bool) {
		if m != nil && m.small != nil {
			for key, value := range m.small.values {
				if !yield(key, value) {
					return
				}
			}
			return
		}
		if m == nil || m.buckets == nil {
			return
		}
		for _, bucket := range m.buckets {
			if bucket != nil {
				for key, value := range bucket.values {
					if !yield(key, value) {
						return
					}
				}
			}
		}
	}
}

func (m *ShardedMap[V]) Keys() iter.Seq[string] {
	return func(yield func(string) bool) {
		for key := range m.All() {
			if !yield(key) {
				return
			}
		}
	}
}

func (m *ShardedMap[V]) MarshalJSON() ([]byte, error) {
	return json.Marshal(maps.Collect(m.All()))
}
