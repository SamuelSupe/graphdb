package storage

import (
	"context"
)

type TenantStoreOptions struct {
	InstanceID string
	ReaderID   string

	MaxWriteCacheBytes         int64
	WriteEntityRecords         bool
	UseEntityRecordsForRead    bool
	EntityPagePackMaxBytes     int64
	MaterializeCollectorStatus bool
	Backpressure               *WritePressure

	IndexObjectCache IndexObjectCacheConfig
}

func NewTenantStoreWithOptions(objects ObjectStore, prefix string, options TenantStoreOptions) *TenantStore {
	store := NewTenantStore(objects, prefix)
	if options.InstanceID != "" {
		store.InstanceID = options.InstanceID
	}
	if options.ReaderID != "" {
		store.ReaderID = options.ReaderID
	}
	store.MaxWriteCacheBytes = options.MaxWriteCacheBytes

	store.WriteEntityRecords = options.WriteEntityRecords
	store.UseEntityRecordsForRead = options.UseEntityRecordsForRead
	store.EntityPagePackMaxBytes = options.EntityPagePackMaxBytes
	store.MaterializeCollectorStatus = options.MaterializeCollectorStatus
	store.Backpressure = options.Backpressure

	store.ConfigureIndexObjectCache(options.IndexObjectCache)
	return store
}

func (s *TenantStore) SetObservers(
	backpressure BackpressureObserver,
	cache ReaderCacheObserver,
) {
	s.backpressureObserver = backpressure
	s.cacheObserver = cache
}

func (s *TenantStore) SetIngestBarrier(barrier func(context.Context, string) error) {
	s.ingestBarrier = barrier
}
