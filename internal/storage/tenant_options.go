package storage

import (
	"context"
	"time"
)

type TenantStoreOptions struct {
	InstanceID                 string
	ReaderID                   string
	RequireCoordinationMarker  bool
	MaxWriteCacheBytes         int64
	WriteEntityRecords         bool
	UseEntityRecordsForRead    bool
	EntityPagePackMaxBytes     int64
	MaterializeCollectorStatus bool
	Backpressure               *WritePressure
	CoordinatorRetryLimit      int
	CoordinatorPendingTTL      time.Duration
	CoordinatorCleanup         CoordinatorCleanupConfig
	IndexObjectCache           IndexObjectCacheConfig
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
	store.RequireCoordinationMarker = options.RequireCoordinationMarker
	store.WriteEntityRecords = options.WriteEntityRecords
	store.UseEntityRecordsForRead = options.UseEntityRecordsForRead
	store.EntityPagePackMaxBytes = options.EntityPagePackMaxBytes
	store.MaterializeCollectorStatus = options.MaterializeCollectorStatus
	store.Backpressure = options.Backpressure
	store.CoordinatorRetryLimit = options.CoordinatorRetryLimit
	store.CoordinatorPendingTTL = options.CoordinatorPendingTTL
	store.CoordinatorCleanup = options.CoordinatorCleanup
	store.ConfigureIndexObjectCache(options.IndexObjectCache)
	return store
}

func (s *TenantStore) SetObservers(
	backpressure BackpressureObserver,
	cache ReaderCacheObserver,
	coordinator CoordinatorObserver,
) {
	s.backpressureObserver = backpressure
	s.cacheObserver = cache
	s.coordinatorObserver = coordinator
}

func (s *TenantStore) SetIngestBarrier(barrier func(context.Context, string) error) {
	s.ingestBarrier = barrier
}
